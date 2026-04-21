// Package cloudscheduler implements the Cloud Scheduler emulator for localgcp.
//
// Per AAP §0.5.1.2 Extension C, this package registers a gRPC service on port
// 8094 that implements the eight in-scope CloudScheduler RPCs:
//
//	CreateJob, GetJob, ListJobs, DeleteJob, UpdateJob, RunJob, PauseJob, ResumeJob
//
// The service owns a single robfig/cron/v3 runner goroutine that iterates
// enabled jobs on their 5-field cron schedules and dispatches each firing to
// either an HTTP target (via the shared internal/dispatch.Dispatcher) or a
// Pub/Sub target (via loopback gRPC at pubsubAddr). RunJob performs a
// single one-shot dispatch WITHOUT mutating schedule or state.
//
// Out-of-scope RPCs: CloudSchedulerServer has no RPCs outside the in-scope
// set, so the unimplemented-dispatch pattern required by Rule 6 is not
// exercised by this service directly (UnimplementedCloudSchedulerServer
// handles the edge case of future protobuf additions).
package cloudscheduler

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/slokam-ai/localgcp/internal/dispatch"
)

// Service implements the Cloud Scheduler gRPC server. It embeds the generated
// UnimplementedCloudSchedulerServer so it satisfies the interface for any
// future RPCs added to the proto (forward-compatibility).
type Service struct {
	schedulerpb.UnimplementedCloudSchedulerServer

	dataDir string
	quiet   bool
	logger  *log.Logger
	store   *Store

	// pubsubAddr is the loopback Pub/Sub gRPC endpoint used to dispatch
	// PubsubTarget jobs. Empty string silently skips Pub/Sub dispatch
	// (per AAP Rule 7a — empty endpoint ⇒ no-op).
	pubsubAddr string

	// dispatcher is the shared HTTP dispatcher used for HttpTarget jobs.
	dispatcher *dispatch.Dispatcher

	// cron runs the single tick goroutine that fires enabled jobs.
	cron *cron.Cron

	// entriesMu guards entryIDs. The cron.Cron type is independently
	// thread-safe but we need an atomic view of "job name -> entry id" to
	// keep Add/Remove sequences consistent under concurrent mutations.
	entriesMu sync.Mutex
	entryIDs  map[string]cron.EntryID
}

// New creates a new Cloud Scheduler service. pubsubAddr, when non-empty, is
// the host:port of the loopback Pub/Sub gRPC endpoint (e.g.
// "localhost:8085"). Callers that do not need PubsubTarget dispatch may pass
// the empty string and Pub/Sub-target dispatch will silently no-op.
func New(dataDir string, quiet bool, pubsubAddr string) *Service {
	logger := log.New(os.Stderr, "[cloudscheduler] ", log.LstdFlags)
	return &Service{
		dataDir:    dataDir,
		quiet:      quiet,
		logger:     logger,
		store:      NewStore(),
		pubsubAddr: pubsubAddr,
		dispatcher: dispatch.New(dispatch.DefaultConfig()),
		cron:       cron.New(),
		entryIDs:   make(map[string]cron.EntryID),
	}
}

// Name satisfies server.Service.
func (s *Service) Name() string { return "Cloud Scheduler" }

// Start satisfies server.Service. It binds a gRPC server on addr, starts the
// cron runner, and blocks until ctx is cancelled.
func (s *Service) Start(ctx context.Context, addr string) error {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(s.loggingInterceptor),
	)
	schedulerpb.RegisterCloudSchedulerServer(srv, s)
	reflection.Register(srv)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Boot the cron runner goroutine. cron.Start launches its own goroutine
	// internally; we do NOT block on it.
	s.cron.Start()

	go func() {
		<-ctx.Done()
		// Stop the cron runner first so no new dispatches are enqueued
		// during shutdown. Stop returns a context that is cancelled when
		// all running jobs finish — we ignore it because the cron tick
		// function itself spawns goroutines and returns immediately.
		_ = s.cron.Stop()
		srv.GracefulStop()
	}()

	if err := srv.Serve(ln); err != nil {
		return err
	}
	return nil
}

// --- RPC implementations ---

// CreateJob inserts a job, assigns a name when one is not provided, and
// registers it with the cron runner if the effective state is ENABLED.
func (s *Service) CreateJob(_ context.Context, req *schedulerpb.CreateJobRequest) (*schedulerpb.Job, error) {
	parent := req.GetParent()
	if parent == "" {
		return nil, status.Error(codes.InvalidArgument, "parent is required")
	}
	job := req.GetJob()
	if job == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}

	// If the caller did not supply a Name, synthesise one from parent. This
	// mirrors the real Cloud Scheduler which accepts "short name in job" and
	// prefers "full name in request"; we prefer whatever's set.
	name := job.GetName()
	if name == "" {
		name = parent + "/jobs/job-" + timestampSuffix()
		job.Name = name
	}

	if !hasJobsPrefix(name, parent) {
		return nil, status.Errorf(codes.InvalidArgument, "job name %q must be under parent %q/jobs/", name, parent)
	}
	if err := validateJob(job); err != nil {
		return nil, err
	}

	created, err := s.store.Create(name, job)
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "%v", err)
	}

	// Register with cron runner if enabled.
	if created.GetState() == schedulerpb.Job_ENABLED {
		if err := s.scheduleJob(created); err != nil {
			// Roll back on schedule-parse failure so the store does not
			// carry a zombie entry.
			_ = s.store.Delete(name)
			return nil, status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", created.GetSchedule(), err)
		}
	}
	return created, nil
}

// GetJob returns an existing job.
func (s *Service) GetJob(_ context.Context, req *schedulerpb.GetJobRequest) (*schedulerpb.Job, error) {
	job, ok := s.store.Get(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "job %s not found", req.GetName())
	}
	return job, nil
}

// ListJobs returns all jobs under a parent.
func (s *Service) ListJobs(_ context.Context, req *schedulerpb.ListJobsRequest) (*schedulerpb.ListJobsResponse, error) {
	jobs := s.store.List(req.GetParent())
	return &schedulerpb.ListJobsResponse{Jobs: jobs}, nil
}

// DeleteJob removes a job from both the store and the cron runner.
func (s *Service) DeleteJob(_ context.Context, req *schedulerpb.DeleteJobRequest) (*emptypb.Empty, error) {
	name := req.GetName()
	if !s.store.Delete(name) {
		return nil, status.Errorf(codes.NotFound, "job %s not found", name)
	}
	s.unscheduleJob(name)
	return &emptypb.Empty{}, nil
}

// UpdateJob replaces an existing job's mutable fields. The job is unscheduled
// and re-scheduled based on its resulting state/schedule.
func (s *Service) UpdateJob(_ context.Context, req *schedulerpb.UpdateJobRequest) (*schedulerpb.Job, error) {
	in := req.GetJob()
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	if in.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "job.name is required")
	}
	if err := validateJob(in); err != nil {
		return nil, err
	}

	updated, err := s.store.Update(in.GetName(), in)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	// Always re-schedule — cron expression or target may have changed.
	s.unscheduleJob(updated.GetName())
	if updated.GetState() == schedulerpb.Job_ENABLED {
		if err := s.scheduleJob(updated); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", updated.GetSchedule(), err)
		}
	}
	return updated, nil
}

// RunJob performs an immediate, one-shot dispatch of the job's target without
// mutating the job's schedule or state. This is the manual "run now" button.
func (s *Service) RunJob(ctx context.Context, req *schedulerpb.RunJobRequest) (*schedulerpb.Job, error) {
	job, ok := s.store.Get(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "job %s not found", req.GetName())
	}

	// Dispatch is fire-and-forget per Rule 3 — the RPC returns before the
	// downstream call completes.
	go s.dispatchOnce(job)
	// Update timestamps only, not schedule or state.
	s.store.TouchRun(job.GetName())
	// Re-read so the timestamps are fresh.
	out, _ := s.store.Get(req.GetName())
	if out == nil {
		return job, nil
	}
	return out, nil
}

// PauseJob transitions a job to PAUSED and unschedules it.
func (s *Service) PauseJob(_ context.Context, req *schedulerpb.PauseJobRequest) (*schedulerpb.Job, error) {
	job, err := s.store.SetState(req.GetName(), schedulerpb.Job_PAUSED)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	s.unscheduleJob(req.GetName())
	return job, nil
}

// ResumeJob transitions a job to ENABLED and re-schedules it.
func (s *Service) ResumeJob(_ context.Context, req *schedulerpb.ResumeJobRequest) (*schedulerpb.Job, error) {
	job, err := s.store.SetState(req.GetName(), schedulerpb.Job_ENABLED)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	// Ensure we do not stack duplicate entries if Resume is called twice.
	s.unscheduleJob(req.GetName())
	if err := s.scheduleJob(job); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", job.GetSchedule(), err)
	}
	return job, nil
}

// --- Cron runner wiring ---

// scheduleJob parses the job's cron expression and installs a tick entry that
// dispatches once per firing. Callers must hold no locks on the store.
func (s *Service) scheduleJob(job *schedulerpb.Job) error {
	spec := job.GetSchedule()
	if spec == "" {
		// Cloud Scheduler allows jobs without a schedule (manual-only); we
		// accept this and simply do not register a tick entry.
		return nil
	}

	name := job.GetName()
	id, err := s.cron.AddFunc(spec, func() {
		// Re-fetch inside the tick so the latest target/state wins.
		current, ok := s.store.Get(name)
		if !ok {
			return
		}
		if current.GetState() != schedulerpb.Job_ENABLED {
			return
		}
		s.dispatchOnce(current)
		s.store.TouchRun(name)
	})
	if err != nil {
		return err
	}

	s.entriesMu.Lock()
	// If an entry was already registered (e.g. concurrent UpdateJob), remove
	// it first so we do not leak cron entries.
	if prev, ok := s.entryIDs[name]; ok {
		s.cron.Remove(prev)
	}
	s.entryIDs[name] = id
	s.entriesMu.Unlock()
	return nil
}

// unscheduleJob removes the cron entry for the named job if one exists.
func (s *Service) unscheduleJob(name string) {
	s.entriesMu.Lock()
	id, ok := s.entryIDs[name]
	delete(s.entryIDs, name)
	s.entriesMu.Unlock()
	if ok {
		s.cron.Remove(id)
	}
}

// dispatchOnce performs a single delivery attempt for the job's target. All
// errors are logged to stderr and never returned — the caller's goroutine is
// transient (Rule 3).
func (s *Service) dispatchOnce(job *schedulerpb.Job) {
	switch t := job.GetTarget().(type) {
	case *schedulerpb.Job_HttpTarget:
		s.dispatchHTTP(job, t.HttpTarget)
	case *schedulerpb.Job_PubsubTarget:
		s.dispatchPubsub(job, t.PubsubTarget)
	case *schedulerpb.Job_AppEngineHttpTarget:
		// AppEngineHttpTarget is explicitly out of scope (AAP §0.6.2). We
		// log a single line to stderr so debugging is not silent.
		s.logger.Printf("job %s has AppEngineHttpTarget which is not supported", job.GetName())
	default:
		s.logger.Printf("job %s has no recognised target", job.GetName())
	}
}

// dispatchHTTP delivers an HttpTarget via the shared dispatcher. The
// dispatcher is HTTP POST-only; honouring HttpMethod enum values other than
// POST is out of scope (AAP §0.6.2).
func (s *Service) dispatchHTTP(job *schedulerpb.Job, t *schedulerpb.HttpTarget) {
	if t == nil || t.GetUri() == "" {
		return
	}
	headers := map[string]string{}
	for k, v := range t.GetHeaders() {
		headers[k] = v
	}
	res := s.dispatcher.Dispatch(context.Background(), t.GetUri(), t.GetBody(), headers)
	if res.Err != nil {
		fmt.Fprintf(os.Stderr, "[cloudscheduler] http dispatch %s: %v (attempts=%d status=%d)\n",
			job.GetName(), res.Err, res.Attempts, res.StatusCode)
	}
}

// dispatchPubsub publishes a PubsubTarget via loopback gRPC. When pubsubAddr
// is empty the publish is silently skipped.
func (s *Service) dispatchPubsub(job *schedulerpb.Job, t *schedulerpb.PubsubTarget) {
	if t == nil || t.GetTopicName() == "" {
		return
	}
	if s.pubsubAddr == "" {
		return
	}
	if err := publishToPubsub(s.pubsubAddr, t.GetTopicName(), t.GetData(), t.GetAttributes()); err != nil {
		fmt.Fprintf(os.Stderr, "[cloudscheduler] pubsub dispatch %s: %v\n", job.GetName(), err)
	}
}

// --- Validation & helpers ---

// validateJob enforces basic structural requirements: schedule must parse and
// exactly one target must be set (AppEngineHttpTarget is accepted as a
// best-effort passthrough — it will simply not dispatch anything).
func validateJob(job *schedulerpb.Job) error {
	if job.GetSchedule() != "" {
		if _, err := cron.ParseStandard(job.GetSchedule()); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", job.GetSchedule(), err)
		}
	}
	targetCount := 0
	if job.GetHttpTarget() != nil {
		targetCount++
	}
	if job.GetPubsubTarget() != nil {
		targetCount++
	}
	if job.GetAppEngineHttpTarget() != nil {
		targetCount++
	}
	if targetCount == 0 {
		return status.Error(codes.InvalidArgument, "job must specify exactly one target")
	}
	if targetCount > 1 {
		return status.Error(codes.InvalidArgument, "job must specify exactly one target")
	}
	return nil
}

// hasJobsPrefix enforces that the job name is "{parent}/jobs/{short}".
func hasJobsPrefix(name, parent string) bool {
	prefix := parent + "/jobs/"
	return len(name) > len(prefix) && name[:len(prefix)] == prefix
}

// timestampSuffix produces a short, stable suffix used when the caller omits
// the job name. Nanoseconds are ample for uniqueness in tests.
func timestampSuffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// loggingInterceptor is the standard repo-wide unary interceptor pattern.
func (s *Service) loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	if !s.quiet {
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		s.logger.Printf("%s %s", info.FullMethod, code)
	}
	return resp, err
}

// Now is a test seam for TouchRun time overrides.
var Now = time.Now

// ensure timestamppb is used (the store.go file consumes it; this keeps the
// linter quiet in case imports drift).
var _ = (*timestamppb.Timestamp)(nil)
