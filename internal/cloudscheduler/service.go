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
// Storage is decoupled from the wire format: the in-memory Store holds
// cloudscheduler.Job records (see store.go) and this file translates at
// the RPC boundary via jobFromProto / jobToProto. Sentinel errors from the
// store (ErrNotFound, ErrAlreadyExists) are mapped to gRPC status codes
// via errors.Is.
//
// Out-of-scope RPCs: CloudSchedulerServer has no RPCs outside the in-scope
// set, so the unimplemented-dispatch pattern required by Rule 6 is not
// exercised by this service directly (UnimplementedCloudSchedulerServer
// handles the edge case of future protobuf additions).
package cloudscheduler

import (
	"context"
	"errors"
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
//
// dataDir, when non-empty, enables best-effort JSON snapshot persistence of
// the in-memory job store. See store.go for details.
func New(dataDir string, quiet bool, pubsubAddr string) *Service {
	logger := log.New(os.Stderr, "[cloudscheduler] ", log.LstdFlags)
	return &Service{
		dataDir:    dataDir,
		quiet:      quiet,
		logger:     logger,
		store:      NewStore(dataDir),
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
// State defaults to ENABLED when the caller omits it, matching real
// Cloud Scheduler semantics.
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

	// Convert the wire-format job into the in-memory form and default
	// State to ENABLED when omitted. UserUpdateTime is stamped server-side.
	internal := jobFromProto(job)
	if internal.State == schedulerpb.Job_STATE_UNSPECIFIED {
		internal.State = schedulerpb.Job_ENABLED
	}
	internal.UserUpdateTime = Now()

	if err := s.store.Create(internal); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	// Register with cron runner if enabled.
	if internal.State == schedulerpb.Job_ENABLED {
		if err := s.scheduleJob(internal); err != nil {
			// Roll back on schedule-parse failure so the store does not
			// carry a zombie entry.
			_ = s.store.Delete(name)
			return nil, status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", internal.Schedule, err)
		}
	}
	return jobToProto(internal), nil
}

// GetJob returns an existing job.
func (s *Service) GetJob(_ context.Context, req *schedulerpb.GetJobRequest) (*schedulerpb.Job, error) {
	j, err := s.store.Get(req.GetName())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "job %s not found", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return jobToProto(j), nil
}

// ListJobs returns all jobs under a parent, deterministically sorted by name.
func (s *Service) ListJobs(_ context.Context, req *schedulerpb.ListJobsRequest) (*schedulerpb.ListJobsResponse, error) {
	jobs := s.store.List(req.GetParent())
	out := make([]*schedulerpb.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToProto(j))
	}
	return &schedulerpb.ListJobsResponse{Jobs: out}, nil
}

// DeleteJob removes a job from both the store and the cron runner.
func (s *Service) DeleteJob(_ context.Context, req *schedulerpb.DeleteJobRequest) (*emptypb.Empty, error) {
	name := req.GetName()
	if err := s.store.Delete(name); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "job %s not found", name)
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	s.unscheduleJob(name)
	return &emptypb.Empty{}, nil
}

// UpdateJob replaces an existing job's mutable fields. The job is unscheduled
// and re-scheduled based on its resulting state/schedule. UserUpdateTime is
// refreshed to the current wall-clock time.
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

	internal := jobFromProto(in)
	if internal.State == schedulerpb.Job_STATE_UNSPECIFIED {
		internal.State = schedulerpb.Job_ENABLED
	}
	internal.UserUpdateTime = Now()

	if err := s.store.Update(internal); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	// Always re-schedule — cron expression or target may have changed.
	s.unscheduleJob(internal.Name)
	if internal.State == schedulerpb.Job_ENABLED {
		if err := s.scheduleJob(internal); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", internal.Schedule, err)
		}
	}
	return jobToProto(internal), nil
}

// RunJob performs an immediate, one-shot dispatch of the job's target without
// mutating the job's schedule or state. This is the manual "run now" button.
// Only LastAttemptTime is updated to reflect that a dispatch was requested.
func (s *Service) RunJob(_ context.Context, req *schedulerpb.RunJobRequest) (*schedulerpb.Job, error) {
	j, err := s.store.Get(req.GetName())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "job %s not found", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	// Dispatch is fire-and-forget per Rule 3 — the RPC returns before the
	// downstream call completes.
	go s.dispatchOnce(j)

	// Update timestamps only — schedule and state MUST remain untouched
	// per AAP §0.1.1 Extension C. We construct a shallow copy so we are
	// not racing with concurrent readers holding the aliased pointer.
	cp := *j
	cp.LastAttemptTime = Now()
	if err := s.store.Update(&cp); err != nil {
		// Persistence failure: return the pre-touch snapshot rather than a
		// 5xx. The dispatch goroutine is already in flight.
		return jobToProto(j), nil
	}

	// Re-read so the returned proto reflects any concurrent mutations.
	out, err := s.store.Get(req.GetName())
	if err != nil {
		return jobToProto(&cp), nil
	}
	return jobToProto(out), nil
}

// PauseJob transitions a job to PAUSED and unschedules it.
func (s *Service) PauseJob(_ context.Context, req *schedulerpb.PauseJobRequest) (*schedulerpb.Job, error) {
	j, err := s.store.Pause(req.GetName())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	s.unscheduleJob(req.GetName())
	return jobToProto(j), nil
}

// ResumeJob transitions a job to ENABLED and re-schedules it.
func (s *Service) ResumeJob(_ context.Context, req *schedulerpb.ResumeJobRequest) (*schedulerpb.Job, error) {
	j, err := s.store.Resume(req.GetName())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	// Ensure we do not stack duplicate entries if Resume is called twice.
	s.unscheduleJob(req.GetName())
	if err := s.scheduleJob(j); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", j.Schedule, err)
	}
	return jobToProto(j), nil
}

// --- Cron runner wiring ---

// scheduleJob parses the job's cron expression and installs a tick entry that
// dispatches once per firing. Callers must hold no locks on the store.
func (s *Service) scheduleJob(job *Job) error {
	spec := job.Schedule
	if spec == "" {
		// Defensive: validateJob rejects empty schedules at the RPC
		// boundary (see validateJob), so this branch is unreachable via
		// public APIs. Kept to prevent a panic if an in-process caller
		// ever installs a Job directly through the store.
		return nil
	}

	name := job.Name
	id, err := s.cron.AddFunc(spec, func() {
		// Re-fetch inside the tick so the latest target/state wins.
		current, err := s.store.Get(name)
		if err != nil {
			// Job was deleted between scheduling and firing; swallow.
			return
		}
		if current.State != schedulerpb.Job_ENABLED {
			return
		}
		s.dispatchOnce(current)
		// Touch LastAttemptTime. Copy to avoid aliasing with in-map state.
		cp := *current
		cp.LastAttemptTime = Now()
		_ = s.store.Update(&cp)
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
// errors are logged via s.logger (stderr-prefixed with "[cloudscheduler]"
// and gated by the quiet flag) and never returned — the caller's goroutine
// is transient (Rule 3).
func (s *Service) dispatchOnce(job *Job) {
	switch {
	case job.HTTPTarget != nil:
		s.dispatchHTTP(job, job.HTTPTarget)
	case job.PubsubTarget != nil:
		s.dispatchPubsub(job, job.PubsubTarget)
	default:
		// AppEngineHttpTarget is explicitly out of scope (AAP §0.6.2) and
		// is now rejected at validateJob time, so a job reaching this
		// branch indicates a store-level inconsistency (e.g., an unknown
		// oneof variant was mutated in-place). Logging at the service
		// logger keeps the observation uniform with the other dispatch
		// paths.
		s.logger.Printf("job %s has no recognised target", job.Name)
	}
}

// dispatchHTTP delivers an HttpTarget via the shared dispatcher. The
// dispatcher is HTTP POST-only; honouring HttpMethod enum values other than
// POST is out of scope (AAP §0.6.2).
func (s *Service) dispatchHTTP(job *Job, t *schedulerpb.HttpTarget) {
	if t == nil || t.GetUri() == "" {
		return
	}
	headers := map[string]string{}
	for k, v := range t.GetHeaders() {
		headers[k] = v
	}
	res := s.dispatcher.Dispatch(context.Background(), t.GetUri(), t.GetBody(), headers)
	if res.Err != nil {
		s.logger.Printf("http dispatch %s: %v (attempts=%d status=%d)",
			job.Name, res.Err, res.Attempts, res.StatusCode)
	}
}

// dispatchPubsub publishes a PubsubTarget via loopback gRPC. When pubsubAddr
// is empty the publish is silently skipped.
func (s *Service) dispatchPubsub(job *Job, t *schedulerpb.PubsubTarget) {
	if t == nil || t.GetTopicName() == "" {
		return
	}
	if s.pubsubAddr == "" {
		return
	}
	if err := publishToPubsub(s.pubsubAddr, t.GetTopicName(), t.GetData(), t.GetAttributes()); err != nil {
		s.logger.Printf("pubsub dispatch %s: %v", job.Name, err)
	}
}

// --- Validation & helpers ---

// validateJob enforces basic structural requirements:
//
//   - schedule must be a non-empty 5-field standard cron expression,
//   - exactly one of HttpTarget or PubsubTarget must be set,
//   - AppEngineHttpTarget is explicitly rejected — AAP §0.6.2 lists App
//     Engine targets as out of scope, so jobs carrying one are refused at
//     create/update time with codes.InvalidArgument rather than silently
//     accepted and then never dispatched (which would surface as
//     per-tick "no recognised target" log spam and confuse SDK users).
//
// Schedule is required per AAP §0.5.1.4 TestCreateJobRequiresTargetAndSchedule
// and matches real Cloud Scheduler API behaviour where the `schedule`
// field is required on every Job. Earlier revisions of this emulator
// accepted an empty schedule as "manual-only" (RunJob-only); that
// allowance was tightened to match both the AAP contract and the public
// Cloud Scheduler API.
func validateJob(job *schedulerpb.Job) error {
	if job.GetSchedule() == "" {
		return status.Error(codes.InvalidArgument, "job.schedule is required")
	}
	if _, err := cron.ParseStandard(job.GetSchedule()); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid schedule %q: %v", job.GetSchedule(), err)
	}
	if job.GetAppEngineHttpTarget() != nil {
		return status.Error(codes.InvalidArgument, "localgcp: AppEngineHttpTarget is not supported")
	}
	targetCount := 0
	if job.GetHttpTarget() != nil {
		targetCount++
	}
	if job.GetPubsubTarget() != nil {
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

// Now is a test seam for LastAttemptTime / UserUpdateTime time overrides.
var Now = time.Now

// --- Wire <-> in-memory conversions ---

// jobFromProto converts a wire-format *schedulerpb.Job to an internal *Job
// suitable for storage. Called at RPC entry points (CreateJob, UpdateJob)
// where the request carries a proto representation.
//
// Note: AppEngineHttpTarget is explicitly out of scope (AAP §0.6.2) and
// rejected at validateJob time with codes.InvalidArgument, so any job
// reaching jobFromProto is guaranteed not to carry one. The internal Job
// struct deliberately has no AppEngine field; no silent-discard path
// exists.
func jobFromProto(p *schedulerpb.Job) *Job {
	if p == nil {
		return nil
	}
	j := &Job{
		Name:         p.GetName(),
		Description:  p.GetDescription(),
		Schedule:     p.GetSchedule(),
		TimeZone:     p.GetTimeZone(),
		State:        p.GetState(),
		HTTPTarget:   p.GetHttpTarget(),
		PubsubTarget: p.GetPubsubTarget(),
	}
	if t := p.GetUserUpdateTime(); t != nil {
		j.UserUpdateTime = t.AsTime()
	}
	if t := p.GetLastAttemptTime(); t != nil {
		j.LastAttemptTime = t.AsTime()
	}
	if t := p.GetScheduleTime(); t != nil {
		j.ScheduleTime = t.AsTime()
	}
	return j
}

// jobToProto converts an internal *Job to a wire-format *schedulerpb.Job.
// Called at RPC exit points (every response that returns a Job). Zero-value
// time.Time fields are elided so the wire does not carry meaningless
// Unix-epoch timestamps on freshly-created jobs.
func jobToProto(j *Job) *schedulerpb.Job {
	if j == nil {
		return nil
	}
	p := &schedulerpb.Job{
		Name:        j.Name,
		Description: j.Description,
		Schedule:    j.Schedule,
		TimeZone:    j.TimeZone,
		State:       j.State,
	}
	// The schedulerpb.Job.Target oneof accepts exactly one of HttpTarget,
	// PubsubTarget, or AppEngineHttpTarget. We populate whichever the
	// internal record carries; if both happen to be set (a caller-provided
	// contract violation) HTTPTarget wins deterministically.
	if j.HTTPTarget != nil {
		p.Target = &schedulerpb.Job_HttpTarget{HttpTarget: j.HTTPTarget}
	} else if j.PubsubTarget != nil {
		p.Target = &schedulerpb.Job_PubsubTarget{PubsubTarget: j.PubsubTarget}
	}
	if !j.UserUpdateTime.IsZero() {
		p.UserUpdateTime = timestamppb.New(j.UserUpdateTime)
	}
	if !j.LastAttemptTime.IsZero() {
		p.LastAttemptTime = timestamppb.New(j.LastAttemptTime)
	}
	if !j.ScheduleTime.IsZero() {
		p.ScheduleTime = timestamppb.New(j.ScheduleTime)
	}
	return p
}
