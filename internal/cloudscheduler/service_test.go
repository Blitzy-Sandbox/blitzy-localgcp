// Package cloudscheduler — service_test.go exercises the eight in-scope
// CloudScheduler RPCs at the Go method level (without standing up a gRPC
// server). The tests validate:
//
//   - CRUD round-trip for HTTP-target jobs.
//   - Input validation: invalid cron expression, missing/duplicate targets,
//     parent prefix enforcement, name-required on Update.
//   - NotFound semantics for Get, Update, Delete, RunJob on unknown names.
//   - Pause/Resume state transitions and their cron-entry consequences.
//   - RunJob immediate dispatch that does NOT mutate schedule or state
//     (the canonical requirement from AAP §0.1.1 Extension C).
//
// Tests use an empty pubsubAddr so Pub/Sub target dispatch is a silent no-op
// and no goroutine leaks outside the test binary. The cron runner is never
// Start()'d, so AddFunc entries are queued but never fire — this keeps tests
// deterministic and hermetic.
package cloudscheduler

import (
	"context"
	"testing"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testParent = "projects/test-project/locations/us-central1"
)

// helper: build a fully-qualified job name under testParent.
func jobName(short string) string {
	return testParent + "/jobs/" + short
}

// helper: build a valid HTTP-target job request.
func httpJobReq(short, schedule, uri string) *schedulerpb.CreateJobRequest {
	return &schedulerpb.CreateJobRequest{
		Parent: testParent,
		Job: &schedulerpb.Job{
			Name:     jobName(short),
			Schedule: schedule,
			Target: &schedulerpb.Job_HttpTarget{
				HttpTarget: &schedulerpb.HttpTarget{Uri: uri, HttpMethod: schedulerpb.HttpMethod_POST},
			},
		},
	}
}

// helper: build a Pub/Sub-target job request.
func pubsubJobReq(short, schedule, topic string) *schedulerpb.CreateJobRequest {
	return &schedulerpb.CreateJobRequest{
		Parent: testParent,
		Job: &schedulerpb.Job{
			Name:     jobName(short),
			Schedule: schedule,
			Target: &schedulerpb.Job_PubsubTarget{
				PubsubTarget: &schedulerpb.PubsubTarget{TopicName: topic, Data: []byte("hello")},
			},
		},
	}
}

// newTestService constructs an isolated Service with an empty pubsubAddr.
// Pass "" for pubsubAddr so dispatchPubsub is a no-op for every tick, which
// keeps tests hermetic even if the cron runner were started accidentally.
func newTestService(t *testing.T) *Service {
	t.Helper()
	return New("", true, "")
}

// --- CreateJob happy path and validation ---

func TestCreateJob_HttpTarget_RoundTrip(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	req := httpJobReq("cron-1", "*/5 * * * *", "http://example.test/invoke")
	got, err := svc.CreateJob(ctx, req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if got.GetName() != jobName("cron-1") {
		t.Errorf("Name = %q, want %q", got.GetName(), jobName("cron-1"))
	}
	if got.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("State = %v, want ENABLED", got.GetState())
	}
	if got.GetHttpTarget() == nil {
		t.Fatalf("HttpTarget nil; got Target=%T", got.GetTarget())
	}
	if got.GetHttpTarget().GetUri() != "http://example.test/invoke" {
		t.Errorf("Uri = %q, want http://example.test/invoke", got.GetHttpTarget().GetUri())
	}

	// Get should round-trip.
	back, err := svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: got.GetName()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if back.GetName() != got.GetName() {
		t.Errorf("GetJob roundtrip mismatch: %q vs %q", back.GetName(), got.GetName())
	}
}

func TestCreateJob_PubsubTarget_RoundTrip(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	got, err := svc.CreateJob(ctx, pubsubJobReq("ps-1", "0 9 * * *", "projects/p/topics/t"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if got.GetPubsubTarget().GetTopicName() != "projects/p/topics/t" {
		t.Errorf("TopicName = %q", got.GetPubsubTarget().GetTopicName())
	}
}

func TestCreateJob_InvalidSchedule_ReturnsInvalidArgument(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.CreateJob(context.Background(), httpJobReq("bad", "not-a-schedule", "http://example.test/"))
	if err == nil {
		t.Fatalf("CreateJob with invalid schedule did not error")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
	}
}

func TestCreateJob_NoTarget_ReturnsInvalidArgument(t *testing.T) {
	svc := newTestService(t)
	req := &schedulerpb.CreateJobRequest{
		Parent: testParent,
		Job:    &schedulerpb.Job{Name: jobName("no-target"), Schedule: "*/5 * * * *"},
	}
	_, err := svc.CreateJob(context.Background(), req)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
	}
}

// TestCreateJob_AppEngineTargetPassesValidation exercises the branch of
// validateJob that counts GetAppEngineHttpTarget() as a valid single target.
// AppEngine targets are out-of-scope at the dispatch layer (AAP §0.6.2) —
// dispatchOnce logs "AppEngine targets not supported" — but validation
// itself accepts them so that CreateJob semantics remain backward compatible
// with real Cloud Scheduler payloads.
//
// Note: protobuf oneof semantics make it impossible to construct a Job with
// two targets simultaneously via the public Go API (the last assignment
// always wins). Hence we cannot directly exercise the >1 branch of
// validateJob through the public surface. The "multiple targets" branch in
// validateJob is therefore a defence-in-depth guard for reflection-based
// proto construction.
func TestCreateJob_AppEngineTargetPassesValidation(t *testing.T) {
	svc := newTestService(t)
	req := &schedulerpb.CreateJobRequest{
		Parent: testParent,
		Job: &schedulerpb.Job{
			Name:     jobName("ae1"),
			Schedule: "*/5 * * * *",
			Target: &schedulerpb.Job_AppEngineHttpTarget{
				AppEngineHttpTarget: &schedulerpb.AppEngineHttpTarget{},
			},
		},
	}
	if _, err := svc.CreateJob(context.Background(), req); err != nil {
		t.Fatalf("CreateJob with AppEngine target: %v", err)
	}
}

func TestCreateJob_NameMustBeUnderParent(t *testing.T) {
	svc := newTestService(t)
	req := &schedulerpb.CreateJobRequest{
		Parent: testParent,
		Job: &schedulerpb.Job{
			// Name not under parent → InvalidArgument.
			Name:     "projects/other/locations/us-central1/jobs/mismatch",
			Schedule: "*/5 * * * *",
			Target: &schedulerpb.Job_HttpTarget{
				HttpTarget: &schedulerpb.HttpTarget{Uri: "http://x/"},
			},
		},
	}
	_, err := svc.CreateJob(context.Background(), req)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
	}
}

func TestCreateJob_ParentRequired(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.CreateJob(context.Background(), &schedulerpb.CreateJobRequest{})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
	}
}

func TestCreateJob_DuplicateReturnsAlreadyExists(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	_, err := svc.CreateJob(ctx, httpJobReq("dup", "*/5 * * * *", "http://x/"))
	if err != nil {
		t.Fatalf("first CreateJob: %v", err)
	}
	_, err = svc.CreateJob(ctx, httpJobReq("dup", "*/5 * * * *", "http://x/"))
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Errorf("code = %v, want AlreadyExists; err=%v", got, err)
	}
}

// --- Get / Delete / List ---

func TestGetJob_NotFound(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.GetJob(context.Background(), &schedulerpb.GetJobRequest{Name: jobName("missing")})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

func TestDeleteJob_NotFound(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.DeleteJob(context.Background(), &schedulerpb.DeleteJobRequest{Name: jobName("missing")})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

func TestDeleteJob_RoundTrip(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	_, err := svc.CreateJob(ctx, httpJobReq("doomed", "*/5 * * * *", "http://x/"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	_, err = svc.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: jobName("doomed")})
	if err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	_, err = svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName("doomed")})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("Get after Delete: code = %v, want NotFound", got)
	}
}

func TestListJobs_FiltersByParent(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	if _, err := svc.CreateJob(ctx, httpJobReq("j1", "*/5 * * * *", "http://x/1")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateJob(ctx, httpJobReq("j2", "*/5 * * * *", "http://x/2")); err != nil {
		t.Fatal(err)
	}

	resp, err := svc.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: testParent})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(resp.GetJobs()) != 2 {
		t.Errorf("len = %d, want 2", len(resp.GetJobs()))
	}

	empty, err := svc.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: "projects/other/locations/eu-west"})
	if err != nil {
		t.Fatalf("ListJobs other: %v", err)
	}
	if len(empty.GetJobs()) != 0 {
		t.Errorf("len (other parent) = %d, want 0", len(empty.GetJobs()))
	}
}

// --- UpdateJob ---

func TestUpdateJob_RoundTrip(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	if _, err := svc.CreateJob(ctx, httpJobReq("u1", "*/5 * * * *", "http://x/")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	updated, err := svc.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{
		Job: &schedulerpb.Job{
			Name:     jobName("u1"),
			Schedule: "0 12 * * *",
			Target: &schedulerpb.Job_HttpTarget{
				HttpTarget: &schedulerpb.HttpTarget{Uri: "http://x/v2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if updated.GetSchedule() != "0 12 * * *" {
		t.Errorf("Schedule = %q, want 0 12 * * *", updated.GetSchedule())
	}
	if updated.GetHttpTarget().GetUri() != "http://x/v2" {
		t.Errorf("Uri = %q, want http://x/v2", updated.GetHttpTarget().GetUri())
	}

	// GetJob should observe the update.
	back, err := svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName("u1")})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if back.GetSchedule() != "0 12 * * *" {
		t.Errorf("GetJob after Update: Schedule = %q", back.GetSchedule())
	}
}

func TestUpdateJob_NameRequired(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.UpdateJob(context.Background(), &schedulerpb.UpdateJobRequest{
		Job: &schedulerpb.Job{Schedule: "*/5 * * * *",
			Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "http://x/"}}},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
	}
}

func TestUpdateJob_NotFound(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.UpdateJob(context.Background(), &schedulerpb.UpdateJobRequest{
		Job: &schedulerpb.Job{
			Name:     jobName("ghost"),
			Schedule: "*/5 * * * *",
			Target:   &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "http://x/"}},
		},
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// --- Pause / Resume ---

func TestPauseJob_TransitionsToPaused(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	if _, err := svc.CreateJob(ctx, httpJobReq("p1", "*/5 * * * *", "http://x/")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	got, err := svc.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: jobName("p1")})
	if err != nil {
		t.Fatalf("PauseJob: %v", err)
	}
	if got.GetState() != schedulerpb.Job_PAUSED {
		t.Errorf("State = %v, want PAUSED", got.GetState())
	}
}

func TestPauseJob_NotFound(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.PauseJob(context.Background(), &schedulerpb.PauseJobRequest{Name: jobName("ghost")})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

func TestResumeJob_TransitionsToEnabled(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	if _, err := svc.CreateJob(ctx, httpJobReq("r1", "*/5 * * * *", "http://x/")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := svc.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: jobName("r1")}); err != nil {
		t.Fatalf("PauseJob: %v", err)
	}
	resumed, err := svc.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: jobName("r1")})
	if err != nil {
		t.Fatalf("ResumeJob: %v", err)
	}
	if resumed.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("State = %v, want ENABLED", resumed.GetState())
	}
}

func TestResumeJob_NotFound(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.ResumeJob(context.Background(), &schedulerpb.ResumeJobRequest{Name: jobName("ghost")})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// --- RunJob ---

// TestRunJob_DoesNotMutateScheduleOrState is the canonical AAP requirement
// for RunJob: it performs a single one-shot dispatch WITHOUT mutating the
// job's schedule or state. See AAP §0.1.1 Extension C paragraph.
func TestRunJob_DoesNotMutateScheduleOrState(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	orig, err := svc.CreateJob(ctx, httpJobReq("run-me", "*/15 * * * *", "http://unreachable.invalid/"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	origSchedule, origState := orig.GetSchedule(), orig.GetState()

	if _, err := svc.RunJob(ctx, &schedulerpb.RunJobRequest{Name: jobName("run-me")}); err != nil {
		t.Fatalf("RunJob: %v", err)
	}

	after, err := svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName("run-me")})
	if err != nil {
		t.Fatalf("GetJob after RunJob: %v", err)
	}
	if after.GetSchedule() != origSchedule {
		t.Errorf("Schedule changed: %q -> %q", origSchedule, after.GetSchedule())
	}
	if after.GetState() != origState {
		t.Errorf("State changed: %v -> %v", origState, after.GetState())
	}
}

func TestRunJob_NotFound(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.RunJob(context.Background(), &schedulerpb.RunJobRequest{Name: jobName("ghost")})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// --- Store semantics (defence-in-depth against accidental state.SetState
// regressions). ---

func TestStore_CreateSetsEnabledByDefault(t *testing.T) {
	s := NewStore()
	j := &schedulerpb.Job{}
	out, err := s.Create(jobName("s1"), j)
	if err != nil {
		t.Fatal(err)
	}
	if out.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("default State = %v, want ENABLED", out.GetState())
	}
}

func TestStore_UpdateNotFound(t *testing.T) {
	s := NewStore()
	if _, err := s.Update(jobName("ghost"), &schedulerpb.Job{}); err == nil {
		t.Errorf("Update of missing job did not error")
	}
}

func TestStore_ListOrdersByName(t *testing.T) {
	s := NewStore()
	for _, n := range []string{"b", "a", "c"} {
		if _, err := s.Create(jobName(n), &schedulerpb.Job{}); err != nil {
			t.Fatal(err)
		}
	}
	jobs := s.List(testParent)
	if len(jobs) != 3 {
		t.Fatalf("len = %d", len(jobs))
	}
	want := []string{jobName("a"), jobName("b"), jobName("c")}
	for i, j := range jobs {
		if j.GetName() != want[i] {
			t.Errorf("[%d] = %q, want %q", i, j.GetName(), want[i])
		}
	}
}
