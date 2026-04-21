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
	"errors"
	"testing"
	"time"

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

// TestCreateJob_AppEngineTarget_ReturnsInvalidArgument verifies that
// AppEngineHttpTarget is rejected at validateJob time with
// codes.InvalidArgument. AppEngine HTTP targets are explicitly out of
// scope (AAP §0.6.2); the emulator refuses them outright rather than
// silently accepting the request and never dispatching (which would
// produce confusing per-tick "no recognised target" log spam).
//
// Note: protobuf oneof semantics make it impossible to construct a Job
// with two targets simultaneously via the public Go API (the last
// assignment always wins), so the ">1 target" branch of validateJob
// cannot be exercised through the public surface — that branch is a
// defence-in-depth guard for reflection-based proto construction.
func TestCreateJob_AppEngineTarget_ReturnsInvalidArgument(t *testing.T) {
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
	_, err := svc.CreateJob(context.Background(), req)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
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
//
// These tests exercise the store layer directly — they construct Store
// instances with NewStore("") so persistence is disabled and the tests
// remain hermetic. They complement the service-level RPC tests by pinning
// down the sentinel-error contract (ErrNotFound, ErrAlreadyExists), the
// deterministic List ordering, and the state-preservation semantics
// relied on by service.go.

// TestStore_CreatePreservesState asserts that the store writes and returns
// whatever State the caller set. The "default to ENABLED on create" policy
// lives in the service layer (CreateJob) — see TestCreateJob_HttpTarget_RoundTrip
// for the service-level coverage of that default.
func TestStore_CreatePreservesState(t *testing.T) {
	s := NewStore("")
	j := &Job{Name: jobName("s1"), State: schedulerpb.Job_ENABLED}
	if err := s.Create(j); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(jobName("s1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != schedulerpb.Job_ENABLED {
		t.Errorf("State = %v, want ENABLED", got.State)
	}
}

// TestStore_CreateDuplicateReturnsAlreadyExists pins down the sentinel
// error contract the service layer relies on for codes.AlreadyExists mapping.
func TestStore_CreateDuplicateReturnsAlreadyExists(t *testing.T) {
	s := NewStore("")
	if err := s.Create(&Job{Name: jobName("dup")}); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	err := s.Create(&Job{Name: jobName("dup")})
	if err == nil {
		t.Fatalf("second Create did not return error")
	}
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("err = %v, want ErrAlreadyExists", err)
	}
}

// TestStore_UpdateNotFound pins down the sentinel error contract the
// service layer relies on for codes.NotFound mapping on Update.
func TestStore_UpdateNotFound(t *testing.T) {
	s := NewStore("")
	err := s.Update(&Job{Name: jobName("ghost")})
	if err == nil {
		t.Fatalf("Update of missing job did not error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestStore_DeleteNotFound pins down the sentinel error contract for
// Delete. The service layer maps this to codes.NotFound.
func TestStore_DeleteNotFound(t *testing.T) {
	s := NewStore("")
	err := s.Delete(jobName("ghost"))
	if err == nil {
		t.Fatalf("Delete of missing job did not error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestStore_GetNotFound pins down the sentinel error contract for Get.
func TestStore_GetNotFound(t *testing.T) {
	s := NewStore("")
	_, err := s.Get(jobName("ghost"))
	if err == nil {
		t.Fatalf("Get of missing job did not error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestStore_PauseResumeStateTransitions exercises the PAUSED <-> ENABLED
// state machine at the store layer.
func TestStore_PauseResumeStateTransitions(t *testing.T) {
	s := NewStore("")
	if err := s.Create(&Job{Name: jobName("p1"), State: schedulerpb.Job_ENABLED}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	paused, err := s.Pause(jobName("p1"))
	if err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if paused.State != schedulerpb.Job_PAUSED {
		t.Errorf("after Pause: State = %v, want PAUSED", paused.State)
	}
	resumed, err := s.Resume(jobName("p1"))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.State != schedulerpb.Job_ENABLED {
		t.Errorf("after Resume: State = %v, want ENABLED", resumed.State)
	}
}

// TestStore_PauseNotFound / TestStore_ResumeNotFound pin down the
// ErrNotFound sentinel for the state-machine RPCs.
func TestStore_PauseNotFound(t *testing.T) {
	s := NewStore("")
	_, err := s.Pause(jobName("ghost"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestStore_ResumeNotFound(t *testing.T) {
	s := NewStore("")
	_, err := s.Resume(jobName("ghost"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestStore_TouchUpdatesLastAttemptTime verifies that Touch advances the
// job's LastAttemptTime to the package-level Now() value and does NOT
// mutate the Schedule, State, or target fields. This is the canonical
// contract for the cron runner tick path and RunJob's "metadata-only"
// update model per AAP §0.5.1.1.
func TestStore_TouchUpdatesLastAttemptTime(t *testing.T) {
	// Fix the clock via the package-level Now test seam so the assertion is
	// deterministic. Restore after the test so other tests are unaffected.
	fixed := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	saved := Now
	Now = func() time.Time { return fixed }
	defer func() { Now = saved }()

	s := NewStore("")
	original := &Job{
		Name:     jobName("touch-me"),
		Schedule: "*/5 * * * *",
		State:    schedulerpb.Job_ENABLED,
		HTTPTarget: &schedulerpb.HttpTarget{
			Uri:        "http://example.com/hook",
			HttpMethod: schedulerpb.HttpMethod_POST,
		},
	}
	if err := s.Create(original); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Touch(jobName("touch-me"))
	if err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if !got.LastAttemptTime.Equal(fixed) {
		t.Errorf("LastAttemptTime = %v, want %v", got.LastAttemptTime, fixed)
	}
	// Schedule, State, and target must remain untouched.
	if got.Schedule != "*/5 * * * *" {
		t.Errorf("Schedule = %q, want %q", got.Schedule, "*/5 * * * *")
	}
	if got.State != schedulerpb.Job_ENABLED {
		t.Errorf("State = %v, want ENABLED", got.State)
	}
	if got.HTTPTarget == nil || got.HTTPTarget.Uri != "http://example.com/hook" {
		t.Errorf("HTTPTarget mutated or cleared: %+v", got.HTTPTarget)
	}

	// A subsequent Get must return the same mutated time.
	fetched, err := s.Get(jobName("touch-me"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !fetched.LastAttemptTime.Equal(fixed) {
		t.Errorf("Get().LastAttemptTime = %v, want %v", fetched.LastAttemptTime, fixed)
	}
}

// TestStore_TouchNotFound pins down the ErrNotFound sentinel contract for
// Touch so that callers in service.go can rely on errors.Is(err,
// ErrNotFound) for the NotFound mapping.
func TestStore_TouchNotFound(t *testing.T) {
	s := NewStore("")
	_, err := s.Touch(jobName("ghost"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestStore_ListOrdersByName asserts the deterministic alphabetical
// ordering relied on by ListJobs and by all test assertions that read
// the result slice positionally.
func TestStore_ListOrdersByName(t *testing.T) {
	s := NewStore("")
	for _, n := range []string{"b", "a", "c"} {
		if err := s.Create(&Job{Name: jobName(n)}); err != nil {
			t.Fatal(err)
		}
	}
	jobs := s.List(testParent)
	if len(jobs) != 3 {
		t.Fatalf("len = %d", len(jobs))
	}
	want := []string{jobName("a"), jobName("b"), jobName("c")}
	for i, j := range jobs {
		if j.Name != want[i] {
			t.Errorf("[%d] = %q, want %q", i, j.Name, want[i])
		}
	}
}

// TestStore_ListParentSeparator verifies that the "/jobs/" separator
// correctly discriminates between parent prefixes that would otherwise be
// ambiguous (e.g. "projects/p/locations/us" vs
// "projects/p/locations/us-central1"). This is the critical correctness
// invariant that the store's List implementation depends on.
func TestStore_ListParentSeparator(t *testing.T) {
	s := NewStore("")
	// Insert under two neighbouring parents whose names share a prefix.
	parentA := "projects/p/locations/us"
	parentB := "projects/p/locations/us-central1"
	if err := s.Create(&Job{Name: parentA + "/jobs/j1"}); err != nil {
		t.Fatalf("Create A: %v", err)
	}
	if err := s.Create(&Job{Name: parentB + "/jobs/j1"}); err != nil {
		t.Fatalf("Create B: %v", err)
	}
	// List under parentA must NOT return the parentB job.
	gotA := s.List(parentA)
	if len(gotA) != 1 {
		t.Fatalf("List(%q) len = %d, want 1", parentA, len(gotA))
	}
	if gotA[0].Name != parentA+"/jobs/j1" {
		t.Errorf("List(%q)[0].Name = %q", parentA, gotA[0].Name)
	}
	// List under parentB must NOT return the parentA job.
	gotB := s.List(parentB)
	if len(gotB) != 1 {
		t.Fatalf("List(%q) len = %d, want 1", parentB, len(gotB))
	}
	if gotB[0].Name != parentB+"/jobs/j1" {
		t.Errorf("List(%q)[0].Name = %q", parentB, gotB[0].Name)
	}
}
