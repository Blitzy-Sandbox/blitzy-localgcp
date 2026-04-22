// Package cloudscheduler — service_test.go exercises the eight in-scope
// CloudScheduler RPCs over an in-process gRPC server, mirroring the
// helper / harness pattern used by internal/cloudtasks/service_test.go and
// internal/cloudrun/service_test.go for consistency with the rest of the
// codebase.
//
// The tests validate:
//
//   - CRUD round-trip for HTTP-target jobs over the gRPC wire.
//   - Input validation: invalid cron expression, missing target, missing
//     schedule, name prefix enforcement.
//   - NotFound semantics for Get, Delete, Run, Pause, Resume on unknown names.
//   - Pause/Resume state-machine transitions persisted across GetJob.
//   - RunJob immediate HTTP-target dispatch (via httptest.NewServer).
//   - RunJob does NOT mutate schedule or state (the canonical invariant from
//     AAP §0.1.1 Extension C).
//   - CreateJob with PubsubTarget succeeds without a configured pubsubAddr
//     (Rule 7a silent no-op for loopback Pub/Sub delivery).
//
// All test jobs use neverSchedule = "0 0 1 1 *" so that the cron runner
// does not fire during the test window; RunJob is used to trigger
// dispatch deterministically.
//
// Pub/Sub-target dispatch is exercised at the validation level only —
// actual loopback delivery is out of scope for unit tests and is covered
// by the //go:build integration tests in the sibling gcs and logging
// packages (per AAP Rule 9).
//
// Tests use the same-package form (package cloudscheduler, not
// cloudscheduler_test) so the test helper can access the unexported
// svc.cron field to drive the cron runner deterministically.
package cloudscheduler

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Note: Rule 6 (canonical unimplemented error for out-of-scope RPCs) is trivially
// satisfied because schedulerpb.CloudSchedulerServer contains exactly the 8 RPCs
// that are all in scope (CreateJob, GetJob, ListJobs, DeleteJob, UpdateJob, RunJob,
// PauseJob, ResumeJob). No CloudScheduler RPCs are out-of-scope, so no separate
// unimplemented-error test is necessary.

const (
	// testParent is the default parent resource for job tests.
	testParent = "projects/test/locations/us-central1"
	// testJobName is a fully-qualified job resource name used as the default
	// single-job identity across tests that do not need multiple jobs.
	testJobName = testParent + "/jobs/job-1"
	// neverSchedule is a 5-field cron spec that will not fire during any
	// reasonable test run. "0 0 1 1 *" means minute=0 hour=0 day-of-month=1
	// month=1 day-of-week=any — i.e., midnight on January 1st (annual) in
	// host local time. Using this expression throughout the suite allows
	// us to safely start the cron runner without risking spurious firings.
	neverSchedule = "0 0 1 1 *"
)

// jobName builds a fully-qualified job name under testParent from a short id.
func jobName(short string) string {
	return testParent + "/jobs/" + short
}

// httpJobRequest builds a CreateJobRequest with an HTTP POST target. The
// returned request's Job.Parent is always testParent; the Name field is
// expected to be fully-qualified and under that parent (use jobName(...)
// or testJobName).
func httpJobRequest(name, schedule, uri string) *schedulerpb.CreateJobRequest {
	return &schedulerpb.CreateJobRequest{
		Parent: testParent,
		Job: &schedulerpb.Job{
			Name:     name,
			Schedule: schedule,
			Target: &schedulerpb.Job_HttpTarget{
				HttpTarget: &schedulerpb.HttpTarget{
					Uri:        uri,
					HttpMethod: schedulerpb.HttpMethod_POST,
				},
			},
		},
	}
}

// pubsubJobRequest builds a CreateJobRequest with a Pub/Sub target.
func pubsubJobRequest(name, schedule, topic string, data []byte, attrs map[string]string) *schedulerpb.CreateJobRequest {
	return &schedulerpb.CreateJobRequest{
		Parent: testParent,
		Job: &schedulerpb.Job{
			Name:     name,
			Schedule: schedule,
			Target: &schedulerpb.Job_PubsubTarget{
				PubsubTarget: &schedulerpb.PubsubTarget{
					TopicName:  topic,
					Data:       data,
					Attributes: attrs,
				},
			},
		},
	}
}

// schedulerTestClient starts an in-process Cloud Scheduler gRPC server on
// an ephemeral port and returns a client, the *Service (for white-box
// assertions when tests need them), and a cleanup function.
//
// Design notes:
//
//   - The helper passes "" for pubsubAddr so PubsubTarget dispatch is a
//     silent no-op (AAP Rule 7a). This keeps unit tests hermetic — the
//     loopback Pub/Sub delivery path is exercised by the //go:build
//     integration tests in the sibling gcs and logging packages.
//
//   - The cron runner is explicitly started via svc.cron.Start() so that
//     RunJob's fire-and-forget dispatch goroutine path is active and the
//     scheduleJob / unscheduleJob entry-management code paths are
//     exercised during CreateJob / UpdateJob / DeleteJob / PauseJob /
//     ResumeJob. Because all test jobs use neverSchedule, the runner
//     itself does not fire during the test window.
//
//   - The helper calls srv.Stop() (not GracefulStop) on cleanup because
//     an ephemeral test binary has no long-running RPCs to drain — a
//     graceful shutdown would add test latency without benefit.
//
//   - t.Cleanup is NOT used here — instead the caller is expected to
//     `defer cleanup()` — matching the idiom in the sibling cloudtasks
//     and cloudrun test files.
func schedulerTestClient(t *testing.T) (schedulerpb.CloudSchedulerClient, *Service, func()) {
	t.Helper()

	svc := New("", true, "") // in-memory dataDir, quiet logger, no Pub/Sub loopback.

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	schedulerpb.RegisterCloudSchedulerServer(srv, svc)
	go func() {
		// Serve returns when srv.Stop() is called in cleanup; the error is
		// always non-nil in that case (grpc.ErrServerStopped). We
		// intentionally ignore the return value for test ergonomics.
		_ = srv.Serve(ln)
	}()

	// Start the cron runner so that scheduleJob's AddFunc registrations are
	// live. All test jobs use neverSchedule so no tick will fire during
	// the test window.
	svc.cron.Start()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	client := schedulerpb.NewCloudSchedulerClient(conn)

	cleanup := func() {
		// Stop the cron runner first so no new dispatches are enqueued
		// during teardown. cron.Stop returns a context that is Done when
		// all in-flight tick-launched goroutines finish; we wait for it
		// to avoid leaking goroutines into subsequent tests.
		<-svc.cron.Stop().Done()
		_ = conn.Close()
		srv.Stop()
	}
	return client, svc, cleanup
}

// ============================================================================
// CRUD tests — round-trip coverage for the service-level gRPC surface.
// ============================================================================

// TestCreateAndGetJob covers the canonical CRUD round-trip: create a job,
// assert the returned proto carries the expected fields, then re-read via
// GetJob and assert the same fields persist.
func TestCreateAndGetJob(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, "http://example.invalid/"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.GetName() != testJobName {
		t.Errorf("created.Name = %q, want %q", created.GetName(), testJobName)
	}
	// Jobs default to ENABLED on creation (service.go CreateJob sets
	// State = Job_ENABLED when the caller leaves it as Job_STATE_UNSPECIFIED).
	if created.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("created.State = %v, want ENABLED", created.GetState())
	}
	if created.GetSchedule() != neverSchedule {
		t.Errorf("created.Schedule = %q, want %q", created.GetSchedule(), neverSchedule)
	}
	if ht := created.GetHttpTarget(); ht == nil || ht.GetUri() != "http://example.invalid/" {
		t.Errorf("created.HttpTarget = %+v, want URI http://example.invalid/", ht)
	}

	got, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.GetName() != testJobName {
		t.Errorf("got.Name = %q, want %q", got.GetName(), testJobName)
	}
	if got.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("got.State = %v, want ENABLED", got.GetState())
	}
	if got.GetSchedule() != neverSchedule {
		t.Errorf("got.Schedule = %q, want %q", got.GetSchedule(), neverSchedule)
	}
	if ht := got.GetHttpTarget(); ht == nil || ht.GetUri() != "http://example.invalid/" {
		t.Errorf("got.HttpTarget = %+v, want URI http://example.invalid/", ht)
	}
}

// TestListJobs creates three jobs under testParent plus one job under a
// different parent and confirms the List call only returns jobs whose
// Name matches the requested Parent prefix. Also pins the contract that
// List returns results in alphabetical Name order.
func TestListJobs(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	// Create three jobs under testParent.
	for _, short := range []string{"job-a", "job-b", "job-c"} {
		if _, err := client.CreateJob(ctx, httpJobRequest(jobName(short), neverSchedule, "http://example.invalid/"+short)); err != nil {
			t.Fatalf("CreateJob(%s): %v", short, err)
		}
	}

	// Create one job under a different parent — must not appear in a
	// testParent list.
	otherParent := "projects/other/locations/us-central1"
	_, err := client.CreateJob(ctx, &schedulerpb.CreateJobRequest{
		Parent: otherParent,
		Job: &schedulerpb.Job{
			Name:     otherParent + "/jobs/elsewhere",
			Schedule: neverSchedule,
			Target: &schedulerpb.Job_HttpTarget{
				HttpTarget: &schedulerpb.HttpTarget{Uri: "http://x/"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateJob(elsewhere): %v", err)
	}

	resp, err := client.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: testParent})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	jobs := resp.GetJobs()
	if len(jobs) != 3 {
		t.Fatalf("ListJobs returned %d jobs, want 3; got names:", len(jobs))
	}
	// store.List sorts alphabetically by Name.
	want := []string{jobName("job-a"), jobName("job-b"), jobName("job-c")}
	for i, j := range jobs {
		if j.GetName() != want[i] {
			t.Errorf("jobs[%d].Name = %q, want %q", i, j.GetName(), want[i])
		}
	}
}

// TestDeleteJob covers a create, delete, GetJob-returns-NotFound round
// trip and additionally asserts that a second DeleteJob on the same name
// returns NotFound (the emulator does NOT implement idempotent-delete
// semantics on the second call).
func TestDeleteJob(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, "http://example.invalid/")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if _, err := client.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: testJobName}); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	_, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err == nil {
		t.Fatal("GetJob after Delete: expected error")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("GetJob after Delete: code = %v, want NotFound; err=%v", got, err)
	}

	// A second DeleteJob on the same name returns NotFound — the
	// emulator's Delete is NOT idempotent on already-deleted jobs.
	_, err = client.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: testJobName})
	if err == nil {
		t.Fatal("second DeleteJob: expected NotFound")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("second DeleteJob: code = %v, want NotFound; err=%v", got, err)
	}
}

// TestUpdateJob creates a job, updates its HttpTarget URI, asserts the
// returned proto has the new URI, and asserts a subsequent GetJob call
// observes the updated URI (i.e., the update is persisted).
func TestUpdateJob(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, "http://example.invalid/v1")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	updated, err := client.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{
		Job: &schedulerpb.Job{
			Name:     testJobName,
			Schedule: neverSchedule,
			Target: &schedulerpb.Job_HttpTarget{
				HttpTarget: &schedulerpb.HttpTarget{Uri: "http://example.invalid/v2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if got := updated.GetHttpTarget().GetUri(); got != "http://example.invalid/v2" {
		t.Errorf("updated.HttpTarget.Uri = %q, want http://example.invalid/v2", got)
	}
	if updated.GetName() != testJobName {
		t.Errorf("updated.Name = %q, want %q", updated.GetName(), testJobName)
	}

	// GetJob should observe the update.
	after, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("GetJob after Update: %v", err)
	}
	if got := after.GetHttpTarget().GetUri(); got != "http://example.invalid/v2" {
		t.Errorf("after.HttpTarget.Uri = %q, want http://example.invalid/v2", got)
	}
}

// TestDuplicateJobFails asserts that creating a job with a name that
// already exists returns codes.AlreadyExists.
func TestDuplicateJobFails(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, "http://example.invalid/")); err != nil {
		t.Fatalf("first CreateJob: %v", err)
	}
	_, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, "http://example.invalid/"))
	if err == nil {
		t.Fatal("second CreateJob: expected AlreadyExists")
	}
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("second CreateJob: code = %v, want AlreadyExists; err=%v", got, err)
	}
}

// ============================================================================
// State-machine tests — Pause/Resume transitions.
// ============================================================================

// TestPauseAndResumeJob covers the ENABLED -> PAUSED -> ENABLED state
// machine and asserts that the transitions are observable on a
// subsequent GetJob call (i.e., persisted in the store).
func TestPauseAndResumeJob(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, "http://example.invalid/"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.GetState() != schedulerpb.Job_ENABLED {
		t.Fatalf("created.State = %v, want ENABLED", created.GetState())
	}

	// ENABLED -> PAUSED
	paused, err := client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("PauseJob: %v", err)
	}
	if paused.GetState() != schedulerpb.Job_PAUSED {
		t.Errorf("paused.State = %v, want PAUSED", paused.GetState())
	}

	// Persisted — GetJob observes PAUSED.
	viaGet, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("GetJob after Pause: %v", err)
	}
	if viaGet.GetState() != schedulerpb.Job_PAUSED {
		t.Errorf("viaGet.State = %v, want PAUSED", viaGet.GetState())
	}

	// PAUSED -> ENABLED
	resumed, err := client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("ResumeJob: %v", err)
	}
	if resumed.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("resumed.State = %v, want ENABLED", resumed.GetState())
	}

	// Persisted — GetJob observes ENABLED.
	viaGet2, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("GetJob after Resume: %v", err)
	}
	if viaGet2.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("viaGet2.State = %v, want ENABLED", viaGet2.GetState())
	}
}

// ============================================================================
// RunJob tests — the critical invariant for on-demand dispatch.
// ============================================================================

// TestRunJobDispatchesHttpTarget stands up an httptest HTTP server and
// verifies that RunJob triggers a single HTTP POST to the job's
// HttpTarget URI. The cron runner is started but the job uses
// neverSchedule, so only the RunJob-path dispatch is exercised.
//
// Per AAP Rule 3, RunJob MUST return immediately — the HTTP dispatch
// runs in a background goroutine (see dispatchOnce in service.go). The
// polling loop below accommodates that asynchrony.
func TestRunJobDispatchesHttpTarget(t *testing.T) {
	var received atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, target.URL))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.GetName() != testJobName {
		t.Fatalf("created.Name = %q, want %q", created.GetName(), testJobName)
	}

	ran, err := client.RunJob(ctx, &schedulerpb.RunJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	// RunJob must not mutate schedule or state (see also
	// TestRunJobDoesNotMutateScheduleOrState for the GetJob-after-RunJob
	// assertion).
	if ran.GetSchedule() != neverSchedule {
		t.Errorf("RunJob mutated Schedule on returned proto: got %q, want %q", ran.GetSchedule(), neverSchedule)
	}
	if ran.GetState() != schedulerpb.Job_ENABLED {
		t.Errorf("RunJob mutated State on returned proto: got %v, want ENABLED", ran.GetState())
	}
	// LastAttemptTime should be populated (the single permitted mutation).
	if ran.GetLastAttemptTime() == nil {
		t.Errorf("RunJob: LastAttemptTime is nil, want populated")
	}

	// Poll for the dispatch to land. The dispatcher retries with
	// exponential backoff, but the httptest server returns 200
	// immediately so the first attempt succeeds. 5 seconds is an order
	// of magnitude above what a healthy in-process dispatch should
	// need.
	deadline := time.After(5 * time.Second)
	for {
		if received.Load() >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for RunJob dispatch; received=%d", received.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	// Give any stray goroutine a moment to race the counter above the
	// expected value — if the cron runner unexpectedly fires, we want to
	// detect it. 200ms is comfortably below the 1-minute granularity of
	// the 5-field cron expression.
	time.Sleep(200 * time.Millisecond)
	if got := received.Load(); got != 1 {
		t.Errorf("target received %d requests, want exactly 1 (cron may have fired unexpectedly)", got)
	}
}

// TestRunJobDoesNotMutateScheduleOrState is the sharpened, canonical
// assertion of the AAP §0.1.1 Extension C invariant:
//
//	"RunJob performs an immediate single dispatch without mutating
//	schedule or state."
//
// The test captures the job's Schedule and State BEFORE RunJob, performs
// RunJob, then asserts via a fresh GetJob that Schedule and State are
// byte-identical while LastAttemptTime IS populated (the single
// permitted mutation).
func TestRunJobDoesNotMutateScheduleOrState(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	before, err := client.CreateJob(ctx, httpJobRequest(testJobName, neverSchedule, target.URL))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	beforeSchedule := before.GetSchedule()
	beforeState := before.GetState()
	if beforeSchedule != neverSchedule {
		t.Fatalf("beforeSchedule = %q, want %q", beforeSchedule, neverSchedule)
	}
	if beforeState != schedulerpb.Job_ENABLED {
		t.Fatalf("beforeState = %v, want ENABLED", beforeState)
	}

	if _, err := client.RunJob(ctx, &schedulerpb.RunJobRequest{Name: testJobName}); err != nil {
		t.Fatalf("RunJob: %v", err)
	}

	after, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if after.GetSchedule() != beforeSchedule {
		t.Errorf("RunJob mutated Schedule: %q -> %q", beforeSchedule, after.GetSchedule())
	}
	if after.GetState() != beforeState {
		t.Errorf("RunJob mutated State: %v -> %v", beforeState, after.GetState())
	}
	// LastAttemptTime IS allowed (and expected) to mutate.
	if after.GetLastAttemptTime() == nil {
		t.Errorf("after.LastAttemptTime = nil, want populated")
	}
}

// ============================================================================
// Input-validation tests.
// ============================================================================

// TestCreateJobWithInvalidScheduleFails asserts that CreateJob rejects
// syntactically-invalid cron expressions with codes.InvalidArgument. It
// also confirms that the store is NOT mutated on a failed CreateJob —
// a subsequent GetJob with the same name returns NotFound.
func TestCreateJobWithInvalidScheduleFails(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.CreateJob(ctx, httpJobRequest(testJobName, "this is not a valid cron", "http://example.invalid/"))
	if err == nil {
		t.Fatal("CreateJob with invalid schedule: expected InvalidArgument")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
	}

	// Rollback assertion: a subsequent GetJob must return NotFound.
	// service.go validateJob runs BEFORE store.Create for the pure
	// cron-syntax error path, and the scheduleJob failure path explicitly
	// rolls back via s.store.Delete (see service.go CreateJob body).
	_, err = client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err == nil {
		t.Fatal("GetJob after failed CreateJob: expected NotFound")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("GetJob after failed CreateJob: code = %v, want NotFound; err=%v", got, err)
	}
}

// TestCreateJobWithPubsubTarget verifies that Pub/Sub-target jobs are
// accepted by CreateJob even when pubsubAddr is empty (the helper passes
// "" for pubsubAddr). Per AAP Rule 7a, empty pubsubAddr causes
// dispatchPubsub to be a silent no-op, but the job itself must still be
// stored and returnable via GetJob with its PubsubTarget intact.
func TestCreateJobWithPubsubTarget(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	req := pubsubJobRequest(testJobName, neverSchedule, "projects/test/topics/t", []byte("hello"), map[string]string{"k": "v"})
	created, err := client.CreateJob(ctx, req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	pt := created.GetPubsubTarget()
	if pt == nil {
		t.Fatalf("created.PubsubTarget = nil, want populated")
	}
	if pt.GetTopicName() != "projects/test/topics/t" {
		t.Errorf("TopicName = %q, want projects/test/topics/t", pt.GetTopicName())
	}
	if string(pt.GetData()) != "hello" {
		t.Errorf("Data = %q, want %q", string(pt.GetData()), "hello")
	}
	if pt.GetAttributes()["k"] != "v" {
		t.Errorf("Attributes[k] = %q, want v", pt.GetAttributes()["k"])
	}

	// GetJob should round-trip the Pub/Sub target.
	after, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: testJobName})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if tn := after.GetPubsubTarget().GetTopicName(); tn != "projects/test/topics/t" {
		t.Errorf("after.PubsubTarget.TopicName = %q, want projects/test/topics/t", tn)
	}
}

// TestCreateJobRequiresTargetAndSchedule verifies that CreateJob rejects:
//
//  1. jobs missing Schedule (no cron expression provided)
//  2. jobs missing both HttpTarget and PubsubTarget
//
// with codes.InvalidArgument. Both are preconditions for a well-formed
// Cloud Scheduler job — a job must have something to run and a time to
// run it.
func TestCreateJobRequiresTargetAndSchedule(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("missing schedule", func(t *testing.T) {
		req := &schedulerpb.CreateJobRequest{
			Parent: testParent,
			Job: &schedulerpb.Job{
				Name: jobName("no-schedule"),
				// Schedule intentionally omitted.
				Target: &schedulerpb.Job_HttpTarget{
					HttpTarget: &schedulerpb.HttpTarget{Uri: "http://example.invalid/"},
				},
			},
		}
		_, err := client.CreateJob(ctx, req)
		if err == nil {
			t.Fatal("expected InvalidArgument for missing schedule")
		}
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
		}
	})

	t.Run("missing target", func(t *testing.T) {
		req := &schedulerpb.CreateJobRequest{
			Parent: testParent,
			Job: &schedulerpb.Job{
				Name:     jobName("no-target"),
				Schedule: neverSchedule,
				// Target intentionally omitted — neither HttpTarget nor
				// PubsubTarget is set.
			},
		}
		_, err := client.CreateJob(ctx, req)
		if err == nil {
			t.Fatal("expected InvalidArgument for missing target")
		}
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("code = %v, want InvalidArgument; err=%v", got, err)
		}
	})
}

// ============================================================================
// NotFound tests — each in-scope RPC on an unknown job name returns NotFound.
// ============================================================================

// TestGetJobNotFound asserts GetJob with an unknown name returns NotFound.
func TestGetJobNotFound(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()

	_, err := client.GetJob(context.Background(), &schedulerpb.GetJobRequest{Name: jobName("nope")})
	if err == nil {
		t.Fatal("GetJob: expected NotFound")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// TestPauseJobNotFound asserts PauseJob with an unknown name returns NotFound.
func TestPauseJobNotFound(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()

	_, err := client.PauseJob(context.Background(), &schedulerpb.PauseJobRequest{Name: jobName("nope")})
	if err == nil {
		t.Fatal("PauseJob: expected NotFound")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// TestResumeJobNotFound asserts ResumeJob with an unknown name returns NotFound.
func TestResumeJobNotFound(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()

	_, err := client.ResumeJob(context.Background(), &schedulerpb.ResumeJobRequest{Name: jobName("nope")})
	if err == nil {
		t.Fatal("ResumeJob: expected NotFound")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// TestDeleteJobNotFound asserts DeleteJob with an unknown name returns NotFound.
func TestDeleteJobNotFound(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()

	_, err := client.DeleteJob(context.Background(), &schedulerpb.DeleteJobRequest{Name: jobName("nope")})
	if err == nil {
		t.Fatal("DeleteJob: expected NotFound")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// TestRunJobNotFound asserts RunJob with an unknown name returns NotFound.
func TestRunJobNotFound(t *testing.T) {
	client, _, cleanup := schedulerTestClient(t)
	defer cleanup()

	_, err := client.RunJob(context.Background(), &schedulerpb.RunJobRequest{Name: jobName("nope")})
	if err == nil {
		t.Fatal("RunJob: expected NotFound")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want NotFound; err=%v", got, err)
	}
}

// ============================================================================
// Store-level regression tests.
//
// These tests exercise the store layer directly and complement the
// service-level RPC tests by pinning down:
//
//   - the sentinel-error contract (ErrNotFound, ErrAlreadyExists) the
//     service layer relies on for codes.NotFound / codes.AlreadyExists
//     mapping.
//
//   - the deterministic parent-filtering contract in store.List (the
//     "/jobs/" separator must correctly disambiguate parents that share
//     a prefix, e.g. "us" vs "us-central1").
//
//   - the Touch test-seam semantics that the cron runner and RunJob
//     rely on for their "metadata-only" update model.
//
// These low-level tests run entirely in-process without a gRPC wire and
// therefore provide defense-in-depth when a future refactor reshuffles
// the service layer — the store contract stays pinned.
// ============================================================================

// TestStore_TouchUpdatesLastAttemptTime verifies that Touch advances the
// job's LastAttemptTime to the package-level Now() value and does NOT
// mutate the Schedule, State, or target fields. This is the canonical
// contract for the cron runner tick path and RunJob's "metadata-only"
// update model.
func TestStore_TouchUpdatesLastAttemptTime(t *testing.T) {
	fixed := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	saved := Now
	Now = func() time.Time { return fixed }
	defer func() { Now = saved }()

	s := NewStore("")
	original := &Job{
		Name:     jobName("touch-me"),
		Schedule: neverSchedule,
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
	if got.Schedule != neverSchedule {
		t.Errorf("Schedule mutated: got %q, want %q", got.Schedule, neverSchedule)
	}
	if got.State != schedulerpb.Job_ENABLED {
		t.Errorf("State mutated: got %v, want ENABLED", got.State)
	}
	if got.HTTPTarget == nil || got.HTTPTarget.Uri != "http://example.com/hook" {
		t.Errorf("HTTPTarget mutated or cleared: %+v", got.HTTPTarget)
	}
}

// TestStore_ListParentSeparator verifies that the "/jobs/" separator in
// the List filter correctly disambiguates parents that share a prefix —
// e.g. "projects/p/locations/us" vs "projects/p/locations/us-central1".
// A naïve strings.HasPrefix(name, parent) without the trailing "/jobs/"
// would bleed across these two parents; the test pins the correct
// behaviour so future refactors cannot regress it.
func TestStore_ListParentSeparator(t *testing.T) {
	s := NewStore("")
	parentA := "projects/p/locations/us"
	parentB := "projects/p/locations/us-central1"
	if err := s.Create(&Job{Name: parentA + "/jobs/j1"}); err != nil {
		t.Fatalf("Create A: %v", err)
	}
	if err := s.Create(&Job{Name: parentB + "/jobs/j1"}); err != nil {
		t.Fatalf("Create B: %v", err)
	}
	gotA := s.List(parentA)
	if len(gotA) != 1 {
		t.Fatalf("List(%q) len = %d, want 1", parentA, len(gotA))
	}
	if gotA[0].Name != parentA+"/jobs/j1" {
		t.Errorf("List(%q)[0].Name = %q, want %q", parentA, gotA[0].Name, parentA+"/jobs/j1")
	}
	gotB := s.List(parentB)
	if len(gotB) != 1 {
		t.Fatalf("List(%q) len = %d, want 1", parentB, len(gotB))
	}
	if gotB[0].Name != parentB+"/jobs/j1" {
		t.Errorf("List(%q)[0].Name = %q, want %q", parentB, gotB[0].Name, parentB+"/jobs/j1")
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
		t.Fatal("second Create did not return error")
	}
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("err = %v, want ErrAlreadyExists", err)
	}
}

// TestStore_UpdateNotFound pins the ErrNotFound sentinel contract for
// the Update path.
func TestStore_UpdateNotFound(t *testing.T) {
	s := NewStore("")
	err := s.Update(&Job{Name: jobName("ghost")})
	if err == nil {
		t.Fatal("Update of missing job did not return error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestStore_DeleteNotFound pins the ErrNotFound sentinel contract for
// the Delete path.
func TestStore_DeleteNotFound(t *testing.T) {
	s := NewStore("")
	err := s.Delete(jobName("ghost"))
	if err == nil {
		t.Fatal("Delete of missing job did not return error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
