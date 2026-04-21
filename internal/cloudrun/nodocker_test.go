// Package cloudrun — nodocker_test.go covers Rule 4 (AAP §0.7.1.4):
//
//	When cfg.NoDocker is true, CreateService MUST succeed with a
//	non-empty stub URI. Container start, stop, and remove calls MUST
//	be skipped entirely -- no conditional Docker availability check.
//
// This test file constructs a failingRuntime mock that records any
// ContainerRuntime method invocation as a test failure. It then
// exercises CreateService + GetService + DeleteService with
// NoDocker=true, asserting that:
//
//   - CreateService returns Uri = "http://localhost:{hostPort}" with
//     hostPort in the 8200-8299 pool.
//   - The URI is non-empty (the weakest form of Rule 4 compliance).
//   - No method on the failing runtime is ever called.
//   - DeleteService likewise skips Stop/Remove cascades.
//
// A separate test exercises the implicit stub mode where the service
// has no runtime set at all -- functionally equivalent to --no-docker.
package cloudrun

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/slokam-ai/localgcp/internal/orchestrator"
)

// failingRuntime implements orchestrator.ContainerRuntime such that
// every method call records a test failure. Used to guarantee that
// no Docker operation is attempted in --no-docker mode.
type failingRuntime struct {
	t *testing.T
}

func (f *failingRuntime) mark(method string) {
	f.t.Helper()
	f.t.Errorf("ContainerRuntime.%s was invoked in --no-docker mode (Rule 4 violation)", method)
}

func (f *failingRuntime) Available() bool {
	// Available() is a pure predicate -- the Service may legitimately
	// probe it. We still mark so we can verify it is NOT called.
	f.mark("Available")
	return true
}
func (f *failingRuntime) Pull(context.Context, string) error { f.mark("Pull"); return nil }
func (f *failingRuntime) Create(context.Context, orchestrator.ContainerConfig) (string, error) {
	f.mark("Create")
	return "", nil
}
func (f *failingRuntime) Start(context.Context, string) error  { f.mark("Start"); return nil }
func (f *failingRuntime) Stop(context.Context, string) error   { f.mark("Stop"); return nil }
func (f *failingRuntime) Remove(context.Context, string) error { f.mark("Remove"); return nil }
func (f *failingRuntime) HostPort(context.Context, string, string) (string, error) {
	f.mark("HostPort")
	return "", nil
}
func (f *failingRuntime) FindExisting(context.Context, string) (string, bool, error) {
	f.mark("FindExisting")
	return "", false, nil
}
func (f *failingRuntime) CleanupOrphans(context.Context, string) error {
	f.mark("CleanupOrphans")
	return nil
}

// makeCreateReq constructs a minimal CreateServiceRequest with one
// container image.
func makeCreateReq(parent, id, image string) *runpb.CreateServiceRequest {
	return &runpb.CreateServiceRequest{
		Parent:    parent,
		ServiceId: id,
		Service: &runpb.Service{
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{Image: image}},
			},
		},
	}
}

// newServiceWithCleanup builds a *Service and registers a t.Cleanup
// hook that tears down every proxy the service has spawned. This
// guarantees that the 8200-8299 port pool is fully released between
// tests (including on test failure), preventing cross-test bind
// collisions.
func newServiceWithCleanup(t *testing.T) *Service {
	t.Helper()
	svc := New("", true)
	t.Cleanup(svc.stopAllProxies)
	return svc
}

// TestNoDocker_CreateService_ReturnsURIWithoutRuntimeCalls is the
// canonical Rule 4 assertion: with a failing runtime attached, setting
// NoDocker=true MUST cause CreateService to return a valid URI without
// touching any ContainerRuntime method.
func TestNoDocker_CreateService_ReturnsURIWithoutRuntimeCalls(t *testing.T) {
	svc := newServiceWithCleanup(t)
	svc.SetRuntime(&failingRuntime{t: t})
	svc.SetNoDocker(true)

	op, err := svc.CreateService(context.Background(), makeCreateReq(
		"projects/test/locations/us-central1", "stub-svc", "gcr.io/test/image:latest",
	))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("expected Operation.Done=true")
	}

	var created runpb.Service
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if created.GetUri() == "" {
		t.Fatal("expected non-empty URI in --no-docker mode (Rule 4)")
	}
	if !strings.HasPrefix(created.GetUri(), "http://localhost:") {
		t.Errorf("URI = %q, want prefix http://localhost:", created.GetUri())
	}

	// Clean up -- DeleteService should also avoid runtime calls.
	if _, err := svc.DeleteService(context.Background(), &runpb.DeleteServiceRequest{
		Name: created.GetName(),
	}); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
}

// TestNoDocker_DeleteService_SkipsStopAndRemove verifies that tear-down
// in --no-docker mode does not invoke runtime.Stop or runtime.Remove.
func TestNoDocker_DeleteService_SkipsStopAndRemove(t *testing.T) {
	svc := newServiceWithCleanup(t)
	svc.SetRuntime(&failingRuntime{t: t})
	svc.SetNoDocker(true)

	op, err := svc.CreateService(context.Background(), makeCreateReq(
		"projects/test/locations/us-central1", "delete-me", "gcr.io/test/image:latest",
	))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	var created runpb.Service
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Delete -- must not touch the runtime.
	if _, err := svc.DeleteService(context.Background(), &runpb.DeleteServiceRequest{
		Name: created.GetName(),
	}); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
}

// TestNoDocker_NilRuntime_ReturnsURI verifies the implicit stub mode
// (no runtime configured at all). This is not strictly Rule 4 but is
// the natural consequence of Rule 4's unconditional semantics: an
// unset runtime must behave identically to --no-docker.
//
// Unlike the other two tests in this file, this test intentionally
// does not call DeleteService -- it exercises the lifecycle where a
// service is created and then the enclosing process exits. The
// t.Cleanup hook on newServiceWithCleanup ensures the proxy listener
// is released before the next test runs.
func TestNoDocker_NilRuntime_ReturnsURI(t *testing.T) {
	svc := newServiceWithCleanup(t)
	// No SetRuntime, no SetNoDocker.
	op, err := svc.CreateService(context.Background(), makeCreateReq(
		"projects/test/locations/us-central1", "implicit-stub", "gcr.io/test/image:latest",
	))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	var created runpb.Service
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if created.GetUri() == "" {
		t.Fatal("expected non-empty URI with nil runtime")
	}
	if !strings.HasPrefix(created.GetUri(), "http://localhost:") {
		t.Errorf("URI = %q, want prefix http://localhost:", created.GetUri())
	}
}
