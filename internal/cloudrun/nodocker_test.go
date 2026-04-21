// Package cloudrun — nodocker_test.go covers Rule 4 (AAP §0.7.1.4):
//
//	When cfg.NoDocker is true, CreateService MUST succeed with a
//	non-empty stub URI. Container start, stop, and remove calls MUST
//	be skipped entirely -- no conditional Docker availability check.
//
// This test file constructs a failingMockRuntime that records any
// ContainerRuntime method invocation as an immediate test failure via
// t.Fatal. It then exercises the full CRUD lifecycle (Create + Get +
// Delete + Get NotFound) with NoDocker=true, asserting that:
//
//   - CreateService returns Uri = "http://localhost:{hostPort}" with
//     a non-empty value (the canonical Rule 4 contract).
//   - GetService also returns a non-empty URI (the stub URI is
//     persisted in the store, not synthesized on each call).
//   - DeleteService completes successfully and makes the service
//     NotFound on subsequent lookups.
//   - No method on the failing runtime is ever called.
//
// A separate supplementary test exercises the implicit stub mode
// where the service has no runtime set at all -- functionally
// equivalent to --no-docker and required by the unconditional Rule 4
// semantics.
//
// The mock implements the full 9-method orchestrator.ContainerRuntime
// interface (Available, Pull, Create, Start, Stop, Remove, HostPort,
// FindExisting, CleanupOrphans). Any deviation from Rule 4 (for
// example, a naive "if runtime.Available()" probe introduced by a
// future refactor) will trigger t.Fatal in the mock and break this
// test -- the intended canary behavior.
package cloudrun

import (
	"context"
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/slokam-ai/localgcp/internal/orchestrator"
)

// failingMockRuntime is a test double that calls t.Fatal on any method
// invocation. It is used to verify Rule 4 (AAP §0.7.1.4): when
// NoDocker=true, no ContainerRuntime methods must be called -- not
// even Available().
//
// The full 9-method orchestrator.ContainerRuntime interface is
// implemented so that svc.SetRuntime(&failingMockRuntime{t: t})
// compiles. Any deviation in the service.go code path that triggers
// a runtime call when NoDocker=true will invoke one of these methods
// and immediately fail the test.
type failingMockRuntime struct {
	t *testing.T
}

// Available is a pure predicate -- the Service may be tempted to
// probe it before executing runtime operations. Rule 4 forbids
// conditional Docker availability checks, so Available MUST NOT be
// called when NoDocker=true.
func (m *failingMockRuntime) Available() bool {
	m.t.Helper()
	m.t.Fatal("Available must not be called when NoDocker=true (Rule 4)")
	return false
}

// Pull downloads a container image. Forbidden in --no-docker mode.
func (m *failingMockRuntime) Pull(_ context.Context, _ string) error {
	m.t.Helper()
	m.t.Fatal("Pull must not be called when NoDocker=true (Rule 4)")
	return nil
}

// Create creates (but does not start) a container. Forbidden in
// --no-docker mode.
func (m *failingMockRuntime) Create(_ context.Context, _ orchestrator.ContainerConfig) (string, error) {
	m.t.Helper()
	m.t.Fatal("Create must not be called when NoDocker=true (Rule 4)")
	return "", nil
}

// Start begins container execution. Forbidden in --no-docker mode.
func (m *failingMockRuntime) Start(_ context.Context, _ string) error {
	m.t.Helper()
	m.t.Fatal("Start must not be called when NoDocker=true (Rule 4)")
	return nil
}

// Stop halts container execution. Forbidden in --no-docker mode
// (including on the DeleteService tear-down path).
func (m *failingMockRuntime) Stop(_ context.Context, _ string) error {
	m.t.Helper()
	m.t.Fatal("Stop must not be called when NoDocker=true (Rule 4)")
	return nil
}

// Remove deletes a stopped container. Forbidden in --no-docker mode
// (including on the DeleteService tear-down path).
func (m *failingMockRuntime) Remove(_ context.Context, _ string) error {
	m.t.Helper()
	m.t.Fatal("Remove must not be called when NoDocker=true (Rule 4)")
	return nil
}

// HostPort resolves the Docker-assigned host port for a container's
// internal port. Forbidden in --no-docker mode because no container
// exists to probe.
func (m *failingMockRuntime) HostPort(_ context.Context, _ string, _ string) (string, error) {
	m.t.Helper()
	m.t.Fatal("HostPort must not be called when NoDocker=true (Rule 4)")
	return "", nil
}

// FindExisting locates a pre-existing container by name (used for
// orphan recovery). Forbidden in --no-docker mode.
func (m *failingMockRuntime) FindExisting(_ context.Context, _ string) (string, bool, error) {
	m.t.Helper()
	m.t.Fatal("FindExisting must not be called when NoDocker=true (Rule 4)")
	return "", false, nil
}

// CleanupOrphans removes orphaned containers by name prefix. Forbidden
// in --no-docker mode.
func (m *failingMockRuntime) CleanupOrphans(_ context.Context, _ string) error {
	m.t.Helper()
	m.t.Fatal("CleanupOrphans must not be called when NoDocker=true (Rule 4)")
	return nil
}

// Compile-time check: failingMockRuntime must implement the full
// orchestrator.ContainerRuntime interface. Adding/removing a method
// on the interface will produce a compile error here, forcing a
// corresponding update to the mock.
var _ orchestrator.ContainerRuntime = (*failingMockRuntime)(nil)

// ---------------------------------------------------------------------------
// Shared test helpers — used by both nodocker_test.go and portpool_test.go
// ---------------------------------------------------------------------------
//
// These helpers live in this file (the lowest-alphabetical test file in the
// package) so that both Rule 4 tests (nodocker_test.go) and Rule 8 port-pool
// tests (portpool_test.go) can construct *Service instances with the same
// lifecycle semantics and the same canonical CreateServiceRequest shape.
// Moving them would require updating the cross-file comment reference in
// portpool_test.go, so they are intentionally kept here as the single
// source of truth for same-package test fixture construction.

// newServiceWithCleanup constructs a *Service with the canonical test
// configuration (empty data-dir, quiet=true) and registers a t.Cleanup hook
// that tears down every proxy the service has spawned. This guarantees the
// 8200-8299 port pool is fully released between tests (including on test
// failure) and prevents cross-test listener bind collisions.
//
// This helper does NOT call SetNoDocker — callers that require --no-docker
// semantics must invoke svc.SetNoDocker(true) themselves (or use
// newNoDockerTestService which wraps this helper and sets NoDocker=true).
//
// Consumed by: portpool_test.go (TestPortPool_Service_FiveCreatesReturnDistinctURIs,
// TestPortPool_Service_DeleteReclaimsPort) and by newNoDockerTestService
// in this file.
func newServiceWithCleanup(t *testing.T) *Service {
	t.Helper()
	svc := New("", true)
	t.Cleanup(svc.stopAllProxies)
	return svc
}

// makeCreateReq returns a canonical *runpb.CreateServiceRequest with the
// provided parent, service ID, and container image baked into a single
// container Template. This matches the minimum shape required by the
// Cloud Run v2 API and is used by every CreateService call in the port
// pool tests and the Rule 4 dedicated-DeleteService test.
//
// Consumed by: portpool_test.go (all TestPortPool_Service_* tests) and by
// the Rule 4 tests in this file when a canonical request shape is desired.
func makeCreateReq(parent, id, image string) *runpb.CreateServiceRequest {
	return &runpb.CreateServiceRequest{
		Parent:    parent,
		ServiceId: id,
		Service: &runpb.Service{
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{
					Image: image,
				}},
			},
		},
	}
}

// newNoDockerTestService constructs a *Service wired with the given
// failing mock runtime and NoDocker=true. It also registers a
// t.Cleanup hook that tears down every proxy the service has
// spawned, guaranteeing that the 8200-8299 port pool is fully
// released between tests (including on test failure) to prevent
// cross-test bind collisions.
//
// A nil runtime argument skips SetRuntime entirely — used by
// TestNoDockerWithNilRuntimeSucceeds to cover the boot-time default
// path where main.go has not wired a runtime.
func newNoDockerTestService(t *testing.T, runtime orchestrator.ContainerRuntime) *Service {
	t.Helper()
	svc := newServiceWithCleanup(t)
	svc.SetNoDocker(true)
	if runtime != nil {
		svc.SetRuntime(runtime)
	}
	return svc
}

// TestNoDockerModeSkipsContainerRuntime is the canonical Rule 4
// assertion: with a failing runtime attached, setting NoDocker=true
// MUST cause CreateService to return a valid non-empty URI without
// touching any ContainerRuntime method. GetService must return the
// same URI. DeleteService must succeed without invoking any runtime
// method and must leave the service NotFound in subsequent lookups.
//
// The test exercises the full CRUD lifecycle (Create + Get + Delete
// + Get NotFound) in a single function so that any Rule 4 violation
// at any lifecycle stage is caught.
func TestNoDockerModeSkipsContainerRuntime(t *testing.T) {
	svc := newNoDockerTestService(t, &failingMockRuntime{t: t})

	ctx := context.Background()
	parent := "projects/test/locations/us-central1"

	// CreateService must succeed without invoking any ContainerRuntime method.
	op, err := svc.CreateService(ctx, &runpb.CreateServiceRequest{
		Parent:    parent,
		ServiceId: "nodocker-svc",
		Service: &runpb.Service{
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{
					Image: "gcr.io/test/myimage:latest",
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("expected completed operation")
	}

	var created runpb.Service
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if created.GetUri() == "" {
		t.Fatal("expected non-empty URI even in NoDocker mode (Rule 4)")
	}
	if created.GetName() == "" {
		t.Fatal("expected non-empty service name after CreateService")
	}

	// GetService must also return the stub URI (verifies persistence
	// in the store, not per-call synthesis).
	fetched, err := svc.GetService(ctx, &runpb.GetServiceRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if fetched.GetUri() == "" {
		t.Fatal("expected non-empty URI from GetService in NoDocker mode")
	}
	if fetched.GetUri() != created.GetUri() {
		t.Fatalf("GetService URI mismatch: got %q, want %q", fetched.GetUri(), created.GetUri())
	}

	// DeleteService must succeed without invoking any ContainerRuntime method.
	delOp, err := svc.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if !delOp.GetDone() {
		t.Fatal("expected completed delete operation")
	}

	// Verify the service is gone (NotFound).
	if _, err := svc.GetService(ctx, &runpb.GetServiceRequest{Name: created.GetName()}); err == nil {
		t.Fatal("expected NotFound after DeleteService, got nil error")
	}
}

// TestNoDockerWithNilRuntimeSucceeds verifies that setting
// NoDocker=true without any runtime injected still produces a valid
// URI. This covers the boot-time default path where main.go has not
// wired a runtime.
//
// Because Rule 4 is unconditional, the service must behave
// identically whether the runtime is nil or a failing mock is
// injected: both paths must skip all container operations and still
// return a non-empty URI from CreateService.
func TestNoDockerWithNilRuntimeSucceeds(t *testing.T) {
	// Intentionally pass nil runtime to newNoDockerTestService (which
	// skips SetRuntime in that case).
	svc := newNoDockerTestService(t, nil)

	ctx := context.Background()
	parent := "projects/test/locations/us-central1"

	op, err := svc.CreateService(ctx, &runpb.CreateServiceRequest{
		Parent:    parent,
		ServiceId: "nodocker-nil-runtime",
		Service:   &runpb.Service{},
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("expected completed operation")
	}

	var created runpb.Service
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if created.GetUri() == "" {
		t.Fatal("expected non-empty URI in NoDocker mode with nil runtime")
	}
}

// TestNoDockerDeleteServiceSkipsStopAndRemove is a supplementary
// test that targets the DeleteService code path specifically. While
// TestNoDockerModeSkipsContainerRuntime already exercises
// DeleteService as part of the full CRUD lifecycle, this dedicated
// test makes the Rule 4 DeleteService contract obvious and serves
// as a narrower canary for any future refactor that adds a Stop or
// Remove call to the tear-down path.
func TestNoDockerDeleteServiceSkipsStopAndRemove(t *testing.T) {
	svc := newNoDockerTestService(t, &failingMockRuntime{t: t})

	ctx := context.Background()
	parent := "projects/test/locations/us-central1"

	op, err := svc.CreateService(ctx, &runpb.CreateServiceRequest{
		Parent:    parent,
		ServiceId: "nodocker-delete-me",
		Service: &runpb.Service{
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{
					Image: "gcr.io/test/myimage:latest",
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	var created runpb.Service
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Delete -- must not touch the runtime (no Stop, no Remove).
	if _, err := svc.DeleteService(ctx, &runpb.DeleteServiceRequest{
		Name: created.GetName(),
	}); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
}
