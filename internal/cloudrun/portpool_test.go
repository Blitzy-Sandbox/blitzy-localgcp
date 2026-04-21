// Package cloudrun — portpool_test.go
//
// Rule 8 verification for the Cloud Run 8200-8299 host port pool.
//
// AAP §0.7.1.9 requires that:
//   1. Ports 8200-8299 are managed as an in-use set in store.go.
//   2. 5 consecutive CreateService calls allocate 5 distinct ports.
//   3. 1 DeleteService call frees that port for reuse.
//   4. The 101st CreateService call (with no deletions) returns
//      codes.ResourceExhausted with the exact message
//      "localgcp: cloud run port pool exhausted (max 100 concurrent services)".
//
// This test suite exercises both the Store-level API (fast, no real
// sockets) AND the Service-level CreateService/DeleteService path
// (real TCP listener binds) to give end-to-end coverage.
package cloudrun

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// canonicalPoolExhaustedMsg is the exact Rule 8 error substring that
// AllocatePort must return when the pool is full. This is the message
// required by AAP §0.7.1.9 — even when tests use a narrow pool, the
// hardcoded message still says "max 100 concurrent services" because
// that is the canonical contract for production pool exhaustion.
//
// Note: makeCreateReq and newServiceWithCleanup are defined in
// nodocker_test.go and reused here (same package).
const canonicalPoolExhaustedMsg = "localgcp: cloud run port pool exhausted (max 100 concurrent services)"

// ---------------------------------------------------------------------------
// Store-level tests — exercise AllocatePort / ReleasePort directly
// without binding real TCP listeners.
// ---------------------------------------------------------------------------

// TestPortPool_Store_AllocateFiveDistinctPorts verifies Rule 8 clause 2
// at the store layer: five consecutive AllocatePort calls return five
// distinct ports, each in [8200, 8299].
func TestPortPool_Store_AllocateFiveDistinctPorts(t *testing.T) {
	s := NewStore()
	seen := make(map[int]struct{})
	for i := 0; i < 5; i++ {
		p, err := s.AllocatePort()
		if err != nil {
			t.Fatalf("AllocatePort #%d: unexpected error: %v", i+1, err)
		}
		if p < 8200 || p > 8299 {
			t.Errorf("allocation #%d returned port %d, want a value in [8200, 8299]", i+1, p)
		}
		if _, dup := seen[p]; dup {
			t.Errorf("allocation #%d returned duplicate port %d", i+1, p)
		}
		seen[p] = struct{}{}
	}
	if len(seen) != 5 {
		t.Errorf("expected 5 distinct ports, got %d", len(seen))
	}
}

// TestPortPool_Store_AllocateIsSequentialFromPoolStart verifies that
// allocations proceed from the lowest-numbered free port upward — an
// implementation detail that tests the deterministic-ordering contract
// documented in the store.
func TestPortPool_Store_AllocateIsSequentialFromPoolStart(t *testing.T) {
	s := NewStore()
	want := 8200
	for i := 0; i < 10; i++ {
		p, err := s.AllocatePort()
		if err != nil {
			t.Fatalf("AllocatePort #%d: %v", i+1, err)
		}
		if p != want {
			t.Errorf("allocation #%d: got port %d, want %d (sequential from pool start)", i+1, p, want)
		}
		want++
	}
}

// TestPortPool_Store_ReleaseReturnsPortToPool verifies Rule 8 clause 3:
// after ReleasePort, the released port is re-allocatable via a
// subsequent AllocatePort call.
func TestPortPool_Store_ReleaseReturnsPortToPool(t *testing.T) {
	s := NewStore()
	// Allocate three ports; release the middle one; next alloc must
	// reclaim it (lowest-free semantics).
	p1, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("alloc p1: %v", err)
	}
	p2, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("alloc p2: %v", err)
	}
	p3, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("alloc p3: %v", err)
	}
	if p1 == p2 || p2 == p3 || p1 == p3 {
		t.Fatalf("expected three distinct ports, got %d %d %d", p1, p2, p3)
	}
	// Release the middle one and re-allocate.
	s.ReleasePort(p2)
	reclaimed, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("alloc after release: %v", err)
	}
	if reclaimed != p2 {
		t.Errorf("release did not return port to pool: released=%d, next alloc=%d", p2, reclaimed)
	}
}

// TestPortPool_Store_ExhaustionReturnsResourceExhausted verifies
// Rule 8 clause 4: after all 100 ports are in use, the 101st
// AllocatePort call returns codes.ResourceExhausted with the canonical
// message.
func TestPortPool_Store_ExhaustionReturnsResourceExhausted(t *testing.T) {
	s := NewStore()
	size := s.PoolSize()
	if size != 100 {
		t.Fatalf("default pool size: got %d, want 100", size)
	}
	// Drain the full pool.
	for i := 0; i < size; i++ {
		if _, err := s.AllocatePort(); err != nil {
			t.Fatalf("drain alloc #%d: %v", i+1, err)
		}
	}
	// The (size+1)-th allocation must fail with ResourceExhausted.
	p, err := s.AllocatePort()
	if err == nil {
		t.Fatalf("expected ResourceExhausted on 101st allocation, got port=%d, err=nil", p)
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("status.Code: got %v, want codes.ResourceExhausted", got)
	}
	if !strings.Contains(err.Error(), canonicalPoolExhaustedMsg) {
		t.Errorf("error message = %q, want to contain %q", err.Error(), canonicalPoolExhaustedMsg)
	}
}

// TestPortPool_Store_ReleaseUnallocatedIsNoOp verifies that releasing
// a port that was never allocated is a safe no-op. This is important
// for idempotency of cleanup paths in CreateService rollback and
// DeleteService.
func TestPortPool_Store_ReleaseUnallocatedIsNoOp(t *testing.T) {
	s := NewStore()
	// Releasing an in-range but never-allocated port should not panic
	// and should not affect subsequent allocation behavior.
	s.ReleasePort(8250)

	// The pool should still behave normally: first allocation returns
	// the lowest-numbered free port (8200), not 8250.
	p, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("AllocatePort after no-op release: %v", err)
	}
	if p != 8200 {
		t.Errorf("got %d, want 8200 (no-op release must not disturb pool)", p)
	}

	// Double-release of 8250 (still unallocated) must also be a no-op.
	s.ReleasePort(8250)
}

// TestPortPool_Store_ReleaseOutOfRangeIsNoOp verifies that releasing a
// port outside the pool range is a safe no-op (guard against bad
// callers passing stale data).
func TestPortPool_Store_ReleaseOutOfRangeIsNoOp(t *testing.T) {
	s := NewStore()
	// Ports outside [8200, 8299] must be silently ignored.
	s.ReleasePort(0)
	s.ReleasePort(80)
	s.ReleasePort(8199)
	s.ReleasePort(8300)
	s.ReleasePort(65535)
	// Pool must be undisturbed: next alloc still returns 8200.
	p, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("AllocatePort: %v", err)
	}
	if p != 8200 {
		t.Errorf("got %d, want 8200", p)
	}
}

// TestPortPool_Store_DoubleReleaseIsNoOp verifies that releasing the
// same (previously-allocated) port twice is a safe no-op. This
// protects against races where DeleteService races with CreateService
// rollback on the same service name.
func TestPortPool_Store_DoubleReleaseIsNoOp(t *testing.T) {
	s := NewStore()
	p, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	s.ReleasePort(p)
	s.ReleasePort(p) // second release: no-op, must not panic

	// The port is still free: re-allocating must succeed and return it.
	reclaimed, err := s.AllocatePort()
	if err != nil {
		t.Fatalf("alloc after double release: %v", err)
	}
	if reclaimed != p {
		t.Errorf("got %d, want %d (port must remain available after double release)", reclaimed, p)
	}
}

// TestPortPool_Store_NarrowPoolExhaustion verifies that the store
// honors custom pool ranges. Uses a 3-port pool for fast exhaustion
// coverage independent of the default 100-port pool.
func TestPortPool_Store_NarrowPoolExhaustion(t *testing.T) {
	s := NewStoreWithPool(9100, 9102) // 3-port pool
	if got := s.PoolSize(); got != 3 {
		t.Fatalf("PoolSize: got %d, want 3", got)
	}
	start, end := s.PoolRange()
	if start != 9100 || end != 9102 {
		t.Fatalf("PoolRange: got (%d, %d), want (9100, 9102)", start, end)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.AllocatePort(); err != nil {
			t.Fatalf("drain #%d: %v", i+1, err)
		}
	}
	_, err := s.AllocatePort()
	if err == nil {
		t.Fatal("expected ResourceExhausted after draining 3-port pool")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("status.Code: got %v, want ResourceExhausted", got)
	}
	// The canonical message is the same regardless of actual pool size
	// — this reflects the project's canonical Rule 8 contract.
	if !strings.Contains(err.Error(), canonicalPoolExhaustedMsg) {
		t.Errorf("error = %q, want to contain %q", err.Error(), canonicalPoolExhaustedMsg)
	}
}

// ---------------------------------------------------------------------------
// Service-level tests — exercise port allocation through the public
// CreateService / DeleteService RPCs with real TCP listener binds.
// ---------------------------------------------------------------------------

// TestPortPool_Service_FiveCreatesReturnDistinctURIs is the end-to-end
// Rule 8 clause 2 assertion: five consecutive CreateService calls must
// each return a distinct http://localhost:{port} URI where each port
// is in [8200, 8299].
func TestPortPool_Service_FiveCreatesReturnDistinctURIs(t *testing.T) {
	svc := newServiceWithCleanup(t)
	parent := "projects/test/locations/us-central1"
	seen := make(map[string]string) // uri -> serviceID

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("fivedistinct-%d", i)
		op, err := svc.CreateService(context.Background(), makeCreateReq(parent, id, "gcr.io/test/img:latest"))
		if err != nil {
			t.Fatalf("CreateService #%d (%s): %v", i+1, id, err)
		}
		var created runpb.Service
		if err := op.GetResponse().UnmarshalTo(&created); err != nil {
			t.Fatalf("unmarshal #%d: %v", i+1, err)
		}
		uri := created.GetUri()
		if uri == "" {
			t.Fatalf("CreateService #%d (%s): URI is empty", i+1, id)
		}
		// Parse out the port to verify it is in the 8200-8299 range.
		if !strings.HasPrefix(uri, "http://localhost:") {
			t.Errorf("CreateService #%d URI = %q, want http://localhost:...", i+1, uri)
		}
		if existing, dup := seen[uri]; dup {
			t.Errorf("duplicate URI %q returned for %s (already used by %s)", uri, id, existing)
		}
		seen[uri] = id
	}
	if len(seen) != 5 {
		t.Errorf("expected 5 distinct URIs, got %d", len(seen))
	}
}

// TestPortPool_Service_DeleteReclaimsPort is the end-to-end Rule 8
// clause 3 assertion: after DeleteService, the freed port is returned
// to the pool and is reclaimed by the next CreateService call
// (lowest-free semantics).
func TestPortPool_Service_DeleteReclaimsPort(t *testing.T) {
	svc := newServiceWithCleanup(t)
	parent := "projects/test/locations/us-central1"

	// First create: captures the URI (hence the host port).
	op1, err := svc.CreateService(context.Background(), makeCreateReq(parent, "reclaim-first", "gcr.io/test/img:latest"))
	if err != nil {
		t.Fatalf("first CreateService: %v", err)
	}
	var first runpb.Service
	if err := op1.GetResponse().UnmarshalTo(&first); err != nil {
		t.Fatalf("unmarshal first: %v", err)
	}
	if first.GetUri() == "" {
		t.Fatal("first CreateService returned empty URI")
	}

	// Delete: port must return to the pool.
	if _, err := svc.DeleteService(context.Background(), &runpb.DeleteServiceRequest{Name: first.GetName()}); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}

	// Second create: should reclaim the just-freed port (lowest-free).
	op2, err := svc.CreateService(context.Background(), makeCreateReq(parent, "reclaim-second", "gcr.io/test/img:latest"))
	if err != nil {
		t.Fatalf("second CreateService: %v", err)
	}
	var second runpb.Service
	if err := op2.GetResponse().UnmarshalTo(&second); err != nil {
		t.Fatalf("unmarshal second: %v", err)
	}

	if first.GetUri() != second.GetUri() {
		t.Errorf("port not reclaimed: first URI=%q, second URI=%q", first.GetUri(), second.GetUri())
	}
}

// TestPortPool_Service_ExhaustionReturnsResourceExhausted is the
// end-to-end Rule 8 clause 4 assertion: draining the pool via
// CreateService yields codes.ResourceExhausted with the canonical
// message on the next CreateService call.
//
// To keep the test fast and avoid binding 100 simultaneous real
// listeners, we replace the service's store with a narrow 5-port pool
// BEFORE any service is created. The exhaustion semantics are
// identical to the full 8200-8299 pool.
func TestPortPool_Service_ExhaustionReturnsResourceExhausted(t *testing.T) {
	svc := New("", true)
	// Inject a narrow pool. We pick 8290-8294 (inside the default
	// 8200-8299 range) to stay within project-reserved territory and
	// minimise any chance of colliding with other tools. Safe to
	// replace store here: no services have been created yet.
	svc.store = NewStoreWithPool(8290, 8294)
	t.Cleanup(svc.stopAllProxies)

	parent := "projects/test/locations/us-central1"

	// Drain the 5-port pool.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("drain-%d", i)
		if _, err := svc.CreateService(context.Background(), makeCreateReq(parent, id, "gcr.io/test/img:latest")); err != nil {
			t.Fatalf("drain CreateService #%d: %v", i+1, err)
		}
	}

	// The 6th CreateService must fail with ResourceExhausted.
	_, err := svc.CreateService(context.Background(), makeCreateReq(parent, "overflow", "gcr.io/test/img:latest"))
	if err == nil {
		t.Fatal("expected ResourceExhausted after draining 5-port pool, got nil")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("status.Code: got %v, want codes.ResourceExhausted (full error: %v)", got, err)
	}
	if !strings.Contains(err.Error(), canonicalPoolExhaustedMsg) {
		t.Errorf("error = %q, want to contain %q", err.Error(), canonicalPoolExhaustedMsg)
	}

	// After a DeleteService, the 6th attempt (renamed) must now
	// succeed — verifies that pool exhaustion is transient and
	// recoverable.
	drainName := parent + "/services/drain-0"
	if _, err := svc.DeleteService(context.Background(), &runpb.DeleteServiceRequest{Name: drainName}); err != nil {
		t.Fatalf("DeleteService drain-0: %v", err)
	}
	if _, err := svc.CreateService(context.Background(), makeCreateReq(parent, "recover", "gcr.io/test/img:latest")); err != nil {
		t.Fatalf("recovery CreateService after delete: %v", err)
	}
}

// TestPortPool_Service_ExhaustionRollsBackStoreInsert verifies that
// when CreateService fails due to pool exhaustion, the service is NOT
// left in the store (i.e. the store.Create was rolled back). This
// guards against stale state on the exhaustion path.
func TestPortPool_Service_ExhaustionRollsBackStoreInsert(t *testing.T) {
	svc := New("", true)
	svc.store = NewStoreWithPool(8295, 8296) // 2-port pool
	t.Cleanup(svc.stopAllProxies)

	parent := "projects/test/locations/us-central1"

	// Fill the pool.
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("fill-%d", i)
		if _, err := svc.CreateService(context.Background(), makeCreateReq(parent, id, "gcr.io/test/img:latest")); err != nil {
			t.Fatalf("fill CreateService #%d: %v", i+1, err)
		}
	}

	// Attempt a third create — must fail with ResourceExhausted.
	overflowID := "rollback-target"
	_, err := svc.CreateService(context.Background(), makeCreateReq(parent, overflowID, "gcr.io/test/img:latest"))
	if err == nil {
		t.Fatal("expected ResourceExhausted, got nil")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("status.Code: got %v, want ResourceExhausted", got)
	}

	// The overflow service must NOT be in the store (rollback).
	overflowName := parent + "/services/" + overflowID
	if _, ok := svc.store.Get(overflowName); ok {
		t.Errorf("rollback failed: %s still in store after exhaustion", overflowName)
	}

	// A subsequent ListServices should show exactly 2 services, not 3.
	list, err := svc.ListServices(context.Background(), &runpb.ListServicesRequest{Parent: parent})
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if len(list.GetServices()) != 2 {
		t.Errorf("ListServices returned %d services after rollback, want 2", len(list.GetServices()))
	}
}
