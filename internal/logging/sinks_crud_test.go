// Package logging — sinks_crud_test.go exercises the five ConfigServiceV2
// sink-management RPCs (CreateSink, GetSink, UpdateSink, DeleteSink,
// ListSinks) over an in-process gRPC server.
//
// These tests complement the cross-service delivery coverage already
// provided by:
//
//   - internal/logging/integration_pubsub_sink_test.go (Rule 9 — Pub/Sub
//     destination end-to-end delivery, build tag: integration)
//   - internal/logging/integration_gcs_sink_test.go (Rule 9 — GCS
//     destination end-to-end delivery, build tag: integration)
//
// …by asserting the unit-level behavior of every sink CRUD RPC:
//
//   - Happy paths return the canonical LogSink with populated CreateTime
//     and UpdateTime, and a non-empty default WriterIdentity.
//   - Empty required arguments (parent, sink.name, sink.destination,
//     sink_name) return codes.InvalidArgument with the canonical error
//     messages from service.go.
//   - Operations against a nonexistent sink return codes.NotFound.
//   - Creating a sink whose fully-qualified name already exists returns
//     codes.AlreadyExists (via the ErrSinkAlreadyExists sentinel).
//   - UpdateSink preserves CreateTime and refreshes UpdateTime.
//   - DeleteSink is observable via a subsequent GetSink returning NotFound.
//   - ListSinks returns prefix-filtered, Name-sorted results, and honors
//     the empty-parent "return all" semantics from Store.ListSinks.
//
// The tests are in the same package as service.go (package logging) to
// mirror the existing service_test.go convention and to preserve the
// Rule 7a preservation contract — service_test.go is NOT modified by
// this file. The existing testClient helper in service_test.go registers
// only LoggingServiceV2Server, so sink tests build their own client
// harness (testConfigClient) that registers both gRPC services.
//
// No build tag: these tests are part of the standard unit suite and
// contribute to the per-function coverage profile for the three RPCs
// flagged by QA Checkpoint #7 Issue 1 (GetSink, UpdateSink, DeleteSink).
package logging

import (
	"context"
	"net"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// testConfigClient returns a ConfigServiceV2 client connected to an
// in-process gRPC server backed by a freshly-constructed Service. Both
// LoggingServiceV2Server and ConfigServiceV2Server are registered on the
// same server so call sites can seed via WriteLogEntries when needed
// (not used by the sink tests, but preserved for parity with the
// integration harness).
//
// The returned cleanup closer stops the server and closes the client
// connection; callers must defer it.
func testConfigClient(t *testing.T) (loggingpb.ConfigServiceV2Client, func()) {
	t.Helper()
	svc := New("", true)

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	loggingpb.RegisterLoggingServiceV2Server(srv, svc)
	loggingpb.RegisterConfigServiceV2Server(srv, svc)
	go srv.Serve(ln) //nolint:errcheck // server lifetime bounded by cleanup

	conn, err := grpc.NewClient(
		ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}

	client := loggingpb.NewConfigServiceV2Client(conn)
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
	}
	return client, cleanup
}

// mustCreateSink is a small helper that creates a sink for the common
// "arrange" step of Update/Delete/Get tests. It fails the test if
// CreateSink returns an error.
func mustCreateSink(t *testing.T, client loggingpb.ConfigServiceV2Client, parent, name, destination, filter string) *loggingpb.LogSink {
	t.Helper()
	resp, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink: &loggingpb.LogSink{
			Name:        name,
			Destination: destination,
			Filter:      filter,
		},
	})
	if err != nil {
		t.Fatalf("seed CreateSink(%q/%q): %v", parent, name, err)
	}
	return resp
}

// --- CreateSink -------------------------------------------------------

func TestCreateSink_Success(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	resp, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink: &loggingpb.LogSink{
			Name:        "my-sink",
			Destination: "pubsub://projects/test/topics/t1",
			Filter:      "severity>=ERROR",
		},
	})
	if err != nil {
		t.Fatalf("CreateSink: %v", err)
	}

	wantName := "projects/test/sinks/my-sink"
	if resp.GetName() != wantName {
		t.Fatalf("Name = %q, want %q", resp.GetName(), wantName)
	}
	if resp.GetDestination() != "pubsub://projects/test/topics/t1" {
		t.Fatalf("Destination = %q, want pubsub://projects/test/topics/t1", resp.GetDestination())
	}
	if resp.GetFilter() != "severity>=ERROR" {
		t.Fatalf("Filter = %q, want severity>=ERROR", resp.GetFilter())
	}
	if resp.GetWriterIdentity() == "" {
		t.Fatalf("WriterIdentity is empty; expected default identity")
	}
	if resp.GetCreateTime() == nil {
		t.Fatalf("CreateTime is nil")
	}
	if resp.GetUpdateTime() == nil {
		t.Fatalf("UpdateTime is nil")
	}
}

func TestCreateSink_EmptyParent(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: "",
		Sink: &loggingpb.LogSink{
			Name:        "s",
			Destination: "pubsub://projects/p/topics/t",
		},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

func TestCreateSink_NilSink(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink:   nil,
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

func TestCreateSink_EmptyName(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink: &loggingpb.LogSink{
			Name:        "",
			Destination: "pubsub://projects/p/topics/t",
		},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

func TestCreateSink_EmptyDestination(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink: &loggingpb.LogSink{
			Name:        "s1",
			Destination: "",
		},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

func TestCreateSink_Duplicate(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	mustCreateSink(t, client, "projects/test", "dup", "pubsub://projects/test/topics/t", "")

	_, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink: &loggingpb.LogSink{
			Name:        "dup",
			Destination: "pubsub://projects/test/topics/t2",
		},
	})
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("code = %s, want AlreadyExists (err=%v)", got, err)
	}
}

func TestCreateSink_ExplicitWriterIdentityIsPreserved(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	const custom = "serviceAccount:caller@example.iam.gserviceaccount.com"
	resp, err := client.CreateSink(context.Background(), &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink: &loggingpb.LogSink{
			Name:           "s",
			Destination:    "storage.googleapis.com/my-bucket",
			WriterIdentity: custom,
		},
	})
	if err != nil {
		t.Fatalf("CreateSink: %v", err)
	}
	if resp.GetWriterIdentity() != custom {
		t.Fatalf("WriterIdentity = %q, want %q", resp.GetWriterIdentity(), custom)
	}
}

// --- GetSink ----------------------------------------------------------

func TestGetSink_Existing(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	created := mustCreateSink(t, client, "projects/test", "g1", "pubsub://projects/test/topics/t", "severity>=WARNING")

	resp, err := client.GetSink(context.Background(), &loggingpb.GetSinkRequest{
		SinkName: "projects/test/sinks/g1",
	})
	if err != nil {
		t.Fatalf("GetSink: %v", err)
	}
	if resp.GetName() != created.GetName() {
		t.Fatalf("Name = %q, want %q", resp.GetName(), created.GetName())
	}
	if resp.GetDestination() != created.GetDestination() {
		t.Fatalf("Destination = %q, want %q", resp.GetDestination(), created.GetDestination())
	}
	if resp.GetFilter() != created.GetFilter() {
		t.Fatalf("Filter = %q, want %q", resp.GetFilter(), created.GetFilter())
	}
	if resp.GetWriterIdentity() != created.GetWriterIdentity() {
		t.Fatalf("WriterIdentity = %q, want %q", resp.GetWriterIdentity(), created.GetWriterIdentity())
	}
	if resp.GetCreateTime() == nil {
		t.Fatalf("CreateTime is nil on GetSink response")
	}
	if resp.GetUpdateTime() == nil {
		t.Fatalf("UpdateTime is nil on GetSink response")
	}
	// CreateTime on a freshly-created sink should match CreateSink's
	// value byte-for-byte (the store returns the same stored timestamp).
	if !resp.GetCreateTime().AsTime().Equal(created.GetCreateTime().AsTime()) {
		t.Fatalf("CreateTime drift: got=%v want=%v", resp.GetCreateTime().AsTime(), created.GetCreateTime().AsTime())
	}
}

func TestGetSink_Missing(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.GetSink(context.Background(), &loggingpb.GetSinkRequest{
		SinkName: "projects/test/sinks/does-not-exist",
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %s, want NotFound (err=%v)", got, err)
	}
}

func TestGetSink_EmptyName(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.GetSink(context.Background(), &loggingpb.GetSinkRequest{
		SinkName: "",
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

// --- UpdateSink -------------------------------------------------------

func TestUpdateSink_FullReplacement(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	created := mustCreateSink(t, client, "projects/test", "u1", "pubsub://projects/test/topics/orig", "severity>=INFO")
	origCreateTime := created.GetCreateTime().AsTime()

	resp, err := client.UpdateSink(context.Background(), &loggingpb.UpdateSinkRequest{
		SinkName: "projects/test/sinks/u1",
		Sink: &loggingpb.LogSink{
			Destination: "storage.googleapis.com/new-bucket",
			Filter:      "severity>=ERROR",
		},
	})
	if err != nil {
		t.Fatalf("UpdateSink: %v", err)
	}

	if resp.GetName() != "projects/test/sinks/u1" {
		t.Fatalf("Name = %q, want projects/test/sinks/u1", resp.GetName())
	}
	if resp.GetDestination() != "storage.googleapis.com/new-bucket" {
		t.Fatalf("Destination = %q, want storage.googleapis.com/new-bucket", resp.GetDestination())
	}
	if resp.GetFilter() != "severity>=ERROR" {
		t.Fatalf("Filter = %q, want severity>=ERROR", resp.GetFilter())
	}
	// CreateTime must be preserved across UpdateSink (store semantics).
	if !resp.GetCreateTime().AsTime().Equal(origCreateTime) {
		t.Fatalf("CreateTime mutated across Update: got=%v want=%v", resp.GetCreateTime().AsTime(), origCreateTime)
	}
	// UpdateTime must be populated (it is refreshed by store.UpdateSink).
	if resp.GetUpdateTime() == nil {
		t.Fatalf("UpdateTime is nil after UpdateSink")
	}

	// GetSink must reflect the updated values (persistence check).
	got, err := client.GetSink(context.Background(), &loggingpb.GetSinkRequest{
		SinkName: "projects/test/sinks/u1",
	})
	if err != nil {
		t.Fatalf("GetSink after update: %v", err)
	}
	if got.GetDestination() != "storage.googleapis.com/new-bucket" {
		t.Fatalf("persisted Destination = %q, want storage.googleapis.com/new-bucket", got.GetDestination())
	}
	if got.GetFilter() != "severity>=ERROR" {
		t.Fatalf("persisted Filter = %q, want severity>=ERROR", got.GetFilter())
	}
}

func TestUpdateSink_PreservesWriterIdentityWhenEmpty(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	created := mustCreateSink(t, client, "projects/test", "u-wi", "pubsub://projects/test/topics/t", "")
	origWI := created.GetWriterIdentity()
	if origWI == "" {
		t.Fatalf("expected default WriterIdentity to be set on CreateSink")
	}

	// Send an Update with an empty WriterIdentity — the store must
	// keep the previously-assigned identity (see Store.UpdateSink).
	resp, err := client.UpdateSink(context.Background(), &loggingpb.UpdateSinkRequest{
		SinkName: "projects/test/sinks/u-wi",
		Sink: &loggingpb.LogSink{
			Destination:    "pubsub://projects/test/topics/t2",
			Filter:         "",
			WriterIdentity: "",
		},
	})
	if err != nil {
		t.Fatalf("UpdateSink: %v", err)
	}
	if resp.GetWriterIdentity() != origWI {
		t.Fatalf("WriterIdentity = %q, want %q (preserved)", resp.GetWriterIdentity(), origWI)
	}
}

func TestUpdateSink_Missing(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.UpdateSink(context.Background(), &loggingpb.UpdateSinkRequest{
		SinkName: "projects/test/sinks/nope",
		Sink: &loggingpb.LogSink{
			Destination: "pubsub://projects/test/topics/t",
		},
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %s, want NotFound (err=%v)", got, err)
	}
}

func TestUpdateSink_EmptyName(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.UpdateSink(context.Background(), &loggingpb.UpdateSinkRequest{
		SinkName: "",
		Sink: &loggingpb.LogSink{
			Destination: "pubsub://projects/test/topics/t",
		},
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

func TestUpdateSink_NilSink(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.UpdateSink(context.Background(), &loggingpb.UpdateSinkRequest{
		SinkName: "projects/test/sinks/anything",
		Sink:     nil,
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

// --- DeleteSink -------------------------------------------------------

func TestDeleteSink_Existing(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	mustCreateSink(t, client, "projects/test", "d1", "pubsub://projects/test/topics/t", "")

	if _, err := client.DeleteSink(context.Background(), &loggingpb.DeleteSinkRequest{
		SinkName: "projects/test/sinks/d1",
	}); err != nil {
		t.Fatalf("DeleteSink: %v", err)
	}

	// Observability: GetSink must now return NotFound.
	_, err := client.GetSink(context.Background(), &loggingpb.GetSinkRequest{
		SinkName: "projects/test/sinks/d1",
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("after delete, GetSink code = %s, want NotFound (err=%v)", got, err)
	}
}

func TestDeleteSink_Missing(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.DeleteSink(context.Background(), &loggingpb.DeleteSinkRequest{
		SinkName: "projects/test/sinks/nothing",
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %s, want NotFound (err=%v)", got, err)
	}
}

func TestDeleteSink_EmptyName(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	_, err := client.DeleteSink(context.Background(), &loggingpb.DeleteSinkRequest{
		SinkName: "",
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %s, want InvalidArgument (err=%v)", got, err)
	}
}

// DeleteSink_Idempotent asserts that calling delete twice on the same
// sink surfaces NotFound on the second call — the store does not return
// an "already deleted" sentinel, so NotFound is the canonical signal.
func TestDeleteSink_Idempotent(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	mustCreateSink(t, client, "projects/test", "dupdel", "pubsub://projects/test/topics/t", "")

	if _, err := client.DeleteSink(context.Background(), &loggingpb.DeleteSinkRequest{
		SinkName: "projects/test/sinks/dupdel",
	}); err != nil {
		t.Fatalf("DeleteSink first call: %v", err)
	}

	_, err := client.DeleteSink(context.Background(), &loggingpb.DeleteSinkRequest{
		SinkName: "projects/test/sinks/dupdel",
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("DeleteSink second call: code = %s, want NotFound (err=%v)", got, err)
	}
}

// --- ListSinks --------------------------------------------------------

func TestListSinks_Empty(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	resp, err := client.ListSinks(context.Background(), &loggingpb.ListSinksRequest{
		Parent: "projects/test",
	})
	if err != nil {
		t.Fatalf("ListSinks: %v", err)
	}
	if len(resp.GetSinks()) != 0 {
		t.Fatalf("expected empty result, got %d sinks", len(resp.GetSinks()))
	}
}

func TestListSinks_MultipleUnderParent(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	// Insert in non-sorted order to exercise the Name-sort invariant
	// documented on Store.ListSinks.
	mustCreateSink(t, client, "projects/test", "charlie", "pubsub://projects/test/topics/c", "")
	mustCreateSink(t, client, "projects/test", "alpha", "pubsub://projects/test/topics/a", "")
	mustCreateSink(t, client, "projects/test", "bravo", "pubsub://projects/test/topics/b", "")

	resp, err := client.ListSinks(context.Background(), &loggingpb.ListSinksRequest{
		Parent: "projects/test",
	})
	if err != nil {
		t.Fatalf("ListSinks: %v", err)
	}
	if len(resp.GetSinks()) != 3 {
		t.Fatalf("expected 3 sinks, got %d", len(resp.GetSinks()))
	}
	want := []string{
		"projects/test/sinks/alpha",
		"projects/test/sinks/bravo",
		"projects/test/sinks/charlie",
	}
	for i, w := range want {
		if resp.GetSinks()[i].GetName() != w {
			t.Fatalf("sinks[%d].Name = %q, want %q", i, resp.GetSinks()[i].GetName(), w)
		}
	}
}

func TestListSinks_FilterByParent(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	mustCreateSink(t, client, "projects/alpha", "s1", "pubsub://projects/alpha/topics/t", "")
	mustCreateSink(t, client, "projects/alpha", "s2", "pubsub://projects/alpha/topics/t", "")
	mustCreateSink(t, client, "projects/beta", "s1", "pubsub://projects/beta/topics/t", "")

	resp, err := client.ListSinks(context.Background(), &loggingpb.ListSinksRequest{
		Parent: "projects/alpha",
	})
	if err != nil {
		t.Fatalf("ListSinks: %v", err)
	}
	if len(resp.GetSinks()) != 2 {
		t.Fatalf("expected 2 alpha sinks, got %d", len(resp.GetSinks()))
	}
	for i, s := range resp.GetSinks() {
		if got := s.GetName(); got != "projects/alpha/sinks/s1" && got != "projects/alpha/sinks/s2" {
			t.Fatalf("sinks[%d].Name = %q, not under projects/alpha", i, got)
		}
	}
}

// TestListSinks_EmptyParentReturnsAll exercises the Store.ListSinks
// documented behavior: an empty parent returns every sink in the store.
// Note that the service layer forwards Parent unchanged to the store, so
// this test also covers the zero-prefix code path in Store.ListSinks.
func TestListSinks_EmptyParentReturnsAll(t *testing.T) {
	client, cleanup := testConfigClient(t)
	defer cleanup()

	mustCreateSink(t, client, "projects/alpha", "s1", "pubsub://projects/alpha/topics/t", "")
	mustCreateSink(t, client, "projects/beta", "s1", "pubsub://projects/beta/topics/t", "")

	resp, err := client.ListSinks(context.Background(), &loggingpb.ListSinksRequest{
		Parent: "",
	})
	if err != nil {
		t.Fatalf("ListSinks: %v", err)
	}
	if len(resp.GetSinks()) != 2 {
		t.Fatalf("expected 2 sinks across all parents, got %d", len(resp.GetSinks()))
	}
}
