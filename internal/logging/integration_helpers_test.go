//go:build integration

// Package logging — integration_helpers_test.go
//
// Shared helpers for the Logging emulator's integration test suite, scoped
// to the `integration` build tag per AAP §0.7.4 Gate 8. This file is
// `package logging` (internal), enabling the GCS sink integration test
// (`integration_gcs_sink_test.go`) — which is also `package logging` — to
// call these helpers without qualification.
//
// The companion Pub/Sub sink integration test
// (`integration_pubsub_sink_test.go`) is `package logging_test` (external)
// per its schema contract and defines its own startup helpers; it does not
// depend on this file.
//
// Helpers provided:
//
//   - startLoggingWithEndpoints: launches the Cloud Logging gRPC server on
//     an ephemeral localhost port with configured pubsubAddr and gcsAddr
//     loopback endpoints, probes readiness, and returns a pair of ready
//     service clients. Empty-string endpoints are a silent no-op per
//     Rule 7a.
//
//   - createSinkHelper: thin wrapper around ConfigServiceV2.CreateSink with
//     a bounded context and consistent failure messaging.
//
//   - writeEntryHelper: thin wrapper around LoggingServiceV2.WriteLogEntries
//     that constructs a deterministically-timestamped, single-entry request
//     (canonical Resource={Type:"global", Labels:{"project_id":"test"}},
//     timestamp fixed at unix 1700000000 UTC for stable object-name
//     derivation in the GCS sink path per internal/logging/sink_delivery.go).
//
//   - parseLogSeverity: maps canonical severity names (DEBUG, INFO, ...) to
//     the loggingtypepb.LogSeverity enum values accepted by LogEntry.

package logging

import (
	"context"
	"net"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	loggingtypepb "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// startLoggingWithEndpoints launches the Cloud Logging gRPC service on an
// ephemeral localhost port and wires it to the provided pubsubAddr and
// gcsAddr loopback endpoints via the exported SetPubsubEndpoint /
// SetGcsEndpoint setters BEFORE calling Start.
//
// Either address may be the empty string to exercise the Rule 7a
// silent-no-op contract: the corresponding sink-delivery branch will never
// dial downstream when its endpoint is empty.
//
// The service is shut down automatically when the test ends via
// t.Cleanup(cancel) of the context wrapping Start().
//
// The readiness probe dials gRPC and issues ListLogs on a project with no
// logs; a nil error confirms the server has completed Start() and is
// serving RPCs. Up to 50 probe attempts are made at 20ms intervals (max
// ~1s wall time), which is generous for a local listener.
//
// Returns a live LoggingServiceV2 client (for WriteLogEntries, ListLogs,
// DeleteLog) and a live ConfigServiceV2 client (for CRUD on sinks),
// sharing a single underlying *grpc.ClientConn that is Close'd via
// t.Cleanup. Callers MUST NOT close these clients themselves.
func startLoggingWithEndpoints(t *testing.T, pubsubAddr, gcsAddr string) (loggingpb.LoggingServiceV2Client, loggingpb.ConfigServiceV2Client) {
	t.Helper()

	svc := New("", true)
	svc.SetPubsubEndpoint(pubsubAddr)
	svc.SetGcsEndpoint(gcsAddr)

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("logging listen: %v", err)
	}
	addr := ln.Addr().String()
	// Close the listener so svc.Start can rebind the same address. There is
	// a tiny race window here — tolerated by the existing test suite.
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	started := make(chan struct{})
	go func() {
		close(started)
		_ = svc.Start(ctx, addr)
	}()
	<-started

	var conn *grpc.ClientConn
	for i := 0; i < 50; i++ {
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			probeCtx, probeCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			logClient := loggingpb.NewLoggingServiceV2Client(conn)
			_, rpcErr := logClient.ListLogs(probeCtx, &loggingpb.ListLogsRequest{Parent: "projects/probe"})
			probeCancel()
			if rpcErr == nil {
				break
			}
			conn.Close()
			conn = nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("integration: failed to connect to logging server")
	}
	t.Cleanup(func() { _ = conn.Close() })

	return loggingpb.NewLoggingServiceV2Client(conn), loggingpb.NewConfigServiceV2Client(conn)
}

// createSinkHelper invokes ConfigServiceV2.CreateSink with the provided
// parent project, sink name, destination URI, and filter. On success it
// returns the server's echo of the created sink; on failure it calls
// t.Fatalf with a diagnostic message that includes all inputs.
//
// The context is bounded at 2 seconds which is comfortably larger than
// any plausible local gRPC round-trip and small enough to surface
// server-side hangs quickly.
func createSinkHelper(t *testing.T, cfg loggingpb.ConfigServiceV2Client, parent, name, destination, filter string) *loggingpb.LogSink {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := cfg.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink: &loggingpb.LogSink{
			Name:        name,
			Destination: destination,
			Filter:      filter,
		},
	})
	if err != nil {
		t.Fatalf("CreateSink(%q -> %q filter=%q): %v", name, destination, filter, err)
	}
	return resp
}

// writeEntryHelper invokes LoggingServiceV2.WriteLogEntries with a single
// LogEntry built from the given logName, severity name, text payload, and
// insert ID. All entries share a canonical, deterministic resource shape
// (Type="global", Labels={"project_id":"test"}) and a fixed timestamp at
// unix epoch 1700000000 UTC so that downstream GCS sink object names are
// reproducible across runs.
//
// The severity is parsed via parseLogSeverity; unknown names map to
// LogSeverity_DEFAULT. Empty strings for payload or insertID are legal
// and passed through.
//
// The context is bounded at 2 seconds. On success the constructed entry
// is returned so tests can use it for later assertions (e.g., deriving
// expected GCS object names from entry.Timestamp + entry.InsertId).
func writeEntryHelper(t *testing.T, log loggingpb.LoggingServiceV2Client, logName, severity, textPayload, insertID string) *loggingpb.LogEntry {
	t.Helper()

	entry := &loggingpb.LogEntry{
		LogName: logName,
		Resource: &monitoredrespb.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": "test"},
		},
		Severity:  parseLogSeverity(severity),
		InsertId:  insertID,
		Timestamp: timestamppb.New(time.Unix(1700000000, 0).UTC()),
		Payload:   &loggingpb.LogEntry_TextPayload{TextPayload: textPayload},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := log.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{entry},
	}); err != nil {
		t.Fatalf("WriteLogEntries(%q, %q, %q): %v", logName, severity, textPayload, err)
	}
	return entry
}

// parseLogSeverity converts a canonical severity-level name (as defined
// by the Cloud Logging API) to the corresponding loggingtypepb.LogSeverity
// enum value. Unknown names map to LogSeverity_DEFAULT (the zero value),
// matching the Cloud Logging API's tolerant behavior for unspecified
// severity.
//
// Accepted names: DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT,
// EMERGENCY. Names are case-sensitive; callers pass the canonical
// uppercase spelling.
func parseLogSeverity(name string) loggingtypepb.LogSeverity {
	switch name {
	case "DEBUG":
		return loggingtypepb.LogSeverity_DEBUG
	case "INFO":
		return loggingtypepb.LogSeverity_INFO
	case "NOTICE":
		return loggingtypepb.LogSeverity_NOTICE
	case "WARNING":
		return loggingtypepb.LogSeverity_WARNING
	case "ERROR":
		return loggingtypepb.LogSeverity_ERROR
	case "CRITICAL":
		return loggingtypepb.LogSeverity_CRITICAL
	case "ALERT":
		return loggingtypepb.LogSeverity_ALERT
	case "EMERGENCY":
		return loggingtypepb.LogSeverity_EMERGENCY
	default:
		return loggingtypepb.LogSeverity_DEFAULT
	}
}
