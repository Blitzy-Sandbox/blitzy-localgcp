//go:build integration

// Package logging — integration_pubsub_sink_test.go
//
// End-to-end integration coverage for AAP §0.5.1.2 Extension D
// (Cloud Logging → Pub/Sub sink delivery), satisfying AAP Rule 9 which
// mandates a dedicated integration test for each cross-service wiring path.
//
// These tests start BOTH a live Cloud Logging gRPC service AND a live
// Pub/Sub gRPC service in the same process (the localgcp single-binary
// topology), wire the Logging service to the Pub/Sub service via
// SetPubsubEndpoint BEFORE starting the gRPC listener, create a sink whose
// Destination is `pubsub.googleapis.com/projects/{project}/topics/{topic}`,
// call WriteLogEntries, and assert that the correct message arrives on a
// Pub/Sub subscription with:
//
//   * Data        = protojson.Marshal(entry) — round-trippable back to
//                   *loggingpb.LogEntry via protojson.Unmarshal.
//   * Attributes  = {"logName", "severity", "sinkName"} — exactly the three
//                   canonical keys produced by publishEntryToPubsub in
//                   sink_delivery.go. No extra attributes, no missing keys.
//
// The fan-out is fire-and-forget per-(entry, sink) pair (Rule 3): the
// WriteLogEntries RPC returns BEFORE the per-sink goroutines dial Pub/Sub
// and publish. Tests therefore use a bounded retry loop (`pullUntilN`)
// rather than assuming immediate availability, mirroring the pattern
// already used by internal/gcs/integration_pubsub_test.go.
//
// Build tag: these tests are compiled only when `go test` is invoked with
// `-tags integration`, per AAP §0.7.4 Gate 8.

package logging

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/slokam-ai/localgcp/internal/pubsub"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	loggingtypepb "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// Integration topology helpers
// ---------------------------------------------------------------------------
//
// These helpers are file-scoped and named with the `Integration` suffix so
// they don't clash with helpers in service_test.go (which has a plain
// `testClient` helper that does NOT expose the ConfigServiceV2 surface
// nor thread pubsub/gcs endpoints through SetPubsubEndpoint / SetGcsEndpoint).

// startPubsubForLoggingIntegration launches an in-memory, quiet Pub/Sub
// service on an ephemeral localhost port. Returns the dialable address plus
// ready Publisher and Subscriber clients sharing a single gRPC connection.
//
// The readiness probe uses GetTopic on a nonexistent topic — a codes.NotFound
// response confirms the server is serving RPCs. This mirrors the canonical
// probe pattern used by internal/pubsub/service_test.go and by
// internal/gcs/integration_pubsub_test.go.
func startPubsubForLoggingIntegration(t *testing.T) (string, pubsubpb.PublisherClient, pubsubpb.SubscriberClient) {
	t.Helper()

	svc := pubsub.New("", true) // in-memory, quiet

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("pubsub listen: %v", err)
	}
	addr := ln.Addr().String()
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
			pub := pubsubpb.NewPublisherClient(conn)
			probeCtx, probeCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			_, rpcErr := pub.GetTopic(probeCtx, &pubsubpb.GetTopicRequest{Topic: "projects/probe/topics/probe"})
			probeCancel()
			if rpcErr != nil && status.Code(rpcErr) == codes.NotFound {
				break
			}
			if rpcErr == nil {
				break
			}
			conn.Close()
			conn = nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("integration: failed to connect to pubsub server")
	}
	t.Cleanup(func() { _ = conn.Close() })

	return addr, pubsubpb.NewPublisherClient(conn), pubsubpb.NewSubscriberClient(conn)
}

// startLoggingWithEndpoints launches an in-memory, quiet Cloud Logging gRPC
// service on an ephemeral localhost port with both the Pub/Sub loopback
// endpoint (pubsubAddr) and the GCS loopback endpoint (gcsAddr) configured
// via the SetPubsubEndpoint / SetGcsEndpoint setters BEFORE Start. This
// ordering is mandatory because the fan-out goroutines inside
// WriteLogEntries read these fields without synchronization.
//
// Returns a connected LoggingServiceV2Client and ConfigServiceV2Client. The
// ConfigServiceV2 client is required because the sink management RPCs
// (CreateSink / GetSink / UpdateSink / DeleteSink / ListSinks) live on a
// different generated gRPC surface from WriteLogEntries, and the existing
// service_test.go `testClient` helper registers only LoggingServiceV2.
//
// When pubsubAddr == "" or gcsAddr == "", the corresponding fan-out path is
// silently disabled per Rule 7a — this is the contract exercised by
// TestIntegration_Logging_PubSubSink_EmptyEndpoint_NoDelivery.
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
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	started := make(chan struct{})
	go func() {
		close(started)
		_ = svc.Start(ctx, addr)
	}()
	<-started

	// Readiness probe: dial and call ListLogs on an unknown parent. A
	// successful response (status.Code == OK) proves both the
	// LoggingServiceV2 surface AND the underlying grpc.Server are ready.
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

// createPubsubTopicAndSub creates a topic and a subscription pointing at
// that topic using the supplied clients. Caller supplies fully-qualified
// resource names (projects/{p}/topics/{t} and projects/{p}/subscriptions/{s}).
func createPubsubTopicAndSub(t *testing.T, pub pubsubpb.PublisherClient, sub pubsubpb.SubscriberClient, topic, subscription string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic %q: %v", topic, err)
	}
	if _, err := sub.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               subscription,
		Topic:              topic,
		AckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatalf("CreateSubscription %q: %v", subscription, err)
	}
}

// createSinkHelper issues a CreateSink RPC and returns the created LogSink
// proto. The sink name is the caller-supplied short name; the canonical
// store-side Name will be "{parent}/sinks/{name}".
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

// writeEntryHelper issues a WriteLogEntries RPC with a single canonical
// LogEntry. Returns the entry that was sent so tests can assert protojson
// round-trip fidelity on the received Pub/Sub payload.
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
		Payload: &loggingpb.LogEntry_TextPayload{
			TextPayload: textPayload,
		},
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

// parseLogSeverity maps a case-sensitive severity name to the generated
// enum constant. Unknown names fall back to DEFAULT.
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

// pullUntilN polls the subscription up to timeout, aggregating received
// messages until it has at least `want` OR the deadline elapses. Returns
// all messages received during the polling window.
//
// The fan-out is goroutine-based and bounded by the 5-second deliveryTimeout
// in sink_delivery.go, so a 3-second polling window is ample for messages
// that SHOULD arrive and short enough to detect messages that should NOT
// arrive.
func pullUntilN(t *testing.T, sub pubsubpb.SubscriberClient, subscription string, want int, timeout time.Duration) []*pubsubpb.ReceivedMessage {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ctx := context.Background()
	var all []*pubsubpb.ReceivedMessage
	for time.Now().Before(deadline) {
		resp, err := sub.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: subscription,
			MaxMessages:  int32(want + 4),
		})
		if err != nil {
			t.Fatalf("Pull %s: %v", subscription, err)
		}
		all = append(all, resp.ReceivedMessages...)
		if want > 0 && len(all) >= want {
			return all
		}
		time.Sleep(50 * time.Millisecond)
	}
	return all
}

// attrKeys returns the sorted list of attribute keys on a message — used
// for deterministic assertion diagnostics.
func attrKeys(m *pubsubpb.PubsubMessage) []string {
	keys := make([]string, 0, len(m.Attributes))
	for k := range m.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------
// Happy-path tests
// ---------------------------------------------------------------------------

// TestIntegration_Logging_PubSubSink_Delivery is the canonical happy path:
// a CreateSink call registers a pubsub.googleapis.com/... sink, a subsequent
// WriteLogEntries call should produce exactly one Pub/Sub message carrying:
//
//   - Data:      protojson.Marshal(entry) — protojson-round-trippable back to
//                *loggingpb.LogEntry.
//   - Attributes:{logName, severity, sinkName}
//
// This exercises the full Extension D → publishEntryToPubsub pipeline.
func TestIntegration_Logging_PubSubSink_Delivery(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	topic := "projects/p1/topics/log-sink-delivery"
	subscription := "projects/p1/subscriptions/log-sink-delivery-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "happy-sink",
		"pubsub.googleapis.com/"+topic, "")

	entry := writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "hello sink world", "entry-1")

	msgs := pullUntilN(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message, got %d", len(msgs))
	}
	m := msgs[0].Message

	// Attribute set must be exactly {logName, severity, sinkName}.
	wantAttrKeys := []string{"logName", "severity", "sinkName"}
	if got := attrKeys(m); !stringSlicesEqual(got, wantAttrKeys) {
		t.Errorf("attribute keys: got %v, want %v", got, wantAttrKeys)
	}
	if m.Attributes["logName"] != "projects/p1/logs/app" {
		t.Errorf("attr logName: got %q, want %q", m.Attributes["logName"], "projects/p1/logs/app")
	}
	if m.Attributes["severity"] != "INFO" {
		t.Errorf("attr severity: got %q, want %q", m.Attributes["severity"], "INFO")
	}
	// The sinkName attribute is the fully-qualified sink resource name,
	// which is "{parent}/sinks/{shortName}".
	if m.Attributes["sinkName"] != parent+"/sinks/happy-sink" {
		t.Errorf("attr sinkName: got %q, want %q",
			m.Attributes["sinkName"], parent+"/sinks/happy-sink")
	}

	// Data must protojson-round-trip to a LogEntry with identical fields.
	var decoded loggingpb.LogEntry
	if err := protojson.Unmarshal(m.Data, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal(msg.Data): %v\nraw: %s", err, string(m.Data))
	}
	if decoded.GetLogName() != entry.GetLogName() {
		t.Errorf("round-trip LogName: got %q, want %q",
			decoded.GetLogName(), entry.GetLogName())
	}
	if decoded.GetInsertId() != entry.GetInsertId() {
		t.Errorf("round-trip InsertId: got %q, want %q",
			decoded.GetInsertId(), entry.GetInsertId())
	}
	if decoded.GetTextPayload() != entry.GetTextPayload() {
		t.Errorf("round-trip TextPayload: got %q, want %q",
			decoded.GetTextPayload(), entry.GetTextPayload())
	}
	if decoded.GetSeverity() != entry.GetSeverity() {
		t.Errorf("round-trip Severity: got %v, want %v",
			decoded.GetSeverity(), entry.GetSeverity())
	}
}

// TestIntegration_Logging_PubSubSink_MultipleSinks verifies that ONE
// WriteLogEntries call triggers delivery to EVERY matching sink — two
// sinks to two different topics, both receive one message.
func TestIntegration_Logging_PubSubSink_MultipleSinks(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	topicA := "projects/p1/topics/multi-a"
	topicB := "projects/p1/topics/multi-b"
	subA := "projects/p1/subscriptions/multi-a-sub"
	subB := "projects/p1/subscriptions/multi-b-sub"
	createPubsubTopicAndSub(t, pub, sub, topicA, subA)
	createPubsubTopicAndSub(t, pub, sub, topicB, subB)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "sink-a", "pubsub.googleapis.com/"+topicA, "")
	createSinkHelper(t, cfgClient, parent, "sink-b", "pubsub.googleapis.com/"+topicB, "")

	writeEntryHelper(t, logClient,
		"projects/p1/logs/multi", "WARNING", "fan-out payload", "multi-1")

	msgsA := pullUntilN(t, sub, subA, 1, 3*time.Second)
	if len(msgsA) != 1 {
		t.Errorf("sub A: expected 1 message, got %d", len(msgsA))
	}
	msgsB := pullUntilN(t, sub, subB, 1, 3*time.Second)
	if len(msgsB) != 1 {
		t.Errorf("sub B: expected 1 message, got %d", len(msgsB))
	}

	// Per-sink sinkName attribute must be specific to each sink.
	if len(msgsA) == 1 && msgsA[0].Message.Attributes["sinkName"] != parent+"/sinks/sink-a" {
		t.Errorf("sub A sinkName: got %q, want %q",
			msgsA[0].Message.Attributes["sinkName"], parent+"/sinks/sink-a")
	}
	if len(msgsB) == 1 && msgsB[0].Message.Attributes["sinkName"] != parent+"/sinks/sink-b" {
		t.Errorf("sub B sinkName: got %q, want %q",
			msgsB[0].Message.Attributes["sinkName"], parent+"/sinks/sink-b")
	}
}

// TestIntegration_Logging_PubSubSink_MultipleEntriesToOneSink verifies
// that a single WriteLogEntries call with N entries produces N messages
// on the sink (one per entry).
func TestIntegration_Logging_PubSubSink_MultipleEntriesToOneSink(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	topic := "projects/p1/topics/multi-entries"
	subscription := "projects/p1/subscriptions/multi-entries-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "entries-sink", "pubsub.googleapis.com/"+topic, "")

	// Build 3 entries in one WriteLogEntries request.
	entries := make([]*loggingpb.LogEntry, 0, 3)
	for i := 0; i < 3; i++ {
		entries = append(entries, &loggingpb.LogEntry{
			LogName: "projects/p1/logs/bulk",
			Resource: &monitoredrespb.MonitoredResource{
				Type:   "global",
				Labels: map[string]string{"project_id": "test"},
			},
			Severity: loggingtypepb.LogSeverity_INFO,
			InsertId: fmt.Sprintf("bulk-%d", i),
			Payload: &loggingpb.LogEntry_TextPayload{
				TextPayload: fmt.Sprintf("entry %d", i),
			},
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := logClient.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: entries,
	}); err != nil {
		t.Fatalf("WriteLogEntries batch: %v", err)
	}

	msgs := pullUntilN(t, sub, subscription, 3, 3*time.Second)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages for 3 entries, got %d", len(msgs))
	}

	// Collect InsertIds from payloads — order is not guaranteed because
	// fan-out goroutines race for the wire.
	gotIDs := make(map[string]bool)
	for _, m := range msgs {
		var decoded loggingpb.LogEntry
		if err := protojson.Unmarshal(m.Message.Data, &decoded); err != nil {
			t.Errorf("protojson.Unmarshal: %v", err)
			continue
		}
		gotIDs[decoded.GetInsertId()] = true
	}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("bulk-%d", i)
		if !gotIDs[id] {
			t.Errorf("missing InsertId %q in delivered messages", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Filter tests
// ---------------------------------------------------------------------------

// TestIntegration_Logging_PubSubSink_SeverityFilter verifies that a sink
// whose Filter is "severity>=ERROR" receives ERROR entries but NOT lower
// severities. This exercises the MatchingSinks → matchesFilter → severityGTE
// path in store.go with the sink in Extension D's role.
func TestIntegration_Logging_PubSubSink_SeverityFilter(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	topic := "projects/p1/topics/sev-filter"
	subscription := "projects/p1/subscriptions/sev-filter-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "sev-sink",
		"pubsub.googleapis.com/"+topic, "severity>=ERROR")

	// Write both an INFO and an ERROR entry. Only ERROR should be delivered.
	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "info msg (should be filtered)", "info-id")
	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "ERROR", "error msg (should be delivered)", "error-id")

	msgs := pullUntilN(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message (ERROR only), got %d", len(msgs))
	}
	var decoded loggingpb.LogEntry
	if err := protojson.Unmarshal(msgs[0].Message.Data, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	if decoded.GetInsertId() != "error-id" {
		t.Errorf("delivered InsertId: got %q, want %q", decoded.GetInsertId(), "error-id")
	}
	if decoded.GetSeverity() != loggingtypepb.LogSeverity_ERROR {
		t.Errorf("delivered Severity: got %v, want ERROR", decoded.GetSeverity())
	}
	if msgs[0].Message.Attributes["severity"] != "ERROR" {
		t.Errorf("attr severity: got %q, want ERROR", msgs[0].Message.Attributes["severity"])
	}
}

// TestIntegration_Logging_PubSubSink_LogNameFilter verifies the
// `logName="..."` filter dialect: a sink filter-bound to one logName
// must NOT receive entries for a different logName.
func TestIntegration_Logging_PubSubSink_LogNameFilter(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	topic := "projects/p1/topics/logname-filter"
	subscription := "projects/p1/subscriptions/logname-filter-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	// Filter: only deliver entries whose logName is exactly this.
	createSinkHelper(t, cfgClient, parent, "only-app-sink",
		"pubsub.googleapis.com/"+topic,
		`logName="projects/p1/logs/app"`)

	// Write to three different logs. Only one should be delivered.
	writeEntryHelper(t, logClient, "projects/p1/logs/app", "INFO", "app msg", "app-id")
	writeEntryHelper(t, logClient, "projects/p1/logs/other", "INFO", "other msg", "other-id")
	writeEntryHelper(t, logClient, "projects/p1/logs/third", "INFO", "third msg", "third-id")

	msgs := pullUntilN(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message (logName match), got %d", len(msgs))
	}
	var decoded loggingpb.LogEntry
	if err := protojson.Unmarshal(msgs[0].Message.Data, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	if decoded.GetInsertId() != "app-id" {
		t.Errorf("delivered InsertId: got %q, want %q", decoded.GetInsertId(), "app-id")
	}
}

// TestIntegration_Logging_PubSubSink_EmptyFilterMatchesAll verifies the
// documented contract that a sink with Filter=="" matches EVERY entry —
// the most common configuration in practice.
func TestIntegration_Logging_PubSubSink_EmptyFilterMatchesAll(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	topic := "projects/p1/topics/empty-filter"
	subscription := "projects/p1/subscriptions/empty-filter-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "catch-all-sink",
		"pubsub.googleapis.com/"+topic, "") // empty filter

	writeEntryHelper(t, logClient, "projects/p1/logs/one", "DEBUG", "dbg", "debug-1")
	writeEntryHelper(t, logClient, "projects/p1/logs/two", "INFO", "inf", "info-1")
	writeEntryHelper(t, logClient, "projects/p1/logs/three", "EMERGENCY", "emg", "emg-1")

	msgs := pullUntilN(t, sub, subscription, 3, 3*time.Second)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages (all severities), got %d", len(msgs))
	}
}

// ---------------------------------------------------------------------------
// Negative-path tests — fire-and-forget / skip contracts
// ---------------------------------------------------------------------------

// TestIntegration_Logging_PubSubSink_EmptyEndpoint_NoDelivery verifies
// Rule 7a: when the Logging service is started with an EMPTY pubsubAddr,
// even a properly-configured pubsub.googleapis.com/... sink produces no
// delivery. WriteLogEntries still returns OK — the fan-out goroutine is
// simply never spawned because the endpoint is empty.
func TestIntegration_Logging_PubSubSink_EmptyEndpoint_NoDelivery(t *testing.T) {
	// Start Pub/Sub but DO NOT thread its address into Logging.
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, "", "")

	topic := "projects/p1/topics/empty-endpoint"
	subscription := "projects/p1/subscriptions/empty-endpoint-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	// Sink points at a real topic, but the Logging service has no
	// pubsubAddr wired, so no dial, no publish, no message.
	createSinkHelper(t, cfgClient, parent, "orphan-sink",
		"pubsub.googleapis.com/"+topic, "")

	// WriteLogEntries succeeds normally.
	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "orphan payload", "orphan-1")

	// Poll for up to 800ms — no delivery should arrive.
	msgs := pullUntilN(t, sub, subscription, 0, 800*time.Millisecond)
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages with empty pubsubAddr, got %d", len(msgs))
	}

	// Silence linter: pubsubAddr is unused because we intentionally do
	// NOT pass it to startLoggingWithEndpoints.
	_ = pubsubAddr
}

// TestIntegration_Logging_PubSubSink_UnsupportedScheme verifies that a
// sink whose Destination carries an unsupported scheme (neither
// pubsub.googleapis.com/ nor storage.googleapis.com/) is silently skipped
// — deliverToSink returns early without error, WriteLogEntries returns OK,
// and no Pub/Sub message arrives on any subscription.
//
// This is the extension-safety clause of Extension D: future sink targets
// (BigQuery, Loki, etc.) can be added without touching the existing
// delivery path.
func TestIntegration_Logging_PubSubSink_UnsupportedScheme(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	// Also create a topic+sub so we can confirm NO message was published
	// to ANY Pub/Sub resource as a result of this write.
	topic := "projects/p1/topics/unsupported"
	subscription := "projects/p1/subscriptions/unsupported-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "unsupported-sink",
		"bigquery.googleapis.com/projects/p1/datasets/ds/tables/tbl", "")

	// WriteLogEntries must succeed.
	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "unsupported scheme payload", "unsup-1")

	// No delivery to any subscription.
	msgs := pullUntilN(t, sub, subscription, 0, 500*time.Millisecond)
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages (unsupported scheme silently skipped), got %d", len(msgs))
	}
}

// TestIntegration_Logging_PubSubSink_NoSinks verifies that when NO sinks
// are registered, WriteLogEntries still succeeds and no Pub/Sub messages
// are published — the fan-out path is a strict no-op when MatchingSinks
// returns empty.
func TestIntegration_Logging_PubSubSink_NoSinks(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, _ := startLoggingWithEndpoints(t, pubsubAddr, "")

	topic := "projects/p1/topics/no-sinks"
	subscription := "projects/p1/subscriptions/no-sinks-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "orphan write", "no-sinks-1")

	msgs := pullUntilN(t, sub, subscription, 0, 500*time.Millisecond)
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages with no sinks registered, got %d", len(msgs))
	}
}

// TestIntegration_Logging_PubSubSink_WriteLogEntriesUnblocked verifies
// Rule 3 — the fan-out is fire-and-forget: WriteLogEntries returns
// promptly even if the per-sink delivery is still in flight. We measure
// the RPC round-trip and assert it completes in well under the 5-second
// deliveryTimeout used by publishEntryToPubsub.
func TestIntegration_Logging_PubSubSink_WriteLogEntriesUnblocked(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForLoggingIntegration(t)
	logClient, cfgClient := startLoggingWithEndpoints(t, pubsubAddr, "")

	topic := "projects/p1/topics/unblocked"
	subscription := "projects/p1/subscriptions/unblocked-sub"
	createPubsubTopicAndSub(t, pub, sub, topic, subscription)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "unblocked-sink",
		"pubsub.googleapis.com/"+topic, "")

	// Measure WriteLogEntries RPC round-trip.
	start := time.Now()
	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "unblocked payload", "unb-1")
	elapsed := time.Since(start)
	// 1 second is an extremely generous upper bound — real round-trip
	// on the same process is usually sub-10ms. If the fan-out were
	// accidentally blocking the response, it would be bounded by the
	// 5s deliveryTimeout and would be ≥ 5s if Pub/Sub were unreachable.
	if elapsed > 1*time.Second {
		t.Errorf("WriteLogEntries RPC took %v — expected <1s (fire-and-forget fan-out contract, Rule 3)", elapsed)
	}

	// Sanity check: the delivery still eventually happens.
	msgs := pullUntilN(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Errorf("expected 1 delivered message, got %d", len(msgs))
	}
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

// stringSlicesEqual is a small order-sensitive equality helper used by
// attribute-key assertions.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
