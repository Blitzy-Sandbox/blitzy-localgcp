//go:build integration

// Integration test for the Cloud Logging -> Pub/Sub sink delivery path.
//
// Per Agent Action Plan (AAP) Extension D and Rule 9: this test stands up
// BOTH the Pub/Sub emulator (internal/pubsub) and the Cloud Logging emulator
// (internal/logging) in the same Go test process, creates a sink whose
// destination is a pubsub:// URI, calls WriteLogEntries with one entry, and
// asserts that the fire-and-forget fan-out goroutine successfully delivers
// the entry to the Pub/Sub subscription bound to the sink's topic.
//
// The test exercises:
//   - logging.New(...) with a non-empty pubsubAddr (Rule 7a additive
//     constructor argument).
//   - The loggingpb.ConfigServiceV2 RPCs (CreateSink in particular).
//   - The fire-and-forget goroutine fan-out in WriteLogEntries (Rule 3 --
//     request handlers must not block on inter-service calls).
//   - End-to-end loopback wiring from Logging to Pub/Sub over gRPC.
//
// Run with:  go test -tags integration ./internal/logging/...

package logging_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	pubsubpb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/slokam-ai/localgcp/internal/logging"
	"github.com/slokam-ai/localgcp/internal/pubsub"
)

// ----------------------------------------------------------------------
// Test-file-local constants (per file schema spec).
// ----------------------------------------------------------------------
//
// These constants MUST live in this file to satisfy the schema. Because
// internal/logging/sink_delivery.go already declares a package-level
// `deliveryTimeout` in the `logging` package, this file uses the external
// test package `logging_test` so the two `deliveryTimeout` symbols live
// in distinct package namespaces.

const (
	// pubsubTestProject is the fully-qualified Pub/Sub parent used for the
	// test's topic, subscription, and log-name resources. Kept at a fixed
	// short value so the assertions below can rely on exact string equality.
	pubsubTestProject = "projects/test"

	// pubsubSinkTopic is the Pub/Sub topic resource name that the
	// Logging sink is configured to publish to.
	pubsubSinkTopic = pubsubTestProject + "/topics/log-sink-topic"

	// pubsubSinkSub is the pull subscription the test uses to fetch the
	// routed log entry after WriteLogEntries returns.
	pubsubSinkSub = pubsubTestProject + "/subscriptions/log-sink-sub"

	// pubsubSinkName is the short (last-segment) name of the Logging sink
	// resource created by the test. Fully-qualified form is derived as
	// "{parent}/sinks/{pubsubSinkName}".
	pubsubSinkName = "pubsub-sink"

	// pubsubLogName is the fully-qualified log name attached to the single
	// LogEntry written by the test. It becomes the `logName` attribute on
	// the delivered Pub/Sub message.
	pubsubLogName = pubsubTestProject + "/logs/sinktest"

	// pubsubTestPayload is the text payload sent with the LogEntry.
	pubsubTestPayload = "hello-pubsub-sink"

	// readyTimeout bounds how long each emulator has to reach a
	// serving state before the test fails with a diagnostic.
	readyTimeout = 5 * time.Second

	// deliveryTimeout bounds how long the test polls the Pub/Sub
	// subscription before declaring that the sink fan-out failed.
	deliveryTimeout = 5 * time.Second
)

// TestLoggingPubSubSink_Delivery verifies the Logging -> Pub/Sub sink path.
//
// Flow:
//
//  1. Start Pub/Sub emulator on an ephemeral port.
//  2. Start Logging emulator on an ephemeral port wired to the Pub/Sub addr
//     via the variadic logging.New(dataDir, quiet, pubsubAddr, gcsAddr).
//  3. Create the destination topic and a pull subscription against it.
//  4. Create a Cloud Logging sink whose Destination targets the topic and
//     an empty Filter (match all entries).
//  5. Call WriteLogEntries with exactly one INFO LogEntry.
//  6. Poll the Pub/Sub subscription via Pull until the routed entry arrives
//     or the delivery timeout elapses.
//  7. Assert the received message's Attributes carry the entry's logName.
//  8. Assert the received message's Data is non-empty JSON that contains
//     the expected logName (tolerating both camelCase and PascalCase
//     field naming so the test is resilient to proto-JSON vs
//     encoding/json marshalling choices in the sink-delivery helper).
func TestLoggingPubSubSink_Delivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start Pub/Sub emulator.
	pubsubAddr := startPubSubForPubSubSinkTest(t, ctx)

	// 2. Start Logging emulator with pubsubAddr wired in (gcsAddr empty --
	// the GCS sink branch is a silent no-op per Rule 7a).
	logAddr := startLoggingForPubSubSinkTest(t, ctx, pubsubAddr, "")

	// 3. Dial Pub/Sub and create topic + subscription.
	pubConn, err := grpc.NewClient(pubsubAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial pubsub %q: %v", pubsubAddr, err)
	}
	defer pubConn.Close()

	pubClient := pubsubpb.NewPublisherClient(pubConn)
	subClient := pubsubpb.NewSubscriberClient(pubConn)

	if _, err := pubClient.CreateTopic(ctx, &pubsubpb.Topic{Name: pubsubSinkTopic}); err != nil {
		t.Fatalf("CreateTopic %q: %v", pubsubSinkTopic, err)
	}
	if _, err := subClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               pubsubSinkSub,
		Topic:              pubsubSinkTopic,
		AckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatalf("CreateSubscription %q: %v", pubsubSinkSub, err)
	}

	// 4. Dial Logging, create the sink.
	logConn, err := grpc.NewClient(logAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial logging %q: %v", logAddr, err)
	}
	defer logConn.Close()

	configClient := loggingpb.NewConfigServiceV2Client(logConn)
	logClient := loggingpb.NewLoggingServiceV2Client(logConn)

	// The sink destination follows the real Cloud Logging URI convention:
	// "pubsub.googleapis.com/{topicResourceName}". The logging service's
	// deliverToSink helper recognises this prefix and routes the entry via
	// the loopback Pub/Sub gRPC client.
	_, err = configClient.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: pubsubTestProject,
		Sink: &loggingpb.LogSink{
			Name:        pubsubSinkName,
			Destination: "pubsub.googleapis.com/" + pubsubSinkTopic,
			Filter:      "", // match all entries
		},
	})
	if err != nil {
		t.Fatalf("CreateSink: %v", err)
	}

	// 5. Write one log entry. WriteLogEntries must return promptly -- Rule 3.
	writeStart := time.Now()
	_, err = logClient.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		LogName:  pubsubLogName,
		Resource: &monitoredres.MonitoredResource{Type: "global"},
		Entries: []*loggingpb.LogEntry{
			{
				LogName:  pubsubLogName,
				Severity: ltype.LogSeverity_INFO,
				Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: pubsubTestPayload},
			},
		},
	})
	writeElapsed := time.Since(writeStart)
	if err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}
	// Sanity check on Rule 3: the fan-out is fire-and-forget; the RPC should
	// not block on Pub/Sub delivery. A ceiling well above reasonable gRPC
	// latency asserts no synchronous wait.
	if writeElapsed > 2*time.Second {
		t.Fatalf("WriteLogEntries blocked on sink delivery (elapsed=%v); Rule 3 violation", writeElapsed)
	}

	// 6. Poll the Pub/Sub subscription for delivery.
	var received *pubsubpb.ReceivedMessage
	deadline := time.Now().Add(deliveryTimeout)
	for time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		pullResp, pullErr := subClient.Pull(pctx, &pubsubpb.PullRequest{
			Subscription: pubsubSinkSub,
			MaxMessages:  10,
		})
		pcancel()
		if pullErr == nil && pullResp != nil && len(pullResp.ReceivedMessages) > 0 {
			received = pullResp.ReceivedMessages[0]
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if received == nil {
		t.Fatal("timed out waiting for sink-routed message on pubsub subscription")
	}

	// 7. Assert the attributes carry the entry's logName.
	//
	// Per the Extension D implementation plan, the sink delivery helper
	// attaches {logName, severity, sinkName} attributes to the published
	// message. Attribute assertions are robust to JSON marshalling strategy
	// choices because attributes are plain string key-value pairs with no
	// marshalling ambiguity.
	msg := received.Message
	if msg == nil {
		t.Fatal("received ReceivedMessage has no inner Message")
	}
	if got, want := msg.Attributes["logName"], pubsubLogName; got != want {
		t.Fatalf("Attributes[logName] = %q; want %q (attrs=%v)", got, want, msg.Attributes)
	}
	if sev := msg.Attributes["severity"]; sev == "" {
		t.Fatalf("Attributes[severity] missing (attrs=%v)", msg.Attributes)
	}

	// 8. Assert the Data payload is valid, non-empty JSON. Field-name
	// casing may vary by marshaller (protojson uses camelCase, encoding/json
	// uses Go field names); the test tolerates either shape as long as the
	// LogEntry payload made it through.
	if len(msg.Data) == 0 {
		t.Fatal("received message has empty Data payload")
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(msg.Data, &decoded); err != nil {
		t.Fatalf("message Data is not valid JSON: %v (body=%q)", err, string(msg.Data))
	}
	if len(decoded) == 0 {
		t.Fatal("decoded JSON object is empty")
	}
	logNameValue := firstNonEmptyString(decoded, "logName", "LogName", "log_name")
	if logNameValue == "" {
		t.Fatalf("decoded entry missing logName under any expected casing: %v", decoded)
	}
	if logNameValue != pubsubLogName {
		t.Fatalf("decoded logName = %q; want %q", logNameValue, pubsubLogName)
	}
}

// firstNonEmptyString returns the first non-empty string value among the
// given keys in m. Used to accommodate differences between protojson
// (camelCase) and encoding/json (Go field name) marshalling outputs so
// that the test is resilient to either JSON-encoding strategy.
func firstNonEmptyString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		if s == "" {
			continue
		}
		return s
	}
	return ""
}

// ----------------------------------------------------------------------
// Test harness helpers.
// ----------------------------------------------------------------------
//
// The helpers use the `ForPubSubSinkTest` suffix to guarantee uniqueness
// against helpers declared in internal/logging/integration_gcs_sink_test.go
// -- both files compile together under the `integration` build tag.

// startPubSubForPubSubSinkTest starts the Pub/Sub emulator on an ephemeral
// localhost port and returns its host:port address. The service is shut
// down automatically when ctx is cancelled.
//
// The readiness probe dials gRPC and issues GetTopic on a nonexistent
// resource; receiving codes.NotFound (or any non-Unavailable status)
// confirms that the server is serving RPCs.
func startPubSubForPubSubSinkTest(t *testing.T, ctx context.Context) string {
	t.Helper()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen ephemeral port for pubsub: %v", err)
	}
	addr := ln.Addr().String()
	// Close the listener so svc.Start can re-bind the same address in its
	// own net.Listen call. This pattern matches the one used by
	// internal/pubsub/service_test.go testClients.
	_ = ln.Close()

	svc := pubsub.New("", true) // empty dataDir, quiet
	started := make(chan struct{})
	go func() {
		close(started)
		_ = svc.Start(ctx, addr)
	}()
	<-started

	// Readiness probe.
	var lastErr error
	probeDeadline := time.Now().Add(readyTimeout)
	for time.Now().Before(probeDeadline) {
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			c := pubsubpb.NewPublisherClient(conn)
			pctx, pcancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, rpcErr := c.GetTopic(pctx, &pubsubpb.GetTopicRequest{
				Topic: "projects/probe/topics/probe",
			})
			pcancel()
			_ = conn.Close()
			if rpcErr == nil || status.Code(rpcErr) == codes.NotFound {
				return addr
			}
			// Any non-Unavailable response also confirms the server is
			// reachable and serving the Publisher service.
			if status.Code(rpcErr) != codes.Unavailable {
				return addr
			}
			lastErr = rpcErr
		} else {
			lastErr = dialErr
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("pubsub emulator did not become ready on %s within %v (last err=%v)", addr, readyTimeout, lastErr)
	return ""
}

// startLoggingForPubSubSinkTest starts the Cloud Logging emulator on an
// ephemeral localhost port, wired with the provided pubsubAddr and gcsAddr
// loopback addresses. An empty address string means that loopback delivery
// path is a silent no-op (Rule 7a). The service is shut down automatically
// when ctx is cancelled.
//
// The readiness probe dials gRPC and issues ListSinks on a project with no
// sinks; any non-Unavailable response confirms the server is serving.
func startLoggingForPubSubSinkTest(t *testing.T, ctx context.Context, pubsubAddr, gcsAddr string) string {
	t.Helper()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen ephemeral port for logging: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	// logging.New is variadic: addrs[0]=pubsubAddr, addrs[1]=gcsAddr.
	svc := logging.New("", true, pubsubAddr, gcsAddr)
	started := make(chan struct{})
	go func() {
		close(started)
		_ = svc.Start(ctx, addr)
	}()
	<-started

	var lastErr error
	probeDeadline := time.Now().Add(readyTimeout)
	for time.Now().Before(probeDeadline) {
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			c := loggingpb.NewConfigServiceV2Client(conn)
			pctx, pcancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, rpcErr := c.ListSinks(pctx, &loggingpb.ListSinksRequest{Parent: "projects/probe"})
			pcancel()
			_ = conn.Close()
			if rpcErr == nil {
				return addr
			}
			// Any non-Unavailable code means the gRPC transport and
			// ConfigServiceV2 handler are reachable.
			if status.Code(rpcErr) != codes.Unavailable {
				return addr
			}
			lastErr = rpcErr
		} else {
			lastErr = dialErr
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("logging emulator did not become ready on %s within %v (pubsubAddr=%q gcsAddr=%q): last err=%v",
		addr, readyTimeout, pubsubAddr, gcsAddr, lastErr)
	return ""
}
