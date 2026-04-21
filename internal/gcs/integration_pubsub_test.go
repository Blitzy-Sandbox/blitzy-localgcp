//go:build integration

// Package gcs_test — integration_pubsub_test.go
//
// End-to-end integration coverage for AAP §0.5.1.2 Extension B
// (GCS → Pub/Sub notification delivery), satisfying AAP Rule 9 which
// mandates a dedicated integration test for each cross-service wiring
// path. This test stands up BOTH a live GCS HTTP service AND a live
// Pub/Sub gRPC service in the same Go test process (mirroring the
// single-binary localgcp topology), wires the GCS service to the
// Pub/Sub service via the variadic gcs.New(..., pubsubAddr) constructor
// (AAP Rule 7a), creates a notification config, exercises the object
// write/delete paths, and asserts that canonical GCS Pub/Sub
// notifications arrive on the subscribed topic with the correct
// attributes and payload shape.
//
// Topology (AAP §0.4.2.2 "GCS → Pub/Sub Notification Delivery"):
//
//     HTTP PUT /upload/... ───▶ gcs.Service ──goroutine──▶ pubsub.Service
//                                                            │
//                                                            ▼
//                                                        subscriber.Pull
//
// Rule 3 (fire-and-forget handler) is verified by measuring the
// elapsed wall time of the upload HTTP call and asserting it returns
// in well under the per-message Pub/Sub publish timeout. A regression
// that accidentally makes the publish synchronous would blow this
// bound immediately.
//
// Rule 7a (constructor additive args) is verified by calling
// gcs.New("", true, pubsubAddr) in the three-argument form and by
// calling pubsub.New("", true) in its unchanged two-argument form.
//
// Build tag: this file compiles only when `go test` is invoked with
// `-tags integration` (AAP §0.7.4 Gate 8), so the standard
// `go test ./internal/gcs/...` unit-test run is unaffected.

package gcs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/slokam-ai/localgcp/internal/gcs"
	"github.com/slokam-ai/localgcp/internal/pubsub"
)

// startPubSubForGCSTest starts an in-process Pub/Sub gRPC emulator on an
// ephemeral localhost port and returns the dialable "host:port" address
// plus a cancel function that shuts the server down.
//
// The Pub/Sub service is constructed with the unchanged two-argument
// form pubsub.New(dataDir, quiet) — immutable per AAP Rule 7 (the
// pubsub package's constructor is NOT extended with loopback addresses
// since Pub/Sub is a terminal destination for the GCS notification
// path, not an initiator).
//
// The readiness probe dials gRPC and issues ListTopics on a probe
// project; any non-transport-level response confirms the Publisher
// service is reachable. This mirrors the readiness pattern used by
// internal/pubsub/service_test.go testClients and
// internal/logging/integration_pubsub_sink_test.go.
func startPubSubForGCSTest(t *testing.T) (string, func()) {
	t.Helper()

	svc := pubsub.New("", true) // empty dataDir, quiet

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral port for pubsub: %v", err)
	}
	addr := ln.Addr().String()
	// Close the listener so svc.Start can re-bind the same address in
	// its own net.Listen call. This pattern matches the one used by
	// internal/pubsub/service_test.go testClients.
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Shutdown-induced errors are expected on cancel and are not
		// reported because this goroutine runs concurrently with the
		// test and t.Fatal from a non-test goroutine has undefined
		// semantics.
		_ = svc.Start(ctx, addr)
	}()

	// Readiness probe — loop up to 3 seconds with 20ms sleeps.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			client := pubsubpb.NewPublisherClient(conn)
			pctx, pcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, rpcErr := client.ListTopics(pctx, &pubsubpb.ListTopicsRequest{
				Project: "projects/localgcp-readiness-probe",
			})
			pcancel()
			_ = conn.Close()
			// Any response — success, NotFound, even Unimplemented or
			// InvalidArgument — confirms the server is serving RPCs
			// on the Publisher service.
			if rpcErr == nil ||
				strings.Contains(rpcErr.Error(), "Unimplemented") ||
				strings.Contains(rpcErr.Error(), "invalid") ||
				strings.Contains(rpcErr.Error(), "NotFound") {
				return addr, cancel
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("pubsub server did not start within deadline")
	return "", nil
}

// startGCSForNotificationTest starts an in-process GCS HTTP emulator on
// an ephemeral localhost port, wired to the provided pubsubAddr for the
// notification-config fan-out path (AAP Rule 7a additive args). Returns
// the base URL (e.g. "http://127.0.0.1:43219") plus a cancel function
// that shuts the server down.
//
// The GCS service is constructed with the three-argument variadic form
// gcs.New(dataDir, quiet, pubsubAddr). When pubsubAddr is empty the
// fan-out path is silently skipped (Rule 7a).
//
// The readiness probe issues HTTP GET on the root path. Any response —
// including 404 — proves the HTTP server is accepting connections.
func startGCSForNotificationTest(t *testing.T, pubsubAddr string) (string, func()) {
	t.Helper()

	svc := gcs.New("", true, pubsubAddr) // Rule 7a three-arg variadic form

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral port for gcs: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	hostPort := fmt.Sprintf("127.0.0.1:%d", port)
	baseURL := fmt.Sprintf("http://%s", hostPort)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Shutdown-induced errors are expected on cancel and are not
		// reported — see startPubSubForGCSTest for the rationale.
		_ = svc.Start(ctx, hostPort)
	}()

	// Readiness probe — loop up to 3 seconds with 20ms sleeps.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/")
		if err == nil {
			_ = resp.Body.Close()
			return baseURL, cancel
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("gcs server did not start within deadline")
	return "", nil
}

// pullOneGCSNotification polls the given subscription until at least one
// message arrives, then ACKs every received message (to prevent
// re-delivery on subsequent pulls) and returns the first message.
//
// Fails the test if no message arrives within the deadline. A per-pull
// context timeout of 1s prevents a single Pull call from blocking
// beyond the outer deadline. The polling uses ReturnImmediately=true
// so each Pull completes promptly when the subscription is empty,
// avoiding long hanging calls that would complicate timeout accounting.
func pullOneGCSNotification(t *testing.T, sub pubsubpb.SubscriberClient, subName string, deadline time.Duration) *pubsubpb.PubsubMessage {
	t.Helper()

	stop := time.Now().Add(deadline)
	for time.Now().Before(stop) {
		pctx, pcancel := context.WithTimeout(context.Background(), 1*time.Second)
		resp, err := sub.Pull(pctx, &pubsubpb.PullRequest{
			Subscription:      subName,
			MaxMessages:       10,
			ReturnImmediately: true,
		})
		pcancel()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		received := resp.GetReceivedMessages()
		if len(received) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// Acknowledge all received messages so the next pull against
		// the same subscription sees a clean queue (critical for the
		// OBJECT_FINALIZE → OBJECT_DELETE sequence in the primary
		// test).
		ackIDs := make([]string, 0, len(received))
		for _, m := range received {
			ackIDs = append(ackIDs, m.GetAckId())
		}
		ackCtx, ackCancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, _ = sub.Acknowledge(ackCtx, &pubsubpb.AcknowledgeRequest{
			Subscription: subName,
			AckIds:       ackIDs,
		})
		ackCancel()
		return received[0].GetMessage()
	}
	t.Fatalf("no message received on %s within %v", subName, deadline)
	return nil
}

// TestGCSNotification_DeliveredToPubSub exercises the full Extension B
// loopback path end-to-end (AAP §0.5.1.2 Extension B, AAP Rule 9):
//
//  1. Start Pub/Sub emulator on an ephemeral port.
//  2. Start GCS emulator on an ephemeral port wired to the Pub/Sub
//     address via the variadic gcs.New(..., pubsubAddr) constructor.
//  3. Dial Pub/Sub; create a topic and a pull subscription.
//  4. Create a GCS bucket via HTTP POST /storage/v1/b.
//  5. Create a NotificationConfig on the bucket pointing at the topic
//     (event types: OBJECT_FINALIZE, OBJECT_DELETE) via HTTP.
//  6. Upload an object via HTTP POST /upload/... and assert the HTTP
//     PUT returned in well under 2 seconds — AAP Rule 3 fire-and-forget
//     verification. A regression that accidentally made the Publish
//     call synchronous from the handler would blow this bound.
//  7. Pull from the subscription; assert the received message has
//     eventType=OBJECT_FINALIZE, bucketId=<bucket>, and a canonical
//     GCS JSON payload with kind=storage#object.
//  8. DELETE the object via HTTP.
//  9. Pull again; assert eventType=OBJECT_DELETE on the delivered
//     message.
//
// The test uses only the public HTTP API of the GCS emulator and the
// public gRPC API of the Pub/Sub emulator — no internal helpers, no
// type assertions into private fields. The external test package
// (gcs_test) guarantees this.
func TestGCSNotification_DeliveredToPubSub(t *testing.T) {
	// 1. Start Pub/Sub emulator.
	pubsubAddr, stopPubSub := startPubSubForGCSTest(t)
	defer stopPubSub()

	// 2. Start GCS emulator wired to Pub/Sub.
	gcsBase, stopGCS := startGCSForNotificationTest(t, pubsubAddr)
	defer stopGCS()

	// 3. Dial Pub/Sub for topic/subscription setup and message pulling.
	conn, err := grpc.NewClient(pubsubAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial pubsub %q: %v", pubsubAddr, err)
	}
	defer func() { _ = conn.Close() }()

	pubClient := pubsubpb.NewPublisherClient(conn)
	subClient := pubsubpb.NewSubscriberClient(conn)

	const (
		projectID  = "test-project"
		topicName  = "projects/test-project/topics/gcs-notifications"
		subName    = "projects/test-project/subscriptions/gcs-notifications-sub"
		bucketName = "notif-bucket"
		objectName = "hello.txt"
		objectBody = "hello from integration test"
	)

	// Create topic.
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setupCancel()

	if _, err := pubClient.CreateTopic(setupCtx, &pubsubpb.Topic{Name: topicName}); err != nil {
		t.Fatalf("CreateTopic %q: %v", topicName, err)
	}

	// Create subscription targeting the topic.
	if _, err := subClient.CreateSubscription(setupCtx, &pubsubpb.Subscription{
		Name:               subName,
		Topic:              topicName,
		AckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatalf("CreateSubscription %q -> %q: %v", subName, topicName, err)
	}

	// 4. Create GCS bucket via HTTP.
	bucketCreateURL := fmt.Sprintf("%s/storage/v1/b?project=%s", gcsBase, projectID)
	bucketCreateBody := fmt.Sprintf(`{"name":%q}`, bucketName)
	resp, err := http.Post(bucketCreateURL, "application/json", strings.NewReader(bucketCreateBody))
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("create bucket status=%d body=%s", resp.StatusCode, body)
	}
	_ = resp.Body.Close()

	// 5. Create notification config on the bucket.
	notifURL := fmt.Sprintf("%s/storage/v1/b/%s/notificationConfigs", gcsBase, bucketName)
	notifBody := fmt.Sprintf(`{"topic":%q,"event_types":["OBJECT_FINALIZE","OBJECT_DELETE"]}`, topicName)
	resp, err = http.Post(notifURL, "application/json", strings.NewReader(notifBody))
	if err != nil {
		t.Fatalf("create notification config: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("create notification config status=%d body=%s", resp.StatusCode, body)
	}
	var notifResp struct {
		ID    string `json:"id"`
		Topic string `json:"topic"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&notifResp); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("decode notification config response: %v", err)
	}
	_ = resp.Body.Close()
	if notifResp.ID == "" {
		t.Fatalf("notification config ID is empty in response: %+v", notifResp)
	}

	// 6. Upload an object and measure elapsed wall time.
	//
	// AAP Rule 3 (fire-and-forget): the HTTP handler MUST NOT block on
	// the downstream Pub/Sub publish. The upload should return in
	// essentially RTT+store-write time (well under 100ms on localhost);
	// a 2-second bound guards against any regression that makes the
	// publish synchronous without being sensitive to ordinary system
	// load.
	uploadURL := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?uploadType=media&name=%s",
		gcsBase, bucketName, objectName)
	uploadStart := time.Now()
	resp, err = http.Post(uploadURL, "text/plain", bytes.NewReader([]byte(objectBody)))
	uploadElapsed := time.Since(uploadStart)
	if err != nil {
		t.Fatalf("upload object: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("upload object status=%d body=%s", resp.StatusCode, body)
	}
	_ = resp.Body.Close()
	if uploadElapsed > 2*time.Second {
		t.Fatalf("upload HTTP call took %v (>2s) — handler appears to block on Pub/Sub publish (AAP Rule 3 violation)",
			uploadElapsed)
	}

	// 7. Poll the subscription for the OBJECT_FINALIZE notification.
	finalizeMsg := pullOneGCSNotification(t, subClient, subName, 5*time.Second)

	if got := finalizeMsg.Attributes["eventType"]; got != "OBJECT_FINALIZE" {
		t.Fatalf("Attributes[eventType] = %q, want OBJECT_FINALIZE (full attrs: %v)",
			got, finalizeMsg.Attributes)
	}
	if got := finalizeMsg.Attributes["bucketId"]; got != bucketName {
		t.Fatalf("Attributes[bucketId] = %q, want %q (full attrs: %v)",
			got, bucketName, finalizeMsg.Attributes)
	}

	// Verify canonical GCS notification payload shape.
	if len(finalizeMsg.Data) == 0 {
		t.Fatalf("FINALIZE message has empty Data payload (JSON_API_V1 payload expected)")
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(finalizeMsg.Data, &payload); err != nil {
		t.Fatalf("FINALIZE payload is not valid JSON: %v (raw=%q)", err, string(finalizeMsg.Data))
	}
	if got := payload["kind"]; got != "storage#object" {
		t.Fatalf(`payload["kind"] = %v, want "storage#object" (payload: %v)`, got, payload)
	}
	if got := payload["bucket"]; got != bucketName {
		t.Fatalf(`payload["bucket"] = %v, want %q (payload: %v)`, got, bucketName, payload)
	}
	if got := payload["name"]; got != objectName {
		t.Fatalf(`payload["name"] = %v, want %q (payload: %v)`, got, objectName, payload)
	}

	// 8. Delete the object via HTTP.
	delURL := fmt.Sprintf("%s/storage/v1/b/%s/o/%s", gcsBase, bucketName, objectName)
	delReq, err := http.NewRequest(http.MethodDelete, delURL, nil)
	if err != nil {
		t.Fatalf("build DELETE request: %v", err)
	}
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if delResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(delResp.Body)
		_ = delResp.Body.Close()
		t.Fatalf("delete object status=%d body=%s (want 204)", delResp.StatusCode, body)
	}
	_ = delResp.Body.Close()

	// 9. Poll the subscription for the OBJECT_DELETE notification.
	deleteMsg := pullOneGCSNotification(t, subClient, subName, 5*time.Second)

	if got := deleteMsg.Attributes["eventType"]; got != "OBJECT_DELETE" {
		t.Fatalf("Attributes[eventType] = %q, want OBJECT_DELETE (full attrs: %v)",
			got, deleteMsg.Attributes)
	}
	if got := deleteMsg.Attributes["bucketId"]; got != bucketName {
		t.Fatalf("Attributes[bucketId] = %q, want %q (full attrs: %v)",
			got, bucketName, deleteMsg.Attributes)
	}
}
