//go:build integration

// Package gcs — integration_pubsub_test.go
//
// End-to-end integration coverage for AAP §0.5.1.2 Extension B
// (GCS → Pub/Sub notification delivery), satisfying AAP Rule 9 which
// mandates a dedicated integration test for each cross-service wiring path.
//
// These tests start BOTH a live GCS HTTP service AND a live Pub/Sub gRPC
// service in the same process (the localgcp single-binary topology), wire
// the GCS service to the Pub/Sub service via SetPubsubEndpoint, create a
// notification configuration, exercise the object write/delete paths, and
// assert that the correct notification arrives on a Pub/Sub subscription
// with the correct attributes and payload shape.
//
// The fan-out is fire-and-forget per-config (Rule 3): the HTTP handler
// returns BEFORE the Publish RPC completes. Tests therefore use a bounded
// retry loop (`pullUntil`) rather than assuming immediate availability.
//
// Build tag: these tests are compiled only when `go test` is invoked with
// `-tags integration`, per AAP §0.7.4 Gate 8.

package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/slokam-ai/localgcp/internal/pubsub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// --- Integration test helpers ---

// startPubsubForIntegration launches an in-memory, quiet Pub/Sub service on
// an ephemeral localhost port. Returns the dialable address plus ready
// Publisher and Subscriber clients sharing a single gRPC connection.
//
// The readiness probe uses GetTopic on a nonexistent topic — a codes.NotFound
// response confirms the server is serving RPCs (mirrors the canonical
// pattern in internal/pubsub/service_test.go).
func startPubsubForIntegration(t *testing.T) (string, pubsubpb.PublisherClient, pubsubpb.SubscriberClient) {
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

// startGCSWithPubsub launches an in-memory, quiet GCS HTTP service on an
// ephemeral localhost port with the Pub/Sub loopback endpoint configured
// via SetPubsubEndpoint. Returns the base HTTP URL.
//
// When pubsubAddr == "", fan-out is disabled (Rule 7a silent skip) — this
// is the path used by TestIntegration_GCS_PubSub_EmptyEndpoint_NoDelivery.
func startGCSWithPubsub(t *testing.T, pubsubAddr string) string {
	t.Helper()

	svc := New("", true)
	svc.SetPubsubEndpoint(pubsubAddr)

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("gcs listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	addr := fmt.Sprintf("localhost:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = svc.Start(ctx, addr) }()

	base := fmt.Sprintf("http://%s", addr)
	for i := 0; i < 50; i++ {
		resp, err := http.Get(base + "/")
		if err == nil {
			resp.Body.Close()
			return base
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("integration: gcs server did not start")
	return ""
}

// createTopicAndSubscription creates a topic and a subscription pointing at
// that topic using the supplied clients. Caller supplies fully-qualified
// resource names (projects/{p}/topics/{t} and projects/{p}/subscriptions/{s}).
func createTopicAndSubscription(t *testing.T, pub pubsubpb.PublisherClient, sub pubsubpb.SubscriberClient, topic, subscription string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("CreateTopic %s: %v", topic, err)
	}
	if _, err := sub.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subscription,
		Topic: topic,
	}); err != nil {
		t.Fatalf("CreateSubscription %s -> %s: %v", subscription, topic, err)
	}
}

// createNotifConfig PUTs a NotificationConfig on bucket pointing at topic,
// with the given optional event types and prefix. Asserts HTTP 200 and
// returns the created NotificationConfig (populated with server-assigned
// ID / Kind / Etag / SelfLink).
func createNotifConfig(t *testing.T, base, bucket, topic string, eventTypes []string, prefix string) NotificationConfig {
	t.Helper()

	cfg := NotificationConfig{
		Topic:            topic,
		EventTypes:       eventTypes,
		ObjectNamePrefix: prefix,
	}
	body, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal notif cfg: %v", err)
	}
	resp := putJSON(t, notifConfigsURL(base, bucket), string(body))
	assertStatus(t, resp, 200)

	var created NotificationConfig
	decodeBody(t, resp, &created)
	if created.ID == "" {
		t.Fatalf("expected ID on created config, got %+v", created)
	}
	return created
}

// pullUntil polls the subscription up to timeout, aggregating received
// messages until it has at least `want` OR the deadline elapses.
// Returns all messages received during the polling window.
//
// The fan-out is goroutine-based and bounded by a 5-second Publish timeout
// (internal/gcs/pubsub.go), so a 3-second polling window is ample for
// messages that SHOULD arrive and short enough to detect messages that
// should NOT arrive.
func pullUntil(t *testing.T, sub pubsubpb.SubscriberClient, subscription string, want int, timeout time.Duration) []*pubsubpb.ReceivedMessage {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ctx := context.Background()
	var all []*pubsubpb.ReceivedMessage
	for time.Now().Before(deadline) {
		resp, err := sub.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: subscription,
			MaxMessages:  int32(want + 4), // pull a margin so stale messages don't hide true count
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

// --- Happy-path tests ---

// TestIntegration_GCS_PubSub_ObjectFinalize is the canonical happy-path:
// an object write on a bucket with a matching NotificationConfig produces
// exactly one Pub/Sub message with eventType=OBJECT_FINALIZE, bucketId=<b>,
// and a JSON_API_V1 payload carrying the object metadata.
func TestIntegration_GCS_PubSub_ObjectFinalize(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topic        = "projects/test/topics/finalize-topic"
		subscription = "projects/test/subscriptions/finalize-sub"
		bucketName   = "finalize-bucket"
		objectName   = "hello.txt"
		objectBody   = "hello world"
	)

	createTopicAndSubscription(t, pub, sub, topic, subscription)
	createBucket(t, base, bucketName)
	cfg := createNotifConfig(t, base, bucketName, topic, []string{"OBJECT_FINALIZE", "OBJECT_DELETE"}, "")

	// Trigger OBJECT_FINALIZE via a simple upload.
	obj := simpleUpload(t, base, bucketName, objectName, objectBody)
	if obj.Name != objectName {
		t.Fatalf("uploaded object name = %q, want %q", obj.Name, objectName)
	}

	// Fan-out is fire-and-forget (Rule 3), so poll.
	msgs := pullUntil(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message, got %d", len(msgs))
	}

	m := msgs[0].Message
	// Assert canonical attributes mandated by AAP §0.1.1 Extension B.
	if m.Attributes["eventType"] != "OBJECT_FINALIZE" {
		t.Errorf("attribute eventType = %q, want OBJECT_FINALIZE (attrs: %+v)", m.Attributes["eventType"], m.Attributes)
	}
	if m.Attributes["bucketId"] != bucketName {
		t.Errorf("attribute bucketId = %q, want %q", m.Attributes["bucketId"], bucketName)
	}
	if m.Attributes["objectId"] != objectName {
		t.Errorf("attribute objectId = %q, want %q", m.Attributes["objectId"], objectName)
	}
	if m.Attributes["payloadFormat"] != "JSON_API_V1" {
		t.Errorf("attribute payloadFormat = %q, want JSON_API_V1", m.Attributes["payloadFormat"])
	}
	wantNotifAttr := fmt.Sprintf("projects/_/buckets/%s/notificationConfigs/%s", bucketName, cfg.ID)
	if m.Attributes["notificationConfig"] != wantNotifAttr {
		t.Errorf("attribute notificationConfig = %q, want %q", m.Attributes["notificationConfig"], wantNotifAttr)
	}

	// Assert JSON_API_V1 payload structure: Object metadata.
	if len(m.Data) == 0 {
		t.Fatal("expected non-empty payload, got zero bytes")
	}
	var decoded Object
	if err := json.Unmarshal(m.Data, &decoded); err != nil {
		t.Fatalf("payload is not an Object JSON: %v — raw=%s", err, string(m.Data))
	}
	if decoded.Name != objectName {
		t.Errorf("payload Object.Name = %q, want %q", decoded.Name, objectName)
	}
	if decoded.Bucket != bucketName {
		t.Errorf("payload Object.Bucket = %q, want %q", decoded.Bucket, bucketName)
	}
	if decoded.Kind != "storage#object" {
		t.Errorf("payload Object.Kind = %q, want storage#object", decoded.Kind)
	}
}

// TestIntegration_GCS_PubSub_ObjectDelete asserts that a DELETE on a stored
// object fires exactly one OBJECT_DELETE notification carrying the object
// metadata snapshot captured before deletion.
func TestIntegration_GCS_PubSub_ObjectDelete(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topic        = "projects/test/topics/delete-topic"
		subscription = "projects/test/subscriptions/delete-sub"
		bucketName   = "delete-bucket"
		objectName   = "gone.txt"
	)

	createTopicAndSubscription(t, pub, sub, topic, subscription)
	createBucket(t, base, bucketName)
	createNotifConfig(t, base, bucketName, topic, []string{"OBJECT_FINALIZE", "OBJECT_DELETE"}, "")

	// Upload to have something to delete. Drain the FINALIZE message so
	// the subsequent DELETE assertion is unambiguous.
	simpleUpload(t, base, bucketName, objectName, "goodbye")
	finalizeMsgs := pullUntil(t, sub, subscription, 1, 3*time.Second)
	if len(finalizeMsgs) != 1 {
		t.Fatalf("setup: expected 1 FINALIZE message before DELETE, got %d", len(finalizeMsgs))
	}

	// Delete the object via the GCS JSON API.
	delReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/storage/v1/b/%s/o/%s", base, bucketName, objectName), nil)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE object: %v", err)
	}
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE object status = %d, want 204", delResp.StatusCode)
	}

	msgs := pullUntil(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 DELETE message, got %d", len(msgs))
	}

	m := msgs[0].Message
	if m.Attributes["eventType"] != "OBJECT_DELETE" {
		t.Errorf("attribute eventType = %q, want OBJECT_DELETE", m.Attributes["eventType"])
	}
	if m.Attributes["bucketId"] != bucketName {
		t.Errorf("attribute bucketId = %q, want %q", m.Attributes["bucketId"], bucketName)
	}
	if m.Attributes["objectId"] != objectName {
		t.Errorf("attribute objectId = %q, want %q", m.Attributes["objectId"], objectName)
	}

	// Payload must still be the object metadata snapshot captured before delete.
	if len(m.Data) == 0 {
		t.Fatal("expected non-empty DELETE payload, got zero bytes")
	}
	var decoded Object
	if err := json.Unmarshal(m.Data, &decoded); err != nil {
		t.Fatalf("payload is not an Object JSON: %v", err)
	}
	if decoded.Name != objectName {
		t.Errorf("payload Object.Name = %q, want %q", decoded.Name, objectName)
	}
}

// --- Filter tests ---

// TestIntegration_GCS_PubSub_EventTypeFilter asserts that a NotificationConfig
// with EventTypes=[OBJECT_FINALIZE] does NOT emit a message on DELETE.
//
// This validates the matchesEvent filter in the fan-out path
// (internal/gcs/service.go#matchesEvent).
func TestIntegration_GCS_PubSub_EventTypeFilter(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topic        = "projects/test/topics/filter-topic"
		subscription = "projects/test/subscriptions/filter-sub"
		bucketName   = "filter-bucket"
		objectName   = "only-finalize.txt"
	)

	createTopicAndSubscription(t, pub, sub, topic, subscription)
	createBucket(t, base, bucketName)
	// Filter to FINALIZE only.
	createNotifConfig(t, base, bucketName, topic, []string{"OBJECT_FINALIZE"}, "")

	// Upload (matches FINALIZE filter) then delete (does NOT match).
	simpleUpload(t, base, bucketName, objectName, "data")
	delReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/storage/v1/b/%s/o/%s", base, bucketName, objectName), nil)
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	delResp.Body.Close()

	// Collect messages for the full window; assert exactly one FINALIZE.
	msgs := pullUntil(t, sub, subscription, 2, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message (FINALIZE only), got %d (attrs: %v)",
			len(msgs), attrsOf(msgs))
	}
	if msgs[0].Message.Attributes["eventType"] != "OBJECT_FINALIZE" {
		t.Errorf("expected single message to be OBJECT_FINALIZE, got %q",
			msgs[0].Message.Attributes["eventType"])
	}
}

// TestIntegration_GCS_PubSub_PrefixFilter asserts that ObjectNamePrefix
// filtering at the NotificationConfig level skips non-matching uploads.
func TestIntegration_GCS_PubSub_PrefixFilter(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topic        = "projects/test/topics/prefix-topic"
		subscription = "projects/test/subscriptions/prefix-sub"
		bucketName   = "prefix-bucket"
		matchName    = "photos/2025/cat.jpg"
		skipName     = "docs/readme.txt"
	)

	createTopicAndSubscription(t, pub, sub, topic, subscription)
	createBucket(t, base, bucketName)
	createNotifConfig(t, base, bucketName, topic, nil, "photos/") // nil = all event types

	// Upload one matching, one non-matching.
	simpleUpload(t, base, bucketName, matchName, "img")
	simpleUpload(t, base, bucketName, skipName, "txt")

	// Expect exactly 1 message (the photos/ one), and poll long enough
	// to catch a stray skipName event if it were (incorrectly) delivered.
	msgs := pullUntil(t, sub, subscription, 2, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message (photos/ only), got %d (objectIds: %v)",
			len(msgs), objectIDsOf(msgs))
	}
	if got := msgs[0].Message.Attributes["objectId"]; got != matchName {
		t.Errorf("expected delivered objectId = %q, got %q", matchName, got)
	}
}

// --- Disabled-path test ---

// TestIntegration_GCS_PubSub_EmptyEndpoint_NoDelivery asserts that when
// pubsubAddr is empty (AAP Rule 7a silent skip), object writes succeed
// normally and no Pub/Sub delivery is attempted even if a NotificationConfig
// is present.
//
// We still start a real Pub/Sub service to host the topic+subscription so
// that we can assert no messages were routed through it. GCS is started
// with pubsubAddr="" — the fan-out path is unconditionally skipped.
func TestIntegration_GCS_PubSub_EmptyEndpoint_NoDelivery(t *testing.T) {
	_, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, "") // Explicitly disabled.

	const (
		topic        = "projects/test/topics/noop-topic"
		subscription = "projects/test/subscriptions/noop-sub"
		bucketName   = "noop-bucket"
		objectName   = "never-delivered.txt"
	)

	createTopicAndSubscription(t, pub, sub, topic, subscription)
	createBucket(t, base, bucketName)
	// Register a notif config — with empty pubsubAddr, the fan-out goroutine
	// is never spawned (see fanoutObjectEvent guard in service.go:413).
	createNotifConfig(t, base, bucketName, topic, []string{"OBJECT_FINALIZE"}, "")

	// Upload succeeds as usual.
	simpleUpload(t, base, bucketName, objectName, "data")

	// Poll briefly — expect ZERO messages to arrive (short window since we
	// want to detect the absence, not wait for a delayed delivery).
	msgs := pullUntil(t, sub, subscription, 1, 800*time.Millisecond)
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages (pubsubAddr empty), got %d (attrs: %v)",
			len(msgs), attrsOf(msgs))
	}
}

// --- Payload format and custom attributes ---

// TestIntegration_GCS_PubSub_NonePayloadFormat asserts that when
// PayloadFormat=NONE, the message is still published but with an empty data
// body (GCS convention per AAP Extension B payload rules).
func TestIntegration_GCS_PubSub_NonePayloadFormat(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topic        = "projects/test/topics/none-topic"
		subscription = "projects/test/subscriptions/none-sub"
		bucketName   = "none-bucket"
		objectName   = "x.bin"
	)

	createTopicAndSubscription(t, pub, sub, topic, subscription)
	createBucket(t, base, bucketName)

	// Build a config with payload_format=NONE via raw JSON — the helper
	// default is JSON_API_V1.
	cfgBody := fmt.Sprintf(`{"topic":%q,"payload_format":"NONE","event_types":["OBJECT_FINALIZE"]}`, topic)
	resp := putJSON(t, notifConfigsURL(base, bucketName), cfgBody)
	assertStatus(t, resp, 200)
	var created NotificationConfig
	decodeBody(t, resp, &created)
	if created.PayloadFormat != "NONE" {
		t.Fatalf("expected PayloadFormat=NONE, got %q", created.PayloadFormat)
	}

	simpleUpload(t, base, bucketName, objectName, "ignored-content")

	msgs := pullUntil(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message, got %d", len(msgs))
	}
	m := msgs[0].Message
	if len(m.Data) != 0 {
		t.Errorf("expected empty payload for NONE format, got %d bytes: %q", len(m.Data), string(m.Data))
	}
	if m.Attributes["payloadFormat"] != "NONE" {
		t.Errorf("attribute payloadFormat = %q, want NONE", m.Attributes["payloadFormat"])
	}
	if m.Attributes["eventType"] != "OBJECT_FINALIZE" {
		t.Errorf("attribute eventType = %q, want OBJECT_FINALIZE", m.Attributes["eventType"])
	}
}

// TestIntegration_GCS_PubSub_CustomAttributes asserts user-supplied
// custom_attributes are forwarded alongside the canonical eventType/bucketId
// attributes, without the user's values overriding any canonical key.
func TestIntegration_GCS_PubSub_CustomAttributes(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topic        = "projects/test/topics/custom-topic"
		subscription = "projects/test/subscriptions/custom-sub"
		bucketName   = "custom-bucket"
		objectName   = "obj.txt"
	)

	createTopicAndSubscription(t, pub, sub, topic, subscription)
	createBucket(t, base, bucketName)

	// Custom attributes include one benign key and one that tries to
	// override the canonical "eventType" — the latter must NOT win.
	cfgBody := fmt.Sprintf(`{
  "topic": %q,
  "event_types": ["OBJECT_FINALIZE"],
  "custom_attributes": {
    "team": "platform",
    "eventType": "SHOULD_NOT_WIN"
  }
}`, topic)
	resp := putJSON(t, notifConfigsURL(base, bucketName), cfgBody)
	assertStatus(t, resp, 200)

	simpleUpload(t, base, bucketName, objectName, "x")

	msgs := pullUntil(t, sub, subscription, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	a := msgs[0].Message.Attributes
	if a["team"] != "platform" {
		t.Errorf("expected custom attribute team=platform, got %q", a["team"])
	}
	if a["eventType"] != "OBJECT_FINALIZE" {
		t.Errorf("canonical eventType must not be overridden by custom_attributes; got %q", a["eventType"])
	}
}

// --- Multi-config & isolation tests ---

// TestIntegration_GCS_PubSub_MultipleConfigs asserts that two
// NotificationConfigs on the same bucket — each pointing at a distinct
// topic — both receive the event on a single object write.
func TestIntegration_GCS_PubSub_MultipleConfigs(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topicA = "projects/test/topics/multi-a"
		topicB = "projects/test/topics/multi-b"
		subA   = "projects/test/subscriptions/multi-sub-a"
		subB   = "projects/test/subscriptions/multi-sub-b"
		bkt    = "multi-bucket"
		obj    = "shared.txt"
	)

	createTopicAndSubscription(t, pub, sub, topicA, subA)
	createTopicAndSubscription(t, pub, sub, topicB, subB)
	createBucket(t, base, bkt)
	createNotifConfig(t, base, bkt, topicA, []string{"OBJECT_FINALIZE"}, "")
	createNotifConfig(t, base, bkt, topicB, []string{"OBJECT_FINALIZE"}, "")

	simpleUpload(t, base, bkt, obj, "body")

	msgsA := pullUntil(t, sub, subA, 1, 3*time.Second)
	msgsB := pullUntil(t, sub, subB, 1, 3*time.Second)

	if len(msgsA) != 1 {
		t.Errorf("topicA: expected 1 message, got %d", len(msgsA))
	}
	if len(msgsB) != 1 {
		t.Errorf("topicB: expected 1 message, got %d", len(msgsB))
	}
	if len(msgsA) == 1 && msgsA[0].Message.Attributes["objectId"] != obj {
		t.Errorf("topicA: objectId = %q, want %q", msgsA[0].Message.Attributes["objectId"], obj)
	}
	if len(msgsB) == 1 && msgsB[0].Message.Attributes["objectId"] != obj {
		t.Errorf("topicB: objectId = %q, want %q", msgsB[0].Message.Attributes["objectId"], obj)
	}
}

// TestIntegration_GCS_PubSub_BucketIsolation asserts that a
// NotificationConfig registered on bucket A does NOT fire for uploads to
// bucket B. This validates per-bucket config scoping.
func TestIntegration_GCS_PubSub_BucketIsolation(t *testing.T) {
	pubsubAddr, pub, sub := startPubsubForIntegration(t)
	base := startGCSWithPubsub(t, pubsubAddr)

	const (
		topic    = "projects/test/topics/iso-topic"
		subName  = "projects/test/subscriptions/iso-sub"
		watched  = "watched-bucket"
		ignored  = "ignored-bucket"
		objectNm = "obj.txt"
	)

	createTopicAndSubscription(t, pub, sub, topic, subName)
	createBucket(t, base, watched)
	createBucket(t, base, ignored)
	createNotifConfig(t, base, watched, topic, []string{"OBJECT_FINALIZE"}, "")

	// Upload to the non-watched bucket — should NOT emit.
	simpleUpload(t, base, ignored, objectNm, "shh")

	msgs := pullUntil(t, sub, subName, 1, 800*time.Millisecond)
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages (upload was to unwatched bucket), got %d", len(msgs))
	}

	// Now upload to the watched bucket — should emit exactly one.
	simpleUpload(t, base, watched, objectNm, "observed")

	msgs = pullUntil(t, sub, subName, 1, 3*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message from watched bucket, got %d", len(msgs))
	}
	if got := msgs[0].Message.Attributes["bucketId"]; got != watched {
		t.Errorf("attribute bucketId = %q, want %q", got, watched)
	}
}

// --- Diagnostic helpers ---

// attrsOf returns a concise representation of the per-message eventType
// attributes for use in test-failure messages.
func attrsOf(msgs []*pubsubpb.ReceivedMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, fmt.Sprintf("%s/%s", m.Message.Attributes["eventType"], m.Message.Attributes["objectId"]))
	}
	return out
}

// objectIDsOf returns the objectId attribute for each message.
func objectIDsOf(msgs []*pubsubpb.ReceivedMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Message.Attributes["objectId"])
	}
	return out
}

