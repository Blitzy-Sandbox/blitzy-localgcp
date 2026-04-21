// Package gcs — notifications_test.go
//
// Unit coverage for the three NotificationConfig HTTP handlers wired by
// the AAP §0.5.1.2 Extension B (GCS → Pub/Sub notifications):
//
//   PUT  /storage/v1/b/{bucket}/notificationConfigs       — create
//   POST /storage/v1/b/{bucket}/notificationConfigs       — create (alternate)
//   GET  /storage/v1/b/{bucket}/notificationConfigs       — list
//   GET  /storage/v1/b/{bucket}/notificationConfigs/{id}  — get
//   DELETE /storage/v1/b/{bucket}/notificationConfigs/{id} — delete
//
// The tests use `testServer(t)` + the shared `postJSON` / `assertStatus` /
// `decodeBody` helpers from gcs_test.go. No call-site changes are made in
// gcs_test.go itself (Rule 7a preservation).
//
// These are unit-level tests — they exercise the HTTP handlers and the
// store interaction, NOT the loopback Pub/Sub fan-out path (which is
// exercised by internal/gcs/integration_pubsub_test.go with the
// //go:build integration tag in Group 4(e)).

package gcs

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// --- Helpers local to the notifications tests ---

// putJSON sends a PUT request with a JSON body and returns the response.
// Matches the shape of postJSON in gcs_test.go.
func putJSON(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest PUT %s: %v", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	return resp
}

// deleteReq sends a DELETE request and returns the response.
func deleteReq(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		t.Fatalf("NewRequest DELETE %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", url, err)
	}
	return resp
}

// methodReq sends a request with a caller-specified method (for 405 tests).
func methodReq(t *testing.T, method, url, body string) *http.Response {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatalf("NewRequest %s %s: %v", method, url, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// createBucket is a small helper for the notifications tests that pre-creates
// a bucket before exercising the notification endpoints. It asserts 200 OK.
func createBucket(t *testing.T, base, name string) {
	t.Helper()
	resp := postJSON(t, base+"/storage/v1/b?project=test",
		fmt.Sprintf(`{"name":%q}`, name))
	assertStatus(t, resp, 200)
	resp.Body.Close()
}

// notifConfigsURL returns the collection-level URL.
func notifConfigsURL(base, bucket string) string {
	return fmt.Sprintf("%s/storage/v1/b/%s/notificationConfigs", base, bucket)
}

// notifConfigURL returns the item-level URL.
func notifConfigURL(base, bucket, id string) string {
	return fmt.Sprintf("%s/storage/v1/b/%s/notificationConfigs/%s", base, bucket, id)
}

// --- Create handler tests ---

// TestNotification_Create_PUT_Success verifies the happy-path PUT of a new
// NotificationConfig to an existing bucket.
func TestNotification_Create_PUT_Success(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "notif-put")

	body := `{"topic":"projects/test/topics/my-topic","event_types":["OBJECT_FINALIZE","OBJECT_DELETE"]}`
	resp := putJSON(t, notifConfigsURL(base, "notif-put"), body)
	assertStatus(t, resp, 200)

	var got NotificationConfig
	decodeBody(t, resp, &got)

	if got.ID == "" {
		t.Fatalf("expected server-assigned ID, got empty")
	}
	if got.Kind != "storage#notification" {
		t.Fatalf("expected kind 'storage#notification', got %q", got.Kind)
	}
	if got.Topic != "projects/test/topics/my-topic" {
		t.Fatalf("expected topic preserved, got %q", got.Topic)
	}
	if len(got.EventTypes) != 2 || got.EventTypes[0] != "OBJECT_FINALIZE" || got.EventTypes[1] != "OBJECT_DELETE" {
		t.Fatalf("expected EventTypes [OBJECT_FINALIZE,OBJECT_DELETE], got %v", got.EventTypes)
	}
	// PayloadFormat defaults to JSON_API_V1 when not supplied.
	if got.PayloadFormat != "JSON_API_V1" {
		t.Fatalf("expected default PayloadFormat 'JSON_API_V1', got %q", got.PayloadFormat)
	}
	if got.Etag == "" {
		t.Fatalf("expected non-empty Etag")
	}
	if !strings.Contains(got.SelfLink, "/storage/v1/b/notif-put/notificationConfigs/") {
		t.Fatalf("expected SelfLink to include bucket+id, got %q", got.SelfLink)
	}
}

// TestNotification_Create_POST_AlsoSucceeds verifies that POST works as an
// alternate method on the collection endpoint (GCS accepts both in the
// handler router).
func TestNotification_Create_POST_AlsoSucceeds(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "notif-post")

	body := `{"topic":"projects/test/topics/t2"}`
	resp := postJSON(t, notifConfigsURL(base, "notif-post"), body)
	assertStatus(t, resp, 200)

	var got NotificationConfig
	decodeBody(t, resp, &got)
	if got.Topic != "projects/test/topics/t2" {
		t.Fatalf("expected topic preserved, got %q", got.Topic)
	}
	if got.ID == "" {
		t.Fatalf("expected server-assigned ID")
	}
}

// TestNotification_Create_MissingBucket expects a 404 when the target bucket
// does not exist. The handler must check bucket existence BEFORE decoding
// the body.
func TestNotification_Create_MissingBucket(t *testing.T) {
	base := testServer(t)

	body := `{"topic":"projects/test/topics/t"}`
	resp := putJSON(t, notifConfigsURL(base, "does-not-exist"), body)
	assertStatus(t, resp, 404)

	var errResp gcpError
	decodeBody(t, resp, &errResp)
	if errResp.Error.Code != 404 {
		t.Fatalf("expected error.code 404, got %d", errResp.Error.Code)
	}
	if !strings.Contains(errResp.Error.Message, "does-not-exist") {
		t.Fatalf("expected bucket name in error message, got %q", errResp.Error.Message)
	}
}

// TestNotification_Create_MissingTopic expects a 400 when the required
// `topic` field is absent. The handler returns the canonical message
// "topic is required (projects/{project}/topics/{topic})" (service.go:341).
func TestNotification_Create_MissingTopic(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "no-topic")

	// Valid JSON but no topic.
	body := `{"event_types":["OBJECT_FINALIZE"]}`
	resp := putJSON(t, notifConfigsURL(base, "no-topic"), body)
	assertStatus(t, resp, 400)

	var errResp gcpError
	decodeBody(t, resp, &errResp)
	if errResp.Error.Code != 400 {
		t.Fatalf("expected error.code 400, got %d", errResp.Error.Code)
	}
	if !strings.Contains(errResp.Error.Message, "topic is required") {
		t.Fatalf("expected 'topic is required' message, got %q", errResp.Error.Message)
	}
}

// TestNotification_Create_InvalidJSON expects a 400 with the canonical
// "Invalid JSON body" message when the body cannot be decoded.
func TestNotification_Create_InvalidJSON(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "bad-json")

	// Malformed JSON.
	body := `{"topic": "oops`
	resp := putJSON(t, notifConfigsURL(base, "bad-json"), body)
	assertStatus(t, resp, 400)

	var errResp gcpError
	decodeBody(t, resp, &errResp)
	if !strings.Contains(errResp.Error.Message, "Invalid JSON body") {
		t.Fatalf("expected 'Invalid JSON body', got %q", errResp.Error.Message)
	}
}

// TestNotification_Create_ExplicitPayloadFormat verifies that when a caller
// supplies PayloadFormat (e.g. "NONE"), the server preserves it without
// overriding to the default.
func TestNotification_Create_ExplicitPayloadFormat(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "explicit-fmt")

	body := `{"topic":"projects/test/topics/t","payload_format":"NONE"}`
	resp := putJSON(t, notifConfigsURL(base, "explicit-fmt"), body)
	assertStatus(t, resp, 200)

	var got NotificationConfig
	decodeBody(t, resp, &got)
	if got.PayloadFormat != "NONE" {
		t.Fatalf("expected PayloadFormat 'NONE' preserved, got %q", got.PayloadFormat)
	}
}

// TestNotification_Create_UniqueIDs ensures that two successive Create calls
// on the same bucket produce distinct IDs.
func TestNotification_Create_UniqueIDs(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "uniq")

	body := `{"topic":"projects/test/topics/t"}`
	resp1 := putJSON(t, notifConfigsURL(base, "uniq"), body)
	assertStatus(t, resp1, 200)
	var a NotificationConfig
	decodeBody(t, resp1, &a)

	resp2 := putJSON(t, notifConfigsURL(base, "uniq"), body)
	assertStatus(t, resp2, 200)
	var b NotificationConfig
	decodeBody(t, resp2, &b)

	if a.ID == "" || b.ID == "" {
		t.Fatalf("expected non-empty IDs, got %q and %q", a.ID, b.ID)
	}
	if a.ID == b.ID {
		t.Fatalf("expected distinct IDs, got duplicate %q", a.ID)
	}
}

// --- Get handler tests ---

// TestNotification_Get_Success verifies a round-trip Create -> Get.
func TestNotification_Get_Success(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "get-ok")

	// Create first.
	resp := putJSON(t, notifConfigsURL(base, "get-ok"), `{"topic":"projects/test/topics/t"}`)
	assertStatus(t, resp, 200)
	var created NotificationConfig
	decodeBody(t, resp, &created)

	// Now GET by id.
	getResp, err := http.Get(notifConfigURL(base, "get-ok", created.ID))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, getResp, 200)
	var got NotificationConfig
	decodeBody(t, getResp, &got)

	if got.ID != created.ID {
		t.Fatalf("expected ID %q, got %q", created.ID, got.ID)
	}
	if got.Topic != created.Topic {
		t.Fatalf("expected Topic preserved, got %q", got.Topic)
	}
}

// TestNotification_Get_MissingBucket expects 404 when the bucket does not
// exist.
func TestNotification_Get_MissingBucket(t *testing.T) {
	base := testServer(t)

	resp, err := http.Get(notifConfigURL(base, "no-bucket", "1"))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, resp, 404)

	var errResp gcpError
	decodeBody(t, resp, &errResp)
	if !strings.Contains(errResp.Error.Message, "no-bucket") {
		t.Fatalf("expected bucket name in error, got %q", errResp.Error.Message)
	}
}

// TestNotification_Get_MissingNotification expects 404 when the bucket
// exists but the notification id does not.
func TestNotification_Get_MissingNotification(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "exists")

	resp, err := http.Get(notifConfigURL(base, "exists", "999"))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, resp, 404)

	var errResp gcpError
	decodeBody(t, resp, &errResp)
	if !strings.Contains(errResp.Error.Message, "exists/999") {
		t.Fatalf("expected 'exists/999' in message, got %q", errResp.Error.Message)
	}
}

// --- List handler tests ---

// TestNotification_List_Empty verifies that a bucket with no configs returns
// 200 and an empty Items array (NOT null, per the handler's defensive
// empty-slice allocation at service.go:377).
func TestNotification_List_Empty(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "empty-list")

	resp, err := http.Get(notifConfigsURL(base, "empty-list"))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, resp, 200)

	var list NotificationList
	decodeBody(t, resp, &list)
	if list.Kind != "storage#notifications" {
		t.Fatalf("expected kind 'storage#notifications', got %q", list.Kind)
	}
	if list.Items == nil {
		t.Fatalf("expected non-nil Items slice, got nil")
	}
	if len(list.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(list.Items))
	}
}

// TestNotification_List_Multiple verifies that List returns all created
// configs, sorted by ID. The store sorts by ID (store.go:256) so the order
// must match insertion order when IDs are assigned sequentially.
func TestNotification_List_Multiple(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "multi")

	// Create three configs.
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf(`{"topic":"projects/test/topics/t%d"}`, i)
		resp := putJSON(t, notifConfigsURL(base, "multi"), body)
		assertStatus(t, resp, 200)
		resp.Body.Close()
	}

	resp, err := http.Get(notifConfigsURL(base, "multi"))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, resp, 200)

	var list NotificationList
	decodeBody(t, resp, &list)
	if list.Kind != "storage#notifications" {
		t.Fatalf("expected kind 'storage#notifications', got %q", list.Kind)
	}
	if len(list.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(list.Items))
	}
	// Verify each has a non-empty id + preserved topic.
	for i, it := range list.Items {
		if it.ID == "" {
			t.Fatalf("item %d has empty ID", i)
		}
		want := fmt.Sprintf("projects/test/topics/t%d", i)
		if it.Topic != want {
			t.Fatalf("item %d topic want %q got %q", i, want, it.Topic)
		}
	}
}

// TestNotification_List_MissingBucket expects 404 when the bucket does not
// exist, distinguishing it from "bucket exists but no configs".
func TestNotification_List_MissingBucket(t *testing.T) {
	base := testServer(t)

	resp, err := http.Get(notifConfigsURL(base, "no-such-bucket"))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, resp, 404)

	var errResp gcpError
	decodeBody(t, resp, &errResp)
	if !strings.Contains(errResp.Error.Message, "no-such-bucket") {
		t.Fatalf("expected bucket name in error, got %q", errResp.Error.Message)
	}
}

// --- Delete handler tests ---

// TestNotification_Delete_Success verifies 204 on delete and a subsequent
// Get returns 404.
func TestNotification_Delete_Success(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "del-ok")

	// Create.
	resp := putJSON(t, notifConfigsURL(base, "del-ok"), `{"topic":"projects/test/topics/t"}`)
	assertStatus(t, resp, 200)
	var created NotificationConfig
	decodeBody(t, resp, &created)

	// Delete.
	dresp := deleteReq(t, notifConfigURL(base, "del-ok", created.ID))
	assertStatus(t, dresp, 204)
	dresp.Body.Close()

	// Verify gone.
	gresp, err := http.Get(notifConfigURL(base, "del-ok", created.ID))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, gresp, 404)
}

// TestNotification_Delete_MissingBucket expects 404. The handler delegates
// to DeleteNotification which returns "not found: bucket" (store.go:267);
// the handler matches on "not found" substring and emits 404.
func TestNotification_Delete_MissingBucket(t *testing.T) {
	base := testServer(t)

	resp := deleteReq(t, notifConfigURL(base, "ghost", "1"))
	assertStatus(t, resp, 404)
}

// TestNotification_Delete_MissingNotification expects 404 when the bucket
// exists but the id does not.
func TestNotification_Delete_MissingNotification(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "del-missing")

	resp := deleteReq(t, notifConfigURL(base, "del-missing", "404"))
	assertStatus(t, resp, 404)

	var errResp gcpError
	decodeBody(t, resp, &errResp)
	if !strings.Contains(errResp.Error.Message, "del-missing/404") {
		t.Fatalf("expected 'del-missing/404' in message, got %q", errResp.Error.Message)
	}
}

// --- Router / method-not-allowed tests ---

// TestNotification_NestedPathReturns404 ensures the handler rejects
// requests with extra path segments after the id. The router explicitly
// returns 404 for nested paths (service.go:204).
func TestNotification_NestedPathReturns404(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "nested")

	resp, err := http.Get(notifConfigURL(base, "nested", "123") + "/extra")
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, resp, 404)
}

// TestNotification_MethodNotAllowed_Collection verifies that unsupported
// methods on the collection endpoint return 405.
func TestNotification_MethodNotAllowed_Collection(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "mna-coll")

	// PATCH is not supported on the collection endpoint.
	resp := methodReq(t, http.MethodPatch, notifConfigsURL(base, "mna-coll"),
		`{"topic":"projects/test/topics/t"}`)
	assertStatus(t, resp, 405)
}

// TestNotification_MethodNotAllowed_Item verifies that PUT/POST/PATCH are
// rejected on item-level URLs (only GET and DELETE are supported —
// service.go:207-213).
func TestNotification_MethodNotAllowed_Item(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "mna-item")

	// Create one config so the id path resolves.
	cresp := putJSON(t, notifConfigsURL(base, "mna-item"),
		`{"topic":"projects/test/topics/t"}`)
	assertStatus(t, cresp, 200)
	var created NotificationConfig
	decodeBody(t, cresp, &created)

	// PUT on item endpoint: 405.
	putResp := methodReq(t, http.MethodPut,
		notifConfigURL(base, "mna-item", created.ID),
		`{"topic":"projects/test/topics/other"}`)
	assertStatus(t, putResp, 405)

	// POST on item endpoint: 405.
	postResp := methodReq(t, http.MethodPost,
		notifConfigURL(base, "mna-item", created.ID),
		`{"topic":"projects/test/topics/other"}`)
	assertStatus(t, postResp, 405)

	// PATCH on item endpoint: 405.
	patchResp := methodReq(t, http.MethodPatch,
		notifConfigURL(base, "mna-item", created.ID),
		`{"topic":"projects/test/topics/other"}`)
	assertStatus(t, patchResp, 405)
}

// --- End-to-end round-trip ---

// TestNotification_RoundTrip exercises Create -> List -> Get -> Delete ->
// List (empty) -> Get (404) as a single flow, confirming that store state
// transitions correctly across all handlers.
func TestNotification_RoundTrip(t *testing.T) {
	base := testServer(t)
	createBucket(t, base, "rt")

	// 1. Create.
	resp := putJSON(t, notifConfigsURL(base, "rt"),
		`{"topic":"projects/test/topics/rt","event_types":["OBJECT_FINALIZE"]}`)
	assertStatus(t, resp, 200)
	var created NotificationConfig
	decodeBody(t, resp, &created)

	// 2. List shows 1 item.
	lresp, err := http.Get(notifConfigsURL(base, "rt"))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, lresp, 200)
	var list NotificationList
	decodeBody(t, lresp, &list)
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 item after create, got %d", len(list.Items))
	}
	if list.Items[0].ID != created.ID {
		t.Fatalf("listed id %q does not match created id %q", list.Items[0].ID, created.ID)
	}

	// 3. Get round-trip.
	gresp, err := http.Get(notifConfigURL(base, "rt", created.ID))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, gresp, 200)
	var got NotificationConfig
	decodeBody(t, gresp, &got)
	if got.Topic != "projects/test/topics/rt" {
		t.Fatalf("round-trip topic mismatch: %q", got.Topic)
	}

	// 4. Delete.
	dresp := deleteReq(t, notifConfigURL(base, "rt", created.ID))
	assertStatus(t, dresp, 204)
	dresp.Body.Close()

	// 5. List is empty again.
	lresp2, err := http.Get(notifConfigsURL(base, "rt"))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, lresp2, 200)
	var emptyList NotificationList
	decodeBody(t, lresp2, &emptyList)
	if len(emptyList.Items) != 0 {
		t.Fatalf("expected 0 items after delete, got %d", len(emptyList.Items))
	}

	// 6. Get after delete returns 404.
	gresp2, err := http.Get(notifConfigURL(base, "rt", created.ID))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, gresp2, 404)
}

// --- Store-level unit tests (no HTTP) ---
//
// These exercise the store API in isolation to guarantee the handlers'
// underlying behavior is tested even if the HTTP routing changes. They
// are kept minimal — the primary test surface is the HTTP round-trip
// above.

// TestStore_CreateNotification_MissingBucket verifies the store returns a
// "not found" error on a bucket that was never created.
func TestStore_CreateNotification_MissingBucket(t *testing.T) {
	s := NewStore("")
	_, err := s.CreateNotification("ghost", NotificationConfig{
		Topic: "projects/test/topics/t",
	})
	if err == nil {
		t.Fatalf("expected error on missing bucket, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' in err, got %q", err.Error())
	}
}

// TestStore_NotificationsForBucket_Snapshot verifies the fan-out helper
// returns a snapshot copy that is safe to read without holding the lock.
// The helper is called from goroutines in the object-write fan-out path.
func TestStore_NotificationsForBucket_Snapshot(t *testing.T) {
	s := NewStore("")
	if _, err := s.CreateBucket("snap", "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := s.CreateNotification("snap", NotificationConfig{
		Topic:      "projects/test/topics/t",
		EventTypes: []string{"OBJECT_FINALIZE"},
	}); err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}

	snap := s.NotificationsForBucket("snap")
	if len(snap) != 1 {
		t.Fatalf("expected 1 config in snapshot, got %d", len(snap))
	}
	if snap[0].Topic != "projects/test/topics/t" {
		t.Fatalf("snapshot topic mismatch: %q", snap[0].Topic)
	}

	// Mutating the snapshot must not affect store state.
	snap[0].Topic = "projects/test/topics/hacked"
	gotList, ok := s.ListNotifications("snap")
	if !ok {
		t.Fatal("expected bucket to exist")
	}
	if gotList[0].Topic != "projects/test/topics/t" {
		t.Fatalf("snapshot mutation leaked into store: %q", gotList[0].Topic)
	}
}

// TestStore_NotificationsForBucket_MissingBucket returns a nil/empty slice
// (not an error) per the fan-out helper's contract — the helper is invoked
// on every object write and the bucket is always expected to exist, but a
// defensive empty result avoids a nil panic.
func TestStore_NotificationsForBucket_MissingBucket(t *testing.T) {
	s := NewStore("")
	snap := s.NotificationsForBucket("ghost")
	if len(snap) != 0 {
		t.Fatalf("expected empty snapshot for missing bucket, got %d items", len(snap))
	}
}

// --- Rule 7a silent-skip anchor test (AAP §0.7.1.8) ---

// TestNoNotificationDeliveryWhenPubsubAddrEmpty verifies AAP Rule 7a: when
// the GCS service is constructed with an empty pubsubAddr (which is
// exactly what testServer(t) produces via New("", true) — see
// gcs_test.go), the notification fan-out path MUST be a complete no-op.
// Object uploads AND deletes must succeed without any attempt at network
// activity and without blocking on a peer Pub/Sub service.
//
// This is the unit-level counterpart to integration_pubsub_test.go — it
// exercises the same handler path (handleSimpleUpload → fanoutObjectEvent)
// but WITHOUT any Pub/Sub peer available. If the fan-out were synchronous
// or required a peer, this test would hang, timeout, or fail the upload.
//
// The test also registers a notification config on the target bucket to
// prove that the silent-skip short-circuit is in fanoutObjectEvent itself
// (pubsubAddr check in service.go), NOT in the "no matching configs" path —
// a regression that moved the check below the config iteration would allow
// an accidental goroutine leak or dial attempt even with pubsubAddr empty.
func TestNoNotificationDeliveryWhenPubsubAddrEmpty(t *testing.T) {
	base := testServer(t) // uses New("", true) → pubsubAddr is empty
	createBucket(t, base, "silent-bucket")

	// Register a notification config on the bucket. The CRUD handlers
	// accept the config regardless of pubsubAddr state — only the
	// fan-out delivery path is gated by pubsubAddr.
	cresp := putJSON(t, notifConfigsURL(base, "silent-bucket"),
		`{"topic":"projects/test/topics/noop"}`)
	assertStatus(t, cresp, http.StatusOK)
	var created NotificationConfig
	decodeBody(t, cresp, &created)
	if created.ID == "" {
		t.Fatalf("expected created config to have id, got %+v", created)
	}

	// Upload an object. This MUST succeed — the OBJECT_FINALIZE fan-out
	// must be silently skipped, not attempted against a non-existent peer.
	// If fan-out were synchronous, this call would either hang (dial
	// timeout) or return an error from the handler.
	uploadURL := base + "/upload/storage/v1/b/silent-bucket/o?uploadType=media&name=quiet.txt"
	resp, err := http.Post(uploadURL, "text/plain", strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("upload status=%d body=%s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Delete the object to exercise the OBJECT_DELETE fan-out path.
	// Same silent-skip contract: DELETE returns 204 No Content and the
	// goroutine-dispatched delivery is bypassed entirely.
	dreq, err := http.NewRequest(http.MethodDelete,
		base+"/storage/v1/b/silent-bucket/o/quiet.txt", nil)
	if err != nil {
		t.Fatalf("NewRequest DELETE: %v", err)
	}
	dresp, err := http.DefaultClient.Do(dreq)
	if err != nil {
		t.Fatalf("DELETE object: %v", err)
	}
	if dresp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(dresp.Body)
		dresp.Body.Close()
		t.Fatalf("DELETE object status=%d body=%s", dresp.StatusCode, body)
	}
	dresp.Body.Close()
}
