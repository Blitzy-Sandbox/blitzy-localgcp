//go:build integration

// Package logging — integration_gcs_sink_test.go
//
// End-to-end integration coverage for AAP §0.5.1.2 Extension D's GCS sink
// delivery path, satisfying AAP Rule 9 which mandates a dedicated integration
// test for each cross-service wiring path (Logging → GCS in this file;
// Logging → Pub/Sub in integration_pubsub_sink_test.go).
//
// Topology: this file stands up BOTH a live Cloud Logging gRPC service AND a
// live Cloud Storage (GCS) HTTP service in the same process — mirroring the
// single-binary localgcp topology — wires the Logging service to the GCS
// service via SetGcsEndpoint BEFORE calling Start, creates a sink whose
// Destination is "storage.googleapis.com/{bucket}", calls WriteLogEntries,
// and asserts that the expected object lands in the bucket.
//
// Object naming is deterministic per internal/logging/sink_delivery.go:
//
//     objectName = "{shortSink}/{YYYY-MM-DD}/{20060102T150405.000000000Z}-{insertId}.json"
//
// where:
//   * shortSink = last "/"-separated segment of sink.Name
//                 (e.g. "happy-sink" from "projects/p1/sinks/happy-sink")
//   * YYYY-MM-DD and timestamp-with-nanoseconds derive from entry.Timestamp
//                 (when set) or from sink_delivery.go's Now() variable (when
//                 entry has zero Timestamp)
//   * insertId  = entry.InsertId (non-empty) or fmt.Sprintf("%d", ts.UnixNano())
//                 (when empty)
//
// Payload is protojson.Marshal(entry) — round-trippable back into a
// *loggingpb.LogEntry via protojson.Unmarshal. Content-Type is
// "application/json".
//
// The fan-out is fire-and-forget per-(entry, sink) pair (Rule 3): the
// WriteLogEntries RPC returns BEFORE the per-sink goroutine dials the GCS
// upload endpoint. Tests therefore use a bounded retry loop
// (`pollGCSObjectsUntilN`) rather than assuming immediate availability,
// mirroring the pattern used by the Pub/Sub sink integration test.
//
// Build tag: these tests are compiled only when `go test` is invoked with
// `-tags integration`, per AAP §0.7.4 Gate 8.

package logging

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

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/slokam-ai/localgcp/internal/gcs"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	loggingtypepb "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// Integration topology helpers (GCS-specific — Pub/Sub helpers live in
// integration_pubsub_sink_test.go; both files are `package logging` so they
// share package scope but use disjoint helper/test names).
// ---------------------------------------------------------------------------

// startGCSForLoggingSink launches an in-memory, quiet Cloud Storage HTTP
// service on an ephemeral localhost port. Returns the host:port string
// suitable for passing to SetGcsEndpoint on a Logging service, plus the
// full base URL (e.g. "http://localhost:12345") usable by the test as a
// REST client to assert that object uploads actually landed.
//
// The readiness probe issues GET on the root path. Any response — including
// 404 — proves the HTTP server is accepting requests.
func startGCSForLoggingSink(t *testing.T) (hostPort, baseURL string) {
	t.Helper()

	svc := gcs.New("", true)

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("gcs listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	hostPort = fmt.Sprintf("localhost:%d", port)
	baseURL = "http://" + hostPort

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = svc.Start(ctx, hostPort) }()

	for i := 0; i < 50; i++ {
		resp, err := http.Get(baseURL + "/")
		if err == nil {
			resp.Body.Close()
			return hostPort, baseURL
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("integration: gcs server did not start")
	return "", ""
}

// createGCSBucketForSink issues a POST /storage/v1/b?project=test request
// to create a bucket on the in-process GCS service. Fails the test on any
// non-2xx response.
func createGCSBucketForSink(t *testing.T, baseURL, bucketName string) {
	t.Helper()

	body, err := json.Marshal(map[string]string{"name": bucketName})
	if err != nil {
		t.Fatalf("marshal bucket payload: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/storage/v1/b?project=test", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build create-bucket request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create-bucket %q: %v", bucketName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		t.Fatalf("create-bucket %q: status=%d body=%s", bucketName, resp.StatusCode, snippet)
	}
}

// listGCSObjectsForSink returns the list of objects currently in the bucket.
// A nil slice is returned (not an error) when the bucket exists but has no
// objects. Fatally fails on any HTTP or decode error.
func listGCSObjectsForSink(t *testing.T, baseURL, bucketName string) []gcs.Object {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/storage/v1/b/%s/o", baseURL, bucketName), nil)
	if err != nil {
		t.Fatalf("build list-objects request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list-objects %q: %v", bucketName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		t.Fatalf("list-objects %q: status=%d body=%s", bucketName, resp.StatusCode, snippet)
	}

	var list gcs.ObjectList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode object list: %v", err)
	}
	return list.Items
}

// downloadGCSObjectForSink fetches the raw object bytes via the GCS
// /download/storage/v1/b/{bucket}/o/{object}?alt=media path. The object
// name is URL-path-escaped so characters like "/" in the object name are
// preserved as real path separators, matching the GCS API contract.
func downloadGCSObjectForSink(t *testing.T, baseURL, bucketName, objectName string) []byte {
	t.Helper()

	// The GCS download URL includes the object name verbatim in the path.
	// Object names contain "/" which MUST be preserved as path separators.
	// url.PathEscape would escape them; instead we build the path with
	// path.Join-style concatenation of already-percent-encoded segments.
	//
	// In practice localgcp's router accepts the raw object name because
	// handleDownload uses strings.TrimPrefix on the path rather than
	// path-segment routing — so a plain string concatenation works.
	dlURL := fmt.Sprintf("%s/download/storage/v1/b/%s/o/%s?alt=media",
		baseURL, bucketName, objectName)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dlURL, nil)
	if err != nil {
		t.Fatalf("build download request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("download %q/%q: %v", bucketName, objectName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		t.Fatalf("download %q/%q: status=%d body=%s", bucketName, objectName, resp.StatusCode, snippet)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return body
}

// pollGCSObjectsUntilN polls ListObjects until it sees at least `want` objects
// in the bucket or the timeout expires. Returns the final snapshot.
//
// This mirrors pullUntilN from integration_pubsub_sink_test.go: the fan-out
// goroutine dispatches the HTTP upload AFTER the WriteLogEntries RPC has
// returned to the caller, so tests cannot assume immediate availability.
func pollGCSObjectsUntilN(t *testing.T, baseURL, bucketName string, want int, timeout time.Duration) []gcs.Object {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var items []gcs.Object
	for time.Now().Before(deadline) {
		items = listGCSObjectsForSink(t, baseURL, bucketName)
		if len(items) >= want {
			return items
		}
		time.Sleep(50 * time.Millisecond)
	}
	return items
}

// expectedGCSObjectName computes the object name that uploadEntryToGcs will
// use for an entry with a fixed Timestamp of time.Unix(1700000000, 0).UTC(),
// given the sink short-name and insertId. This mirrors the exact formatting
// used by internal/logging/sink_delivery.go.
//
// The fixed timestamp is:
//
//	2023-11-14 22:13:20 UTC  →  "2023-11-14"
//	                          →  "20231114T221320.000000000Z"
//
// so a sink named "happy-sink" with insertId "e-001" yields:
//
//	"happy-sink/2023-11-14/20231114T221320.000000000Z-e-001.json"
//
// This function is used only in the deterministic Delivery test; tests that
// don't care about the exact name use pollGCSObjectsUntilN + ListObjects
// counts instead.
func expectedGCSObjectName(shortSink, insertID string) string {
	ts := time.Unix(1700000000, 0).UTC()
	return fmt.Sprintf("%s/%s/%s-%s.json",
		shortSink,
		ts.Format("2006-01-02"),
		ts.Format("20060102T150405.000000000Z"),
		insertID,
	)
}

// fetchAndGuard verifies that the object name is free of characters that
// would confuse the URL parser, then downloads and returns the content.
// Object names produced by uploadEntryToGcs contain only characters that
// are valid in an HTTP URL path — letters, digits, "/", "-", "." and ":"
// — so no percent-encoding is required. This helper is a single call-site
// wrapper that centralises the guard.
func fetchAndGuard(t *testing.T, baseURL, bucketName, objectName string) []byte {
	t.Helper()
	if strings.Contains(objectName, "?") {
		t.Fatalf("unexpected ? in object name %q — parsing would misinterpret as query string", objectName)
	}
	return downloadGCSObjectForSink(t, baseURL, bucketName, objectName)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestIntegration_Logging_GCSSink_Delivery is the happy-path end-to-end
// assertion for the Logging → GCS wiring required by AAP Rule 9.
//
// It creates a sink whose Destination is "storage.googleapis.com/{bucket}",
// calls WriteLogEntries with one entry carrying a fixed Timestamp and a
// known InsertId, waits for the fan-out goroutine to upload, then asserts:
//
//	(1) The bucket has exactly one object.
//	(2) The object's Name equals the deterministic expected name derived
//	    from the sink short-name, the fixed timestamp, and the InsertId.
//	(3) The object's content bytes round-trip back into a *loggingpb.LogEntry
//	    whose LogName, InsertId, Severity, and TextPayload match the entry
//	    that was sent.
func TestIntegration_Logging_GCSSink_Delivery(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "happy-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	// Empty pubsubAddr — this test exercises only the GCS branch.
	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	parent := "projects/p1"
	sinkShort := "happy-sink"
	destination := "storage.googleapis.com/" + bucket
	createSinkHelper(t, cfgClient, parent, sinkShort, destination, "")

	// writeEntryHelper sets entry.Timestamp = time.Unix(1700000000, 0).UTC()
	// so the object name is deterministic.
	entry := writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "hello gcs sink", "e-001")

	items := pollGCSObjectsUntilN(t, baseURL, bucket, 1, 3*time.Second)
	if len(items) != 1 {
		t.Fatalf("expected 1 object in bucket %q, got %d: %+v", bucket, len(items), items)
	}

	wantName := expectedGCSObjectName(sinkShort, "e-001")
	if items[0].Name != wantName {
		t.Errorf("object name: got %q, want %q", items[0].Name, wantName)
	}
	if items[0].Bucket != bucket {
		t.Errorf("object bucket: got %q, want %q", items[0].Bucket, bucket)
	}

	payload := fetchAndGuard(t, baseURL, bucket, items[0].Name)

	var decoded loggingpb.LogEntry
	if err := protojson.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal(payload): %v (payload=%s)", err, payload)
	}
	if decoded.GetLogName() != entry.GetLogName() {
		t.Errorf("decoded.LogName: got %q, want %q", decoded.GetLogName(), entry.GetLogName())
	}
	if decoded.GetInsertId() != "e-001" {
		t.Errorf("decoded.InsertId: got %q, want %q", decoded.GetInsertId(), "e-001")
	}
	if decoded.GetSeverity() != loggingtypepb.LogSeverity_INFO {
		t.Errorf("decoded.Severity: got %v, want INFO", decoded.GetSeverity())
	}
	if decoded.GetTextPayload() != "hello gcs sink" {
		t.Errorf("decoded.TextPayload: got %q, want %q", decoded.GetTextPayload(), "hello gcs sink")
	}
}

// TestIntegration_Logging_GCSSink_MultipleSinks wires TWO sinks at TWO
// different bucket destinations. A single WriteLogEntries call with one
// entry must produce ONE object in EACH bucket. This proves per-sink
// goroutine fan-out actually parallelises across sinks.
func TestIntegration_Logging_GCSSink_MultipleSinks(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucketA := "bucket-a"
	bucketB := "bucket-b"
	createGCSBucketForSink(t, baseURL, bucketA)
	createGCSBucketForSink(t, baseURL, bucketB)

	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "sink-a", "storage.googleapis.com/"+bucketA, "")
	createSinkHelper(t, cfgClient, parent, "sink-b", "storage.googleapis.com/"+bucketB, "")

	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "multi-sink payload", "multi-1")

	// Each sink fan-out is an independent goroutine. Poll each bucket.
	gotA := pollGCSObjectsUntilN(t, baseURL, bucketA, 1, 3*time.Second)
	gotB := pollGCSObjectsUntilN(t, baseURL, bucketB, 1, 3*time.Second)

	if len(gotA) != 1 {
		t.Errorf("bucket %q object count: got %d, want 1", bucketA, len(gotA))
	}
	if len(gotB) != 1 {
		t.Errorf("bucket %q object count: got %d, want 1", bucketB, len(gotB))
	}

	// Both objects must refer to the same entry (same InsertId decoded).
	for _, pair := range []struct {
		bucket string
		items  []gcs.Object
		sink   string
	}{
		{bucketA, gotA, "sink-a"},
		{bucketB, gotB, "sink-b"},
	} {
		if len(pair.items) == 0 {
			continue
		}
		want := expectedGCSObjectName(pair.sink, "multi-1")
		if pair.items[0].Name != want {
			t.Errorf("bucket %q: object name got %q, want %q", pair.bucket, pair.items[0].Name, want)
		}
	}
}

// TestIntegration_Logging_GCSSink_MultipleEntriesToOneSink issues ONE
// WriteLogEntries RPC carrying THREE entries, all routed to a single sink.
// Three separate goroutines must fire, producing three objects. Entry
// timestamps are the fixed helper timestamp so object names are predictable.
func TestIntegration_Logging_GCSSink_MultipleEntriesToOneSink(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "bulk-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	parent := "projects/p1"
	sinkShort := "bulk-sink"
	createSinkHelper(t, cfgClient, parent, sinkShort, "storage.googleapis.com/"+bucket, "")

	// Build 3 entries in one WriteLogEntries request. Use the helper fixed
	// timestamp so object names are deterministic.
	fixedTS := timestamppb.New(time.Unix(1700000000, 0).UTC())
	entries := make([]*loggingpb.LogEntry, 0, 3)
	for i := 0; i < 3; i++ {
		entries = append(entries, &loggingpb.LogEntry{
			LogName: "projects/p1/logs/bulk",
			Resource: &monitoredrespb.MonitoredResource{
				Type:   "global",
				Labels: map[string]string{"project_id": "test"},
			},
			Severity:  loggingtypepb.LogSeverity_INFO,
			InsertId:  fmt.Sprintf("bulk-%d", i),
			Timestamp: fixedTS,
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

	items := pollGCSObjectsUntilN(t, baseURL, bucket, 3, 3*time.Second)
	if len(items) != 3 {
		t.Fatalf("expected 3 objects, got %d: %+v", len(items), items)
	}

	// All three InsertIds must be represented exactly once.
	seen := map[string]bool{}
	for _, obj := range items {
		for i := 0; i < 3; i++ {
			want := expectedGCSObjectName(sinkShort, fmt.Sprintf("bulk-%d", i))
			if obj.Name == want {
				if seen[want] {
					t.Errorf("duplicate object for %q", want)
				}
				seen[want] = true
			}
		}
	}
	for i := 0; i < 3; i++ {
		want := expectedGCSObjectName(sinkShort, fmt.Sprintf("bulk-%d", i))
		if !seen[want] {
			t.Errorf("missing object for InsertId bulk-%d (wanted name %q)", i, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Filter tests
// ---------------------------------------------------------------------------

// TestIntegration_Logging_GCSSink_SeverityFilter asserts that
// severity>=ERROR causes INFO entries to be filtered out, exercising the
// MatchingSinks → matchesFilter → severityGTE path with a GCS-destined sink.
func TestIntegration_Logging_GCSSink_SeverityFilter(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "sev-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	parent := "projects/p1"
	sinkShort := "sev-sink"
	createSinkHelper(t, cfgClient, parent, sinkShort,
		"storage.googleapis.com/"+bucket, "severity>=ERROR")

	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "below filter", "info-id")
	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "ERROR", "should deliver", "error-id")

	items := pollGCSObjectsUntilN(t, baseURL, bucket, 1, 3*time.Second)
	if len(items) != 1 {
		t.Fatalf("expected exactly 1 object (ERROR only), got %d: %+v", len(items), items)
	}
	want := expectedGCSObjectName(sinkShort, "error-id")
	if items[0].Name != want {
		t.Errorf("object name: got %q, want %q", items[0].Name, want)
	}

	// Round-trip the payload to assert severity == ERROR.
	payload := downloadGCSObjectForSink(t, baseURL, bucket, items[0].Name)
	var decoded loggingpb.LogEntry
	if err := protojson.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	if decoded.GetSeverity() != loggingtypepb.LogSeverity_ERROR {
		t.Errorf("decoded.Severity: got %v, want ERROR", decoded.GetSeverity())
	}
}

// TestIntegration_Logging_GCSSink_LogNameFilter asserts the `logName="..."`
// EXACT-match filter dialect: a sink bound to one logName must NOT receive
// entries from different logNames.
func TestIntegration_Logging_GCSSink_LogNameFilter(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "logname-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	parent := "projects/p1"
	sinkShort := "logname-sink"
	targetLog := "projects/p1/logs/app"
	createSinkHelper(t, cfgClient, parent, sinkShort,
		"storage.googleapis.com/"+bucket, `logName="`+targetLog+`"`)

	// Only the `app` log should match; the other two logs must be filtered.
	writeEntryHelper(t, logClient, "projects/p1/logs/other", "INFO", "other", "other-id")
	writeEntryHelper(t, logClient, targetLog, "INFO", "match", "match-id")
	writeEntryHelper(t, logClient, "projects/p1/logs/different", "INFO", "different", "diff-id")

	items := pollGCSObjectsUntilN(t, baseURL, bucket, 1, 3*time.Second)
	if len(items) != 1 {
		t.Fatalf("expected exactly 1 object (logName match only), got %d: %+v", len(items), items)
	}
	want := expectedGCSObjectName(sinkShort, "match-id")
	if items[0].Name != want {
		t.Errorf("object name: got %q, want %q", items[0].Name, want)
	}
}

// TestIntegration_Logging_GCSSink_EmptyFilterMatchesAll proves that when
// sink.Filter == "" every entry matches — MatchingSinks' fast-path at
// store.go:256 (`sink.Filter == "" || matchesFilter(entry, sink.Filter)`).
func TestIntegration_Logging_GCSSink_EmptyFilterMatchesAll(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "allpass-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	parent := "projects/p1"
	sinkShort := "allpass-sink"
	createSinkHelper(t, cfgClient, parent, sinkShort,
		"storage.googleapis.com/"+bucket, "")

	writeEntryHelper(t, logClient, "projects/p1/logs/a", "DEBUG", "debug msg", "d-id")
	writeEntryHelper(t, logClient, "projects/p1/logs/a", "INFO", "info msg", "i-id")
	writeEntryHelper(t, logClient, "projects/p1/logs/a", "EMERGENCY", "emerg msg", "e-id")

	items := pollGCSObjectsUntilN(t, baseURL, bucket, 3, 3*time.Second)
	if len(items) != 3 {
		t.Fatalf("expected 3 objects (empty filter matches all), got %d: %+v", len(items), items)
	}

	// Assert each InsertId is represented exactly once.
	wants := []string{
		expectedGCSObjectName(sinkShort, "d-id"),
		expectedGCSObjectName(sinkShort, "i-id"),
		expectedGCSObjectName(sinkShort, "e-id"),
	}
	got := map[string]bool{}
	for _, o := range items {
		got[o.Name] = true
	}
	for _, w := range wants {
		if !got[w] {
			t.Errorf("missing expected object %q", w)
		}
	}
}

// ---------------------------------------------------------------------------
// Negative-path tests
// ---------------------------------------------------------------------------

// TestIntegration_Logging_GCSSink_EmptyEndpoint_NoDelivery exercises Rule 7a:
// when the Logging service is started with an empty gcsAddr, the GCS branch
// of deliverToSink silently skips (via uploadEntryToGcs's gcsAddr == ""
// guard at sink_delivery.go:145). The bucket stays empty after a generous
// wait window.
func TestIntegration_Logging_GCSSink_EmptyEndpoint_NoDelivery(t *testing.T) {
	// Start a real GCS service so the bucket is reachable for list/read,
	// but pass an empty gcsAddr to the Logging service so its delivery
	// path is disabled.
	_, baseURL := startGCSForLoggingSink(t)
	bucket := "empty-endpoint-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	// Critical: gcsAddr="" here disables delivery even though a GCS service
	// IS running in this process.
	logClient, cfgClient := startLoggingWithEndpoints(t, "", "")

	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "silent-sink",
		"storage.googleapis.com/"+bucket, "")

	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "should not be delivered", "ignore-id")

	// Poll for 800ms to give any stray goroutine a chance to fire.
	items := pollGCSObjectsUntilN(t, baseURL, bucket, 1, 800*time.Millisecond)
	if len(items) != 0 {
		t.Errorf("expected 0 objects with empty gcsAddr, got %d: %+v", len(items), items)
	}
}

// TestIntegration_Logging_GCSSink_UnsupportedScheme exercises the default
// arm of deliverToSink's switch — when the destination starts with neither
// "pubsub.googleapis.com/" nor "storage.googleapis.com/" the router
// silently returns without calling any delivery function. This provides
// future-safe behaviour for BigQuery, Loki, Splunk, etc. sinks that
// localgcp does not yet implement.
func TestIntegration_Logging_GCSSink_UnsupportedScheme(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "unused-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	// Destination uses a non-canonical prefix. Despite gcsAddr being set,
	// the router MUST NOT upload anything to GCS because the scheme is
	// unrecognised.
	parent := "projects/p1"
	createSinkHelper(t, cfgClient, parent, "bq-sink",
		"bigquery.googleapis.com/projects/p1/datasets/ds/tables/tbl", "")

	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "bigquery-destined", "bq-id")

	items := pollGCSObjectsUntilN(t, baseURL, bucket, 1, 500*time.Millisecond)
	if len(items) != 0 {
		t.Errorf("expected 0 objects for unsupported scheme, got %d: %+v", len(items), items)
	}
}

// TestIntegration_Logging_GCSSink_NoSinks confirms that when no sinks are
// registered, WriteLogEntries is a no-op for the fan-out path. The bucket
// stays empty; no delivery goroutine fires.
func TestIntegration_Logging_GCSSink_NoSinks(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "nosinks-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	logClient, _ := startLoggingWithEndpoints(t, "", gcsAddr)

	writeEntryHelper(t, logClient,
		"projects/p1/logs/app", "INFO", "no sinks configured", "nop-id")

	items := pollGCSObjectsUntilN(t, baseURL, bucket, 1, 500*time.Millisecond)
	if len(items) != 0 {
		t.Errorf("expected 0 objects with no sinks registered, got %d: %+v", len(items), items)
	}
}

// TestIntegration_Logging_GCSSink_WriteLogEntriesUnblocked is the Rule 3
// timing assertion: the WriteLogEntries RPC MUST return well before the
// per-sink fan-out goroutine completes its GCS upload. We assert the RPC
// elapses in under 1s (generous margin; real path is microseconds) and then
// verify eventual delivery within 3s.
func TestIntegration_Logging_GCSSink_WriteLogEntriesUnblocked(t *testing.T) {
	gcsAddr, baseURL := startGCSForLoggingSink(t)
	bucket := "unblock-bucket"
	createGCSBucketForSink(t, baseURL, bucket)

	logClient, cfgClient := startLoggingWithEndpoints(t, "", gcsAddr)

	parent := "projects/p1"
	sinkShort := "unblock-sink"
	createSinkHelper(t, cfgClient, parent, sinkShort,
		"storage.googleapis.com/"+bucket, "")

	entry := &loggingpb.LogEntry{
		LogName: "projects/p1/logs/app",
		Resource: &monitoredrespb.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": "test"},
		},
		Severity:  loggingtypepb.LogSeverity_INFO,
		InsertId:  "unblock-id",
		Timestamp: timestamppb.New(time.Unix(1700000000, 0).UTC()),
		Payload:   &loggingpb.LogEntry_TextPayload{TextPayload: "timing-sensitive"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	if _, err := logClient.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{entry},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Errorf("WriteLogEntries took %v — should return promptly (<1s) per Rule 3", elapsed)
	}

	// After the RPC returns, eventual delivery must still happen.
	items := pollGCSObjectsUntilN(t, baseURL, bucket, 1, 3*time.Second)
	if len(items) != 1 {
		t.Fatalf("expected 1 object after RPC return, got %d: %+v", len(items), items)
	}
	want := expectedGCSObjectName(sinkShort, "unblock-id")
	if items[0].Name != want {
		t.Errorf("object name: got %q, want %q", items[0].Name, want)
	}
}
