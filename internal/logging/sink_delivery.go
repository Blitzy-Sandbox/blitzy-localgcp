// Package logging — sink_delivery.go implements the two fire-and-forget
// loopback delivery paths used by the WriteLogEntries sink fan-out:
//
//   - Pub/Sub: destinations are routed via a short-lived gRPC client to
//     pubsubAddr. Two equivalent schemes are accepted:
//     `pubsub://projects/{p}/topics/{t}` (the canonical short-form
//     specified by AAP §0.1.1) and
//     `pubsub.googleapis.com/projects/{p}/topics/{t}` (the long-form that
//     matches real Cloud Logging sink URIs).
//   - GCS: `storage.googleapis.com/{bucket}` destinations are routed via a
//     short-lived HTTP POST (simple-upload) to gcsAddr.
//
// Both helpers are consumed only by fan-out goroutines spawned from
// WriteLogEntries. They are no-ops when the corresponding endpoint address
// is empty (Rule 7a silent skip) and never block the caller's request
// (Rule 3).
//
// Delivery failures are logged to stderr via the Service's logger field and
// are NEVER surfaced to the WriteLogEntries caller.
package logging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/encoding/protojson"
)

// deliveryTimeout caps the total time spent delivering a single entry to
// one sink destination. The fan-out goroutine is fire-and-forget; a short
// bounded timeout prevents hung peers from leaking goroutines.
const deliveryTimeout = 5 * time.Second

// pubsubDestinationPrefix identifies Pub/Sub sink destinations in their
// real-Cloud-Logging long form. Any Sink whose Destination begins with
// this prefix is routed through the Pub/Sub loopback gRPC client. The
// suffix is expected to be of the form "projects/{project}/topics/{topic}".
const pubsubDestinationPrefix = "pubsub.googleapis.com/"

// pubsubShortScheme identifies Pub/Sub sink destinations in the canonical
// short form specified by AAP §0.1.1. Any Sink whose Destination begins
// with this scheme is routed through the Pub/Sub loopback gRPC client.
// The suffix is expected to be of the form
// "projects/{project}/topics/{topic}". This form is semantically
// equivalent to pubsubDestinationPrefix; both are accepted so that
// emulator users can write either the short URI (per the AAP spec) or
// the long-form URI (matching real sink destinations).
const pubsubShortScheme = "pubsub://"

// gcsDestinationPrefix identifies Cloud Storage sink destinations. Any Sink
// with Destination beginning with this prefix is routed through the GCS
// loopback HTTP client. The suffix is expected to be of the form
// "{bucket}" (a plain bucket name) — log entries are uploaded as JSON
// objects with timestamp-keyed names.
const gcsDestinationPrefix = "storage.googleapis.com/"

// parsePubsubDestination strips the Pub/Sub destination prefix (either
// the canonical pubsub:// short scheme or the pubsub.googleapis.com/
// long-form) and returns the canonical topic resource name
// "projects/.../topics/...". Returns empty string if the destination
// does not match either expected form.
func parsePubsubDestination(dest string) string {
	if strings.HasPrefix(dest, pubsubShortScheme) {
		return strings.TrimPrefix(dest, pubsubShortScheme)
	}
	if strings.HasPrefix(dest, pubsubDestinationPrefix) {
		return strings.TrimPrefix(dest, pubsubDestinationPrefix)
	}
	return ""
}

// parseGcsDestination strips the storage.googleapis.com/ prefix and
// returns the bucket name. Returns empty string if the destination does
// not match the expected shape.
func parseGcsDestination(dest string) string {
	if !strings.HasPrefix(dest, gcsDestinationPrefix) {
		return ""
	}
	return strings.TrimPrefix(dest, gcsDestinationPrefix)
}

// publishEntryToPubsub opens a short-lived gRPC client to pubsubAddr and
// publishes one log entry to the topic carried in sink.Destination.
//
// The entry is serialized as canonical JSON via protojson so downstream
// subscribers can round-trip it back to *loggingpb.LogEntry. The published
// message carries attributes {logName, severity, sinkName} for filtering.
//
// Returns the first error encountered. Callers MUST route errors to stderr
// only — they must never be surfaced to the WriteLogEntries RPC caller
// (Rule 3).
func publishEntryToPubsub(pubsubAddr string, sink Sink, entry *loggingpb.LogEntry) error {
	if pubsubAddr == "" {
		return nil
	}
	topic := parsePubsubDestination(sink.Destination)
	if topic == "" {
		// Not a Pub/Sub destination — caller routed us wrongly; silently
		// skip rather than error so adding new schemes later is safe.
		return nil
	}

	payload, err := protojson.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}

	attrs := map[string]string{
		"logName":  entry.GetLogName(),
		"severity": entry.GetSeverity().String(),
		"sinkName": sink.Name,
	}

	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()

	conn, err := grpc.NewClient(pubsubAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", pubsubAddr, err)
	}
	defer conn.Close()

	client := pubsubpb.NewPublisherClient(conn)
	if _, err := client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic,
		Messages: []*pubsubpb.PubsubMessage{
			{
				Data:       payload,
				Attributes: attrs,
			},
		},
	}); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

// uploadEntryToGcs opens a short-lived HTTP client to gcsAddr and uploads
// one log entry as a JSON object into the bucket carried in
// sink.Destination.
//
// Object names are of the form
// "{sink-short-name}/{yyyy-mm-dd}/{entry-timestamp}-{insertId}.json" so
// entries are sharded by sink and day, which matches the behavior of the
// real Cloud Logging-to-GCS sink export. When the entry has no Timestamp
// the current wall-clock time is used.
//
// Returns the first error encountered. Callers MUST route errors to stderr
// only.
func uploadEntryToGcs(gcsAddr string, sink Sink, entry *loggingpb.LogEntry) error {
	if gcsAddr == "" {
		return nil
	}
	bucket := parseGcsDestination(sink.Destination)
	if bucket == "" {
		return nil
	}

	payload, err := protojson.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}

	// Build a deterministic object name: {sink-short}/{YYYY-MM-DD}/{ts}-{id}.json
	//
	// Defensive zero-time handling: (*timestamppb.Timestamp).AsTime() is
	// nil-safe but when the receiver is nil (i.e. the LogEntry had no
	// Timestamp field) the returned value is time.Unix(0, 0).UTC() — a
	// *valid* non-zero Go time representing the Unix epoch (1970-01-01
	// 00:00:00 UTC). Go's time.Time zero value (time.Time{}, detected by
	// IsZero()) is distinct from the Unix epoch, so a bare IsZero check
	// would let nil-timestamp entries shard into a bogus 1970-01-01
	// bucket. Treat any of the following as "missing timestamp" and fall
	// back to the current wall-clock time: the proto pointer is nil, the
	// Go time is the zero value, or the time is exactly the Unix epoch
	// (no LogEntry in practice carries a legitimate 1970-01-01 timestamp
	// and the real Cloud Logging API rejects such values outright).
	ts := entry.GetTimestamp().AsTime()
	if entry.GetTimestamp() == nil || ts.IsZero() || ts.Unix() == 0 {
		ts = Now()
	}
	shortSink := sink.Name
	if idx := strings.LastIndex(shortSink, "/"); idx >= 0 {
		shortSink = shortSink[idx+1:]
	}
	insertID := entry.GetInsertId()
	if insertID == "" {
		insertID = fmt.Sprintf("%d", ts.UnixNano())
	}
	objectName := fmt.Sprintf("%s/%s/%s-%s.json",
		shortSink,
		ts.UTC().Format("2006-01-02"),
		ts.UTC().Format("20060102T150405.000000000Z"),
		insertID,
	)

	// GCS simple-upload URL: POST /upload/storage/v1/b/{bucket}/o?uploadType=media&name=...
	//
	// Both the bucket path segment and the object name query parameter are
	// escaped to survive arbitrary characters in sink destinations and
	// insert IDs (forward slashes in objectName, reserved URL characters
	// in bucket names). PathEscape preserves forward slashes in query
	// values where appropriate; QueryEscape is used for the "name"
	// parameter because that is a query parameter, not a path segment.
	reqURL := fmt.Sprintf("http://%s/upload/storage/v1/b/%s/o?uploadType=media&name=%s",
		gcsAddr, url.PathEscape(bucket), url.QueryEscape(objectName),
	)

	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("upload to %s: %w", bucket, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		// Best-effort read of the response body for diagnostics.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("upload to %s returned %d: %s", bucket, resp.StatusCode, string(body))
	}
	return nil
}

// deliverToSink dispatches one log entry to one sink destination. This is
// the single entry-point used by the WriteLogEntries fan-out goroutine. It
// routes by destination scheme and translates the result into a stderr log
// line on failure, never propagating errors.
//
// This helper is exported-internal (lowercase) because it is only meant to
// be invoked from WriteLogEntries' goroutine fan-out. Sink delivery MUST
// NOT block the WriteLogEntries RPC (Rule 3).
func deliverToSink(pubsubAddr, gcsAddr string, sink Sink, entry *loggingpb.LogEntry) {
	var err error
	switch {
	case strings.HasPrefix(sink.Destination, pubsubShortScheme),
		strings.HasPrefix(sink.Destination, pubsubDestinationPrefix):
		err = publishEntryToPubsub(pubsubAddr, sink, entry)
	case strings.HasPrefix(sink.Destination, gcsDestinationPrefix):
		err = uploadEntryToGcs(gcsAddr, sink, entry)
	default:
		// Unsupported destination scheme — silently skip. A future
		// extension may add BigQuery / Loki / etc.
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[logging] sink delivery failed for sink=%q dest=%q: %v\n",
			sink.Name, sink.Destination, err,
		)
	}
}
