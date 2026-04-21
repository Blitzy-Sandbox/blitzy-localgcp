package logging

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxEntries is the maximum number of log entries retained in memory.
const maxEntries = 10000

// Sink is an in-memory representation of a Cloud Logging sink. The fields are
// a subset of google.logging.v2.LogSink — only those required by AAP §0.5.1.2
// Extension D (CreateSink / GetSink / UpdateSink / DeleteSink / ListSinks) are
// materialised. A sink's Destination MUST be one of the two supported schemes:
//
//   - "pubsub.googleapis.com/projects/{project}/topics/{topic}" — routed via
//     loopback Pub/Sub gRPC at pubsubAddr.
//   - "storage.googleapis.com/{bucket}" — routed via loopback HTTP PUT at
//     gcsAddr.
//
// The fan-out path is goroutine-based and fire-and-forget: delivery failures
// are logged to stderr and never returned to the WriteLogEntries caller
// (Rule 3).
type Sink struct {
	// Name is the fully-qualified resource name, e.g. "projects/p/sinks/s".
	Name string
	// Destination is the sink target. Must start with "pubsub.googleapis.com/"
	// or "storage.googleapis.com/" to route correctly; other schemes are
	// accepted but silently produce no delivery.
	Destination string
	// Filter is an advanced logs filter. Empty string means "match every
	// entry". The simple filter dialect supported by matchesFilter is reused.
	Filter string
	// WriterIdentity is returned to clients but never verified — the emulator
	// has no IAM enforcement.
	WriterIdentity string
	// CreateTime is set on first create and never mutated.
	CreateTime *timestamppb.Timestamp
	// UpdateTime is refreshed on every Update.
	UpdateTime *timestamppb.Timestamp
}

// Store is the in-memory log entry and sink registry.
type Store struct {
	mu      sync.RWMutex
	entries []*loggingpb.LogEntry

	// sinks is keyed by the fully-qualified sink name,
	// e.g. "projects/my-project/sinks/my-sink".
	sinks map[string]*Sink
}

func NewStore() *Store {
	return &Store{
		sinks: make(map[string]*Sink),
	}
}

// Write appends log entries to the store, trimming if over capacity.
func (s *Store) Write(entries []*loggingpb.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
	if len(s.entries) > maxEntries {
		s.entries = s.entries[len(s.entries)-maxEntries:]
	}
}

// List returns log entries matching the filter for the given resource names.
// Supports simple filters: severity>=LEVEL, textPayload contains, logName match.
func (s *Store) List(resourceNames []string, filter string, pageSize int) []*loggingpb.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	var result []*loggingpb.LogEntry
	for _, entry := range s.entries {
		if !matchesResources(entry, resourceNames) {
			continue
		}
		if filter != "" && !matchesFilter(entry, filter) {
			continue
		}
		result = append(result, entry)
		if len(result) >= pageSize {
			break
		}
	}
	return result
}

// ListLogs returns distinct log names.
func (s *Store) ListLogs(parent string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]bool)
	for _, entry := range s.entries {
		if strings.HasPrefix(entry.GetLogName(), parent+"/") || parent == "" {
			seen[entry.GetLogName()] = true
		}
	}

	var logs []string
	for name := range seen {
		logs = append(logs, name)
	}
	sort.Strings(logs)
	return logs
}

// DeleteLog removes all entries for a given log name.
func (s *Store) DeleteLog(logName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var remaining []*loggingpb.LogEntry
	for _, entry := range s.entries {
		if entry.GetLogName() != logName {
			remaining = append(remaining, entry)
		}
	}
	s.entries = remaining
}

// --- Sink operations ---

// CreateSink inserts a sink into the registry. Returns an error if the sink
// already exists (mirroring the Cloud Logging API's ALREADY_EXISTS semantics).
// The caller is responsible for supplying a fully-qualified Name of the form
// "projects/{project}/sinks/{sink}" — the store does not synthesise or
// validate the format beyond using Name as the map key.
func (s *Store) CreateSink(sink Sink) (*Sink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sink.Name == "" {
		return nil, fmt.Errorf("sink name is required")
	}
	if _, exists := s.sinks[sink.Name]; exists {
		return nil, fmt.Errorf("sink %q already exists", sink.Name)
	}

	now := timestamppb.New(Now())
	// Defensive copy to avoid caller mutation after insert.
	entry := sink
	entry.CreateTime = now
	entry.UpdateTime = now
	// Supply a non-empty writer identity by default so SDK clients that expect
	// the field to echo back non-empty are satisfied. Real IAM semantics are
	// out of scope (AAP §0.6.2).
	if entry.WriterIdentity == "" {
		entry.WriterIdentity = "serviceAccount:localgcp-emulator@localgcp.iam.gserviceaccount.com"
	}
	s.sinks[sink.Name] = &entry

	// Return a defensive copy so the caller cannot mutate the stored value
	// after the lock is released.
	out := entry
	return &out, nil
}

// GetSink returns a copy of the sink with the given name, or (nil, false) if
// it does not exist.
func (s *Store) GetSink(name string) (*Sink, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sink, ok := s.sinks[name]
	if !ok {
		return nil, false
	}
	out := *sink
	return &out, true
}

// UpdateSink replaces the Destination, Filter and WriterIdentity fields of an
// existing sink; CreateTime is preserved and UpdateTime is refreshed. Returns
// an error if the sink does not exist.
func (s *Store) UpdateSink(sink Sink) (*Sink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sink.Name == "" {
		return nil, fmt.Errorf("sink name is required")
	}
	existing, ok := s.sinks[sink.Name]
	if !ok {
		return nil, fmt.Errorf("sink %q not found", sink.Name)
	}

	existing.Destination = sink.Destination
	existing.Filter = sink.Filter
	if sink.WriterIdentity != "" {
		existing.WriterIdentity = sink.WriterIdentity
	}
	existing.UpdateTime = timestamppb.New(Now())

	out := *existing
	return &out, nil
}

// DeleteSink removes a sink. Returns false if the sink did not exist, so
// callers can surface the Cloud Logging NOT_FOUND semantics if required.
func (s *Store) DeleteSink(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sinks[name]; !ok {
		return false
	}
	delete(s.sinks, name)
	return true
}

// ListSinks returns all sinks whose Name begins with parent+"/sinks/". Empty
// parent returns all sinks. The response is sorted by Name for deterministic
// paging.
func (s *Store) ListSinks(parent string) []Sink {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := ""
	if parent != "" {
		prefix = parent + "/sinks/"
	}

	out := make([]Sink, 0, len(s.sinks))
	for _, sink := range s.sinks {
		if prefix == "" || strings.HasPrefix(sink.Name, prefix) {
			out = append(out, *sink)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// MatchingSinks returns a snapshot of sinks whose Filter (if any) matches the
// given log entry. The returned slice is a defensive copy, so callers can
// safely iterate and spawn delivery goroutines without holding the store
// lock. Entries whose Filter is empty match every log entry. This is the
// fan-out hook used by WriteLogEntries (AAP §0.5.1.2 Extension D).
func (s *Store) MatchingSinks(entry *loggingpb.LogEntry) []Sink {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Sink, 0, len(s.sinks))
	for _, sink := range s.sinks {
		if sink.Filter == "" || matchesFilter(entry, sink.Filter) {
			out = append(out, *sink)
		}
	}
	return out
}

// matchesResources checks if a log entry's resource matches any of the resource names.
func matchesResources(entry *loggingpb.LogEntry, resourceNames []string) bool {
	if len(resourceNames) == 0 {
		return true
	}
	logName := entry.GetLogName()
	for _, rn := range resourceNames {
		if strings.HasPrefix(logName, rn+"/") || strings.HasPrefix(logName, "projects/"+rn+"/") {
			return true
		}
	}
	return false
}

// matchesFilter applies a simple filter to a log entry.
// Supports: severity>=LEVEL, textPayload:"substring", logName="name".
func matchesFilter(entry *loggingpb.LogEntry, filter string) bool {
	parts := strings.Fields(filter)
	for _, part := range parts {
		if strings.HasPrefix(part, "severity>=") {
			level := strings.TrimPrefix(part, "severity>=")
			if !severityGTE(entry.GetSeverity().String(), level) {
				return false
			}
		} else if strings.Contains(part, ":") {
			kv := strings.SplitN(part, ":", 2)
			if kv[0] == "textPayload" {
				text := entry.GetTextPayload()
				search := strings.Trim(kv[1], "\"")
				if !strings.Contains(text, search) {
					return false
				}
			}
		} else if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			if kv[0] == "logName" {
				if entry.GetLogName() != strings.Trim(kv[1], "\"") {
					return false
				}
			}
		}
	}
	return true
}

var severityOrder = map[string]int{
	"DEFAULT":   0,
	"DEBUG":     100,
	"INFO":      200,
	"NOTICE":    300,
	"WARNING":   400,
	"ERROR":     500,
	"CRITICAL":  600,
	"ALERT":     700,
	"EMERGENCY": 800,
}

func severityGTE(entrySev, filterSev string) bool {
	return severityOrder[entrySev] >= severityOrder[filterSev]
}

// Now returns the current time — used for testing seams.
var Now = time.Now
