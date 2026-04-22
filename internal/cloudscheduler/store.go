// Package cloudscheduler — store.go implements the foundational in-memory
// state layer for the Cloud Scheduler emulator.
//
// This file defines the Job record, the thread-safe Store, and the sentinel
// errors consumed by service.go to map to gRPC status codes. Jobs are keyed
// by their fully-qualified resource name:
//
//	projects/{project}/locations/{location}/jobs/{jobId}
//
// When constructed with a non-empty dataDir, the Store transparently
// persists its state to "{dataDir}/cloudscheduler/state.json" using the
// atomic temp-file + rename pattern shared with the sibling cloudtasks
// package. Missing or corrupt snapshots are recovered-from by starting with
// an empty store — this mirrors the "best-effort" persistence convention
// used throughout the other native services.
package cloudscheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
)

// ErrNotFound is returned when a job lookup fails for Get, Update, Delete,
// Pause, or Resume. Callers may use errors.Is to test for this sentinel so
// that the mapping layer in service.go can produce codes.NotFound.
var ErrNotFound = errors.New("cloudscheduler: job not found")

// ErrAlreadyExists is returned when Create is invoked with a Job.Name that
// collides with an existing job. Callers may use errors.Is to test for this
// sentinel so that the mapping layer in service.go can produce
// codes.AlreadyExists.
var ErrAlreadyExists = errors.New("cloudscheduler: job already exists")

// Job is the in-memory representation of a Cloud Scheduler job.
//
// All mutating access to Job fields MUST be serialized through the Store's
// methods (which hold the Store's sync.RWMutex). Direct mutation of a
// pointer returned from Get is permitted only when the caller is willing to
// follow up with an Update call — otherwise the change will not be visible
// to concurrent readers under the store's locking discipline.
//
// The JSON struct tags drive the wire format for the on-disk snapshot.
// Zero-value time.Time fields are omitted via omitempty so snapshot files
// do not carry meaningless timestamps on freshly-created jobs.
type Job struct {
	// Name is the fully qualified resource name:
	//   projects/{project}/locations/{location}/jobs/{jobId}
	Name string `json:"name"`

	// Description is a user-provided free-form description (optional).
	Description string `json:"description,omitempty"`

	// Schedule is a 5-field standard cron expression
	// (minute hour dom month dow). Validated at Create/Update time with
	// cron.ParseStandard before being written into the Store.
	Schedule string `json:"schedule"`

	// TimeZone is reserved for future use. The emulator always runs in the
	// host's local time zone per AAP §0.6.2 (time zones other than the host's
	// local time zone are explicitly out of scope).
	TimeZone string `json:"timeZone,omitempty"`

	// State is the scheduler state machine value. Valid values:
	//   - schedulerpb.Job_ENABLED  (1)
	//   - schedulerpb.Job_PAUSED   (2)
	// Jobs are conventionally created in ENABLED state by the service layer;
	// PauseJob transitions to PAUSED, ResumeJob transitions back to ENABLED.
	State schedulerpb.Job_State `json:"state"`

	// HTTPTarget is set for HTTP-delivered jobs. Exactly one of HTTPTarget
	// and PubsubTarget is non-nil per schedulerpb oneof semantics; this is
	// enforced by the service layer at validation time.
	HTTPTarget *schedulerpb.HttpTarget `json:"httpTarget,omitempty"`

	// PubsubTarget is set for Pub/Sub-delivered jobs.
	PubsubTarget *schedulerpb.PubsubTarget `json:"pubsubTarget,omitempty"`

	// UserUpdateTime is the wall-clock time of the most recent Create or
	// Update RPC. Exposed to the wire as schedulerpb.Job.user_update_time.
	UserUpdateTime time.Time `json:"userUpdateTime,omitempty"`

	// LastAttemptTime is the wall-clock time of the most recent dispatch
	// attempt (whether from a cron tick or a RunJob call). Exposed to the
	// wire as schedulerpb.Job.last_attempt_time.
	LastAttemptTime time.Time `json:"lastAttemptTime,omitempty"`

	// ScheduleTime is a convenience field for "next scheduled run" — set by
	// the cron runner for observability. Reserved for future expansion.
	ScheduleTime time.Time `json:"scheduleTime,omitempty"`
}

// Store holds all scheduler jobs in memory, keyed by fully-qualified name.
//
// Thread-safety: all public methods acquire the RWMutex appropriately
// (RLock for read methods, Lock for mutating methods). Callers MUST NOT
// reach through the Store directly to the jobs map — doing so would race
// with all other public method calls.
type Store struct {
	mu      sync.RWMutex
	jobs    map[string]*Job
	dataDir string // empty when persistence is disabled
}

// NewStore constructs an empty Store.
//
// When dataDir is non-empty the store attempts to load a prior snapshot
// from "{dataDir}/cloudscheduler/state.json"; corrupt or missing snapshots
// are treated as empty state (non-fatal) so that a single flaky snapshot
// file never prevents the scheduler service from starting.
//
// When dataDir is empty the store operates purely in-memory and all
// persistence hooks (persistLocked / load) become no-ops.
func NewStore(dataDir string) *Store {
	s := &Store{
		jobs:    make(map[string]*Job),
		dataDir: dataDir,
	}
	if dataDir != "" {
		// load() runs synchronously inside NewStore before any other
		// goroutine can hold a reference to the Store, so no lock
		// acquisition is required here. Best-effort: a corrupt or missing
		// snapshot yields an empty store rather than a startup failure.
		_ = s.load()
	}
	return s
}

// Create inserts a new Job. Returns ErrAlreadyExists if a job with the
// given Name already resides in the store. On success the job is persisted
// to disk (when persistence is enabled). Any persistence error propagates
// to the caller so the service layer can surface it as codes.Internal.
func (s *Store) Create(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.Name]; ok {
		return ErrAlreadyExists
	}
	s.jobs[j.Name] = j
	return s.persistLocked()
}

// Get retrieves a Job by fully-qualified name. Returns ErrNotFound if no
// such job exists.
//
// The returned pointer aliases the Store's internal state — callers MUST
// NOT mutate it for any observable change. For mutation, the canonical
// idiom is: Get + copy + field assignment + Update. Tests that simply
// read the returned Job are safe because the Store's read lock is held
// only during the map lookup, not after Get returns.
func (s *Store) Get(name string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[name]
	if !ok {
		return nil, ErrNotFound
	}
	return j, nil
}

// List returns all jobs whose names are children of the given parent
// ("projects/{p}/locations/{l}"). The results are sorted alphabetically by
// Name for deterministic wire output and reliable test assertions.
//
// The "/jobs/" separator on the prefix is essential: a parent of
// "projects/p/locations/us" must NOT match a job under
// "projects/p/locations/us-central1". By requiring the "/jobs/" separator
// in the prefix the implementation guarantees exact parent-boundary
// matching regardless of how similarly-named projects or locations are
// arranged.
func (s *Store) List(parent string) []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := parent + "/jobs/"
	out := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if strings.HasPrefix(j.Name, prefix) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Name < out[k].Name })
	return out
}

// Update replaces the stored Job with the given record. Returns
// ErrNotFound if no job exists under j.Name.
//
// This is the canonical update-by-replace semantic — the service layer
// is responsible for any patch-vs-replace field merging required by the
// wire-level proto semantics. On success the updated job is persisted to
// disk (when persistence is enabled).
func (s *Store) Update(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.Name]; !ok {
		return ErrNotFound
	}
	s.jobs[j.Name] = j
	return s.persistLocked()
}

// Delete removes a job by name. Returns ErrNotFound if the named job
// does not exist. On success the updated snapshot is persisted to disk
// (when persistence is enabled).
func (s *Store) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[name]; !ok {
		return ErrNotFound
	}
	delete(s.jobs, name)
	return s.persistLocked()
}

// Pause sets the named job's State field to schedulerpb.Job_PAUSED and
// returns the updated Job pointer (the same pointer the Store holds, for
// caller convenience). Returns ErrNotFound if the named job does not
// exist. On success the updated job is persisted to disk (when
// persistence is enabled). Any persistence error short-circuits the
// return so the caller observes a nil Job on failure.
func (s *Store) Pause(name string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[name]
	if !ok {
		return nil, ErrNotFound
	}
	j.State = schedulerpb.Job_PAUSED
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return j, nil
}

// Resume sets the named job's State field to schedulerpb.Job_ENABLED and
// returns the updated Job pointer. Returns ErrNotFound if the named job
// does not exist. On success the updated job is persisted to disk (when
// persistence is enabled).
func (s *Store) Resume(name string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[name]
	if !ok {
		return nil, ErrNotFound
	}
	j.State = schedulerpb.Job_ENABLED
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return j, nil
}

// Touch records that a dispatch attempt occurred for the named job by
// setting its LastAttemptTime to the current wall-clock time (via the
// package-level Now test seam so tests can fix time). The job's Schedule,
// State, and target fields are intentionally NOT mutated — Touch is a
// pure metadata update suitable for both the cron runner tick path and
// the manual RunJob path.
//
// Returns the updated Job pointer (the same pointer the Store holds) or
// ErrNotFound if the named job does not exist. On success the updated
// job is persisted to disk (when persistence is enabled). Any
// persistence error short-circuits the return so the caller observes a
// nil Job on failure, matching the Pause/Resume error contract.
//
// Thread-safety: acquires the Store's write lock for the duration of
// the update. Callers MUST NOT hold s.mu when invoking Touch.
func (s *Store) Touch(name string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[name]
	if !ok {
		return nil, ErrNotFound
	}
	j.LastAttemptTime = Now()
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return j, nil
}

// persistLocked writes the current in-memory state to
// "{dataDir}/cloudscheduler/state.json". The caller MUST hold s.mu (write
// lock) when invoking this helper.
//
// The write is atomic from the perspective of concurrent readers on other
// processes: data is first written to a ".tmp" sibling file and then
// os.Rename'd into place. This mirrors the sibling cloudtasks package
// convention and prevents readers from observing a partially-written
// snapshot after a crash.
//
// When dataDir is empty this method is a no-op and returns nil. All
// filesystem errors are wrapped with %w so callers can use errors.Is/As
// for rich diagnostics.
func (s *Store) persistLocked() error {
	if s.dataDir == "" {
		return nil
	}
	dir := filepath.Join(s.dataDir, "cloudscheduler")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cloudscheduler: mkdir %q: %w", dir, err)
	}
	file := filepath.Join(dir, "state.json")
	data, err := json.MarshalIndent(s.jobs, "", "  ")
	if err != nil {
		return fmt.Errorf("cloudscheduler: marshal state: %w", err)
	}
	tmp := file + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("cloudscheduler: write %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, file); err != nil {
		return fmt.Errorf("cloudscheduler: rename %q: %w", file, err)
	}
	return nil
}

// load attempts to restore state from
// "{dataDir}/cloudscheduler/state.json". This method is deliberately
// best-effort: a missing snapshot file (fresh install) or a corrupt
// snapshot (previous crash, manual edit, version skew) is treated as empty
// state and the Store begins life with zero jobs. The return value is
// always nil in practice; the signature retains an error return so future
// callers can convert to a strict-load semantic without a breaking change.
//
// load MUST be called only from NewStore, before any other goroutine can
// hold a reference to the Store — NewStore enforces this by invoking
// load synchronously.
func (s *Store) load() error {
	if s.dataDir == "" {
		return nil
	}
	file := filepath.Join(s.dataDir, "cloudscheduler", "state.json")
	data, err := os.ReadFile(file)
	if err != nil {
		// Missing file is the normal case on first boot. Any other I/O
		// error (permissions, stale NFS mount, etc.) also yields an empty
		// store rather than a startup failure — the scheduler service's
		// correctness does not depend on snapshot recoverability.
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return nil
	}
	var jobs map[string]*Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		// Corrupt snapshot — ignore and start empty. The next successful
		// write (Create/Update/Delete/Pause/Resume) will overwrite the
		// corrupt file with a valid snapshot.
		return nil
	}
	if jobs != nil {
		s.jobs = jobs
	}
	return nil
}
