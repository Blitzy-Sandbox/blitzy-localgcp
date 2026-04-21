// Package cloudscheduler implements the Google Cloud Scheduler emulator.
//
// The service provides a gRPC-compatible in-memory implementation of the eight
// in-scope Cloud Scheduler RPCs (CreateJob, GetJob, ListJobs, DeleteJob,
// UpdateJob, RunJob, PauseJob, ResumeJob) backed by a single goroutine running
// `github.com/robfig/cron/v3` for tick-driven dispatch. Jobs with HTTP targets
// are dispatched through the shared `internal/dispatch` package; Pub/Sub
// targets are dispatched through a loopback gRPC publisher client.
//
// This file contains the Store — an in-memory, thread-safe job registry.
package cloudscheduler

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store is the in-memory Cloud Scheduler job registry.
//
// Jobs are keyed by their fully-qualified name
// (`projects/{project}/locations/{location}/jobs/{job}`). The store is safe
// for concurrent use and protects all internal state with a sync.RWMutex.
type Store struct {
	mu   sync.RWMutex
	jobs map[string]*schedulerpb.Job
}

// NewStore returns a ready-to-use, empty Store.
func NewStore() *Store {
	return &Store{
		jobs: make(map[string]*schedulerpb.Job),
	}
}

// Create inserts a new job keyed by `name`. If a job with the same name
// already exists the error is returned and no mutation occurs. The stored
// job has its State set to ENABLED (per AAP default behavior) and
// UserUpdateTime set to "now". The input job is mutated in place and also
// returned for caller convenience.
func (s *Store) Create(name string, job *schedulerpb.Job) (*schedulerpb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[name]; exists {
		return nil, fmt.Errorf("already exists")
	}
	job.Name = name
	if job.State == schedulerpb.Job_STATE_UNSPECIFIED {
		job.State = schedulerpb.Job_ENABLED
	}
	now := timestamppb.Now()
	job.UserUpdateTime = now
	s.jobs[name] = job
	return job, nil
}

// Get returns the job keyed by `name`, or (nil, false) if not present.
func (s *Store) Get(name string) (*schedulerpb.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[name]
	return j, ok
}

// List returns all jobs whose name begins with `parent + "/jobs/"`,
// sorted by full name for deterministic output.
func (s *Store) List(parent string) []*schedulerpb.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := parent + "/jobs/"
	var out []*schedulerpb.Job
	for n, j := range s.jobs {
		if strings.HasPrefix(n, prefix) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Name < out[k].Name })
	return out
}

// Update replaces the job keyed by `name`. Immutable-by-emulator fields
// (Name, UserUpdateTime) are preserved/refreshed. If the job does not
// exist an error is returned and no mutation occurs.
func (s *Store) Update(name string, job *schedulerpb.Job) (*schedulerpb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.jobs[name]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	job.Name = existing.Name
	if job.State == schedulerpb.Job_STATE_UNSPECIFIED {
		job.State = existing.State
	}
	job.UserUpdateTime = timestamppb.Now()
	s.jobs[name] = job
	return job, nil
}

// Delete removes the job keyed by `name`. Returns true if a job was
// removed, false if no such job existed.
func (s *Store) Delete(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[name]; !ok {
		return false
	}
	delete(s.jobs, name)
	return true
}

// SetState transitions the job's State field. If the job does not exist
// an error is returned and no mutation occurs. The returned job pointer
// is the stored pointer (so callers observe subsequent updates).
func (s *Store) SetState(name string, state schedulerpb.Job_State) (*schedulerpb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[name]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	j.State = state
	j.UserUpdateTime = timestamppb.Now()
	return j, nil
}

// TouchRun records that the job was attempted now. The store advances
// the LastAttemptTime and ScheduleTime fields so that subsequent Get and
// List observers see an accurate run log.
func (s *Store) TouchRun(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[name]
	if !ok {
		return
	}
	now := timestamppb.Now()
	j.LastAttemptTime = now
	j.ScheduleTime = now
}
