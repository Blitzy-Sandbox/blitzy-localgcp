// Package cloudrun implements the Google Cloud Run (Services API) emulator.
//
// In addition to the in-memory service registry, this file provides a bounded
// host-port pool (default range 8200-8299, 100 concurrent services maximum)
// used by CreateService to reserve a per-service reverse-proxy listener port.
// The pool is thread-safe, allocation is deterministic (lowest free port
// first), and an exhausted pool returns the canonical
// codes.ResourceExhausted error required by AAP Rule 8.
package cloudrun

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ContainerRef records the per-service container metadata used by the
// reverse-proxy and DeleteService cleanup paths.
type ContainerRef struct {
	// HostPort is the external port the reverse-proxy listens on
	// (allocated from the pool, range 8200-8299 by default).
	HostPort int
	// ContainerID is the Docker container ID (empty when --no-docker is
	// in effect, or before first request lazily starts the container).
	ContainerID string
	// Image is the container image reference supplied by CreateService.
	Image string
	// InternalPort is the container port the application listens on
	// inside the container (defaults to "8080/tcp").
	InternalPort string
}

// Store is the in-memory Cloud Run service store combined with the
// bounded host-port pool used for per-service reverse-proxy listeners.
type Store struct {
	mu         sync.RWMutex
	services   map[string]*runpb.Service // full name -> service
	refs       map[string]*ContainerRef  // full name -> container metadata
	poolStart  int
	poolEnd    int
	allocPorts map[int]struct{} // in-use ports for deterministic overflow
	nextTry    int              // round-robin hint for allocatePort scan start
}

// NewStore returns a ready-to-use, empty Store with the default port pool
// 8200-8299 (inclusive). Callers that need to run multiple emulators in a
// single process or on constrained hosts can use NewStoreWithPool.
func NewStore() *Store {
	return NewStoreWithPool(8200, 8299)
}

// NewStoreWithPool returns a Store with a custom inclusive port range.
// Invalid ranges (end < start) are silently clamped to a single port.
func NewStoreWithPool(start, end int) *Store {
	if end < start {
		end = start
	}
	return &Store{
		services:   make(map[string]*runpb.Service),
		refs:       make(map[string]*ContainerRef),
		poolStart:  start,
		poolEnd:    end,
		allocPorts: make(map[int]struct{}),
		nextTry:    start,
	}
}

// PoolRange returns the inclusive [start, end] of the host-port pool.
// Exported for telemetry/testing; callers should not rely on this for
// allocation decisions.
func (s *Store) PoolRange() (int, int) {
	return s.poolStart, s.poolEnd
}

// PoolSize returns the total number of ports the pool can allocate.
func (s *Store) PoolSize() int {
	return s.poolEnd - s.poolStart + 1
}

// AllocatePort reserves the lowest-numbered free port in the pool and
// returns it. If every port in the range is already in use, it returns
// the canonical codes.ResourceExhausted error required by AAP Rule 8.
func (s *Store) AllocatePort() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p := s.poolStart; p <= s.poolEnd; p++ {
		if _, inUse := s.allocPorts[p]; !inUse {
			s.allocPorts[p] = struct{}{}
			return p, nil
		}
	}
	return 0, status.Error(codes.ResourceExhausted,
		"localgcp: cloud run port pool exhausted (max 100 concurrent services)")
}

// ReleasePort returns a port to the pool. Releasing a port that was never
// allocated, or that lies outside the pool range, is a no-op (idempotent).
func (s *Store) ReleasePort(p int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p < s.poolStart || p > s.poolEnd {
		return
	}
	delete(s.allocPorts, p)
}

// SetRef attaches container metadata to the service keyed by `name`.
// If the service does not exist, SetRef is a no-op (the ref is stored
// anyway to support lazy registration; consumers should call Get first).
func (s *Store) SetRef(name string, ref *ContainerRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs[name] = ref
}

// GetRef returns the container metadata for the named service, or
// (nil, false) if none was recorded.
func (s *Store) GetRef(name string) (*ContainerRef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.refs[name]
	return r, ok
}

// SetContainerID updates ONLY the ContainerID field of the existing
// ContainerRef for the named service. This is the persistence hook
// invoked by serviceProxy.boot() once a Docker container has been
// successfully created and started, so that subsequent DeleteService
// calls (or future --data-dir restore paths) can correctly locate the
// container for Stop + Remove.
//
// Returns an error when no ContainerRef exists for the given service
// name — callers should have invoked SetRef at CreateService time.
// The update is performed atomically under the existing write lock.
func (s *Store) SetContainerID(name, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.refs[name]
	if !ok {
		return fmt.Errorf("cloudrun: no container ref for service %q", name)
	}
	ref.ContainerID = id
	return nil
}

// Create inserts a new service into the store. If a service with the
// same name already exists, an error is returned. The service's URI
// is populated by the caller via SetURI after CreateService allocates
// a host port via AllocatePort (to avoid a circular dependency on the
// store's port pool inside Create).
//
// The function populates CreateTime, UpdateTime, Uid, and marks the
// service as Ready.
func (s *Store) Create(name string, svc *runpb.Service) (*runpb.Service, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.services[name]; exists {
		return nil, fmt.Errorf("already exists")
	}

	now := timestamppb.Now()
	svc.Name = name
	svc.CreateTime = now
	svc.UpdateTime = now
	svc.Uid = fmt.Sprintf("uid-%d", len(s.services)+1)

	// Mark as ready.
	svc.Reconciling = false
	svc.Conditions = []*runpb.Condition{{
		Type:  "Ready",
		State: runpb.Condition_CONDITION_SUCCEEDED,
	}}

	s.services[name] = svc
	return svc, nil
}

// SetURI updates the URI of an existing service. Used by CreateService
// to populate the URI after a port has been allocated.
func (s *Store) SetURI(name, uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if svc, ok := s.services[name]; ok {
		svc.Uri = uri
	}
}

// Get returns the service keyed by `name`.
func (s *Store) Get(name string) (*runpb.Service, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	svc, ok := s.services[name]
	return svc, ok
}

// List returns all services whose name begins with `parent + "/services/"`,
// sorted by full name for deterministic output.
func (s *Store) List(parent string) []*runpb.Service {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := parent + "/services/"
	var result []*runpb.Service
	for name, svc := range s.services {
		if strings.HasPrefix(name, prefix) {
			result = append(result, svc)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Update replaces the service keyed by `name`, preserving immutable
// fields (Name, CreateTime, Uid, Uri, Conditions).
func (s *Store) Update(name string, svc *runpb.Service) (*runpb.Service, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.services[name]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	// Preserve immutable fields.
	svc.Name = existing.Name
	svc.CreateTime = existing.CreateTime
	svc.Uid = existing.Uid
	svc.Uri = existing.Uri
	svc.UpdateTime = timestamppb.Now()
	svc.Conditions = existing.Conditions

	s.services[name] = svc
	return svc, nil
}

// Delete removes the service and any associated container ref. Returns
// true if a service was removed, false if no such service existed.
// Callers are responsible for releasing the port via ReleasePort and
// for stopping/removing the associated container.
func (s *Store) Delete(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.services[name]; !ok {
		return false
	}
	delete(s.services, name)
	delete(s.refs, name)
	return true
}
