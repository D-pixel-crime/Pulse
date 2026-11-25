package scheduler

import (
	"sync"

	"github.com/google/uuid"
)

// InFlightSet tracks monitor IDs that are currently queued or executing a check.
// It is safe for concurrent use by the scheduler and worker goroutines.
type InFlightSet struct {
	mu sync.Mutex
	m  map[uuid.UUID]struct{}
}

// NewInFlightSet initializes an empty concurrency-safe InFlightSet.
func NewInFlightSet() *InFlightSet {
	return &InFlightSet{
		m: make(map[uuid.UUID]struct{}),
	}
}

// TryAdd marks id as in-flight if it is not already present and returns true.
// If id is uuid.Nil or already present, it returns false without modifying the set.
func (s *InFlightSet) TryAdd(id uuid.UUID) bool {
	if id == uuid.Nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[id]; exists {
		return false
	}
	s.m[id] = struct{}{}
	return true
}

// Remove deletes id from the in-flight set.
func (s *InFlightSet) Remove(id uuid.UUID) {
	if id == uuid.Nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id)
}

// Contains reports whether id is currently tracked as in-flight.
func (s *InFlightSet) Contains(id uuid.UUID) bool {
	if id == uuid.Nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.m[id]
	return exists
}

// Len returns the number of monitor IDs currently in-flight.
func (s *InFlightSet) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}
