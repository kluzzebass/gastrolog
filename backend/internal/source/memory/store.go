// Package memory provides in-memory persistence for source metadata.
package memory

import (
	"sync"

	"gastrolog/internal/source"
)

// Store is an in-memory Store implementation for testing.
type Store struct {
	mu      sync.Mutex
	sources map[string]*source.Source // keyed by ID string
}

// NewStore creates a new in-memory store.
func NewStore() *Store {
	return &Store{
		sources: make(map[string]*source.Source),
	}
}

// Save persists a source in memory.
func (s *Store) Save(src *source.Source) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources[src.ID.String()] = copySource(src)
	return nil
}

// LoadAll retrieves all persisted sources.
func (s *Store) LoadAll() ([]*source.Source, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]*source.Source, 0, len(s.sources))
	for _, src := range s.sources {
		result = append(result, copySource(src))
	}
	return result, nil
}

// Count returns the number of stored sources. For testing.
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sources)
}

// copySource creates a copy of a Source.
func copySource(src *source.Source) *source.Source {
	attrs := make(map[string]string, len(src.Attributes))
	for k, v := range src.Attributes {
		attrs[k] = v
	}
	return &source.Source{
		ID:         src.ID,
		Attributes: attrs,
		CreatedAt:  src.CreatedAt,
	}
}
