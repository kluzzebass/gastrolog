package source

import "sync"

// MemoryStore is an in-memory Store implementation for testing.
type MemoryStore struct {
	mu      sync.Mutex
	sources map[string]*Source // keyed by ID string
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sources: make(map[string]*Source),
	}
}

// Save persists a source in memory.
func (s *MemoryStore) Save(src *Source) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources[src.ID.String()] = copySource(src)
	return nil
}

// LoadAll retrieves all persisted sources.
func (s *MemoryStore) LoadAll() ([]*Source, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]*Source, 0, len(s.sources))
	for _, src := range s.sources {
		result = append(result, copySource(src))
	}
	return result, nil
}

// Count returns the number of stored sources. For testing.
func (s *MemoryStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sources)
}
