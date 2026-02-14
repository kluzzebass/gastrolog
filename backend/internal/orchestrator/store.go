package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
)

// Store bundles the chunk manager, index manager, and query engine for a single store.
// The invariant that every store ID has all three is now structurally enforced.
type Store struct {
	ID      string
	Chunks  chunk.ChunkManager
	Indexes index.IndexManager
	Query   *query.Engine
	Enabled bool
}

// NewStore creates a Store from its components.
func NewStore(id string, cm chunk.ChunkManager, im index.IndexManager, qe *query.Engine) *Store {
	return &Store{
		ID:      id,
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
		Enabled: true,
	}
}

// scheduleIndexFunc is called when a chunk is sealed to trigger an async index build.
type scheduleIndexFunc func(chunkID chunk.ChunkID)

// Append writes the record to the chunk manager, detects seal, and calls onSeal
// if the active chunk changed (indicating the previous chunk was sealed).
// onSeal is invoked to schedule an asynchronous index build for the sealed chunk.
func (s *Store) Append(rec chunk.Record, onSeal scheduleIndexFunc) error {
	activeBefore := s.Chunks.Active()
	_, _, err := s.Chunks.Append(rec)
	if err != nil {
		return err
	}
	activeAfter := s.Chunks.Active()
	if activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID) && onSeal != nil {
		onSeal(activeBefore.ID)
	}
	return nil
}
