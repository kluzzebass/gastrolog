package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// Vault bundles the chunk manager, index manager, and query engine for a single vault.
// The invariant that every vault ID has all three is now structurally enforced.
type Vault struct {
	ID      uuid.UUID
	Chunks  chunk.ChunkManager
	Indexes index.IndexManager
	Query   *query.Engine
	Enabled bool
}

// NewVault creates a Vault from its components.
func NewVault(id uuid.UUID, cm chunk.ChunkManager, im index.IndexManager, qe *query.Engine) *Vault {
	return &Vault{
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
func (s *Vault) Append(rec chunk.Record, onSeal scheduleIndexFunc) error {
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
