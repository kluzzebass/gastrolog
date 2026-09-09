package query

import (
	"context"
	"errors"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// VaultReadError reports that a chunk's bytes could not be opened or read on
// this node. It is the failure a search cannot route around and the one an
// operator must hear about. Query-shape errors — an unsupported ordering, a
// missing index for the requested order — are not read errors and are never
// wrapped in it.
type VaultReadError struct {
	VaultID glid.GLID
	ChunkID chunk.ChunkID
	Err     error
}

func (e *VaultReadError) Error() string {
	return fmt.Sprintf("vault %s chunk %s: %v", e.VaultID, e.ChunkID, e.Err)
}

func (e *VaultReadError) Unwrap() error { return e.Err }

// asReadError wraps a cursor open or read failure so callers can attribute
// it to the vault and chunk. Cancellation is the caller's doing, not the
// vault's, and passes through unchanged.
func asReadError(vaultID glid.GLID, chunkID chunk.ChunkID, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := errors.AsType[*VaultReadError](err); ok {
		return err
	}
	return &VaultReadError{VaultID: vaultID, ChunkID: chunkID, Err: err}
}
