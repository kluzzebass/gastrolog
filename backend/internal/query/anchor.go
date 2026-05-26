package query

import (
	"context"
	"errors"
	"fmt"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// ErrInvalidContextAnchor is returned when a context anchor ref is malformed.
var ErrInvalidContextAnchor = errors.New("invalid context anchor")

// ErrSpoolAnchorNotAvailable is returned for vault_seq anchors when no spool reader is wired.
var ErrSpoolAnchorNotAvailable = errors.New("vault_seq anchor requires spool reader (V2 pre-materialized record)")

// SpoolAnchorReader resolves V2 pre-materialized records by destination-vault sequence.
// Wired during Phase 6 rollout; optional until then.
type SpoolAnchorReader interface {
	ReadByVaultSeq(ctx context.Context, vaultID glid.GLID, seq uint64) (chunk.Record, error)
}

// ContextRef identifies the GetContext anchor record.
//
// Materialized anchors use ChunkID+Pos. Pre-materialized V2 anchors use VaultSeq
// with zero ChunkID — see docs/fan-out/v2/anchor-model.md.
type ContextRef struct {
	VaultID  glid.GLID
	ChunkID  chunk.ChunkID
	Pos      uint64
	VaultSeq uint64
}

// IsMaterialized reports whether the anchor uses a sealed/active chunk position.
func (r ContextRef) IsMaterialized() bool {
	return r.ChunkID != chunk.ChunkID{}
}

// IsVaultSequence reports whether the anchor targets a V2 spool/pre-materialized record.
func (r ContextRef) IsVaultSequence() bool {
	return !r.IsMaterialized() && r.VaultSeq > 0
}

// ValidateContextRef checks anchor shape against the locked migration contract.
func ValidateContextRef(r ContextRef) error {
	if r.VaultID == glid.Nil {
		return fmt.Errorf("%w: vault_id required", ErrInvalidContextAnchor)
	}
	if r.IsMaterialized() && r.VaultSeq != 0 {
		return fmt.Errorf("%w: materialized anchor must not set vault_seq", ErrInvalidContextAnchor)
	}
	if !r.IsMaterialized() && r.VaultSeq == 0 {
		return fmt.Errorf("%w: anchor requires chunk ref or vault_seq", ErrInvalidContextAnchor)
	}
	return nil
}

// ContextRefFromProto converts an API RecordRef to a ContextRef.
func ContextRefFromProto(p *apiv1.RecordRef) (ContextRef, error) {
	if p == nil {
		return ContextRef{}, fmt.Errorf("%w: ref is nil", ErrInvalidContextAnchor)
	}
	ref := ContextRef{
		VaultID:  glid.FromBytes(p.GetVaultId()),
		ChunkID:  chunk.ChunkID(glid.FromBytes(p.GetChunkId())),
		Pos:      p.GetPos(),
		VaultSeq: p.GetVaultSeq(),
	}
	if err := ValidateContextRef(ref); err != nil {
		return ContextRef{}, err
	}
	return ref, nil
}

// SetSpoolAnchorReader wires optional V2 spool anchor resolution.
func (e *Engine) SetSpoolAnchorReader(r SpoolAnchorReader) {
	e.spoolAnchorReader = r
}

// ReadAnchor loads the anchor record for GetContext (materialized or vault_seq).
func (e *Engine) ReadAnchor(ctx context.Context, ref ContextRef) (chunk.Record, error) {
	return e.resolveAnchor(ctx, ref)
}

func (e *Engine) resolveAnchor(ctx context.Context, ref ContextRef) (chunk.Record, error) {
	if err := ValidateContextRef(ref); err != nil {
		return chunk.Record{}, err
	}
	if ref.IsMaterialized() {
		return e.ReadRecord(ctx, ref.VaultID, ref.ChunkID, ref.Pos)
	}
	if e.spoolAnchorReader == nil {
		return chunk.Record{}, ErrSpoolAnchorNotAvailable
	}
	rec, err := e.spoolAnchorReader.ReadByVaultSeq(ctx, ref.VaultID, ref.VaultSeq)
	if err != nil {
		return chunk.Record{}, err
	}
	rec.VaultID = ref.VaultID
	return rec, nil
}

// recordMatchesAnchor reports whether rec is the anchor, across materialized and
// pre-materialized lifecycles. EventID is the stable identity axis.
func recordMatchesAnchor(rec chunk.Record, anchor chunk.Record, ref ContextRef) bool {
	if !rec.VaultID.IsZero() && rec.VaultID != ref.VaultID {
		return false
	}
	return rec.EventID == anchor.EventID
}
