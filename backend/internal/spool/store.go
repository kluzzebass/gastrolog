package spool

import (
	"gastrolog/internal/chunk"
)

// Store is the spool persistence surface used by the sequenced write path.
// Accepts are keyed by VaultSeq slots inside allocator-aligned sequence windows.
type Store interface {
	// EnsureWindow materializes a sequence window for swath [start..end] inclusive.
	EnsureWindow(start, end uint64) error
	// PutSlot durably writes one (VaultSeq, record) slot; out-of-order arrival is allowed.
	PutSlot(rec chunk.Record) error
	ReadByVaultSeq(seq uint64) (chunk.Record, bool)
	LookupEventID(id chunk.EventID) (uint64, bool)
	DurableWatermark() uint64
	Close() error
}
