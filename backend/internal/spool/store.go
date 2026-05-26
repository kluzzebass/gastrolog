package spool

import (
	"gastrolog/internal/chunk"
)

// Store is the spool persistence surface used by the sequenced write path.
// Implementations must provide index-last durable append semantics (file)
// or equivalent in-memory behavior for tests.
type Store interface {
	Append(rec chunk.Record) (SegmentMeta, error)
	ReadByVaultSeq(seq uint64) (chunk.Record, bool)
	LookupEventID(id chunk.EventID) (uint64, bool)
	DurableWatermark() uint64
	Close() error
}
