package record

import (
	"time"
)

// Record is the immutable log entry carried through the pipeline
// (ingest → digest → route → segment write). It is identified by EventID;
// order and dedup keys use EventID, not processing order.
//
// Timestamps:
//   - SourceTS: when the log was generated at the source (zero if unknown)
//   - IngestTS: when the ingester received the message (also on EventID)
//   - WriteTS: set when the record is durably appended (segment write)
//
// RecordRef and VaultID are not part of this type — they exist only on
// query results (chunk.Record) after a record has been stored and read back.
type Record struct {
	SourceTS time.Time
	IngestTS time.Time
	WriteTS  time.Time
	EventID  EventID
	Attrs    Attributes
	Raw      []byte

	// WaitForReplica signals that the caller wants confirmation that
	// secondaries received this record before acking. Set by ack-gated
	// ingesters (RELP, HTTP X-Wait-Ack). When true, appendRecord skips
	// fire-and-forget forwarding — the caller does sync forwarding instead.
	WaitForReplica bool
}

// Copy returns a deep copy of the record with its own Raw slice and Attrs map.
// Use this when the record needs to outlive the cursor that created it.
func (r Record) Copy() Record {
	raw := make([]byte, len(r.Raw))
	copy(raw, r.Raw)
	return Record{
		SourceTS:       r.SourceTS,
		IngestTS:       r.IngestTS,
		WriteTS:        r.WriteTS,
		EventID:        r.EventID,
		Attrs:          r.Attrs.Copy(),
		Raw:            raw,
		WaitForReplica: r.WaitForReplica,
	}
}
