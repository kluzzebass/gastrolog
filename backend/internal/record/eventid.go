package record

import (
	"time"

	"gastrolog/internal/glid"
)

// EventID uniquely identifies a record across the cluster.
// Composed of the ingester's UUID, the emitting node's UUID, the ingestion
// timestamp, and a per-ingester rolling sequence number. NodeID is required
// because singleton/parallel HA (gastrolog-2kcw4) allows the same ingester
// to run concurrently on multiple nodes, each maintaining its own
// per-ingester sequence counter — without NodeID in the identity key,
// two nodes can legitimately mint the same (IngesterID, IngestTS, IngestSeq)
// tuple in the same microsecond.
// All fields are fixed-size value types, so EventID is comparable and usable
// as a map key.
type EventID struct {
	IngesterID glid.GLID
	NodeID     glid.GLID
	IngestTS   time.Time
	IngestSeq  uint32
}

// Compare returns -1, 0, or +1 comparing e to o in canonical EventID order:
// IngestTS, then NodeID, then IngesterID, then IngestSeq. This is a total
// order suitable for merge and dedup keys.
func (e EventID) Compare(o EventID) int {
	if c := e.IngestTS.Compare(o.IngestTS); c != 0 {
		return c
	}
	if c := e.NodeID.Compare(o.NodeID); c != 0 {
		return c
	}
	if c := e.IngesterID.Compare(o.IngesterID); c != 0 {
		return c
	}
	if e.IngestSeq < o.IngestSeq {
		return -1
	}
	if e.IngestSeq > o.IngestSeq {
		return 1
	}
	return 0
}

// Less reports whether e precedes o in canonical EventID order.
func (e EventID) Less(o EventID) bool {
	return e.Compare(o) < 0
}
