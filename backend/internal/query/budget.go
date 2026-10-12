package query

import (
	"fmt"
	"sync/atomic"

	"gastrolog/internal/chunk"
)

// MaxQueryMemoryBytes is the ceiling on the in-memory working set a single
// query may build on a single node: buffered records, sort buffers, and stats
// accumulator state. Materializing work grows with the data a query touches,
// not with the size of its result, so without a ceiling one well-formed query
// over a wide time range allocates until the node dies — taking down every
// vault that node serves.
//
// 256 MiB holds on the order of 250 000 buffered records, or tens of millions
// of distinct dcount values or median samples. That is far past the 10 000
// record default result cap and past what any investigation reads, so a query
// a person actually wants stays well inside it, while several concurrent
// queries at the ceiling still leave a node with room to ingest.
//
// The ceiling is fixed rather than operator-tunable on purpose: it is a node
// safety invariant, not a workload preference. A node runs queries for every
// vault it leads, so raising the ceiling to make one query fit re-arms the
// same exhaustion for every other vault on that node. The remedy for a query
// that does not fit is in the query — a narrower time range, a head, or a
// lower-cardinality group — and MemoryLimitError says so.
const MaxQueryMemoryBytes int64 = 256 << 20

// Names for the structures charged against a budget. They appear verbatim in
// MemoryLimitError, so they read as something an operator can act on.
const (
	consumerGroupState     = "stats group state"
	consumerDistinctSet    = "dcount distinct-value set"
	consumerMedianSamples  = "median sample buffer"
	consumerValuesList     = "values list"
	consumerLatchedValue   = "first/last value state"
	consumerRecordBuffer   = "record buffer"
	consumerSortBuffer     = "sort buffer"
	consumerTimechartState = "timechart group state"
	consumerDedupState     = "dedup seen-event set"
)

// Byte sizes the Go runtime retains beyond the payload of a string, a map
// entry, or a record. Charging payload length alone would undercount a working
// set of many small records or map keys by more than it counts.
const (
	// recordStructBytes is the footprint of a chunk.Record value itself:
	// three time.Time fields, an EventID, a RecordRef, a vault ID, and the
	// slice and map headers, rounded up to a size class.
	recordStructBytes = 208

	// mapEntryBytes covers one Go map slot holding two strings: the bucket
	// entry, its hash byte, and the two string headers.
	mapEntryBytes = 48

	// stringHeaderBytes is a string header — data pointer plus length.
	stringHeaderBytes = 16

	// float64Bytes is one median sample.
	float64Bytes = 8

	// dedupEntryBytes is one entry in a dedup tracker: an EventID key (two
	// GLIDs, a timestamp, a sequence number), a time.Time value, and the map
	// slot holding them.
	dedupEntryBytes = 112
)

// MemoryLimitError reports that a query's working set outgrew the per-node
// query memory budget. It names the structure that overflowed and the ceiling
// so an operator reading the failure knows which part of the query is too
// large rather than guessing.
type MemoryLimitError struct {
	Consumer string // which structure overflowed
	Limit    int64  // the ceiling it was measured against, in bytes
}

func (e *MemoryLimitError) Error() string {
	if e.Limit <= 0 {
		return "query reached its " + e.Consumer + " with no memory budget installed: refusing to run it unbounded"
	}
	return fmt.Sprintf("query exceeded the %d MiB per-node query memory budget while building its %s: narrow the time range, add | head N, or group by a lower-cardinality field",
		e.Limit>>20, e.Consumer)
}

// Budget accounts the memory one query execution has committed to in-memory
// working sets on this node. Every materializing path charges what it retains
// and releases what it drops, so the total a query can hold at once is
// bounded no matter how much data it scans.
//
// The budget is per-query and per-node. A fan-out query costs up to one budget
// on each node that executes part of it, plus the coordinator's own share:
// there is no cluster-wide total. That is deliberate — the exhaustion being
// prevented is a node running out of memory, which is a per-node resource, and
// a shared counter would need a cluster round trip on the per-record path to
// maintain. Each node protects itself, which is the property that keeps any
// node from dying.
//
// One query's structures run concurrently — the page-1 histogram beside the
// record scan — so a Budget is safe for concurrent use. The ledger is a single
// atomic counter: charging takes no lock.
type Budget struct {
	limit int64
	used  atomic.Int64

	// parent is set on a sub-account: every charge and release passes
	// through to it, and the ceiling is the parent's.
	parent *Budget
}

// NewBudget returns a budget at this engine's ceiling: the ledger one query
// runs on, on this node. Callers that drive several parts of one query — a
// search and its histogram, a pipeline and the records it gathers from peers —
// create it once and hand it to every part, through Query.WithBudget or
// RunPipelineWithRemote.
func (e *Engine) NewBudget() *Budget {
	limit := e.memLimit
	if limit <= 0 {
		limit = MaxQueryMemoryBytes
	}
	return &Budget{limit: limit}
}

// SetMemoryLimit lowers this engine's per-query ceiling. Production engines
// run at MaxQueryMemoryBytes; tests use it to drive the overflow path without
// building hundreds of megabytes of working set.
func (e *Engine) SetMemoryLimit(limit int64) {
	e.memLimit = limit
}

// budgetFor returns the ledger q runs on, opening one at this engine's
// ceiling when the caller attached none. Every engine entry point that scans
// resolves its budget here once, so all the chunks of one query charge the
// same ledger.
func (e *Engine) budgetFor(q Query) *Budget {
	if q.budget != nil {
		return q.budget
	}
	return e.NewBudget()
}

// Charge accounts n bytes against the budget and fails when the ceiling is
// crossed. consumer names the structure being grown.
//
// It fails closed on a nil Budget. Every materializing path is required to
// carry one, so arriving here without a budget is a wiring bug, and running
// the query unbounded is precisely the failure this type exists to prevent.
// A zero-value Budget resolves to the standard ceiling rather than rejecting
// everything, so an engine assembled without going through NewBudget is
// bounded rather than broken.
func (b *Budget) Charge(consumer string, n int64) error {
	if b == nil {
		return &MemoryLimitError{Consumer: consumer}
	}
	used := b.used.Add(n)
	if b.parent != nil {
		return b.parent.Charge(consumer, n)
	}
	limit := b.limit
	if limit <= 0 {
		limit = MaxQueryMemoryBytes
	}
	if used > limit {
		return &MemoryLimitError{Consumer: consumer, Limit: limit}
	}
	return nil
}

// Release returns n bytes to the budget when a working set drops what it was
// holding — an evicted ring-buffer slot, a record trimmed out of a top-N sort,
// a latched value replaced by a newer one. The ledger never goes below zero.
func (b *Budget) Release(n int64) {
	if b == nil {
		return
	}
	released := b.drop(n)
	if b.parent != nil {
		b.parent.Release(released)
	}
}

// drop lowers used by up to n, never below zero, and returns how much it
// lowered it by.
func (b *Budget) drop(n int64) int64 {
	for {
		cur := b.used.Load()
		d := min(n, cur)
		if b.used.CompareAndSwap(cur, cur-d) {
			return d
		}
	}
}

// subAccount returns a ledger whose charges land on b, for a part of the
// query that finishes before the query does and must hand back everything it
// held when it ends. close returns what it still holds.
func (b *Budget) subAccount() *Budget {
	return &Budget{parent: b}
}

// close returns everything a sub-account still holds to its parent.
func (b *Budget) close() {
	if b == nil || b.parent == nil {
		return
	}
	b.parent.Release(b.used.Swap(0))
}

// ChargeRecord accounts one buffered record.
func (b *Budget) ChargeRecord(consumer string, rec chunk.Record) error {
	return b.Charge(consumer, RecordFootprint(rec))
}

// RecordFootprint estimates the bytes a buffered record retains: the record
// struct, its raw payload, and every attribute key and value with their map
// overhead.
func RecordFootprint(rec chunk.Record) int64 {
	return recordStructBytes + recordPayloadBytes(rec)
}

// recordPayloadBytes is RecordFootprint without the record struct itself, for
// collectors that charge their slot array up front and then charge only what
// each slot points at.
func recordPayloadBytes(rec chunk.Record) int64 {
	n := int64(len(rec.Raw))
	for k, v := range rec.Attrs {
		n += int64(len(k)) + int64(len(v)) + mapEntryBytes
	}
	return n
}

// stringSetEntryBytes is the cost of holding s as a key in a set.
func stringSetEntryBytes(s string) int64 {
	return int64(len(s)) + mapEntryBytes
}
