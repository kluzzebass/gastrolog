// dedupWindow tests for cross-replica duplicate collapse
// (gastrolog-6bt8s).
//
// Under the fan-out data-plane (gastrolog-2ujjh), an active-chunk read
// fans out to every node in the chunk's Receiving set. Replicas may
// each return their own view; replicas that hold the same record both
// emit it. The records flow through the search node's emit boundary
// where dedupWindow collapses them by EventID.
//
// dedupWindow has been EventID-keyed since the cross-vault fan-out
// work (Phase 5). These tests confirm that the SAME mechanism handles
// cross-replica duplicates with no key-logic change — gastrolog-6bt8s
// acceptance #1 ("dedupWindow correctly collapses cross-replica
// duplicates by EventID"). The actual routing-to-Receiving (per-chunk
// fan-out targets in remoteVaultsByNodeFiltered) lands in
// gastrolog-nd6sz alongside the per-chunk WriteModel dispatch, since
// the read path's FanOut-vs-LeaderDriven decision depends on the same
// chunk-level WriteModel that gastrolog-nd6sz introduces.

package server

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

// dedupTestRecord returns a record whose EventID is fully populated so
// dedupWindow treats it as a "real" record (zero-IngesterID records
// are passed through without dedup tracking — see shouldSkip).
func dedupTestRecord(t *testing.T, ts time.Time, ingesterID glid.GLID, ingestSeq uint32, raw string) chunk.Record {
	t.Helper()
	return chunk.Record{
		SourceTS: ts,
		IngestTS: ts,
		EventID: chunk.EventID{
			IngesterID: ingesterID,
			NodeID:     ingesterID, // distinct GLID also fine; reuse for compactness
			IngestTS:   ts,
			IngestSeq:  ingestSeq,
		},
		Attrs: chunk.Attributes{"msg": raw},
		Raw:   []byte(raw),
	}
}

func TestDedupWindowSkipsCrossReplicaDuplicate(t *testing.T) {
	t.Parallel()
	d := newDedupWindow(query.OrderByIngestTS)
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing := glid.New()

	r := dedupTestRecord(t, now, ing, 1, "alpha")
	// First arrival: keep.
	if d.shouldSkip(r) {
		t.Fatal("first record skipped; want kept")
	}
	// Second arrival of the same record from a different replica:
	// same EventID → same arrival TS → adjacent in the merge stream
	// → dedup collapses.
	if !d.shouldSkip(r) {
		t.Fatal("cross-replica duplicate not collapsed")
	}
}

func TestDedupWindowKeepsDistinctRecordsAtSameTimestamp(t *testing.T) {
	t.Parallel()
	d := newDedupWindow(query.OrderByIngestTS)
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing := glid.New()

	r1 := dedupTestRecord(t, now, ing, 1, "alpha")
	r2 := dedupTestRecord(t, now, ing, 2, "beta") // distinct IngestSeq

	if d.shouldSkip(r1) {
		t.Fatal("r1 skipped; want kept")
	}
	if d.shouldSkip(r2) {
		t.Fatal("r2 (distinct EventID) skipped; want kept")
	}
}

func TestDedupWindowAdvancesTSAndClearsState(t *testing.T) {
	t.Parallel()
	d := newDedupWindow(query.OrderByIngestTS)
	now := time.Unix(0, 1_700_000_000_000_000_000)
	later := now.Add(time.Millisecond)
	ing := glid.New()

	r1 := dedupTestRecord(t, now, ing, 1, "alpha")
	r2 := dedupTestRecord(t, later, ing, 1, "alpha-later") // same fields except TS

	if d.shouldSkip(r1) {
		t.Fatal("r1 skipped; want kept")
	}
	if d.shouldSkip(r2) {
		t.Fatal("r2 at later TS skipped; want kept (window advances + clears)")
	}
	// And a duplicate of r2 at the same later TS is now caught.
	if !d.shouldSkip(r2) {
		t.Fatal("duplicate at advanced TS not caught")
	}
}

func TestDedupWindowPassesThroughZeroIngesterIDRecords(t *testing.T) {
	t.Parallel()
	// Synthetic test fixtures and pre-EventID-introduction records
	// carry a zero IngesterID. dedupWindow passes them through
	// untracked — collapsing distinct records that share zero NodeID
	// / IngestSeq would falsely collapse legitimate distinct records.
	d := newDedupWindow(query.OrderByIngestTS)
	now := time.Unix(0, 1_700_000_000_000_000_000)
	var zero glid.GLID
	r := dedupTestRecord(t, now, zero, 0, "zero-ingester")
	if d.shouldSkip(r) {
		t.Fatal("zero-IngesterID record skipped; want pass-through")
	}
	if d.shouldSkip(r) {
		t.Fatal("zero-IngesterID record skipped on second pass; want pass-through (untracked)")
	}
}

func TestDedupWindowCrossReplicaThreeReplicasSameRecord(t *testing.T) {
	t.Parallel()
	// Real-world fan-out shape: a record is in all three Receiving
	// members. Search results arrive in some order from all three;
	// the merge boundary sees three arrivals with identical EventID,
	// identical IngestTS. dedup emits the record exactly once.
	d := newDedupWindow(query.OrderByIngestTS)
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing := glid.New()
	r := dedupTestRecord(t, now, ing, 42, "fan-out")

	emitted := 0
	for range 3 {
		if !d.shouldSkip(r) {
			emitted++
		}
	}
	if emitted != 1 {
		t.Errorf("emitted=%d; want 1 across 3-replica fan-out", emitted)
	}
}
