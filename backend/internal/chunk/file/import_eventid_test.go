package file

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// TestImportRecordsPreservesEventID is the regression test for
// gastrolog-5qwkw: importState.writeRecord was calling
// EncodeIdxEntry without IngestSeq, IngesterID, or NodeID, so any
// chunk imported via ImportRecords (cross-node sealed-chunk
// replication, MoveChunk fallback, etc.) ended up with zero
// EventIDs in idx.log. The downstream effect: histogram dedup
// can't match follower-replicated records against leader
// originals (rec.EventID == zero skips the dedup branch entirely),
// silently double-counting during cross-node search aggregation.
//
// The fix existed in a stash from gastrolog-4xusf but never
// landed; this commit moves it to main and pins it with the test.
//
// Test flow:
//   1. Append a record into Manager A with an explicit non-zero
//      EventID. Seal.
//   2. Open the sealed chunk via cursor and verify its EventID
//      survives the encode/decode round-trip on the source.
//   3. ImportRecords into Manager B (a fresh manager) using the
//      cursor as the iterator. This is the cross-node-style
//      transfer path.
//   4. Open Manager B's imported chunk via cursor; assert the
//      record's EventID matches the original.
//
// Pre-fix: step 4's EventID would be all zeros. Post-fix: it
// matches the original.
func TestImportRecordsPreservesEventID(t *testing.T) {
	t.Parallel()

	dirA := t.TempDir()
	dirB := t.TempDir()

	cmA, err := NewManager(Config{Dir: dirA, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmA.Close() }()

	cmB, err := NewManager(Config{Dir: dirB, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmB.Close() }()

	// Construct an explicit EventID — the values that should survive
	// the full encode → wire-transfer → import → decode cycle.
	originalEventID := chunk.EventID{
		IngesterID: glid.MustParse("11111111-1111-1111-1111-111111111111"),
		NodeID:     glid.MustParse("22222222-2222-2222-2222-222222222222"),
		IngestTS:   time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC),
		IngestSeq:  42,
	}

	rec := chunk.Record{
		IngestTS: originalEventID.IngestTS,
		WriteTS:  originalEventID.IngestTS,
		EventID:  originalEventID,
		Raw:      []byte("payload"),
	}

	if _, _, err := cmA.Append(rec); err != nil {
		t.Fatalf("Append on A: %v", err)
	}
	activeA := cmA.Active()
	if activeA == nil {
		t.Fatal("no active chunk on A")
	}
	srcChunkID := activeA.ID
	if err := cmA.Seal(); err != nil {
		t.Fatalf("Seal on A: %v", err)
	}

	// Open a cursor on the sealed chunk in A. Iterate it as the
	// source for ImportRecords on B.
	cursorA, err := cmA.OpenCursor(srcChunkID)
	if err != nil {
		t.Fatalf("OpenCursor on A: %v", err)
	}
	defer func() { _ = cursorA.Close() }()

	// Verify EventID is intact on the source side first.
	srcRec, _, err := cursorA.Next()
	if err != nil {
		t.Fatalf("cursorA.Next: %v", err)
	}
	if !eventIDEqual(srcRec.EventID, originalEventID) {
		t.Fatalf("source-side EventID after seal+cursor = %+v, want %+v",
			srcRec.EventID, originalEventID)
	}

	// Now import into B. Build a one-record iterator that yields
	// the source record we just read.
	delivered := false
	iter := func() (chunk.Record, error) {
		if delivered {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		delivered = true
		return srcRec, nil
	}

	dstChunkID := chunk.NewChunkID()
	if _, err := cmB.ImportRecords(dstChunkID, iter); err != nil {
		t.Fatalf("ImportRecords on B: %v", err)
	}

	// Read the imported record back via cursor on B and assert EventID.
	cursorB, err := cmB.OpenCursor(dstChunkID)
	if err != nil {
		t.Fatalf("OpenCursor on B: %v", err)
	}
	defer func() { _ = cursorB.Close() }()

	dstRec, _, err := cursorB.Next()
	if err != nil {
		t.Fatalf("cursorB.Next: %v", err)
	}

	// THE assertion that fails pre-fix: imported record's EventID
	// must match the original. Pre-fix all three EventID fields
	// (IngesterID, NodeID, IngestSeq) come back zero.
	if !eventIDEqual(dstRec.EventID, originalEventID) {
		t.Errorf("imported record EventID = %+v\nwant                       %+v\n(gastrolog-5qwkw: importState.writeRecord must include all EventID fields in EncodeIdxEntry)",
			dstRec.EventID, originalEventID)
	}
}

// eventIDEqual compares two EventIDs treating IngestTS via
// time.Time.Equal (which compares the absolute moment, ignoring
// the local timezone the kernel returned the value in). idx.log
// stores IngestTS as int64 nanoseconds-since-epoch with no zone
// info, so the read-back value's Location is the local TZ even
// though the absolute moment matches the original.
func eventIDEqual(a, b chunk.EventID) bool {
	return a.IngesterID == b.IngesterID &&
		a.NodeID == b.NodeID &&
		a.IngestSeq == b.IngestSeq &&
		a.IngestTS.Equal(b.IngestTS)
}
