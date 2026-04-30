package file

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestUccg6_EnsureSealedDemotesLocalActive is the regression test for
// gastrolog-uccg6: when the FSM tells the local chunk manager that a
// chunk is sealed but that chunk is still the local active pointer,
// EnsureSealed must NOT silently skip — it must demote the chunk from
// active so subsequent appends don't keep landing on a chunk the rest
// of the cluster considers immutable.
//
// Pre-fix behavior (gastrolog-51gme step 8 EnsureSealed): "skip if
// local active; let the next rotation reconcile." That comment
// assumed steady-state: leader's record-stream would naturally swap
// the follower's active pointer to the new chunk on the next
// CmdAppend with a different chunk_id, and EnsureSealed would re-fire
// on the now-non-active old chunk.
//
// uccg6's failure mode is the offline-during-seal restart path:
// follower crashes mid-write, cluster seals the chunk while the
// follower is offline, follower restarts, Manager.Open picks the
// (still unsealed on disk) chunk as its active pointer. The leader's
// record-stream may have already moved on — but the follower's
// snapshot install fires EnsureSealed for the now-FSM-sealed chunk
// while the local active pointer still points at it. Pre-fix:
// silently skipped, and the chunk keeps growing past its rotation
// cap with records that never replicate (replica_count=1).
//
// Post-fix: EnsureSealed force-demotes the local active pointer when
// the FSM says sealed. Subsequent Appends either land on a fresh
// active chunk or fail with an explicit error — never silently land
// on a chunk the cluster has frozen.
//
// This test runs entirely in-process (single Manager) — the failure
// mode is purely a Manager-FSM-projection race, no clustering
// required. The 4-node cluster harness scenario described in uccg6's
// acceptance criteria is a higher-fidelity but more expensive
// follow-up; this unit-level test pins the contract.
func TestUccg6_EnsureSealedDemotesLocalActive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := NewManager(Config{
		Dir: dir,
		Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	// Append a few records. The first append creates an active chunk;
	// subsequent appends land on it.
	for i := range 5 {
		ts := time.Now().Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte(fmt.Sprintf("pre-seal-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	activeBeforeSeal := cm.Active()
	if activeBeforeSeal == nil {
		t.Fatal("no active chunk after initial appends")
	}
	frozenChunkID := activeBeforeSeal.ID

	// Simulate the FSM saying "this chunk is sealed" while the
	// Manager's local active pointer still points at it. This is what
	// projectAllSealedFromFSM does on the follower after snapshot
	// install: it iterates FSM-sealed entries and calls EnsureSealed
	// for each.
	if err := cm.EnsureSealed(frozenChunkID); err != nil {
		t.Fatalf("EnsureSealed: %v", err)
	}

	// Acceptance #1: the local active pointer must NOT still point at
	// the now-FSM-sealed chunk. Either it's nil (Manager will create
	// a fresh one on the next Append) or it's a different chunk ID.
	activeAfterSeal := cm.Active()
	if activeAfterSeal != nil && activeAfterSeal.ID == frozenChunkID {
		t.Errorf("active pointer still points at FSM-sealed chunk %s after EnsureSealed (uccg6 regression: appends will keep landing on a frozen chunk)",
			frozenChunkID)
	}

	// Acceptance #2: a subsequent Append must NOT land records on the
	// FSM-sealed chunk. Either it succeeds (landing on a fresh active)
	// or it returns ErrChunkSealed for the explicit-rejection variant
	// of the fix. Both are valid; what's NOT valid is silently
	// extending the sealed chunk.
	postSealTS := time.Now().Add(time.Hour) // ensure later than pre-seal records
	landedID, _, appendErr := cm.Append(chunk.Record{
		IngestTS: postSealTS, WriteTS: postSealTS,
		Raw: []byte("post-seal-attempt"),
	})
	if appendErr != nil {
		// Explicit rejection variant — must be ErrChunkSealed (or wrap
		// it) so the caller can rotate. Generic errors aren't
		// acceptable because they don't tell the leader's record
		// forwarder how to recover.
		if !errors.Is(appendErr, chunk.ErrChunkSealed) {
			t.Errorf("Append after EnsureSealed returned %v; want ErrChunkSealed for the rejection variant",
				appendErr)
		}
		// Rejection variant accepted — done.
		return
	}

	// Auto-rotate variant: append succeeded but on a NEW chunk, not
	// the frozen one.
	if landedID == frozenChunkID {
		t.Errorf("Append after EnsureSealed landed on frozen chunk %s (uccg6 regression: 53K-record incident exactly)",
			frozenChunkID)
	}

	// And the frozen chunk must still be marked sealed on disk — no
	// silent un-seal during the demote path.
	frozenMeta, err := cm.Meta(frozenChunkID)
	if err != nil {
		t.Fatalf("Meta(frozen): %v", err)
	}
	if !frozenMeta.Sealed {
		t.Errorf("frozen chunk %s is no longer Sealed=true after EnsureSealed (the seal must persist)",
			frozenChunkID)
	}
}
