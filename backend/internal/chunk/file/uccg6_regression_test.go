package file

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestUccg6_EnsureSealedAndDemoteDemotesLocalActive is the regression
// test for gastrolog-uccg6: when the FSM tells the local chunk
// manager that a chunk is sealed but that chunk is still the local
// active pointer, the recovery path (EnsureSealedAndDemote) MUST
// force-demote so subsequent appends don't keep landing on a chunk
// the rest of the cluster considers immutable.
//
// Pre-fix behavior (gastrolog-51gme step 8 EnsureSealed): "skip if
// local active; let the next rotation reconcile." That assumption is
// correct in steady state because the leader's record-stream's next
// TierReplicationAppend (for the new active chunk) authoritatively
// swaps the follower's active pointer within a few ms — but it's
// wrong on the offline-during-seal restart path because the
// record-stream that would do the swap is gone for any chunk sealed
// in this node's absence.
//
// uccg6's failure mode: follower crashes mid-write, cluster seals
// the chunk while the follower is offline, follower restarts,
// Manager.Open picks the (still unsealed on disk) chunk as its
// active pointer, snapshot install brings the FSM forward — the
// reconciler's projectAllSealedFromFSM walk fires
// EnsureSealedAndDemote for the now-FSM-sealed chunk while the local
// active pointer still points at it. The recovery method MUST
// force-demote (close files, mark sealed=true, clear m.active) so
// subsequent appends rotate to a fresh active chunk.
//
// This test runs entirely in-process (single Manager) — the failure
// mode is purely a Manager-FSM-projection contract test, no
// clustering required. The 4-node cluster harness scenario described
// in uccg6's acceptance criteria is a higher-fidelity but more
// expensive follow-up; this unit-level test pins the contract.
func TestUccg6_EnsureSealedAndDemoteDemotesLocalActive(t *testing.T) {
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
	// install: it iterates FSM-sealed entries and calls
	// EnsureSealedAndDemote for each (the recovery variant — no
	// record-stream is coming to swap active).
	if err := cm.EnsureSealedAndDemote(frozenChunkID); err != nil {
		t.Fatalf("EnsureSealedAndDemote: %v", err)
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
		t.Errorf("frozen chunk %s is no longer Sealed=true after EnsureSealedAndDemote (the seal must persist)",
			frozenChunkID)
	}
}

// TestEnsureSealedSkipsLocalActiveInSteadyState pins the steady-state
// half of the EnsureSealed/EnsureSealedAndDemote contract split: the
// non-recovery path (called from onSeal when CmdSealChunk applies in
// the live cluster) MUST skip when the local active pointer matches
// the FSM-sealed chunk, deferring to the leader's record-stream to
// swap active via the next TierReplicationAppend for the new chunk.
//
// Without this skip, every normal seal-rotation would force-demote
// the follower's active pointer (the FSM apply consistently lands
// before the record-stream's swap), producing one log line per chunk
// per follower per seal — the spam observed when uccg6 first landed
// with both paths collapsed into one method. See the SealEnsurer
// interface doc for the steady-state vs recovery split.
func TestEnsureSealedSkipsLocalActiveInSteadyState(t *testing.T) {
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

	// Append a few records so there's an active chunk.
	for i := range 3 {
		ts := time.Now().Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: []byte(fmt.Sprintf("steady-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	activeBefore := cm.Active()
	if activeBefore == nil {
		t.Fatal("no active chunk")
	}
	activeID := activeBefore.ID

	// Steady-state: EnsureSealed must skip — the active pointer stays.
	if err := cm.EnsureSealed(activeID); err != nil {
		t.Fatalf("EnsureSealed: %v", err)
	}

	activeAfter := cm.Active()
	if activeAfter == nil || activeAfter.ID != activeID {
		t.Errorf("EnsureSealed must NOT change active pointer in steady state; was %s, now %v",
			activeID, activeAfter)
	}

	// And the chunk must NOT be sealed on disk yet — the natural
	// rotation triggered by the record-stream's swap is what seals it.
	meta, err := cm.Meta(activeID)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if meta.Sealed {
		t.Errorf("EnsureSealed prematurely sealed active chunk %s in steady state",
			activeID)
	}
}
