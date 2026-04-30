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
// EnsureSealed MUST force-demote so subsequent appends don't keep
// landing on a chunk the rest of the cluster considers immutable.
//
// Topology-independent contract: the FSM is authoritative; if it
// says sealed, the local Manager's active pointer must yield. This
// holds equally for ingest tiers (where a continuous record-stream
// would eventually swap active too) and for downstream tiers (where
// no continuous record-stream means no natural swap, see
// gastrolog-2yeht). The "skip-active in steady state" variant
// previously introduced was wrong for downstream tiers — reverted
// in 2yeht.
//
// Original incident: a follower restarted after the cluster sealed
// its in-flight chunk in absence; pre-fix, EnsureSealed silently
// skipped the still-active chunk and the local Manager kept
// appending past the rotation cap (53K records on a 10K-cap tier;
// replica_count=1).
//
// Post-fix: EnsureSealed force-demotes the local active pointer
// whenever the FSM says sealed. Subsequent Appends either land on
// a fresh active chunk or fail with an explicit error — never
// silently land on a chunk the cluster has frozen.
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
	// Manager's local active pointer still points at it.
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
	// or it returns ErrChunkSealed for the explicit-rejection variant.
	postSealTS := time.Now().Add(time.Hour) // ensure later than pre-seal records
	landedID, _, appendErr := cm.Append(chunk.Record{
		IngestTS: postSealTS, WriteTS: postSealTS,
		Raw: []byte("post-seal-attempt"),
	})
	if appendErr != nil {
		if !errors.Is(appendErr, chunk.ErrChunkSealed) {
			t.Errorf("Append after EnsureSealed returned %v; want ErrChunkSealed for the rejection variant",
				appendErr)
		}
		return
	}
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
