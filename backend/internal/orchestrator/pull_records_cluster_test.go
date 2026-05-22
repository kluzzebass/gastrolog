// Multi-node integration test for the pull-by-EventID flow
// (gastrolog-4t3y4 step 6). Exercises the end-to-end loop via the
// in-process directChunkReplicator bridge:
//
//	puller.PullRecords(source, vault, chunk, eventIDs) →
//	  source.PullSelectedRecords → dispatchFillRecords →
//	  puller.AppendToVault (via SendFillRecords on the bridged replicator)
//
// Covers the "puller missing records that source has" scenario that
// reconcile / drain / catchup will all use.

package orchestrator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// TestClusterPullRecords_PullerMissingRecordsRecoversFromSource models
// the seal-time slow-path reconcile or node-return reconcile scenario:
// source holds records that the puller doesn't, the puller knows the
// missing EventIDs (in reality from a Merkle exchange — here injected),
// PullRecords + Fill frames deliver them.
//
// 4-node file-backed cluster. Records 5 events on the leader (gets all 5),
// then ingests 2 more records on a follower so the follower has 5+2 records
// but the leader is missing 2 EventIDs. Puller (the leader) issues
// PullRecords against the follower for those 2 EventIDs. After the async
// fill push completes, the leader has all 7 records cursor-verified.
func TestClusterPullRecords_PullerMissingRecordsRecoversFromSource(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"puller", "source", "f2", "f3"}, 1, 10000)

	puller := h.nodes["puller"]
	source := h.nodes["source"]

	// Phase 1: Ingest 5 records on the leader; replicates to all followers
	// via the existing replication primitives. Capture EventIDs as we go so
	// we can later request them via pull (not strictly needed for this test
	// but mirrors how 37k2b-b's Merkle slow path will produce them).
	allEventIDs := make([]chunk.EventID, 0, 7)
	t0 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	for i := range 5 {
		ingesterID := glid.New()
		nodeID := glid.New()
		ts := t0.Add(time.Duration(i) * time.Millisecond)
		rec := chunk.Record{
			SourceTS: ts,
			IngestTS: ts,
			WriteTS:  ts,
			EventID: chunk.EventID{
				IngesterID: ingesterID,
				NodeID:     nodeID,
				IngestTS:   ts,
				IngestSeq:  uint32(i),
			},
			Attrs: chunk.Attributes{"phase": "1"},
			Raw:   fmt.Appendf(nil, "rec-%d", i),
		}
		if err := puller.orch.AppendToVault(h.vaultID, chunk.ChunkID{}, rec); err != nil {
			t.Fatalf("phase 1 append %d: %v", i, err)
		}
		allEventIDs = append(allEventIDs, rec.EventID)
	}

	// Verify phase 1 baseline: puller has 5, source (follower) has 5.
	if got := cursorCountRecords(t, puller.instances[0].Chunks); got != 5 {
		t.Fatalf("phase 1: puller has %d records, want 5", got)
	}
	if got := cursorCountRecords(t, source.instances[0].Chunks); got != 5 {
		t.Fatalf("phase 1: source has %d records, want 5", got)
	}

	// Active chunk's ID is now established on both nodes (same ID — that's
	// the LeaderDriven-era convention this branch still uses).
	activeOnPuller := puller.instances[0].Chunks.Active()
	if activeOnPuller == nil {
		t.Fatal("puller has no active chunk after phase 1")
	}
	activeOnSource := source.instances[0].Chunks.Active()
	if activeOnSource == nil {
		t.Fatal("source has no active chunk after phase 1")
	}
	if activeOnPuller.ID != activeOnSource.ID {
		t.Fatalf("active chunk ID divergence: puller=%s source=%s",
			activeOnPuller.ID, activeOnSource.ID)
	}
	chunkID := activeOnPuller.ID

	// Phase 2: Ingest 2 more records DIRECTLY on the source (follower),
	// without going through the leader's replication. This simulates the
	// fan-out divergence we're recovering from — the source got records the
	// puller didn't.
	missingEventIDs := make([]chunk.EventID, 0, 2)
	for i := 5; i < 7; i++ {
		ingesterID := glid.New()
		nodeID := glid.New()
		ts := t0.Add(time.Duration(i) * time.Millisecond)
		rec := chunk.Record{
			SourceTS: ts,
			IngestTS: ts,
			WriteTS:  ts,
			EventID: chunk.EventID{
				IngesterID: ingesterID,
				NodeID:     nodeID,
				IngestTS:   ts,
				IngestSeq:  uint32(i),
			},
			Attrs: chunk.Attributes{"phase": "2"},
			Raw:   fmt.Appendf(nil, "rec-%d", i),
		}
		if err := source.orch.AppendToVault(h.vaultID, chunkID, rec); err != nil {
			t.Fatalf("phase 2 append %d: %v", i, err)
		}
		missingEventIDs = append(missingEventIDs, rec.EventID)
		allEventIDs = append(allEventIDs, rec.EventID)
	}

	// Verify the divergence: source has 7, puller still has 5.
	if got := cursorCountRecords(t, source.instances[0].Chunks); got != 7 {
		t.Fatalf("phase 2: source has %d records, want 7", got)
	}
	if got := cursorCountRecords(t, puller.instances[0].Chunks); got != 5 {
		t.Fatalf("phase 2: puller has %d records, want 5 (no divergence yet)", got)
	}

	// Phase 3: The puller's reconcile loop (here simulated as a direct call
	// to the replicator) requests the missing EventIDs from the source.
	scheduled, missing, err := puller.orch.chunkReplicator.PullRecords(
		context.Background(), "source", h.vaultID, chunkID, missingEventIDs, "puller",
	)
	if err != nil {
		t.Fatalf("PullRecords: %v", err)
	}
	if scheduled != 2 || missing != 0 {
		t.Errorf("scheduled=%d missing=%d, want 2/0", scheduled, missing)
	}

	// Phase 4: The async fill push runs in a goroutine on the source side;
	// wait for it to converge the puller's record set. The Fill frame
	// dispatches through directChunkReplicator.SendFillRecords → puller's
	// AppendToVault, which writes the records into the puller's active
	// chunk.
	if err := waitForPullerConverged(t, puller.instances[0].Chunks, 7, 2*time.Second); err != nil {
		t.Fatalf("phase 4 convergence: %v", err)
	}

	// Final verification: puller has all 7 records, by cursor.
	if got := cursorCountRecords(t, puller.instances[0].Chunks); got != 7 {
		t.Errorf("final: puller has %d records, want 7", got)
	}

	// Spot-check that the specific missing EventIDs are present on puller.
	seen := collectEventIDs(t, puller.instances[0].Chunks)
	for _, want := range missingEventIDs {
		if !seen[want] {
			t.Errorf("missing EventID %v not on puller after pull", want)
		}
	}
}

// waitForPullerConverged polls until the puller's local chunk record count
// reaches the expected total. dispatchFillRecords runs in a goroutine; the
// test must wait for it to land the records.
func waitForPullerConverged(t *testing.T, cm chunk.ChunkManager, wantTotal int64, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cursorCountRecords(t, cm) >= wantTotal {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("puller record count never reached %d within %v (current: %d)",
		wantTotal, timeout, cursorCountRecords(t, cm))
}

// collectEventIDs walks every record in the chunk manager via cursor and
// builds a set of seen EventIDs. Used to assert that specific records
// (not just a count) are present after a pull completes.
func collectEventIDs(t *testing.T, cm chunk.ChunkManager) map[chunk.EventID]bool {
	t.Helper()
	seen := make(map[chunk.EventID]bool)
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	chunkIDs := make([]chunk.ChunkID, 0, len(metas)+1)
	for _, m := range metas {
		chunkIDs = append(chunkIDs, m.ID)
	}
	if active := cm.Active(); active != nil {
		// List() returns sealed only on file manager; include active explicitly.
		alreadySeen := false
		for _, id := range chunkIDs {
			if id == active.ID {
				alreadySeen = true
				break
			}
		}
		if !alreadySeen {
			chunkIDs = append(chunkIDs, active.ID)
		}
	}
	for _, id := range chunkIDs {
		cur, err := cm.OpenCursor(id)
		if err != nil {
			t.Fatalf("open cursor %s: %v", id, err)
		}
		for {
			rec, _, err := cur.Next()
			if err != nil {
				break
			}
			seen[rec.EventID] = true
		}
		_ = cur.Close()
	}
	return seen
}
