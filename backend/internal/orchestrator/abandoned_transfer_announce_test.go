package orchestrator

// gastrolog-2l918 review finding 4: when a transfer defers terminally at
// the source (disposition changed away from transfer, target changed,
// corruption mismatch), nothing retracts the destination's announce-
// imported placeholder entry. reconcileAbandonedTransferAnnounces
// (vault_lifecycle_reconciler.go) is the destination-side GC that closes
// this: a manifest entry introduced by transfer (TransferSourceVaultID
// set) with zero confirmed holders, sitting past
// abandonedTransferAnnounceGCAge, gets retracted via the same
// receipt-based deleteChunk every other retirement path in the
// reconciler uses.

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestSweepAbandonedTransferAnnouncesRetractsOnlyTheStalePhantom(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	now := time.Now()
	old := now.Add(-(abandonedTransferAnnounceGCAge + time.Hour))    // past the GC age
	recent := now.Add(-(abandonedTransferAnnounceGCAge - time.Hour)) // still within grace

	sourceVaultID := glid.New()
	idAbandoned := chunk.NewChunkID()
	idRecent := chunk.NewChunkID()
	idHasHolder := chunk.NewChunkID()
	idNotTransfer := chunk.NewChunkID()

	seedTransfer := func(id chunk.ChunkID, sealedAt time.Time, holders []string) {
		data, err := vaultctlfsm.MarshalRepatriateChunk(vaultctlfsm.ManifestEntry{
			ID: id, RecordCount: 1, SealedAt: sealedAt,
			TransferSourceVaultID: sourceVaultID, Holders: holders,
		})
		if err != nil {
			t.Fatalf("marshal repatriate %s: %v", id, err)
		}
		if err := fsm.Apply(&hraft.Log{Data: data}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	seedTransfer(idAbandoned, old, nil)                // zero holders, old: must be retracted
	seedTransfer(idRecent, recent, nil)                // zero holders, recent: must survive
	seedTransfer(idHasHolder, old, []string{"node-B"}) // old but landed: must survive

	// A normal (non-transfer) old sealed chunk must be untouched by this
	// sweep — that's SweepStaleLeaderFSMEntries's job, not this one's.
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(idNotTransfer, old, old, old)}); err != nil {
		t.Fatalf("create idNotTransfer: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idNotTransfer, old, 1, 1, old, old, old, false, old)}); err != nil {
		t.Fatalf("seal idNotTransfer: %v", err)
	}

	var deletedRequests []chunk.ChunkID
	var deleteReasons []string
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		IsFollower: false,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftRequestDelete: func(id chunk.ChunkID, reason string, _ []string) error {
				deletedRequests = append(deletedRequests, id)
				deleteReasons = append(deleteReasons, reason)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepAbandonedTransferAnnounces()

	if len(deletedRequests) != 1 {
		t.Fatalf("delete proposals = %d (%v), want exactly 1 (only the abandoned phantom)", len(deletedRequests), deletedRequests)
	}
	if deletedRequests[0] != idAbandoned {
		t.Fatalf("retracted chunk = %s, want %s", deletedRequests[0], idAbandoned)
	}
	if deleteReasons[0] != "abandoned-transfer-announce" {
		t.Fatalf("delete reason = %q, want abandoned-transfer-announce", deleteReasons[0])
	}
}

// TestSweepAbandonedTransferAnnouncesFollowerNoOp verifies only the
// destination's config placement leader proposes retractions — matching
// every other write path in the reconciler.
func TestSweepAbandonedTransferAnnouncesFollowerNoOp(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	old := time.Now().Add(-(abandonedTransferAnnounceGCAge + time.Hour))
	id := chunk.NewChunkID()
	data, err := vaultctlfsm.MarshalRepatriateChunk(vaultctlfsm.ManifestEntry{
		ID: id, RecordCount: 1, SealedAt: old, TransferSourceVaultID: glid.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: data}); err != nil {
		t.Fatal(err)
	}

	var deleted []chunk.ChunkID
	vaultInst := &VaultInstance{
		VaultID:    glid.New(),
		IsFollower: true,
		RaftApplyFacet: RaftApplyFacet{
			ApplyRaftRequestDelete: func(id chunk.ChunkID, _ string, _ []string) error {
				deleted = append(deleted, id)
				return nil
			},
		},
	}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.fsm = fsm

	rec.SweepAbandonedTransferAnnounces()

	if len(deleted) != 0 {
		t.Fatalf("follower proposed %d retractions, want 0", len(deleted))
	}
}
