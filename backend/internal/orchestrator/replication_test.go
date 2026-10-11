package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
	"log/slog"
	"testing"
	"time"

	"fmt"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// ---------- helpers ----------

func newReplicationInstance(t *testing.T, vaultID glid.GLID, followers []system.ReplicationTarget, isFollower bool, leaderNodeID string) *VaultInstance {
	t.Helper()
	cm, err := chunkmem.NewFactory()(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	im, err := indexmem.NewFactory()(nil, cm, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &VaultInstance{
		VaultID:         vaultID,
		Type:            "memory",
		Chunks:          cm,
		Indexes:         im,
		Query:           query.New(cm, im, nil),
		IsFollower:      isFollower,
		LeaderNodeID:    leaderNodeID,
		FollowerTargets: followers,
	}
}

func testRecord(raw string) chunk.Record {
	return chunk.Record{
		SourceTS: time.Now(),
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"msg": raw},
		Raw:      []byte(raw),
	}
}

// ================================================================
// SEAL ACTIVE CHUNK TESTS
// ================================================================

func TestSealActiveChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	vault.Name = "seal-test"
	orch.RegisterVault(vault)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord("seal-me")); err != nil {
		t.Fatal(err)
	}

	active := vault.Instance.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	chunkID := active.ID

	if err := orch.SealActiveChunk(vaultID, chunkID); err != nil {
		t.Fatal(err)
	}

	newActive := vault.Instance.Chunks.Active()
	if newActive != nil && newActive.ID == chunkID {
		t.Error("expected active chunk to change after seal")
	}
}

func TestSealActiveChunkMismatchSkipsSeal(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	vault.Name = "mismatch"
	orch.RegisterVault(vault)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	// Seal with a wrong chunk ID — should be a no-op (the expected chunk
	// was already rotated by the follower's own rotation policy).
	wrongID := chunkIDAt(time.Now().Add(-1 * time.Hour))
	if err := orch.SealActiveChunk(vaultID, wrongID); err != nil {
		t.Fatal(err)
	}

	metas, _ := vault.Instance.Chunks.List()
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed != 0 {
		t.Error("expected NO seal when chunk ID doesn't match — seal should be skipped")
	}
}

func TestSealActiveChunkNoActiveChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	orch.RegisterVault(vault)

	// No records appended — no active chunk.
	err := orch.SealActiveChunk(vaultID, chunk.ChunkID{})
	if err != nil {
		t.Errorf("expected nil error for no active chunk, got %v", err)
	}
}

// TestClusterDeletePropagation verifies that a receipt-protocol delete
// removes the chunk from every node's chunk manager AND its filesystem
// directory.
func TestClusterDeletePropagation(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node convergence test")
	}
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 1, 100)

	leaderNode := h.nodes["leader"]
	leaderInst := leaderNode.instances[0]

	// Ingest 500 records (5 sealed chunks).
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 500 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToVault(h.vaultID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "del-prop-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := leaderInst.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderInst.Chunks.Seal()
	}

	metas, _ := leaderInst.Chunks.List()
	t.Logf("leader: %d sealed chunks", len(metas))

	processor, ok := leaderInst.Chunks.(chunk.ChunkPostSealProcessor)
	if ok {
		for _, m := range metas {
			_ = processor.PostSealProcess(context.Background(), m.ID)
		}
	}
	h.copySealedToFollowers(t, leaderNode, 0, metas)

	for _, fid := range []string{"f1", "f2", "f3"} {
		count := cursorCountRecords(t, h.nodes[fid].instances[0].Chunks)
		if count != 500 {
			t.Fatalf("follower %s holds %d records before the delete, want 500", fid, count)
		}
	}

	// Delete all chunks cluster-wide via the receipt protocol. The leader's
	// reconciler proposes CmdRequestDelete with every node in expectedFrom;
	// setupCluster's fake FSM applier fulfills the obligation on each node.
	for _, m := range metas {
		if err := leaderInst.Reconciler.deleteChunk(m.ID, "test-delete", h.allNodeIDs()); err != nil {
			t.Errorf("deleteChunk(%s): %v", m.ID, err)
		}
	}

	// ---- Verify: all nodes have 0 cursor-readable records ----
	for _, nid := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nid].instances[0].Chunks)
		if count != 0 {
			t.Errorf("%s: cursor read %d records after delete (should be 0)", nid, count)
		}
	}

	// ---- Verify: no chunk directories on disk on ANY node ----
	h.assertVaultDirEmpty(t, 0)
}
