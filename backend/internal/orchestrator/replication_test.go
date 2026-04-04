package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"fmt"
	"os"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// ---------- fake forwarder ----------

type replicationFakeForwarder struct{}

func (f *replicationFakeForwarder) Forward(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (f *replicationFakeForwarder) ForwardToTier(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}

// ---------- fake transferrer that records seal commands ----------

type replicationFakeTransferrer struct {
	sealCalls    []sealCall
	sealErr      error
	forwardCalls []transitionTransferCall
}

type sealCall struct {
	nodeID  string
	vaultID uuid.UUID
	tierID  uuid.UUID
	chunkID chunk.ChunkID
}

func (m *replicationFakeTransferrer) TransferRecords(_ context.Context, _ string, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}
func (m *replicationFakeTransferrer) ForwardAppend(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (m *replicationFakeTransferrer) ForwardTierAppend(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (m *replicationFakeTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil
}
func (m *replicationFakeTransferrer) ForwardSealTier(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error {
	if m.sealErr != nil {
		return m.sealErr
	}
	m.sealCalls = append(m.sealCalls, sealCall{nodeID: nodeID, vaultID: vaultID, tierID: tierID, chunkID: chunkID})
	return nil
}
func (m *replicationFakeTransferrer) ForwardDeleteChunk(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (m *replicationFakeTransferrer) ReplicateSealedChunk(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (m *replicationFakeTransferrer) StreamToTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}
// ---------- helpers ----------

func newReplicationTier(t *testing.T, tierID uuid.UUID, followers []config.ReplicationTarget, isFollower bool, leaderNodeID string) *TierInstance {
	t.Helper()
	cm, err := chunkmem.NewFactory()(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	im, err := indexmem.NewFactory()(nil, cm, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &TierInstance{
		TierID:           tierID,
		Type:             "memory",
		Chunks:           cm,
		Indexes:          im,
		Query:            query.New(cm, im, nil),
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
// SEAL ACTIVE TIER TESTS
// ================================================================

func TestSealActiveTier(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	vault.Name = "seal-test"
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testRecord("seal-me")); err != nil {
		t.Fatal(err)
	}

	active := vault.Tiers[0].Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	chunkID := active.ID

	if err := orch.SealActiveTier(vaultID, tierID, chunkID); err != nil {
		t.Fatal(err)
	}

	newActive := vault.Tiers[0].Chunks.Active()
	if newActive != nil && newActive.ID == chunkID {
		t.Error("expected active chunk to change after seal")
	}
}

func TestSealActiveTierMismatchSkipsSeal(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	vault.Name = "mismatch"
	orch.RegisterVault(vault)

	if _, _, err := orch.Append(vaultID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	// Seal with a wrong chunk ID — should be a no-op (the expected chunk
	// was already rotated by the follower's own rotation policy).
	wrongID := chunkIDAt(time.Now().Add(-1 * time.Hour))
	if err := orch.SealActiveTier(vaultID, tierID, wrongID); err != nil {
		t.Fatal(err)
	}

	metas, _ := vault.Tiers[0].Chunks.List()
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

func TestSealActiveTierTierNotFound(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	wrongTierID := uuid.Must(uuid.NewV7())
	err := orch.SealActiveTier(vaultID, wrongTierID, chunk.ChunkID{})
	if !errors.Is(err, ErrVaultNotFound) {
		t.Errorf("expected ErrVaultNotFound, got %v", err)
	}
}

func TestSealActiveTierNoActiveChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	// No records appended — no active chunk.
	err := orch.SealActiveTier(vaultID, tierID, chunk.ChunkID{})
	if err != nil {
		t.Errorf("expected nil error for no active chunk, got %v", err)
	}
}

// ================================================================
// CATCHUP TESTS
// ================================================================

func TestCatchupSecondaryNoSealedChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	mock := &replicationFakeTransferrer{}
	orch.transferrer = mock

	// No sealed chunks — catchup should be a no-op.
	err := orch.catchupFollower(context.Background(), vaultID, tierID, "node-2")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCatchupSecondaryOnlyPrimary(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	// This is a follower — should not initiate catchup.
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, true, "node-2"))
	orch.RegisterVault(vault)

	err := orch.catchupFollower(context.Background(), vaultID, tierID, "node-3")
	if err != nil {
		t.Fatalf("expected nil (no-op) for follower, got %v", err)
	}
}

func TestCatchupSecondaryNoTransferrer(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)
	// No transferrer set.

	err := orch.catchupFollower(context.Background(), vaultID, tierID, "node-2")
	if err == nil {
		t.Fatal("expected error for missing transferrer")
	}
}

// ==========================================================================
// Multi-node file-backed replication tests
//
// These use setupCluster (from transition_test.go) with directTransferrer
// to test real in-process replication between file-backed orchestrators.
// ==========================================================================

// TestClusterReplicationSealedChunksArriveOnFollowers verifies that when the
// leader seals chunks (via burst ingestion with rotation policy), calling
// replicateSealedChunk delivers the chunks to all follower nodes. Verified
// via cursor reads AND filesystem directory checks on each follower.
func TestClusterReplicationSealedChunksArriveOnFollowers(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 1, 100)

	leaderNode := h.nodes["leader"]
	tier0 := leaderNode.tiers[0]

	// Burst ingest 1K records → 10 sealed chunks via 100-record rotation.
	const totalRecords = 1_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "repl-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Seal remaining active chunk.
	if active := tier0.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = tier0.Chunks.Seal()
	}

	// Get sealed chunks on leader.
	metas, _ := tier0.Chunks.List()
	if len(metas) < 5 {
		t.Fatalf("expected many sealed chunks, got %d", len(metas))
	}
	t.Logf("leader: %d sealed chunks to replicate", len(metas))

	// Run PostSealProcess on each chunk (compress + index) — required before
	// replication because replicateToFollower opens a cursor which needs the
	// chunk to be readable.
	processor, ok := tier0.Chunks.(chunk.ChunkPostSealProcessor)
	if ok {
		for _, m := range metas {
			if err := processor.PostSealProcess(context.Background(), m.ID); err != nil {
				t.Fatalf("PostSealProcess(%s): %v", m.ID, err)
			}
		}
	}

	// Replicate each sealed chunk to all followers.
	followerTargets := tier0.FollowerTargets
	ctx := context.Background()
	for _, m := range metas {
		leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, h.tierIDs[0], m.ID, followerTargets)
	}

	// ---- Verify: each follower has all records (cursor-verified) ----
	for _, fid := range []string{"f1", "f2", "f3"} {
		followerCM := h.nodes[fid].tiers[0].Chunks
		count := cursorCountRecords(t, followerCM)
		if count != totalRecords {
			t.Errorf("follower %s: cursor read %d records, expected %d", fid, count, totalRecords)
		}
	}

	// ---- Verify: followers have chunk directories on disk ----
	for _, fid := range []string{"f1", "f2", "f3"} {
		dir := h.nodes[fid].tierDirs[0]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		var chunkDirs int
		for _, e := range entries {
			if e.IsDir() && len(e.Name()) == 26 {
				chunkDirs++
			}
		}
		if chunkDirs == 0 {
			t.Errorf("follower %s: no chunk directories on disk after replication", fid)
		}
		t.Logf("follower %s: %d chunk directories on disk", fid, chunkDirs)
	}
}

// TestClusterReplicationSealSync verifies that ForwardSealTier causes the
// follower to seal its active chunk at the same boundary as the leader.
func TestClusterReplicationSealSync(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2"}, 1, 10000) // high rotation so we control seal manually

	leaderNode := h.nodes["leader"]
	leaderTier := leaderNode.tiers[0]

	// Ingest 50 records on leader.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 50 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "seal-sync-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Get leader's active chunk ID BEFORE appending on followers so we can
	// sync it. In production, the leader's ForwardTierAppend sends the chunk ID
	// and followers call SetNextChunkID.
	leaderActive := leaderTier.Chunks.Active()
	if leaderActive == nil {
		t.Fatal("expected active chunk on leader after append")
	}
	leaderChunkID := leaderActive.ID

	// Forward the same records to followers with synchronized chunk IDs.
	// This mirrors the production ForwardTierAppend path which syncs the ID.
	for _, fid := range []string{"f1", "f2"} {
		followerCM := h.nodes[fid].tiers[0].Chunks
		followerCM.SetNextChunkID(leaderChunkID)
		for i := range 50 {
			ts := t0.Add(time.Duration(i) * time.Microsecond)
			if _, _, err := followerCM.Append(chunk.Record{
				IngestTS: ts,
				WriteTS:  ts,
				Raw:      fmt.Appendf(nil, "seal-sync-%d", i),
			}); err != nil {
				t.Fatalf("append to %s record %d: %v", fid, i, err)
			}
		}
	}

	// Verify followers have the same active chunk ID as the leader.
	for _, fid := range []string{"f1", "f2"} {
		active := h.nodes[fid].tiers[0].Chunks.Active()
		if active == nil || active.ID != leaderChunkID {
			t.Fatalf("follower %s: active chunk ID mismatch — sync failed", fid)
		}
	}

	// Seal on leader.
	if err := leaderTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Forward seal to followers via the directTransferrer (uses SealActiveTier
	// which checks the expected chunk ID matches the follower's active chunk).
	for _, fid := range []string{"f1", "f2"} {
		if err := leaderNode.orch.transferrer.ForwardSealTier(
			context.Background(), fid, h.vaultID, h.tierIDs[0], leaderChunkID,
		); err != nil {
			t.Fatalf("ForwardSealTier to %s: %v", fid, err)
		}
	}

	// Verify: followers have sealed the chunk.
	for _, fid := range []string{"f1", "f2"} {
		followerCM := h.nodes[fid].tiers[0].Chunks
		metas, _ := followerCM.List()
		sealed := 0
		for _, m := range metas {
			if m.Sealed {
				sealed++
			}
		}
		if sealed == 0 {
			t.Errorf("follower %s: expected at least 1 sealed chunk after ForwardSealTier, got 0", fid)
		}

		// Verify follower records via cursor.
		count := cursorCountRecords(t, followerCM)
		if count != 50 {
			t.Errorf("follower %s: cursor read %d records, expected 50", fid, count)
		}
	}
}

// TestClusterReplicationDeletePropagation verifies that ForwardDeleteChunk
// removes the chunk from the follower's chunk manager AND its filesystem
// directory.
func TestClusterReplicationDeletePropagation(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 1, 100)

	leaderNode := h.nodes["leader"]
	leaderTier := leaderNode.tiers[0]

	// Ingest 500 records (5 sealed chunks).
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 500 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "del-prop-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := leaderTier.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderTier.Chunks.Seal()
	}

	metas, _ := leaderTier.Chunks.List()
	t.Logf("leader: %d sealed chunks", len(metas))

	// Post-seal process and replicate to followers.
	processor, ok := leaderTier.Chunks.(chunk.ChunkPostSealProcessor)
	if ok {
		for _, m := range metas {
			_ = processor.PostSealProcess(context.Background(), m.ID)
		}
	}
	ctx := context.Background()
	for _, m := range metas {
		leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, h.tierIDs[0], m.ID, leaderTier.FollowerTargets)
	}

	// Verify followers have chunks.
	for _, fid := range []string{"f1", "f2", "f3"} {
		count := cursorCountRecords(t, h.nodes[fid].tiers[0].Chunks)
		if count == 0 {
			t.Fatalf("follower %s has 0 records before delete test — replication failed", fid)
		}
	}

	// Now delete all chunks on leader AND forward delete to followers.
	for _, m := range metas {
		// Delete on leader.
		if err := leaderTier.Indexes.DeleteIndexes(m.ID); err != nil {
			t.Logf("leader DeleteIndexes(%s): %v", m.ID, err)
		}
		if err := leaderTier.Chunks.Delete(m.ID); err != nil {
			t.Fatalf("leader Delete(%s): %v", m.ID, err)
		}
		// Forward delete to each follower.
		for _, fid := range []string{"f1", "f2", "f3"} {
			if err := leaderNode.orch.transferrer.ForwardDeleteChunk(
				ctx, fid, h.vaultID, h.tierIDs[0], m.ID,
			); err != nil {
				t.Errorf("ForwardDeleteChunk(%s, %s): %v", fid, m.ID, err)
			}
		}
	}

	// ---- Verify: all nodes have 0 cursor-readable records ----
	for _, nid := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nid].tiers[0].Chunks)
		if count != 0 {
			t.Errorf("%s: cursor read %d records after delete (should be 0)", nid, count)
		}
	}

	// ---- Verify: no chunk directories on disk on ANY node ----
	h.assertTierDirEmpty(t, 0)
}
