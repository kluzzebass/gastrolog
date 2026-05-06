package orchestrator

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"log/slog"
	"testing"
	"time"

	"fmt"
	"os"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// ---------- fake forwarder ----------

type replicationFakeForwarder struct{}

func (f *replicationFakeForwarder) Forward(_ context.Context, _ string, _ glid.GLID, _ []chunk.Record) error {
	return nil
}

// ---------- fake tier replicator that records operations ----------

type replicationFakeReplicator struct {
	sealCalls        []sealCall
	sealErr          error
	replicatedChunks []chunk.ChunkID
}

type sealCall struct {
	nodeID  string
	vaultID glid.GLID
	tierID  glid.GLID
	chunkID chunk.ChunkID
}

func (m *replicationFakeReplicator) AppendRecords(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (m *replicationFakeReplicator) SealVault(_ context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID) error {
	if m.sealErr != nil {
		return m.sealErr
	}
	m.sealCalls = append(m.sealCalls, sealCall{nodeID: nodeID, vaultID: vaultID, tierID: tierID, chunkID: chunkID})
	return nil
}
func (m *replicationFakeReplicator) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, chunkID chunk.ChunkID, _ []chunk.Record) error {
	m.replicatedChunks = append(m.replicatedChunks, chunkID)
	return nil
}
func (m *replicationFakeReplicator) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (m *replicationFakeReplicator) RequestReplicaCatchup(_ context.Context, _ string, _, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

// ---------- helpers ----------

func newReplicationTier(t *testing.T, tierID glid.GLID, followers []system.ReplicationTarget, isFollower bool, leaderNodeID string) *VaultInstance {
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
		TierID:          tierID,
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
// SEAL ACTIVE TIER TESTS
// ================================================================

func TestSealActiveTier(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
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

	tierID := glid.New()
	vaultID := glid.New()
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

	tierID := glid.New()
	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	wrongTierID := glid.New()
	err := orch.SealActiveTier(vaultID, wrongTierID, chunk.ChunkID{})
	// gastrolog-2t48z: this path is "tier not registered on this node",
	// not "vault not found" — the vault itself is registered. Assert
	// the new sentinel and the old wording is gone from the message.
	if !errors.Is(err, ErrTierNotLocal) {
		t.Errorf("expected ErrTierNotLocal, got %v", err)
	}
	if errors.Is(err, ErrVaultNotFound) {
		t.Errorf("must NOT be ErrVaultNotFound — vault exists, only tier instance is missing: %v", err)
	}
}

func TestSealActiveTierNoActiveChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
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

	tierID := glid.New()
	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// No sealed chunks — catchup should be a no-op.
	err := orch.catchupFollower(context.Background(), vaultID, tierID, "node-2")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCatchupSecondaryOnlyPrimary(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
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

	tierID := glid.New()
	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationTier(t, tierID, nil, false, ""))
	orch.RegisterVault(vault)
	// No transferrer set.

	err := orch.catchupFollower(context.Background(), vaultID, tierID, "node-2")
	if err == nil {
		t.Fatal("expected error for missing transferrer")
	}
}

// TestCatchupSkipsFSMRetiredChunks is the regression test for gastrolog-5grpa.
// Before the fix, catchupFollower used the leader's on-disk chunk list as the
// authoritative set, which could include chunks that the tier Raft FSM had
// already retired (DeleteChunk applied) but whose local file hadn't been
// unlinked yet. Catchup would ship those orphans to the follower, where the
// follower's reconcile loop would then delete them within ~1 minute. Net
// result: catchup work wasted, follower under-replicated, repeat forever.
//
// The fix filters the catchup list by tier.ListManifest() — the FSM's
// authoritative view of what should exist. This test populates a tier with
// 3 sealed chunks, configures ListManifest to return only 2 of them
// (simulating the FSM having retired the third), and asserts that catchup
// transferred only the 2 manifest-included chunks.
func TestCatchupSkipsFSMRetiredChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	tierID := glid.New()
	vaultID := glid.New()
	tier := newReplicationTier(t, tierID, nil, false, "")
	vault := NewVault(vaultID, tier)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// Append + seal three chunks, capturing each chunk ID.
	var ids []chunk.ChunkID
	for i := 0; i < 3; i++ {
		if _, _, err := orch.Append(vaultID, testRecord(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		active := tier.Chunks.Active()
		if active == nil {
			t.Fatalf("chunk %d: no active chunk after append", i)
		}
		id := active.ID
		if err := orch.SealActiveTier(vaultID, tierID, id); err != nil {
			t.Fatalf("seal chunk %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 sealed chunks, got %d", len(ids))
	}

	// Confirm all 3 chunks are present on disk (the leader's local view).
	metas, err := tier.Chunks.List()
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	sealedCount := 0
	for _, m := range metas {
		if m.Sealed {
			sealedCount++
		}
	}
	if sealedCount != 3 {
		t.Fatalf("expected 3 sealed chunks on disk, got %d", sealedCount)
	}

	// Configure the FSM manifest to return only chunks 0 and 2, simulating
	// chunk 1 being retired by the FSM (DeleteChunk applied) while still
	// existing on disk in the brief window before unlink.
	tier.ListManifest = func() []chunk.ChunkID {
		return []chunk.ChunkID{ids[0], ids[2]}
	}

	if err := orch.catchupFollower(context.Background(), vaultID, tierID, "node-2"); err != nil {
		t.Fatalf("catchupFollower: %v", err)
	}

	// Catchup must have transferred ONLY the 2 manifest-included chunks.
	// The retired chunk (ids[1]) must NOT have been transferred — sending
	// it would re-create the orphan-and-reconcile-delete cycle.
	if len(mock.replicatedChunks) != 2 {
		t.Fatalf("expected 2 chunks transferred, got %d (%v)",
			len(mock.replicatedChunks), mock.replicatedChunks)
	}
	transferred := make(map[chunk.ChunkID]bool, len(mock.replicatedChunks))
	for _, id := range mock.replicatedChunks {
		transferred[id] = true
	}
	if !transferred[ids[0]] {
		t.Errorf("chunk ids[0] %s should have been transferred (in manifest)", ids[0])
	}
	if transferred[ids[1]] {
		t.Errorf("chunk ids[1] %s should NOT have been transferred (FSM-retired)", ids[1])
	}
	if !transferred[ids[2]] {
		t.Errorf("chunk ids[2] %s should have been transferred (in manifest)", ids[2])
	}
}

// TestCatchupNilManifestUsesAllChunks verifies that when ListManifest is nil
// (e.g., a tier with no Raft group, or a memory tier without FSM tracking),
// catchupFollower falls back to the leader's on-disk list — the pre-fix
// behaviour. This is the backward-compatibility guarantee.
func TestCatchupNilManifestUsesAllChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	tierID := glid.New()
	vaultID := glid.New()
	tier := newReplicationTier(t, tierID, nil, false, "")
	vault := NewVault(vaultID, tier)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// Append + seal two chunks.
	for i := 0; i < 2; i++ {
		if _, _, err := orch.Append(vaultID, testRecord(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		active := tier.Chunks.Active()
		if err := orch.SealActiveTier(vaultID, tierID, active.ID); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}

	// ListManifest is nil — catchup must fall back to disk list.
	tier.ListManifest = nil

	if err := orch.catchupFollower(context.Background(), vaultID, tierID, "node-2"); err != nil {
		t.Fatalf("catchupFollower: %v", err)
	}

	if len(mock.replicatedChunks) != 2 {
		t.Errorf("expected 2 chunks transferred (nil manifest = all sealed), got %d",
			len(mock.replicatedChunks))
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
		if err := leaderNode.orch.AppendToVault(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
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

// TestClusterReplicationSealedIdxWriteTSMatchesLeader verifies that after
// sealed-chunk replication, follower idx.log entries match the leader for
// WriteTS and IngestTS (offline read — same contract as tier ImportSealed).
func TestClusterReplicationSealedIdxWriteTSMatchesLeader(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2"}, 1, 100)

	leaderNode := h.nodes["leader"]
	leaderTier := leaderNode.tiers[0]

	// Fewer records than rotation threshold → single active chunk, then one
	// sealed chunk after explicit Seal().
	const totalRecords = 50
	t0 := time.Date(2025, 7, 1, 12, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToVault(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "idxcmp-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if active := leaderTier.Chunks.Active(); active != nil && active.RecordCount > 0 {
		if err := leaderTier.Chunks.Seal(); err != nil {
			t.Fatal(err)
		}
	}

	metas, err := leaderTier.Chunks.List()
	if err != nil {
		t.Fatal(err)
	}
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed && m.RecordCount == totalRecords {
			sealedID = m.ID
			break
		}
	}
	if sealedID == (chunk.ChunkID{}) {
		t.Fatalf("no sealed chunk with %d records", totalRecords)
	}

	processor, ok := leaderTier.Chunks.(chunk.ChunkPostSealProcessor)
	if !ok {
		t.Fatal("leader tier chunks must implement ChunkPostSealProcessor")
	}
	if err := processor.PostSealProcess(context.Background(), sealedID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	ctx := context.Background()
	leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, h.tierIDs[0], sealedID, leaderTier.FollowerTargets)

	leaderEntries := chunkRecordTimestamps(t, leaderTier.Chunks, sealedID)
	if len(leaderEntries) != totalRecords {
		t.Fatalf("leader entries: want %d got %d", totalRecords, len(leaderEntries))
	}

	for _, fid := range []string{"f1", "f2"} {
		got := chunkRecordTimestamps(t, h.nodes[fid].tiers[0].Chunks, sealedID)
		if len(got) != len(leaderEntries) {
			t.Fatalf("follower %s: entries %d, leader %d", fid, len(got), len(leaderEntries))
		}
		for i := range leaderEntries {
			if !got[i].WriteTS.Equal(leaderEntries[i].WriteTS) {
				t.Errorf("follower %s pos %d WriteTS: leader=%s follower=%s",
					fid, i, leaderEntries[i].WriteTS.UTC(), got[i].WriteTS.UTC())
			}
			if !got[i].IngestTS.Equal(leaderEntries[i].IngestTS) {
				t.Errorf("follower %s pos %d IngestTS: leader=%s follower=%s",
					fid, i, leaderEntries[i].IngestTS.UTC(), got[i].IngestTS.UTC())
			}
		}
	}
}

// chunkRecordTimestamps opens a cursor on the given chunk and collects each
// record's IngestTS / WriteTS pair. Routes through cm.OpenCursor so the
// helper works regardless of whether the sealed chunk is multi-file or
// data.glcb on disk — the chunk redesign (gastrolog-24m1t) flips this
// over time, and tests that assert per-record timestamp invariants
// shouldn't be coupled to the on-disk format.
func chunkRecordTimestamps(t *testing.T, cm chunk.ChunkManager, id chunk.ChunkID) []recordTimestamps {
	t.Helper()
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("open cursor for %s: %v", id, err)
	}
	defer func() { _ = cursor.Close() }()
	var out []recordTimestamps
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return out
		}
		if err != nil {
			t.Fatalf("cursor next on %s: %v", id, err)
		}
		out = append(out, recordTimestamps{IngestTS: rec.IngestTS, WriteTS: rec.WriteTS})
	}
}

type recordTimestamps struct {
	IngestTS time.Time
	WriteTS  time.Time
}

// TestClusterReplicationSealSync verifies that ChunkReplicator.SealVault causes
// the follower to seal its active chunk at the same boundary as the leader.
func TestClusterReplicationSealSync(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2"}, 1, 10000) // high rotation so we control seal manually

	leaderNode := h.nodes["leader"]
	leaderTier := leaderNode.tiers[0]

	// Ingest 50 records on leader. With chunkReplicator wired, AppendToVault
	// auto-forwards to followers via AppendRecords, so the followers end up
	// with synchronized active chunk IDs and record counts.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 50 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToVault(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "seal-sync-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	leaderActive := leaderTier.Chunks.Active()
	if leaderActive == nil {
		t.Fatal("expected active chunk on leader after append")
	}
	leaderChunkID := leaderActive.ID

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

	// Forward seal to followers via the tier replicator (uses SealActiveTier
	// which checks the expected chunk ID matches the follower's active chunk).
	for _, fid := range []string{"f1", "f2"} {
		if err := leaderNode.orch.chunkReplicator.SealVault(
			context.Background(), fid, h.vaultID, h.tierIDs[0], leaderChunkID,
		); err != nil {
			t.Fatalf("SealVault to %s: %v", fid, err)
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
			t.Errorf("follower %s: expected at least 1 sealed chunk after SealVault, got 0", fid)
		}

		// Verify follower records via cursor.
		count := cursorCountRecords(t, followerCM)
		if count != 50 {
			t.Errorf("follower %s: cursor read %d records, expected 50", fid, count)
		}
	}
}

// TestClusterReplicationDeletePropagation verifies that ChunkReplicator.DeleteChunk
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
		if err := leaderNode.orch.AppendToVault(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
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
			if err := leaderNode.orch.chunkReplicator.DeleteChunk(
				ctx, fid, h.vaultID, h.tierIDs[0], m.ID,
			); err != nil {
				t.Errorf("DeleteChunk(%s, %s): %v", fid, m.ID, err)
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
