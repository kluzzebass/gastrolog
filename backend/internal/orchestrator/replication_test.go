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

// ---------- fake forwarder ----------

type replicationFakeForwarder struct{}

func (f *replicationFakeForwarder) Forward(_ context.Context, _ string, _ glid.GLID, _ []chunk.Record) error {
	return nil
}

// ---------- fake instance replicator that records operations ----------

type replicationFakeReplicator struct {
	sealCalls        []sealCall
	sealErr          error
	replicatedChunks []chunk.ChunkID
}

type sealCall struct {
	nodeID  string
	vaultID glid.GLID
	chunkID chunk.ChunkID
}

func (m *replicationFakeReplicator) AppendRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (m *replicationFakeReplicator) SealVault(_ context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID) error {
	if m.sealErr != nil {
		return m.sealErr
	}
	m.sealCalls = append(m.sealCalls, sealCall{nodeID: nodeID, vaultID: vaultID, chunkID: chunkID})
	return nil
}
func (m *replicationFakeReplicator) ImportSealedChunk(_ context.Context, _ string, _ glid.GLID, chunkID chunk.ChunkID, _ chunk.RecordIterator) error {
	m.replicatedChunks = append(m.replicatedChunks, chunkID)
	return nil
}
func (m *replicationFakeReplicator) DeleteChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (m *replicationFakeReplicator) RequestReplicaCatchup(_ context.Context, _ string, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

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
		VaultID:          vaultID,
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

	if _, _, err := orch.Append(vaultID, testRecord("seal-me")); err != nil {
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

	if _, _, err := orch.Append(vaultID, testRecord("data")); err != nil {
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

// ================================================================
// CATCHUP TESTS
// ================================================================

func TestCatchupSecondaryNoSealedChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// No sealed chunks — catchup should be a no-op.
	err := orch.catchupFollower(context.Background(), vaultID, "node-2")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCatchupSecondaryOnlyPrimary(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	// This is a follower — should not initiate catchup.
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, true, "node-2"))
	orch.RegisterVault(vault)

	err := orch.catchupFollower(context.Background(), vaultID, "node-3")
	if err != nil {
		t.Fatalf("expected nil (no-op) for follower, got %v", err)
	}
}

func TestCatchupSecondaryNoTransferrer(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vault := NewVault(vaultID, newReplicationInstance(t, vaultID, nil, false, ""))
	orch.RegisterVault(vault)
	// No transferrer set.

	err := orch.catchupFollower(context.Background(), vaultID, "node-2")
	if err == nil {
		t.Fatal("expected error for missing transferrer")
	}
}

// TestCatchupSkipsFSMRetiredChunks is the regression test for gastrolog-5grpa.
// Before the fix, catchupFollower used the leader's on-disk chunk list as the
// authoritative set, which could include chunks that the instance Raft FSM had
// already retired (DeleteChunk applied) but whose local file hadn't been
// unlinked yet. Catchup would ship those orphans to the follower, where the
// follower's reconcile loop would then delete them within ~1 minute. Net
// result: catchup work wasted, follower under-replicated, repeat forever.
//
// The fix filters the catchup list by instance.ListManifest() — the FSM's
// authoritative view of what should exist. This test populates an instance with
// 3 sealed chunks, configures ListManifest to return only 2 of them
// (simulating the FSM having retired the third), and asserts that catchup
// transferred only the 2 manifest-included chunks.
func TestCatchupSkipsFSMRetiredChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vaultInst := newReplicationInstance(t, vaultID, nil, false, "")
	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// Append + seal three chunks, capturing each chunk ID.
	var ids []chunk.ChunkID
	for i := 0; i < 3; i++ {
		if _, _, err := orch.Append(vaultID, testRecord(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		active := vaultInst.Chunks.Active()
		if active == nil {
			t.Fatalf("chunk %d: no active chunk after append", i)
		}
		id := active.ID
		if err := orch.SealActiveChunk(vaultID, id); err != nil {
			t.Fatalf("seal chunk %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 sealed chunks, got %d", len(ids))
	}

	// Confirm all 3 chunks are present on disk (the leader's local view).
	metas, err := vaultInst.Chunks.List()
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
	vaultInst.ListManifest = func() []chunk.ChunkID {
		return []chunk.ChunkID{ids[0], ids[2]}
	}

	if err := orch.catchupFollower(context.Background(), vaultID, "node-2"); err != nil {
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
// (e.g., an instance with no Raft group, or a memory instance without FSM tracking),
// catchupFollower falls back to the leader's on-disk list — the pre-fix
// behaviour. This is the backward-compatibility guarantee.
func TestCatchupNilManifestUsesAllChunks(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	vaultInst := newReplicationInstance(t, vaultID, nil, false, "")
	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	// Append + seal two chunks.
	for i := 0; i < 2; i++ {
		if _, _, err := orch.Append(vaultID, testRecord(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		active := vaultInst.Chunks.Active()
		if err := orch.SealActiveChunk(vaultID, active.ID); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}

	// ListManifest is nil — catchup must fall back to disk list.
	vaultInst.ListManifest = nil

	if err := orch.catchupFollower(context.Background(), vaultID, "node-2"); err != nil {
		t.Fatalf("catchupFollower: %v", err)
	}

	if len(mock.replicatedChunks) != 2 {
		t.Errorf("expected 2 chunks transferred (nil manifest = all sealed), got %d",
			len(mock.replicatedChunks))
	}
}

// gastrolog-19241: symmetric peer-to-peer catchup. A follower with the
// chunk locally must be allowed to push it to a requester that lacks it,
// regardless of which side is the placement leader. Pre-fix this errored
// with "not placement leader for vault X (follower)", which is exactly
// what blocked the leader-from-follower backfill path needed to recover
// after leadership transferred to a node that didn't have historical
// chunks.
func TestCatchupSelectedChunksFromFollowerSucceeds(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-follower"})
	orch.logger = slog.Default()

	vaultID := glid.New()
	// Build the instance as a FOLLOWER. Pre-fix, this would cause
	// CatchupSelectedChunks to reject the request outright.
	vaultInst := newReplicationInstance(t, vaultID, nil, true, "node-leader")
	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	mock := &replicationFakeReplicator{}
	orch.SetChunkReplicator(mock)

	if _, _, err := orch.Append(vaultID, testRecord("rec-0")); err != nil {
		t.Fatalf("append: %v", err)
	}
	active := vaultInst.Chunks.Active()
	if err := orch.SealActiveChunk(vaultID, active.ID); err != nil {
		t.Fatalf("seal: %v", err)
	}

	scheduled, err := orch.CatchupSelectedChunks(
		context.Background(), vaultID, "node-leader-requester", []chunk.ChunkID{active.ID})
	if err != nil {
		t.Fatalf("expected nil error from follower-side catchup, got %v", err)
	}
	if scheduled != 1 {
		t.Errorf("scheduled = %d, want 1 (the single sealed chunk)", scheduled)
	}
}

