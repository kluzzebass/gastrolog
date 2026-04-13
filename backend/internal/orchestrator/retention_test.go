package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/system"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

// ---------- fake chunk manager ----------

type retentionFakeChunkManager struct {
	chunks  []chunk.ChunkMeta
	deleted []chunk.ChunkID
}

func (f *retentionFakeChunkManager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (f *retentionFakeChunkManager) Seal() error              { return nil }
func (f *retentionFakeChunkManager) Active() *chunk.ChunkMeta { return nil }
func (f *retentionFakeChunkManager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	return chunk.ChunkMeta{}, nil
}
func (f *retentionFakeChunkManager) List() ([]chunk.ChunkMeta, error) {
	return f.chunks, nil
}
func (f *retentionFakeChunkManager) Delete(id chunk.ChunkID) error {
	f.deleted = append(f.deleted, id)
	return nil
}
func (f *retentionFakeChunkManager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) {
	return nil, nil
}
func (f *retentionFakeChunkManager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *retentionFakeChunkManager) FindIngestStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *retentionFakeChunkManager) FindSourceStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *retentionFakeChunkManager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	return nil, nil
}
func (f *retentionFakeChunkManager) SetRotationPolicy(policy chunk.RotationPolicy) {}
func (f *retentionFakeChunkManager) CheckRotation() *string                        { return nil }
func (f *retentionFakeChunkManager) ImportRecords(chunk.RecordIterator) (chunk.ChunkMeta, error) { return chunk.ChunkMeta{}, nil }
func (f *retentionFakeChunkManager) ScanAttrs(_ chunk.ChunkID, _ uint64, _ func(time.Time, chunk.Attributes) bool) error {
	return nil
}
func (f *retentionFakeChunkManager) SetNextChunkID(_ chunk.ChunkID) {}
func (f *retentionFakeChunkManager) Close() error                   { return nil }

// ---------- fake index manager ----------

type retentionFakeIndexManager struct {
	deleted []chunk.ChunkID
}

func (f *retentionFakeIndexManager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return nil
}
func (f *retentionFakeIndexManager) DeleteIndexes(chunkID chunk.ChunkID) error {
	f.deleted = append(f.deleted, chunkID)
	return nil
}
func (f *retentionFakeIndexManager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenAttrKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKeyIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenAttrValueIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrValueIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenAttrKVIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKVIndexEntry], error) {
	return nil, nil
}
func (f *retentionFakeIndexManager) OpenKVKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.KVKeyIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *retentionFakeIndexManager) OpenKVValueIndex(chunkID chunk.ChunkID) (*index.Index[index.KVValueIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *retentionFakeIndexManager) OpenKVIndex(chunkID chunk.ChunkID) (*index.Index[index.KVIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *retentionFakeIndexManager) IndexesComplete(chunkID chunk.ChunkID) (bool, error) {
	return true, nil
}
func (f *retentionFakeIndexManager) FindIngestStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}
func (f *retentionFakeIndexManager) FindSourceStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}
func (f *retentionFakeIndexManager) OpenJSONPathIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPathIndexEntry], index.JSONIndexStatus, error) {
	return nil, index.JSONComplete, nil
}
func (f *retentionFakeIndexManager) OpenJSONPVIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPVIndexEntry], index.JSONIndexStatus, error) {
	return nil, index.JSONComplete, nil
}
func (f *retentionFakeIndexManager) LoadIngestEntries(chunkID chunk.ChunkID) ([]index.TSEntry, error) {
	return nil, index.ErrIndexNotFound
}
func (f *retentionFakeIndexManager) LoadSourceEntries(chunkID chunk.ChunkID) ([]index.TSEntry, error) {
	return nil, index.ErrIndexNotFound
}
func (f *retentionFakeIndexManager) IndexSizes(chunkID chunk.ChunkID) map[string]int64 {
	return map[string]int64{}
}
func (f *retentionFakeIndexManager) BuildAdapter() chunk.ChunkIndexBuilder { return nil }

// ---------- helpers ----------

func chunkIDAt(_ time.Time) chunk.ChunkID {
	return chunk.NewChunkID()
}

func newRetentionRunner(cm chunk.ChunkManager, im index.IndexManager, policy chunk.RetentionPolicy) (*retentionRunner, []retentionRule) {
	var rules []retentionRule
	if policy != nil {
		rules = []retentionRule{
			{policy: policy, action: system.RetentionActionExpire},
		}
	}
	r := &retentionRunner{
		isLeader: true,
		vaultID: uuid.Must(uuid.NewV7()),
		cm:      cm,
		im:      im,
		now:     time.Now,
		logger:  slog.Default(),
	}
	return r, rules
}

// ---------- tests ----------

func TestSweepDeletesExpiredChunks(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	id0 := chunkIDAt(base)
	id1 := chunkIDAt(base.Add(1 * time.Hour))
	id2 := chunkIDAt(base.Add(2 * time.Hour))
	id3 := chunkIDAt(base.Add(3 * time.Hour))

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: id0, WriteStart: base, WriteEnd: base.Add(30 * time.Minute), Sealed: true},
			{ID: id1, WriteStart: base.Add(1 * time.Hour), WriteEnd: base.Add(90 * time.Minute), Sealed: true},
			{ID: id2, WriteStart: base.Add(2 * time.Hour), WriteEnd: base.Add(150 * time.Minute), Sealed: true},
			{ID: id3, WriteStart: base.Add(3 * time.Hour), WriteEnd: base.Add(210 * time.Minute), Sealed: true},
		},
	}
	im := &retentionFakeIndexManager{}

	policy := chunk.NewCountRetentionPolicy(2)
	r, rules := newRetentionRunner(cm, im, policy)

	r.sweep(rules)

	// With max 2, the 2 oldest (id0, id1) should be deleted.
	if len(cm.deleted) != 2 {
		t.Fatalf("expected 2 chunk deletions, got %d", len(cm.deleted))
	}
	if cm.deleted[0] != id0 {
		t.Errorf("expected first deleted chunk %s, got %s", id0, cm.deleted[0])
	}
	if cm.deleted[1] != id1 {
		t.Errorf("expected second deleted chunk %s, got %s", id1, cm.deleted[1])
	}

	// Indexes should be deleted first (same IDs, same order).
	if len(im.deleted) != 2 {
		t.Fatalf("expected 2 index deletions, got %d", len(im.deleted))
	}
	if im.deleted[0] != id0 {
		t.Errorf("expected first deleted index %s, got %s", id0, im.deleted[0])
	}
	if im.deleted[1] != id1 {
		t.Errorf("expected second deleted index %s, got %s", id1, im.deleted[1])
	}
}

func TestSweepSkipsActiveChunks(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	idSealed0 := chunkIDAt(base)
	idSealed1 := chunkIDAt(base.Add(1 * time.Hour))
	idSealed2 := chunkIDAt(base.Add(2 * time.Hour))
	idActive := chunkIDAt(base.Add(3 * time.Hour))

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: idSealed0, WriteStart: base, WriteEnd: base.Add(30 * time.Minute), Sealed: true},
			{ID: idSealed1, WriteStart: base.Add(1 * time.Hour), WriteEnd: base.Add(90 * time.Minute), Sealed: true},
			{ID: idSealed2, WriteStart: base.Add(2 * time.Hour), WriteEnd: base.Add(150 * time.Minute), Sealed: true},
			{ID: idActive, WriteStart: base.Add(3 * time.Hour), Sealed: false}, // active, unsealed
		},
	}
	im := &retentionFakeIndexManager{}

	// Keep max 2 sealed chunks. With 3 sealed, oldest 1 should be deleted.
	// The active chunk must not be considered.
	policy := chunk.NewCountRetentionPolicy(2)
	r, rules := newRetentionRunner(cm, im, policy)

	r.sweep(rules)

	if len(cm.deleted) != 1 {
		t.Fatalf("expected 1 chunk deletion, got %d", len(cm.deleted))
	}
	if cm.deleted[0] != idSealed0 {
		t.Errorf("expected deleted chunk %s, got %s", idSealed0, cm.deleted[0])
	}

	// Verify the active chunk was not deleted.
	for _, id := range cm.deleted {
		if id == idActive {
			t.Error("active (unsealed) chunk should not be deleted")
		}
	}
	for _, id := range im.deleted {
		if id == idActive {
			t.Error("active (unsealed) chunk indexes should not be deleted")
		}
	}
}

func TestSweepWithNoBindings(t *testing.T) {
	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunkIDAt(time.Now()), Sealed: true},
		},
	}
	im := &retentionFakeIndexManager{}

	r, rules := newRetentionRunner(cm, im, nil)

	r.sweep(rules)

	if len(cm.deleted) != 0 {
		t.Errorf("expected no chunk deletions with no rules, got %d", len(cm.deleted))
	}
	if len(im.deleted) != 0 {
		t.Errorf("expected no index deletions with no rules, got %d", len(im.deleted))
	}
}

func TestSetBindingsHotSwap(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	id0 := chunkIDAt(base)
	id1 := chunkIDAt(base.Add(1 * time.Hour))
	id2 := chunkIDAt(base.Add(2 * time.Hour))

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: id0, WriteStart: base, WriteEnd: base.Add(30 * time.Minute), Sealed: true},
			{ID: id1, WriteStart: base.Add(1 * time.Hour), WriteEnd: base.Add(90 * time.Minute), Sealed: true},
			{ID: id2, WriteStart: base.Add(2 * time.Hour), WriteEnd: base.Add(150 * time.Minute), Sealed: true},
		},
	}
	im := &retentionFakeIndexManager{}

	// Start with keep-all (max 10) so nothing gets deleted.
	r, rules := newRetentionRunner(cm, im, chunk.NewCountRetentionPolicy(10))

	r.sweep(rules)

	if len(cm.deleted) != 0 {
		t.Fatalf("expected no deletions with generous policy, got %d", len(cm.deleted))
	}

	// Hot-swap to keep-1 policy. Next sweep should delete the 2 oldest.
	newRules := []retentionRule{
		{policy: chunk.NewCountRetentionPolicy(1), action: system.RetentionActionExpire},
	}

	r.sweep(newRules)

	if len(cm.deleted) != 2 {
		t.Fatalf("expected 2 chunk deletions after rule swap, got %d", len(cm.deleted))
	}
	if cm.deleted[0] != id0 {
		t.Errorf("expected first deleted chunk %s, got %s", id0, cm.deleted[0])
	}
	if cm.deleted[1] != id1 {
		t.Errorf("expected second deleted chunk %s, got %s", id1, cm.deleted[1])
	}
}

func TestExpireChunkAppliesRaftDeleteBeforeLocal(t *testing.T) {
	id := chunkIDAt(time.Now())
	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{{ID: id, Sealed: true}},
	}
	im := &retentionFakeIndexManager{}

	var raftApplied bool
	r := &retentionRunner{
		isLeader: true,
		vaultID: uuid.Must(uuid.NewV7()),
		tierID:  uuid.Must(uuid.NewV7()),
		cm:      cm,
		im:      im,
		now:     time.Now,
		logger:  slog.Default(),
		applyRaftDelete: func(deleteID chunk.ChunkID) error {
			if deleteID != id {
				t.Errorf("unexpected chunk ID: %s", deleteID)
			}
			raftApplied = true
			return nil
		},
	}

	r.expireChunk(id)

	if !raftApplied {
		t.Error("expected Raft delete to be applied before local delete")
	}
	if len(cm.deleted) != 1 || cm.deleted[0] != id {
		t.Errorf("expected local chunk deletion, got %v", cm.deleted)
	}
}

func TestExpireChunkSkipsLocalOnRaftFailure(t *testing.T) {
	id := chunkIDAt(time.Now())
	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{{ID: id, Sealed: true}},
	}
	im := &retentionFakeIndexManager{}

	r := &retentionRunner{
		isLeader: true,
		vaultID: uuid.Must(uuid.NewV7()),
		tierID:  uuid.Must(uuid.NewV7()),
		cm:      cm,
		im:      im,
		now:     time.Now,
		logger:  slog.Default(),
		applyRaftDelete: func(_ chunk.ChunkID) error {
			return fmt.Errorf("not leader")
		},
	}

	r.expireChunk(id)

	if len(cm.deleted) != 0 {
		t.Error("local delete should NOT happen when Raft apply fails")
	}
}

type testSystemLoader struct{ cfg *system.Config }

func (l testSystemLoader) Load(_ context.Context) (*system.Config, error) {
	return l.cfg, nil
}

func strPtr(s string) *string { return &s }

// ==========================================================================
// Multi-node retention sweep tests
//
// Uses setupCluster (from transition_test.go) with file-backed tiers and
// directTransferrer to verify that retention sweep expiry correctly
// propagates chunk deletions to all follower nodes.
// ==========================================================================

// TestClusterRetentionSweepDeletesOnAllNodes sets up a 4-node cluster,
// ingests records to create 10 sealed chunks, replicates to all followers,
// then runs a retention sweep with keepN=3 on the leader. Verifies:
//   - Expired chunks (7 oldest) deleted from leader AND all followers
//   - Retained chunks (3 newest) still readable on leader AND all followers
//   - Expired chunk directories removed from disk on ALL nodes
func TestClusterRetentionSweepDeletesOnAllNodes(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 1, 100)

	leaderNode := h.nodes["leader"]
	leaderTier := leaderNode.tiers[0]

	// Ingest 1000 records → 10 sealed chunks.
	const totalRecords = 1_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "retention-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := leaderTier.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderTier.Chunks.Seal()
	}

	metas, _ := leaderTier.Chunks.List()
	t.Logf("leader: %d sealed chunks before retention", len(metas))
	if len(metas) < 5 {
		t.Fatalf("expected at least 5 sealed chunks, got %d", len(metas))
	}

	// PostSealProcess + replicate to all followers.
	processor, ok := leaderTier.Chunks.(chunk.ChunkPostSealProcessor)
	ctx := context.Background()
	for _, m := range metas {
		if ok {
			_ = processor.PostSealProcess(ctx, m.ID)
		}
		leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, h.tierIDs[0], m.ID, leaderTier.FollowerTargets)
	}

	// Verify followers have all records before sweep.
	for _, fid := range []string{"f1", "f2", "f3"} {
		count := cursorCountRecords(t, h.nodes[fid].tiers[0].Chunks)
		if count != totalRecords {
			t.Fatalf("follower %s: expected %d records before sweep, got %d", fid, totalRecords, count)
		}
	}

	// Run retention sweep with keepN=3 — keeps 3 newest, expires the rest.
	const keepN = 3
	rules := []retentionRule{{
		policy: chunk.NewCountRetentionPolicy(keepN),
		action: system.RetentionActionExpire,
	}}
	runner := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[0], leaderTier)
	runner.sweep(rules)

	// ---- Verify: leader retained exactly keepN chunks ----
	metasAfter, _ := leaderTier.Chunks.List()
	if len(metasAfter) != keepN {
		t.Errorf("leader: expected %d retained chunks, got %d", keepN, len(metasAfter))
	}
	leaderRecords := cursorCountRecords(t, leaderTier.Chunks)
	expectedRetained := int64(keepN) * 100 // 100 records per chunk
	if leaderRecords != expectedRetained {
		t.Errorf("leader: cursor read %d records, expected %d (keepN=%d × 100)", leaderRecords, expectedRetained, keepN)
	}

	// ---- Verify: followers also have exactly keepN chunks ----
	for _, fid := range []string{"f1", "f2", "f3"} {
		followerCM := h.nodes[fid].tiers[0].Chunks
		followerMetas, _ := followerCM.List()
		if len(followerMetas) != keepN {
			t.Errorf("follower %s: expected %d retained chunks, got %d", fid, keepN, len(followerMetas))
		}
		followerRecords := cursorCountRecords(t, followerCM)
		if followerRecords != expectedRetained {
			t.Errorf("follower %s: cursor read %d records, expected %d", fid, followerRecords, expectedRetained)
		}
	}

	// ---- Verify: expired chunk directories gone from disk on ALL nodes ----
	// Each node should have exactly keepN chunk directories remaining.
	for _, nid := range h.allNodeIDs() {
		dirs := h.listChunkDirsOnNode(t, nid, 0)
		if len(dirs) != keepN {
			t.Errorf("%s: expected %d chunk dirs on disk, got %d: %v", nid, keepN, len(dirs), dirs)
		}
	}
}

// TestClusterRetentionSweepWithTTLOnAllNodes uses a TTL policy (expire chunks
// older than 1 minute) with a frozen clock. Verifies cross-node cleanup.
func TestClusterRetentionSweepWithTTLOnAllNodes(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 1, 50)

	leaderNode := h.nodes["leader"]
	leaderTier := leaderNode.tiers[0]

	// Ingest 500 records → 10 sealed chunks.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 500 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "ttl-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := leaderTier.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderTier.Chunks.Seal()
	}

	metas, _ := leaderTier.Chunks.List()
	t.Logf("leader: %d sealed chunks", len(metas))

	// Replicate to followers.
	processor, ok := leaderTier.Chunks.(chunk.ChunkPostSealProcessor)
	ctx := context.Background()
	for _, m := range metas {
		if ok {
			_ = processor.PostSealProcess(ctx, m.ID)
		}
		leaderNode.orch.replicateSealedChunk(ctx, h.vaultID, h.tierIDs[0], m.ID, leaderTier.FollowerTargets)
	}

	// Run TTL sweep with clock set 5 minutes in the future — all chunks expired.
	frozenNow := time.Now().Add(5 * time.Minute)
	rules := []retentionRule{{
		policy: chunk.NewTTLRetentionPolicy(1 * time.Minute),
		action: system.RetentionActionExpire,
	}}
	runner := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[0], leaderTier)
	runner.now = func() time.Time { return frozenNow }
	runner.sweep(rules)

	// ---- Verify: ALL chunks expired on ALL nodes ----
	for _, nid := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nid].tiers[0].Chunks)
		if count != 0 {
			t.Errorf("%s: cursor read %d records after TTL sweep (should be 0)", nid, count)
		}
	}

	// ---- Verify: no chunk directories on disk on ANY node ----
	h.assertTierDirEmpty(t, 0)
}

// TestRetentionTargetRefreshesCmOnExistingRunner verifies that
// retentionTargetForTier updates cm and im on an existing runner
// when the tier's chunk manager changes (e.g., after vault rebuild).
func TestRetentionTargetRefreshesCmOnExistingRunner(t *testing.T) {
	t.Parallel()

	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	policyID := uuid.Must(uuid.NewV7())

	cm1 := &retentionFakeChunkManager{}
	im1 := &retentionFakeIndexManager{}
	cm2 := &retentionFakeChunkManager{}
	im2 := &retentionFakeIndexManager{}

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{ID: vaultID, Enabled: true}},
		Tiers: []system.TierConfig{{
			ID:      tierID,
			VaultID: vaultID,
			RetentionRules: []system.RetentionRule{{
				RetentionPolicyID: policyID,
			}},
		}},
		RetentionPolicies: []system.RetentionPolicyConfig{{
			ID:     policyID,
			MaxAge: strPtr("1h"),
		}},
	}

	orch, err := New(Config{
		SystemLoader: testSystemLoader{cfg: cfg},
		Logger:       slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer orch.Stop()

	// First call: creates a new runner with cm1/im1.
	tier1 := &TierInstance{
		TierID:  tierID,
		Chunks:  cm1,
		Indexes: im1,
	}
	active := make(map[string]bool)
	vaultCfg := cfg.Vaults[0]
	target1 := orch.retentionTargetForTier(cfg, vaultCfg, tier1, active)
	if target1 == nil {
		t.Fatal("expected non-nil sweep target")
	}
	if target1.runner.cm != cm1 {
		t.Error("expected runner.cm == cm1 on first call")
	}
	if target1.runner.im != im1 {
		t.Error("expected runner.im == im1 on first call")
	}

	// Second call with different chunk manager: runner is reused, cm/im refreshed.
	tier2 := &TierInstance{
		TierID:  tierID,
		Chunks:  cm2,
		Indexes: im2,
	}
	active2 := make(map[string]bool)
	target2 := orch.retentionTargetForTier(cfg, vaultCfg, tier2, active2)
	if target2 == nil {
		t.Fatal("expected non-nil sweep target on second call")
	}
	if target2.runner.cm != cm2 {
		t.Error("expected runner.cm refreshed to cm2 on second call")
	}
	if target2.runner.im != im2 {
		t.Error("expected runner.im refreshed to im2 on second call")
	}
	// Same runner object — reused, not recreated.
	if target1.runner != target2.runner {
		t.Error("expected same runner instance across calls")
	}
}

// ---------- reconcileFollower tests ----------

func TestReconcileFollowerSkipsWhenFSMNotReady(t *testing.T) {
	t.Parallel()

	orphanID := chunk.NewChunkID()
	manifestID := chunk.NewChunkID()

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: orphanID, Sealed: true},
			{ID: manifestID, Sealed: true},
		},
	}

	tier := &TierInstance{
		TierID:     uuid.Must(uuid.NewV7()),
		Chunks:     cm,
		Indexes:    &retentionFakeIndexManager{},
		// FSM not ready — manifest incomplete, unsafe to reconcile.
		IsFSMReady:   func() bool { return false },
		ListManifest: func() []chunk.ChunkID { return []chunk.ChunkID{manifestID} },
	}

	orch, err := New(Config{Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	defer orch.Stop()

	orch.reconcileFollower(tier)

	if len(cm.deleted) != 0 {
		t.Errorf("expected no deletions when FSM not ready, got %d", len(cm.deleted))
	}
}

func TestReconcileFollowerDeletesOrphansWhenLeaderPresent(t *testing.T) {
	t.Parallel()

	orphanID := chunk.NewChunkID()
	keptID := chunk.NewChunkID()
	activeID := chunk.NewChunkID()

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: orphanID, Sealed: true},              // sealed, not in manifest → delete
			{ID: keptID, Sealed: true},                // sealed, in manifest → keep
			{ID: activeID, Sealed: false},             // unsealed → keep regardless
		},
	}
	im := &retentionFakeIndexManager{}

	tier := &TierInstance{
		TierID:       uuid.Must(uuid.NewV7()),
		Chunks:       cm,
		Indexes:      im,
		IsFSMReady:   func() bool { return true },
		ListManifest: func() []chunk.ChunkID { return []chunk.ChunkID{keptID} },
	}

	orch, err := New(Config{Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	defer orch.Stop()

	orch.reconcileFollower(tier)

	if len(cm.deleted) != 1 || cm.deleted[0] != orphanID {
		t.Errorf("expected orphanID deleted, got %v", cm.deleted)
	}
	if len(im.deleted) != 1 || im.deleted[0] != orphanID {
		t.Errorf("expected orphan indexes deleted, got %v", im.deleted)
	}
}

func TestReconcileFollowerDeletesAllWhenManifestEmpty(t *testing.T) {
	t.Parallel()

	orphanID := chunk.NewChunkID()
	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: orphanID, Sealed: true},
		},
	}
	im := &retentionFakeIndexManager{}

	// FSM is ready but manifest is empty — all sealed chunks are orphans.
	tier := &TierInstance{
		TierID:       uuid.Must(uuid.NewV7()),
		Chunks:       cm,
		Indexes:      im,
		IsFSMReady:   func() bool { return true },
		ListManifest: func() []chunk.ChunkID { return nil },
	}

	orch, err := New(Config{Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	defer orch.Stop()

	orch.reconcileFollower(tier)

	if len(cm.deleted) != 1 || cm.deleted[0] != orphanID {
		t.Errorf("expected orphan deleted when manifest empty but FSM ready, got %v", cm.deleted)
	}
}

func TestReconcileFollowerSkipsWhenNilCallbacks(t *testing.T) {
	t.Parallel()

	cm := &retentionFakeChunkManager{
		chunks: []chunk.ChunkMeta{
			{ID: chunk.NewChunkID(), Sealed: true},
		},
	}

	// IsFSMReady is nil — single-node/memory mode, no Raft group.
	// Reconciliation should proceed (manifest is always authoritative locally).
	tier := &TierInstance{
		TierID:       uuid.Must(uuid.NewV7()),
		Chunks:       cm,
		ListManifest: func() []chunk.ChunkID { return []chunk.ChunkID{chunk.NewChunkID()} },
	}

	orch, err := New(Config{Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	defer orch.Stop()

	orch.reconcileFollower(tier)

	// Nil HasRaftLeader means no Raft group — reconciliation should proceed
	// (single-node mode, manifest is always authoritative).
	if len(cm.deleted) != 1 {
		t.Errorf("expected 1 deletion in single-node mode, got %d", len(cm.deleted))
	}
}
