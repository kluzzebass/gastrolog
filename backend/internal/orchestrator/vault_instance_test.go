package orchestrator

import (
	"errors"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// fixedMetaChunkManager is a minimal chunk.ChunkManager stub whose List/Meta
// report a single fixed meta — enough to drive the orchestrator's grounded-read
// seam (GetChunkMeta / ListAllChunkMetas) without a real storage backend.
type fixedMetaChunkManager struct {
	chunk.ChunkManager // embedded nil — only List/Meta are called
	meta               chunk.ChunkMeta
}

func (m *fixedMetaChunkManager) List() ([]chunk.ChunkMeta, error) {
	return []chunk.ChunkMeta{m.meta}, nil
}

func (m *fixedMetaChunkManager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	if id == m.meta.ID {
		return m.meta, nil
	}
	return chunk.ChunkMeta{}, chunk.ErrChunkNotFound
}

// Active reports no open chunk — this stub only serves the one sealed meta.
func (m *fixedMetaChunkManager) Active() *chunk.ChunkMeta { return nil }

func newMemInstance(t *testing.T, vaultID glid.GLID, isFollower bool, followers []system.ReplicationTarget) *VaultInstance {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		Now:            time.Now,
		MetaStore:      chunkmem.NewMetaStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	im, _ := indexmem.NewFactory()(nil, cm, nil)
	return &VaultInstance{
		VaultID:         vaultID,
		Type:            "memory",
		Chunks:          cm,
		Indexes:         im,
		Query:           query.New(cm, im, nil),
		IsFollower:      isFollower,
		FollowerTargets: followers,
	}
}

func testIter(records []chunk.Record) chunk.RecordIterator {
	i := 0
	return func() (chunk.Record, error) {
		if i >= len(records) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := records[i]
		i++
		return r, nil
	}
}

func smallRecords(n int) []chunk.Record {
	recs := make([]chunk.Record, n)
	for i := range recs {
		recs[i] = chunk.Record{
			Raw:      []byte("test-record"),
			SourceTS: time.Now(),
			IngestTS: time.Now(),
		}
	}
	return recs
}

// --- ListAllChunkMetas ---

// TestListAllChunkMetasOverlaysFromFSM is the regression test for the follower
// cloud-badge gap, updated for the grounded-read seam. The local chunk manager
// only sets CloudBacked=true on the node that actually uploaded the blob (the
// cold instance raft leader); followers strip
// sealed_backing from their chunk-manager params and never see the cloud state,
// so their local CloudBacked is permanently false. The fix grounds the
// cluster-wide FSM view onto each chunk meta the orchestrator returns. Without
// it the inspector showed "no cloud badge" 75% of the time on a 4-node cluster
// (whichever 3 of 4 nodes the query landed on were always followers).
//
// This stands up a REAL single-voter vault-ctl raft group (not a faked
// instance-level overlay): the FSM records CloudBacked=true via CmdUploadChunk
// while the local chunk manager reports CloudBacked=false, exactly the follower
// divergence the seam must correct.
func TestListAllChunkMetasOverlaysFromFSM(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch, g := newSingleNodeVaultCtlOrch(t, vaultID)

	chunkID := chunk.NewChunkID()
	now := time.Now().UTC()
	apply := func(data []byte) {
		t.Helper()
		if err := g.Raft.Apply(vaultraft.MarshalVaultChunkCommand(vaultID, data), 5*time.Second).Error(); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	apply(vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now))
	apply(vaultctlfsm.MarshalSealChunk(chunkID, now, 1, 100, now, now, now, true, now, [32]byte{}))
	// Some other node uploaded the blob: the replicated FSM now says
	// CloudBacked=true.
	apply(vaultctlfsm.MarshalUploadChunk(chunkID, 1234, 0, 0, 0, 0, [32]byte{}, glid.GLID{}, 0))

	// The local follower instance has no CloudStore, so its chunk manager
	// reports CloudBacked=false for the same chunk. DiskBytes is live warm-cache
	// state only this node knows — grounding must not clobber it.
	local := chunk.ChunkMeta{
		ID: chunkID, Sealed: true, State: chunk.ChunkStateSealed,
		CloudBacked: false, DiskBytes: 777, WriteStart: now, WriteEnd: now,
	}
	inst := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: &fixedMetaChunkManager{meta: local}}
	vault := NewVault(vaultID, inst)
	vault.Name = "follower-grounded-from-fsm"
	orch.RegisterVault(vault)

	// Sanity: the raw local meta reports CloudBacked=false — the bug condition
	// the grounded seam corrects.
	if raw, err := inst.Chunks.Meta(chunkID); err != nil || raw.CloudBacked {
		t.Fatalf("test setup wrong: raw local meta should have CloudBacked=false (err=%v, meta=%+v)", err, raw)
	}

	// ListAllChunkMetas reflects the cluster-wide CloudBacked truth.
	metas, err := orch.ListAllChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(metas))
	}
	if got := metas[0].ChunkMeta; !got.CloudBacked {
		t.Errorf("CloudBacked not grounded from FSM: got %+v", got)
	}

	// GetChunkMeta grounds the LOCAL meta directly: CloudBacked flips to the
	// cluster truth while the node-local DiskBytes survives untouched.
	single, err := orch.GetChunkMeta(vaultID, chunkID)
	if err != nil {
		t.Fatalf("GetChunkMeta: %v", err)
	}
	if !single.CloudBacked {
		t.Errorf("GetChunkMeta did not ground CloudBacked from FSM: %+v", single)
	}
	if single.DiskBytes != 777 {
		t.Errorf("GetChunkMeta clobbered local DiskBytes: got %d, want 777", single.DiskBytes)
	}
}

// TestListAllChunkMetasNilOverlayPassthrough verifies that instances with no
// vault-ctl FSM (single-node mode, memory vaults) pass the local chunk
// manager's view through unchanged. Grounding is a no-op there.
func TestListAllChunkMetasNilOverlayPassthrough(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()

	vaultInst := newMemInstance(t, vaultID, false, nil)
	// Note: this orchestrator has no groupMgr, so groundChunkMeta is a no-op —
	// simulating an instance with no Raft group.

	vault := NewVault(vaultID, vaultInst)
	orch.RegisterVault(vault)

	if _, _, err := vaultInst.Chunks.Append(testRecord("payload")); err != nil {
		t.Fatal(err)
	}
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, err := orch.ListAllChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(metas))
	}
	if metas[0].CloudBacked {
		t.Errorf("nil overlay should not flip CloudBacked: got %+v", metas[0].ChunkMeta)
	}
}

// TestListAllChunkMetasSkipsFollowerInstances is the regression test for
// double-counted record totals. When a vault has both a leader and a follower
// instance for the same instance on the same node, ListAllChunkMetas must
// return only the leader's chunks. Including the follower's view double-
// counts records and produces non-authoritative counts in the Inspector.

// TestListAllChunkMetasIncludesFollowerOnlyInstances verifies that vaults where
// this node is a follower-only (no leader instance locally) ARE included.
// The leader node lives elsewhere, but this node's follower view is still
// needed at the server layer to count replica presence.
func TestListAllChunkMetasIncludesFollowerOnlyInstances(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	followerOnlyVaultID := glid.New()
	vaultID := glid.New()

	followerOnly := newMemInstance(t, followerOnlyVaultID, true, nil)
	if _, _, err := followerOnly.Chunks.Append(testRecord("follower-only")); err != nil {
		t.Fatal(err)
	}
	if err := followerOnly.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, followerOnly)
	vault.Name = "follower-only"
	orch.RegisterVault(vault)

	metas, err := orch.ListAllChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 chunk from follower-only vaultInst, got %d", len(metas))
	}
}

// --- LocalLeaderVaultIDs ---

func TestLocalLeaderVaultIDsExcludesFollowerOnlyVaults(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	leaderVaultID := glid.New()
	followerVaultID := glid.New()

	// Vault with a leader instance on this node — should be in the result.
	leader := newMemInstance(t, leaderVaultID, false, nil)
	leaderVault := NewVault(leaderVaultID, leader)
	leaderVault.Name = "leader-vault"
	orch.RegisterVault(leaderVault)

	// Vault with only a follower instance on this node — should NOT be in result.
	follower := newMemInstance(t, followerVaultID, true, nil)
	followerVault := NewVault(followerVaultID, follower)
	followerVault.Name = "follower-vault"
	orch.RegisterVault(followerVault)

	ids := orch.LocalLeaderVaultIDs()
	if !ids[leaderVaultID] {
		t.Error("vault with a leader vaultInst should be in LocalLeaderVaultIDs")
	}
	if ids[followerVaultID] {
		t.Error("vault with only follower instances should NOT be in LocalLeaderVaultIDs")
	}
}

// --- Retention action from position ---

// Phase 4 deleted TestRetentionActionDerivedFromPosition:
// the action enum is gone, retention rules carry only the policy, and the
// "is this the last instance?" position-based action derivation was removed
// alongside the multi-transition chain (Phase 2 collapsed the chain).

// --- AppendToVault ---

func TestAppendToInstanceSecondaryUsesChunkID(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, true, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "id-sync"
	orch.RegisterVault(vault)

	leaderChunkID := chunk.NewChunkID()
	if err := orch.AppendToVault(vaultID, leaderChunkID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	// The follower's active chunk should have the leader's chunk ID.
	active := vaultInst.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk on follower")
	}
	if active.ID != leaderChunkID {
		t.Errorf("follower chunk ID = %s, want leader's %s", active.ID, leaderChunkID)
	}
}

func TestAppendToInstanceSecondarySkipsPostSeal(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	vaultID := glid.New()
	// Small rotation policy to trigger seal.
	cm, cErr := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1),
		Now:            time.Now,
		MetaStore:      chunkmem.NewMetaStore(),
	})
	if cErr != nil {
		t.Fatal(cErr)
	}
	im, _ := indexmem.NewFactory()(nil, cm, nil)
	vaultInst := &VaultInstance{
		VaultID:    vaultID,
		Type:       "memory",
		Chunks:     cm,
		Indexes:    im,
		Query:      query.New(cm, im, nil),
		IsFollower: true,
	}
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "skip-postseal"
	orch.RegisterVault(vault)

	leaderChunkID := chunk.NewChunkID()
	// First record fills the chunk (policy = 1 record), triggering seal on the second.
	if err := orch.AppendToVault(vaultID, leaderChunkID, testRecord("rec-1")); err != nil {
		t.Fatal(err)
	}
	if err := orch.AppendToVault(vaultID, leaderChunkID, testRecord("rec-2")); err != nil {
		t.Fatal(err)
	}

	// The test verifies no panic occurred and the seal happened cleanly.
	metas, _ := cm.List()
	sealed := 0
	for _, m := range metas {
		if m.Sealed {
			sealed++
		}
	}
	if sealed == 0 {
		t.Error("expected at least one sealed chunk after 2 appends with policy=1")
	}
}

// --- Active record forwarding ---

func TestAppendToInstanceNoForwarderSingleNode(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	// No forwarder set — single-node mode.

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, false, []system.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "no-forwarder"
	orch.RegisterVault(vault)

	rec := testRecord("single-node")
	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, rec); err != nil {
		t.Fatal(err)
	}

	// Record should be appended locally.
	active := vaultInst.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk after append")
	}
	if active.RecordCount != 1 {
		t.Errorf("expected 1 record, got %d", active.RecordCount)
	}
}

func TestAppendToInstanceVaultNotFound(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	bogusVaultID := glid.New()

	err := orch.AppendToVault(bogusVaultID, chunk.ChunkID{}, testRecord("data"))
	if err == nil {
		t.Fatal("expected error for non-existent vault")
	}
	if !errors.Is(err, ErrVaultNotFound) {
		t.Errorf("expected ErrVaultNotFound, got %v", err)
	}
}

// ================================================================
// HIGH-VOLUME STRESS TESTS
// ================================================================

// TestAppendToInstanceForwardingDoesNotBlockOnFullChannel verifies fire-and-forget
// semantics: AppendToVault commits the record locally and succeeds with
// follower targets configured. High-volume ingestion (exceeding typical queue
// capacity) must complete without error.
func TestAppendToInstanceForwardingDoesNotBlockOnFullChannel(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, false, []system.ReplicationTarget{{NodeID: "node-2"}, {NodeID: "node-3"}})
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "non-blocking"
	orch.RegisterVault(vault)

	// Append 200 records — well above typical queue capacity.
	const total = 200
	for i := 0; i < total; i++ {
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, testRecord("burst")); err != nil {
			t.Fatalf("AppendToVault %d: %v", i, err)
		}
	}

	// Verify all records committed locally.
	active := vaultInst.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk after appends")
	}
	if active.RecordCount != total {
		t.Errorf("expected %d records in active chunk, got %d", total, active.RecordCount)
	}
}
