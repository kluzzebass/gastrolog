package orchestrator

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

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

// --- ImportToVault ---

func TestImportToInstancePreservesChunkID(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, true, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "import-id"
	orch.RegisterVault(vault)

	targetID := chunk.NewChunkID()
	err := orch.ImportToVault(context.Background(), vaultID, targetID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatal(err)
	}

	metas, err := vaultInst.Chunks.List()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range metas {
		if m.ID == targetID {
			found = true
			if m.RecordCount != 5 {
				t.Errorf("expected 5 records, got %d", m.RecordCount)
			}
		}
	}
	if !found {
		t.Errorf("chunk with target ID %s not found", targetID)
	}
}

func TestImportToInstanceConcurrentSafe(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, true, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "concurrent-import"
	orch.RegisterVault(vault)

	const n = 5
	ids := make([]chunk.ChunkID, n)
	for i := range ids {
		ids[i] = chunk.NewChunkID()
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = orch.ImportToVault(context.Background(), vaultID, ids[idx], testIter(smallRecords(3)))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("import %d failed: %v", i, err)
		}
	}

	metas, err := vaultInst.Chunks.List()
	if err != nil {
		t.Fatal(err)
	}

	idSet := make(map[chunk.ChunkID]bool)
	for _, m := range metas {
		idSet[m.ID] = true
	}
	for _, id := range ids {
		if !idSet[id] {
			t.Errorf("missing chunk ID %s", id)
		}
	}
}

// --- ListAllChunkMetas ---

// TestListAllChunkMetasOverlaysFromFSM is the regression test for
// gastrolog-asg4l. The local chunk manager only sets CloudBacked=true on the
// node that actually uploaded the blob (the cold instance raft leader);
// followers strip sealed_backing from their chunk-manager params and never
// see the cloud state, so their local CloudBacked is permanently false. The
// fix is to overlay the cluster-wide FSM view onto each chunk meta returned
// from ListAllChunkMetas. Without the overlay the inspector showed "no cloud
// badge" 75% of the time on a 4-node cluster (whichever 3 of 4 nodes the
// query happened to land on were always followers).
func TestListAllChunkMetasOverlaysFromFSM(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()

	vaultInst := newMemInstance(t, vaultID, false, nil)
	// Simulate the follower scenario: the FSM has CloudBacked=true (because
	// some other node — the leader — uploaded the blob) but the local chunk
	// manager has no CloudStore so its local meta reports CloudBacked=false.
	// The OverlayFromFSM callback closes the gap.
	vaultInst.OverlayFromFSM = func(m chunk.ChunkMeta) chunk.ChunkMeta {
		m.CloudBacked = true
		m.Archived = true
		return m
	}

	vault := NewVault(vaultID, vaultInst)
	vault.Name = "follower-with-fsm-overlay"
	orch.RegisterVault(vault)

	if _, _, err := vaultInst.Chunks.Append(testRecord("payload")); err != nil {
		t.Fatal(err)
	}
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Sanity-check: the local chunk manager itself reports CloudBacked=false
	// (because it has no CloudStore wired up). This is the bug condition we
	// expect the overlay to correct.
	rawMetas, err := vaultInst.Chunks.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(rawMetas) != 1 {
		t.Fatalf("expected 1 raw meta, got %d", len(rawMetas))
	}
	if rawMetas[0].CloudBacked {
		t.Fatal("test setup wrong: raw meta should have CloudBacked=false")
	}

	// The overlaid view from ListAllChunkMetas should have CloudBacked=true
	// and Archived=true — the cluster-wide truth from the FSM.
	metas, err := orch.ListAllChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(metas))
	}
	got := metas[0].ChunkMeta
	if !got.CloudBacked {
		t.Errorf("CloudBacked not overlaid from FSM: got %+v", got)
	}
	if !got.Archived {
		t.Errorf("Archived not overlaid from FSM: got %+v", got)
	}

	// GetChunkMeta should also apply the overlay.
	chunkID := got.ID
	single, err := orch.GetChunkMeta(vaultID, chunkID)
	if err != nil {
		t.Fatalf("GetChunkMeta: %v", err)
	}
	if !single.CloudBacked || !single.Archived {
		t.Errorf("GetChunkMeta did not apply overlay: %+v", single)
	}
}

// TestListAllChunkMetasNilOverlayPassthrough verifies that instances without an
// OverlayFromFSM callback (single-node mode, memory vaults) pass the local
// chunk manager's view through unchanged. The overlay is opt-in.
func TestListAllChunkMetasNilOverlayPassthrough(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()

	vaultInst := newMemInstance(t, vaultID, false, nil)
	// Note: instance.OverlayFromFSM is nil, simulating an instance with no Raft group.

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
// gastrolog-2rvak. When a vault has both a leader and a follower instance
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

// --- followerReplicationTargets ---

// --- Retention action from position ---

// Phase 4 (gastrolog-42f9z) deleted TestRetentionActionDerivedFromPosition:
// the action enum is gone, retention rules carry only the policy, and the
// "is this the last instance?" position-based action derivation was removed
// alongside the multi-transition chain (Phase 2 collapsed the chain).

// --- Import idempotency ---

func TestImportToInstanceIdempotent(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, false, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "idempotent"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// First import — should succeed.
	err := orch.ImportToVault(context.Background(), vaultID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatal(err)
	}

	// Second import with same chunk ID — idempotent skip (chunk already exists).
	err = orch.ImportToVault(context.Background(), vaultID, chunkID, testIter(smallRecords(3)))
	if err != nil {
		t.Fatal(err)
	}

	// Verify only one chunk exists with that ID, with 5 records (first import kept).
	metas, _ := vaultInst.Chunks.List()
	count := 0
	for _, m := range metas {
		if m.ID == chunkID {
			count++
			if m.RecordCount != 5 {
				t.Errorf("expected 5 records from first import, got %d", m.RecordCount)
			}
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 chunk with ID %s, got %d", chunkID, count)
	}
}

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

	// If post-seal were scheduled on a follower, it would queue compression
	// work that races with ImportToVault's delete-and-replace. The test just
	// verifies no panic occurred and the seal happened cleanly.
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

// --- Import keeps forwarded version on follower (no delete-and-replace) ---

func TestImportToInstanceSecondarySealsActiveAndKeeps(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, true, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "seal-and-keep"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate active record forwarding: follower has an active chunk
	// with the leader's ID, still receiving records.
	vaultInst.Chunks.SetNextChunkID(chunkID)
	for range 3 {
		if _, _, err := vaultInst.Chunks.Append(testRecord("forwarded")); err != nil {
			t.Fatal(err)
		}
	}
	active := vaultInst.Chunks.Active()
	if active == nil || active.ID != chunkID {
		t.Fatal("expected active chunk with leader's ID")
	}

	// Primary seals and sends canonical version. ImportToVault should
	// seal the active chunk and keep it (no delete-and-replace).
	err := orch.ImportToVault(context.Background(), vaultID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	// Forwarded version was replaced by canonical (5 records).
	meta, err := vaultInst.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("expected canonical chunk to exist: %v", err)
	}
	if !meta.Sealed {
		t.Error("canonical chunk should be sealed")
	}
	if meta.RecordCount != 5 {
		t.Errorf("canonical chunk should have 5 records, got %d", meta.RecordCount)
	}
}

func TestImportToInstanceSecondaryKeepsSealedForwarded(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, true, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "keep-sealed"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate: forwarded version is already sealed (e.g., follower
	// received SealActiveChunk before the canonical import arrives).
	vaultInst.Chunks.SetNextChunkID(chunkID)
	for range 3 {
		if _, _, err := vaultInst.Chunks.Append(testRecord("forwarded")); err != nil {
			t.Fatal(err)
		}
	}
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// ImportToVault should replace the forwarded version with canonical.
	err := orch.ImportToVault(context.Background(), vaultID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	// Canonical version replaces forwarded (5 records, not 3).
	meta, err := vaultInst.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("expected canonical chunk to exist: %v", err)
	}
	if meta.RecordCount != 5 {
		t.Errorf("canonical should have 5 records, got %d", meta.RecordCount)
	}

	// Only one chunk with this ID.
	metas, _ := vaultInst.Chunks.List()
	count := 0
	for _, m := range metas {
		if m.ID == chunkID {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 chunk, got %d", count)
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

func TestImportToInstanceDrainsIteratorOnSkip(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, true, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "drain-on-skip"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Pre-populate a sealed chunk with this ID so ImportToVault will skip.
	vaultInst.Chunks.SetNextChunkID(chunkID)
	if _, _, err := vaultInst.Chunks.Append(testRecord("existing")); err != nil {
		t.Fatal(err)
	}
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Build a tracking iterator that counts consumed records.
	const totalRecords = 7
	consumed := 0
	trackingIter := func() (chunk.Record, error) {
		if consumed >= totalRecords {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		consumed++
		return chunk.Record{
			Raw:      []byte("drain-me"),
			SourceTS: time.Now(),
			IngestTS: time.Now(),
		}, nil
	}

	err := orch.ImportToVault(context.Background(), vaultID, chunkID, trackingIter)
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	if consumed != totalRecords {
		t.Errorf("expected all %d records consumed (drained), got %d", totalRecords, consumed)
	}
}

// ================================================================
// HIGH-VOLUME STRESS TESTS
// ================================================================

// TestImportToInstanceReplacesIncompleteForwardedChunk verifies that ImportToVault
// replaces a forwarded chunk that has fewer records (simulating fire-and-forget
// drops) with the canonical version containing all records.
func TestImportToInstanceReplacesIncompleteForwardedChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, true, nil) // follower receives forwarded + canonical
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "incomplete-forward"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate fire-and-forget forwarding: only 70 of 100 records arrive.
	vaultInst.Chunks.SetNextChunkID(chunkID)
	for i := 0; i < 70; i++ {
		if _, _, err := vaultInst.Chunks.Append(testRecord("forwarded")); err != nil {
			t.Fatal(err)
		}
	}
	// Seal the incomplete forwarded chunk.
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	meta, err := vaultInst.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("expected forwarded chunk to exist: %v", err)
	}
	if meta.RecordCount != 70 {
		t.Fatalf("expected 70 forwarded records, got %d", meta.RecordCount)
	}

	// ImportToVault with canonical version: all 100 records.
	err = orch.ImportToVault(context.Background(), vaultID, chunkID, testIter(smallRecords(100)))
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	// Verify: chunk now has 100 records (canonical replaced incomplete).
	meta, err = vaultInst.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("expected canonical chunk to exist: %v", err)
	}
	if meta.RecordCount != 100 {
		t.Errorf("expected 100 records after canonical import, got %d", meta.RecordCount)
	}
	if !meta.Sealed {
		t.Error("expected canonical chunk to be sealed")
	}

	// Verify exactly one chunk with this ID.
	metas, _ := vaultInst.Chunks.List()
	count := 0
	for _, m := range metas {
		if m.ID == chunkID {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 chunk with ID %s, got %d", chunkID, count)
	}
}

// TestAppendToInstanceForwardingDoesNotBlockOnFullChannel verifies fire-and-forget
// semantics: AppendToVault commits the record locally and succeeds even with no
// chunk replicator wired. High-volume ingestion (exceeding typical queue
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
