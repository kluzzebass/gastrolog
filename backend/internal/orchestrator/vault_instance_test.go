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
		VaultID:          vaultID,
		Type:            "memory",
		Chunks:          cm,
		Indexes:         im,
		Query:           query.New(cm, im, nil),
		PeerPlacementTargets: followers,
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

// vaultTestReplicator records AppendRecords calls on the ChunkReplicator interface.
// Satisfies orchestrator.ChunkReplicator.
type vaultTestReplicator struct {
	mu    sync.Mutex
	calls []vaultForwardCall
}

type vaultForwardCall struct {
	NodeID  string
	VaultID glid.GLID
	InstanceID  glid.GLID
	ChunkID chunk.ChunkID
	Records []chunk.Record
}

func (r *vaultTestReplicator) AppendRecords(_ context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, vaultForwardCall{
		NodeID: nodeID, VaultID: vaultID,
		ChunkID: chunkID, Records: records,
	})
	return nil
}

func (r *vaultTestReplicator) SealVault(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

func (r *vaultTestReplicator) ImportSealedChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}

func (r *vaultTestReplicator) DeleteChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

func (r *vaultTestReplicator) RequestReplicaCatchup(_ context.Context, _ string, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}
func (r *vaultTestReplicator) SendFillRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record, _ bool) error {
	return nil
}
func (r *vaultTestReplicator) SendFillComplete(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ uint32, _ string) error {
	return nil
}
func (r *vaultTestReplicator) PullRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.EventID, _ string) (uint32, uint32, error) {
	return 0, 0, nil
}

func (r *vaultTestReplicator) getCalls() []vaultForwardCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]vaultForwardCall(nil), r.calls...)
}

// TestAppendToVaultDoesNotForward verifies that AppendToVault — the
// receiver-side handler for cross-node fan-out RPCs — appends locally
// only and never re-forwards. Loops would otherwise be possible: peer A
// fans out to peer B, B's AppendToVault calls something that fans out
// to A, and the record bounces. The fan-out dispatcher on the sender
// side is the sole forwarding origin.
func TestAppendToVaultDoesNotForward(t *testing.T) {
	t.Parallel()
	fwd := &vaultTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})
	orch.SetChunkReplicator(fwd)

	vaultID := glid.New()
	vaultInst := newMemInstance(t, vaultID, false, nil)
	vault := NewVault(vaultID, vaultInst)
	vault.Name = "no-reforward"
	orch.RegisterVault(vault)

	if err := orch.AppendToVault(vaultID, chunk.NewChunkID(), testRecord("data")); err != nil {
		t.Fatal(err)
	}

	if len(fwd.getCalls()) != 0 {
		t.Error("AppendToVault must NOT forward to other nodes (prevents fan-out loops)")
	}
}

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
		VaultID:     vaultID,
		Type:       "memory",
		Chunks:     cm,
		Indexes:    im,
		Query:      query.New(cm, im, nil),
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

// TestTransitionLocalPreservesAllRecords verifies zero record loss when
// transitioning a large sealed chunk from instance 0 to instance 1. The 5000 records
// may span multiple chunks in the destination instance due to rotation policy.

// errorCursor is a RecordCursor that returns N records, then returns a
// configurable error (not ErrNoMoreRecords) to simulate mid-read failures.
type errorCursor struct {
	records []chunk.Record
	pos     int
	err     error // returned after records are exhausted
}

func (c *errorCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.pos < len(c.records) {
		rec := c.records[c.pos]
		c.pos++
		return rec, chunk.RecordRef{Pos: uint64(c.pos)}, nil
	}
	return chunk.Record{}, chunk.RecordRef{}, c.err
}

func (c *errorCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	return chunk.Record{}, chunk.RecordRef{}, errors.New("not implemented")
}

func (c *errorCursor) Seek(_ chunk.RecordRef) error {
	return errors.New("not implemented")
}

func (c *errorCursor) Close() error { return nil }

// TestTransitionLocalCursorErrorRetainsSource verifies that when a cursor
// returns an unexpected error (not ErrNoMoreRecords), streamLocal propagates
// it so transitionChunk does NOT call expireChunk — the source chunk is retained.

