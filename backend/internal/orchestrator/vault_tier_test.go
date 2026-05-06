package orchestrator

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

func newMemTier(t *testing.T, tierID glid.GLID, isFollower bool, followers []system.ReplicationTarget) *VaultInstance {
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
		TierID:          tierID,
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

func TestImportToTierPreservesChunkID(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "import-id"
	orch.RegisterVault(vault)

	targetID := chunk.NewChunkID()
	err := orch.ImportToVault(context.Background(), vaultID, tierID, targetID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatal(err)
	}

	metas, err := tier.Chunks.List()
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

func TestImportToTierConcurrentSafe(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
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
			errs[idx] = orch.ImportToVault(context.Background(), vaultID, tierID, ids[idx], testIter(smallRecords(3)))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("import %d failed: %v", i, err)
		}
	}

	metas, err := tier.Chunks.List()
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
// node that actually uploaded the blob (the cold tier raft leader);
// followers strip sealed_backing from their chunk-manager params and never
// see the cloud state, so their local CloudBacked is permanently false. The
// fix is to overlay the cluster-wide FSM view onto each chunk meta returned
// from ListAllChunkMetas. Without the overlay the inspector showed "no cloud
// badge" 75% of the time on a 4-node cluster (whichever 3 of 4 nodes the
// query happened to land on were always followers).
func TestListAllChunkMetasOverlaysFromFSM(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	tier := newMemTier(t, tierID, false, nil)
	// Simulate the follower scenario: the FSM has CloudBacked=true (because
	// some other node — the leader — uploaded the blob) but the local chunk
	// manager has no CloudStore so its local meta reports CloudBacked=false.
	// The OverlayFromFSM callback closes the gap.
	tier.OverlayFromFSM = func(m chunk.ChunkMeta) chunk.ChunkMeta {
		m.CloudBacked = true
		m.Archived = true
		return m
	}

	vault := NewVault(vaultID, tier)
	vault.Name = "follower-with-fsm-overlay"
	orch.RegisterVault(vault)

	if _, _, err := tier.Chunks.Append(testRecord("payload")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Sanity-check: the local chunk manager itself reports CloudBacked=false
	// (because it has no CloudStore wired up). This is the bug condition we
	// expect the overlay to correct.
	rawMetas, err := tier.Chunks.List()
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

// TestListAllChunkMetasNilOverlayPassthrough verifies that tiers without an
// OverlayFromFSM callback (single-node mode, memory tiers) pass the local
// chunk manager's view through unchanged. The overlay is opt-in.
func TestListAllChunkMetasNilOverlayPassthrough(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	tier := newMemTier(t, tierID, false, nil)
	// Note: tier.OverlayFromFSM is nil, simulating a tier with no Raft group.

	vault := NewVault(vaultID, tier)
	orch.RegisterVault(vault)

	if _, _, err := tier.Chunks.Append(testRecord("payload")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
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
// gastrolog-2rvak. When a vault has both a leader and a follower tier
// instance for the same tier on the same node, ListAllChunkMetas must
// return only the leader's chunks. Including the follower's view double-
// counts records and produces non-authoritative counts in the Inspector.

// TestListAllChunkMetasIncludesFollowerOnlyTiers verifies that tiers where
// this node is a follower-only (no leader instance locally) ARE included.
// The leader node lives elsewhere, but this node's follower view is still
// needed at the server layer to count replica presence.
func TestListAllChunkMetasIncludesFollowerOnlyTiers(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	followerOnlyTierID := glid.New()
	vaultID := glid.New()

	followerOnly := newMemTier(t, followerOnlyTierID, true, nil)
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
		t.Fatalf("expected 1 chunk from follower-only tier, got %d", len(metas))
	}
}

// --- LocalLeaderVaultIDs ---

func TestLocalLeaderVaultIDsExcludesFollowerOnlyVaults(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	leaderTierID := glid.New()
	followerTierID := glid.New()
	leaderVaultID := glid.New()
	followerVaultID := glid.New()

	// Vault with a leader tier on this node — should be in the result.
	leader := newMemTier(t, leaderTierID, false, nil)
	leaderVault := NewVault(leaderVaultID, leader)
	leaderVault.Name = "leader-vault"
	orch.RegisterVault(leaderVault)

	// Vault with only a follower tier on this node — should NOT be in result.
	follower := newMemTier(t, followerTierID, true, nil)
	followerVault := NewVault(followerVaultID, follower)
	followerVault.Name = "follower-vault"
	orch.RegisterVault(followerVault)

	ids := orch.LocalLeaderVaultIDs()
	if !ids[leaderVaultID] {
		t.Error("vault with a leader tier should be in LocalLeaderVaultIDs")
	}
	if ids[followerVaultID] {
		t.Error("vault with only follower tiers should NOT be in LocalLeaderVaultIDs")
	}
}

// --- tierReplicationInfo ---


// --- Retention action from position ---

func TestRetentionActionDerivedFromPosition(t *testing.T) {
	t.Parallel()

	tier1ID := glid.New()
	tier2ID := glid.New()
	tier3ID := glid.New()
	policyID := glid.New()

	vaultID := glid.New()
	vaultCfg := system.VaultConfig{
		ID: vaultID,
	}

	cfg := &system.Config{
		RetentionPolicies: []system.RetentionPolicyConfig{
			{ID: policyID, MaxAge: func() *string { s := "1s"; return &s }()},
		},
		Tiers: []system.TierConfig{
			{ID: tier1ID, VaultID: vaultID, Position: 0},
			{ID: tier2ID, VaultID: vaultID, Position: 1},
			{ID: tier3ID, VaultID: vaultID, Position: 2},
		},
	}

	// Tier at position 0 (not last) — should be transition.
	tier1Cfg := &system.TierConfig{
		ID: tier1ID, VaultID: vaultID, Position: 0,
		RetentionRules: []system.RetentionRule{
			{RetentionPolicyID: policyID, Action: system.RetentionActionExpire}, // stored as expire
		},
	}
	rules1, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tier1Cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules1) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules1))
	}
	if rules1[0].action != system.RetentionActionTransition {
		t.Errorf("position 0 of 3: expected transition, got %v", rules1[0].action)
	}

	// Tier at position 2 (last) — should be expire.
	tier3Cfg := &system.TierConfig{
		ID: tier3ID, VaultID: vaultID, Position: 2,
		RetentionRules: []system.RetentionRule{
			{RetentionPolicyID: policyID, Action: system.RetentionActionTransition}, // stored as transition
		},
	}
	rules3, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tier3Cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules3) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules3))
	}
	if rules3[0].action != system.RetentionActionExpire {
		t.Errorf("position 2 of 3: expected expire, got %v", rules3[0].action)
	}
}

// --- Import idempotency ---

func TestImportToTierIdempotent(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "idempotent"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// First import — should succeed.
	err := orch.ImportToVault(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatal(err)
	}

	// Second import with same chunk ID — idempotent skip (chunk already exists).
	err = orch.ImportToVault(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(3)))
	if err != nil {
		t.Fatal(err)
	}

	// Verify only one chunk exists with that ID, with 5 records (first import kept).
	metas, _ := tier.Chunks.List()
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

// tierTestReplicator records AppendRecords calls on the ChunkReplicator interface.
// Satisfies orchestrator.ChunkReplicator.
type tierTestReplicator struct {
	mu    sync.Mutex
	calls []tierForwardCall
}

type tierForwardCall struct {
	NodeID  string
	VaultID glid.GLID
	TierID  glid.GLID
	ChunkID chunk.ChunkID
	Records []chunk.Record
}

func (r *tierTestReplicator) AppendRecords(_ context.Context, nodeID string, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, tierForwardCall{
		NodeID: nodeID, VaultID: vaultID, TierID: tierID,
		ChunkID: chunkID, Records: records,
	})
	return nil
}

func (r *tierTestReplicator) SealVault(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

func (r *tierTestReplicator) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}

func (r *tierTestReplicator) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

func (r *tierTestReplicator) RequestReplicaCatchup(_ context.Context, _ string, _, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

func (r *tierTestReplicator) getCalls() []tierForwardCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]tierForwardCall(nil), r.calls...)
}

func TestAppendToTierLeaderForwardsToFollowers(t *testing.T) {
	t.Parallel()
	fwd := &tierTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(fwd)

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, []system.ReplicationTarget{{NodeID: "node-2"}, {NodeID: "node-3"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "fwd-test"
	orch.RegisterVault(vault)

	rec := testRecord("hello")
	if err := orch.AppendToVault(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
		t.Fatal(err)
	}

	calls := fwd.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 AppendRecords calls (one per follower), got %d", len(calls))
	}
	nodes := map[string]bool{}
	for _, c := range calls {
		nodes[c.NodeID] = true
		if c.VaultID != vaultID {
			t.Errorf("call.VaultID = %s, want %s", c.VaultID, vaultID)
		}
		if c.TierID != tierID {
			t.Errorf("call.TierID = %s, want %s", c.TierID, tierID)
		}
		if c.ChunkID == (chunk.ChunkID{}) {
			t.Error("call.ChunkID should be non-zero (active chunk ID)")
		}
		if len(c.Records) != 1 {
			t.Errorf("expected 1 record per call, got %d", len(c.Records))
		}
	}
	if !nodes["node-2"] || !nodes["node-3"] {
		t.Errorf("expected forwards to node-2 and node-3, got %v", nodes)
	}
}

func TestAppendToTierSecondaryDoesNotForward(t *testing.T) {
	t.Parallel()
	fwd := &tierTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})
	orch.SetChunkReplicator(fwd)

	tierID := glid.New()
	vaultID := glid.New()
	// Follower tier — should NOT re-forward.
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "no-reforward"
	orch.RegisterVault(vault)

	leaderChunkID := chunk.NewChunkID()
	if err := orch.AppendToVault(vaultID, tierID, leaderChunkID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	if len(fwd.getCalls()) != 0 {
		t.Error("follower should NOT forward to other nodes (prevents loops)")
	}
}

func TestAppendToTierSecondaryUsesChunkID(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "id-sync"
	orch.RegisterVault(vault)

	leaderChunkID := chunk.NewChunkID()
	if err := orch.AppendToVault(vaultID, tierID, leaderChunkID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	// The follower's active chunk should have the leader's chunk ID.
	active := tier.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk on follower")
	}
	if active.ID != leaderChunkID {
		t.Errorf("follower chunk ID = %s, want leader's %s", active.ID, leaderChunkID)
	}
}

func TestAppendToTierSecondarySkipsPostSeal(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	tierID := glid.New()
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
	tier := &VaultInstance{
		TierID:     tierID,
		Type:       "memory",
		Chunks:     cm,
		Indexes:    im,
		Query:      query.New(cm, im, nil),
		IsFollower: true,
	}
	vault := NewVault(vaultID, tier)
	vault.Name = "skip-postseal"
	orch.RegisterVault(vault)

	leaderChunkID := chunk.NewChunkID()
	// First record fills the chunk (policy = 1 record), triggering seal on the second.
	if err := orch.AppendToVault(vaultID, tierID, leaderChunkID, testRecord("rec-1")); err != nil {
		t.Fatal(err)
	}
	if err := orch.AppendToVault(vaultID, tierID, leaderChunkID, testRecord("rec-2")); err != nil {
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

func TestImportToTierSecondarySealsActiveAndKeeps(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "seal-and-keep"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate active record forwarding: follower has an active chunk
	// with the leader's ID, still receiving records.
	tier.Chunks.SetNextChunkID(chunkID)
	for range 3 {
		if _, _, err := tier.Chunks.Append(testRecord("forwarded")); err != nil {
			t.Fatal(err)
		}
	}
	active := tier.Chunks.Active()
	if active == nil || active.ID != chunkID {
		t.Fatal("expected active chunk with leader's ID")
	}

	// Primary seals and sends canonical version. ImportToVault should
	// seal the active chunk and keep it (no delete-and-replace).
	err := orch.ImportToVault(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	// Forwarded version was replaced by canonical (5 records).
	meta, err := tier.Chunks.Meta(chunkID)
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

func TestImportToTierSecondaryKeepsSealedForwarded(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "keep-sealed"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate: forwarded version is already sealed (e.g., follower
	// received SealActiveTier before the canonical import arrives).
	tier.Chunks.SetNextChunkID(chunkID)
	for range 3 {
		if _, _, err := tier.Chunks.Append(testRecord("forwarded")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// ImportToVault should replace the forwarded version with canonical.
	err := orch.ImportToVault(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	// Canonical version replaces forwarded (5 records, not 3).
	meta, err := tier.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("expected canonical chunk to exist: %v", err)
	}
	if meta.RecordCount != 5 {
		t.Errorf("canonical should have 5 records, got %d", meta.RecordCount)
	}

	// Only one chunk with this ID.
	metas, _ := tier.Chunks.List()
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

func TestAppendToTierNoForwarderSingleNode(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	// No forwarder set — single-node mode.

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, []system.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "no-forwarder"
	orch.RegisterVault(vault)

	rec := testRecord("single-node")
	if err := orch.AppendToVault(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
		t.Fatal(err)
	}

	// Record should be appended locally.
	active := tier.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk after append")
	}
	if active.RecordCount != 1 {
		t.Errorf("expected 1 record, got %d", active.RecordCount)
	}
}

func TestAppendToTierVaultNotFound(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	bogusVaultID := glid.New()
	tierID := glid.New()

	err := orch.AppendToVault(bogusVaultID, tierID, chunk.ChunkID{}, testRecord("data"))
	if err == nil {
		t.Fatal("expected error for non-existent vault")
	}
	if !errors.Is(err, ErrVaultNotFound) {
		t.Errorf("expected ErrVaultNotFound, got %v", err)
	}
}

func TestAppendToTierTierNotFound(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "tier-not-found"
	orch.RegisterVault(vault)

	bogusTierID := glid.New()
	err := orch.AppendToVault(vaultID, bogusTierID, chunk.ChunkID{}, testRecord("data"))
	if err == nil {
		t.Fatal("expected error for non-existent tier")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error containing 'not found', got %v", err)
	}
}

// TestImportToTierTierNotLocal pins gastrolog-2t48z: when ImportToVault
// is invoked against a (vault, tier) pair where the tier instance has
// been evicted from this node by placement reconfiguration, the error
// must be ErrTierNotLocal — not ErrVaultNotFound — so log lines don't
// suggest the vault was deleted.
func TestImportToTierTierNotLocal(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, nil)
	vault := NewVault(vaultID, tier)
	orch.RegisterVault(vault)

	// Vault exists, but ask for a tier that doesn't live here.
	bogusTierID := glid.New()
	iter := func() (chunk.Record, error) {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}
	err := orch.ImportToVault(context.Background(), vaultID, bogusTierID, chunk.NewChunkID(), iter)
	if err == nil {
		t.Fatal("expected error for non-resident tier")
	}
	if !errors.Is(err, ErrTierNotLocal) {
		t.Errorf("expected ErrTierNotLocal, got %v", err)
	}
	if errors.Is(err, ErrVaultNotFound) {
		t.Errorf("must NOT be ErrVaultNotFound — vault is registered, only tier instance is missing: %v", err)
	}
}

func TestImportToTierDrainsIteratorOnSkip(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "drain-on-skip"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Pre-populate a sealed chunk with this ID so ImportToVault will skip.
	tier.Chunks.SetNextChunkID(chunkID)
	if _, _, err := tier.Chunks.Append(testRecord("existing")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
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

	err := orch.ImportToVault(context.Background(), vaultID, tierID, chunkID, trackingIter)
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	if consumed != totalRecords {
		t.Errorf("expected all %d records consumed (drained), got %d", totalRecords, consumed)
	}
}

func TestAppendToTierForwardLifecycle(t *testing.T) {
	t.Parallel()
	fwd := &tierTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(fwd)

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, []system.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "forward-lifecycle"
	orch.RegisterVault(vault)

	// Append 3 records.
	for i := range 3 {
		rec := testRecord("rec-" + string(rune('a'+i)))
		if err := orch.AppendToVault(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Verify 3 AppendRecords calls.
	calls := fwd.getCalls()
	if len(calls) != 3 {
		t.Fatalf("expected 3 AppendRecords calls, got %d", len(calls))
	}

	// All calls should target the same vault, tier, and chunk ID.
	firstChunkID := calls[0].ChunkID
	if firstChunkID == (chunk.ChunkID{}) {
		t.Fatal("expected non-zero chunk ID in forward calls")
	}
	for i, c := range calls {
		if c.VaultID != vaultID {
			t.Errorf("call %d: VaultID = %s, want %s", i, c.VaultID, vaultID)
		}
		if c.TierID != tierID {
			t.Errorf("call %d: TierID = %s, want %s", i, c.TierID, tierID)
		}
		if c.ChunkID != firstChunkID {
			t.Errorf("call %d: ChunkID = %s, want consistent %s", i, c.ChunkID, firstChunkID)
		}
		if c.NodeID != "node-2" {
			t.Errorf("call %d: NodeID = %s, want node-2", i, c.NodeID)
		}
	}

	// Verify local tier has 3 records in active chunk.
	active := tier.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	if active.RecordCount != 3 {
		t.Errorf("expected 3 records in active chunk, got %d", active.RecordCount)
	}
}

// ================================================================
// ACK-GATED INGESTION TESTS
// ================================================================

// ackTestReplicator records AppendRecords calls and returns a configurable error.
// Implements orchestrator.ChunkReplicator.
type ackTestReplicator struct {
	tierAppendCalls atomic.Int32
	tierAppendErr   error
}

func (m *ackTestReplicator) AppendRecords(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	m.tierAppendCalls.Add(1)
	return m.tierAppendErr
}
func (m *ackTestReplicator) SealVault(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (m *ackTestReplicator) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}
func (m *ackTestReplicator) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (m *ackTestReplicator) RequestReplicaCatchup(_ context.Context, _ string, _, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}
func TestAppendRecordWaitForReplicaReturnsTask(t *testing.T) {
	t.Parallel()
	fwd := &tierTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(fwd)

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, []system.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "ack-gated"
	orch.RegisterVault(vault)

	rec := testRecord("ack-me")
	rec.WaitForReplica = true

	orch.mu.RLock()
	_, _, task, _, err := orch.appendRecord(vaultID, rec)
	orch.mu.RUnlock()

	if err != nil {
		t.Fatalf("appendRecord: %v", err)
	}
	if task == nil {
		t.Fatal("expected non-nil replicationTask for WaitForReplica=true")
	}
	if task.vaultID != vaultID {
		t.Errorf("task.vaultID = %s, want %s", task.vaultID, vaultID)
	}
	if task.tierID != tierID {
		t.Errorf("task.tierID = %s, want %s", task.tierID, tierID)
	}
	if len(task.targets) != 1 || task.targets[0].NodeID != "node-2" {
		t.Errorf("task.targets = %v, want [node-2]", task.targets)
	}

	// Fire-and-forget must NOT have been called.
	calls := fwd.getCalls()
	if len(calls) != 0 {
		t.Errorf("expected 0 fire-and-forget forward calls, got %d", len(calls))
	}
}

func TestAppendRecordNoWaitForReplicaFiresAndForgets(t *testing.T) {
	t.Parallel()
	fwd := &tierTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(fwd)

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, []system.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "no-ack"
	orch.RegisterVault(vault)

	rec := testRecord("fire-and-forget")
	rec.WaitForReplica = false

	orch.mu.RLock()
	_, _, task, remotes, err := orch.appendRecord(vaultID, rec)
	orch.mu.RUnlock()

	if err != nil {
		t.Fatalf("appendRecord: %v", err)
	}
	if task != nil {
		t.Error("expected nil replicationTask for WaitForReplica=false")
	}

	// Remote targets must have been collected (not fired yet — caller's responsibility).
	if len(remotes) != 1 {
		t.Fatalf("expected 1 remote forward target, got %d", len(remotes))
	}
	if remotes[0].nodeID != "node-2" {
		t.Errorf("forward target nodeID = %s, want node-2", remotes[0].nodeID)
	}

	// Fire and verify the forwarder was called.
	orch.fireAndForgetRemote(remotes, rec)
	calls := fwd.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 fire-and-forget forward call, got %d", len(calls))
	}
}

func TestIngestReturnsReplicationTasks(t *testing.T) {
	t.Parallel()
	fwd := &tierTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(fwd)

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, []system.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "ingest-ack"
	orch.RegisterVault(vault)

	// Set up a filter that routes everything to our vault.
	filter, err := CompileFilter(vaultID, "*")
	if err != nil {
		t.Fatal(err)
	}
	orch.SetFilterSet(NewFilterSet([]*CompiledFilter{filter}))

	rec := testRecord("ingest-me")
	rec.WaitForReplica = true

	pa, err := orch.ingest(rec)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if pa.isEmpty() {
		t.Fatal("expected non-empty pendingAcks for WaitForReplica=true")
	}
	if len(pa.replication) == 0 {
		t.Fatal("expected at least one replication task for WaitForReplica=true")
	}
	if pa.replication[0].vaultID != vaultID {
		t.Errorf("task[0].vaultID = %s, want %s", pa.replication[0].vaultID, vaultID)
	}
}

func TestAckAfterReplicationSuccess(t *testing.T) {
	t.Parallel()
	mock := &ackTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(mock)

	pa := &pendingAcks{
		replication: []replicationTask{
			{
				vaultID: glid.New(),
				tierID:  glid.New(),
				chunkID: chunk.NewChunkID(),
				targets: []system.ReplicationTarget{{NodeID: "node-2"}},
			},
		},
	}

	ack := make(chan error, 1)
	orch.ackAfterReplication(ack, pa, testRecord("ack-ok"))

	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("expected nil ack error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ack")
	}

	if mock.tierAppendCalls.Load() != 1 {
		t.Errorf("expected 1 AppendRecords call, got %d", mock.tierAppendCalls.Load())
	}
}

func TestAckAfterReplicationInvokesEveryReplicationTarget(t *testing.T) {
	t.Parallel()
	mock := &ackTestReplicator{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(mock)

	vaultID := glid.New()
	tierID := glid.New()
	chunkID := chunk.NewChunkID()
	pa := &pendingAcks{
		replication: []replicationTask{
			{
				vaultID: vaultID,
				tierID:  tierID,
				chunkID: chunkID,
				targets: []system.ReplicationTarget{
					{NodeID: "node-2"},
					{NodeID: "node-3"},
					{NodeID: "node-4"},
				},
			},
		},
	}

	ack := make(chan error, 1)
	orch.ackAfterReplication(ack, pa, testRecord("fanout"))

	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("expected nil ack, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ack")
	}

	if got := mock.tierAppendCalls.Load(); got != 3 {
		t.Errorf("expected 3 AppendRecords calls (one per follower), got %d", got)
	}
}

func TestAckAfterReplicationFailure(t *testing.T) {
	t.Parallel()
	mock := &ackTestReplicator{
		tierAppendErr: errors.New("replication failed"),
	}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(mock)

	pa := &pendingAcks{
		replication: []replicationTask{
			{
				vaultID: glid.New(),
				tierID:  glid.New(),
				chunkID: chunk.NewChunkID(),
				targets: []system.ReplicationTarget{{NodeID: "node-2"}},
			},
		},
	}

	ack := make(chan error, 1)
	orch.ackAfterReplication(ack, pa, testRecord("ack-fail"))

	select {
	case err := <-ack:
		if err == nil {
			t.Fatal("expected non-nil ack error")
		}
		if !strings.Contains(err.Error(), "replication failed") {
			t.Errorf("expected error to contain 'replication failed', got %q", err.Error())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ack")
	}
}

// ================================================================
// HIGH-VOLUME STRESS TESTS
// ================================================================

// TestImportToTierReplacesIncompleteForwardedChunk verifies that ImportToVault
// replaces a forwarded chunk that has fewer records (simulating fire-and-forget
// drops) with the canonical version containing all records.
func TestImportToTierReplacesIncompleteForwardedChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, true, nil) // follower receives forwarded + canonical
	vault := NewVault(vaultID, tier)
	vault.Name = "incomplete-forward"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate fire-and-forget forwarding: only 70 of 100 records arrive.
	tier.Chunks.SetNextChunkID(chunkID)
	for i := 0; i < 70; i++ {
		if _, _, err := tier.Chunks.Append(testRecord("forwarded")); err != nil {
			t.Fatal(err)
		}
	}
	// Seal the incomplete forwarded chunk.
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	meta, err := tier.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("expected forwarded chunk to exist: %v", err)
	}
	if meta.RecordCount != 70 {
		t.Fatalf("expected 70 forwarded records, got %d", meta.RecordCount)
	}

	// ImportToVault with canonical version: all 100 records.
	err = orch.ImportToVault(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(100)))
	if err != nil {
		t.Fatalf("ImportToVault: %v", err)
	}

	// Verify: chunk now has 100 records (canonical replaced incomplete).
	meta, err = tier.Chunks.Meta(chunkID)
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
	metas, _ := tier.Chunks.List()
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

// TestTransitionLocalPreservesAllRecords verifies zero record loss when
// transitioning a large sealed chunk from tier 0 to tier 1. The 5000 records
// may span multiple chunks in the destination tier due to rotation policy.

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

// failingForwarder is a ChunkReplicator that records AppendRecords calls and
// returns configurable errors. Used to verify fire-and-forget error handling
// on the replication path.
type failingForwarder struct {
	mu        sync.Mutex
	calls     int
	returnErr error
}

func (f *failingForwarder) AppendRecords(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.returnErr
}

func (f *failingForwarder) SealVault(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

func (f *failingForwarder) ImportSealedChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return nil
}

func (f *failingForwarder) RequestReplicaCatchup(_ context.Context, _ string, _, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

func (f *failingForwarder) DeleteChunk(_ context.Context, _ string, _, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}

func (f *failingForwarder) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestAppendToTierForwardingDoesNotBlockOnFullChannel verifies fire-and-forget
// semantics: AppendToVault commits the record locally and succeeds even when
// the forwarder returns errors. The local append must not be rolled back, and
// high-volume ingestion (exceeding typical queue capacity) must complete
// without error regardless of forwarder failures.
func TestAppendToTierForwardingDoesNotBlockOnFullChannel(t *testing.T) {
	t.Parallel()

	fwd := &failingForwarder{
		returnErr: errors.New("simulated network partition"),
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetChunkReplicator(fwd)

	tierID := glid.New()
	vaultID := glid.New()
	tier := newMemTier(t, tierID, false, []system.ReplicationTarget{{NodeID: "node-2"}, {NodeID: "node-3"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "non-blocking"
	orch.RegisterVault(vault)

	// Append 200 records — well above typical queue capacity.
	// Every forwarder call fails, but AppendToVault must still succeed.
	const total = 200
	for i := 0; i < total; i++ {
		if err := orch.AppendToVault(vaultID, tierID, chunk.ChunkID{}, testRecord("burst")); err != nil {
			t.Fatalf("AppendToVault %d: %v", i, err)
		}
	}

	// Verify all records committed locally despite forwarder failures.
	active := tier.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk after appends")
	}
	if active.RecordCount != total {
		t.Errorf("expected %d records in active chunk, got %d", total, active.RecordCount)
	}

	// The circuit breaker stops forwarding after consecutive failures,
	// so we expect at least 1 call per follower (to detect the failure)
	// but not necessarily all 400. The important thing: local records
	// are committed and the forwarder was attempted.
	if got := fwd.callCount(); got < 2 {
		t.Errorf("expected at least 2 AppendRecords calls (one per follower), got %d", got)
	}
}
