package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

func newMemTier(t *testing.T, tierID uuid.UUID, isFollower bool, followers []config.ReplicationTarget) *TierInstance {
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
	return &TierInstance{
		TierID:           tierID,
		Type:             "memory",
		Chunks:           cm,
		Indexes:          im,
		Query:            query.New(cm, im, nil),
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

// --- ImportToTier ---

func TestImportToTierPreservesChunkID(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "import-id"
	orch.RegisterVault(vault)

	targetID := chunk.NewChunkID()
	err := orch.ImportToTier(context.Background(), vaultID, tierID, targetID, testIter(smallRecords(5)))
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

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
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
			errs[idx] = orch.ImportToTier(context.Background(), vaultID, tierID, ids[idx], testIter(smallRecords(3)))
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

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	tier := newMemTier(t, tierID, false, nil)
	// Simulate the follower scenario: the FSM has CloudBacked=true (because
	// some other node — the leader — uploaded the blob) but the local chunk
	// manager has no CloudStore so its local meta reports CloudBacked=false.
	// The OverlayFromFSM callback closes the gap.
	tier.OverlayFromFSM = func(m chunk.ChunkMeta) chunk.ChunkMeta {
		m.CloudBacked = true
		m.Archived = true
		m.NumFrames = 7
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

	// The overlaid view from ListAllChunkMetas should have CloudBacked=true,
	// Archived=true, NumFrames=7 — the cluster-wide truth from the FSM.
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
	if got.NumFrames != 7 {
		t.Errorf("NumFrames not overlaid from FSM: got %d, want 7", got.NumFrames)
	}

	// GetChunkMeta should also apply the overlay.
	chunkID := got.ID
	single, err := orch.GetChunkMeta(vaultID, chunkID)
	if err != nil {
		t.Fatalf("GetChunkMeta: %v", err)
	}
	if !single.CloudBacked || !single.Archived || single.NumFrames != 7 {
		t.Errorf("GetChunkMeta did not apply overlay: %+v", single)
	}
}

// TestListAllChunkMetasNilOverlayPassthrough verifies that tiers without an
// OverlayFromFSM callback (single-node mode, memory tiers) pass the local
// chunk manager's view through unchanged. The overlay is opt-in.
func TestListAllChunkMetasNilOverlayPassthrough(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

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

func TestListAllChunkMetasIncludesAllTiers(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tier1ID := uuid.Must(uuid.NewV7())
	tier2ID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	tier1 := newMemTier(t, tier1ID, false, nil)
	tier2 := newMemTier(t, tier2ID, false, nil)
	vault := NewVault(vaultID, tier1, tier2)
	vault.Name = "multi-tier"
	orch.RegisterVault(vault)

	// Append and seal in each tier.
	if _, _, err := tier1.Chunks.Append(testRecord("t1")); err != nil {
		t.Fatal(err)
	}
	if err := tier1.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tier2.Chunks.Append(testRecord("t2")); err != nil {
		t.Fatal(err)
	}
	if err := tier2.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, err := orch.ListAllChunkMetas(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(metas))
	}

	tierIDs := map[uuid.UUID]bool{}
	for _, m := range metas {
		tierIDs[m.TierID] = true
	}
	if !tierIDs[tier1ID] {
		t.Error("missing tier1 chunk")
	}
	if !tierIDs[tier2ID] {
		t.Error("missing tier2 chunk")
	}
}

// --- LocalPrimaryTierIDs ---

func TestLocalPrimaryTierIDsExcludesFollowers(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	primaryTierID := uuid.Must(uuid.NewV7())
	followerTierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	primary := newMemTier(t, primaryTierID, false, nil)
	follower := newMemTier(t, followerTierID, true, nil)
	vault := NewVault(vaultID, primary, follower)
	vault.Name = "mixed-roles"
	orch.RegisterVault(vault)

	ids := orch.LocalPrimaryTierIDs()
	if !ids[primaryTierID] {
		t.Error("primary tier should be in LocalPrimaryTierIDs")
	}
	if ids[followerTierID] {
		t.Error("follower tier should NOT be in LocalPrimaryTierIDs")
	}
}

// --- tierReplicationInfo ---

func TestTierReplicationInfoSkipsFollowers(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	primaryTierID := uuid.Must(uuid.NewV7())
	followerTierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	primary := newMemTier(t, primaryTierID, false, []config.ReplicationTarget{{NodeID: "node-2"}})
	follower := newMemTier(t, followerTierID, true, nil)
	vault := NewVault(vaultID, primary, follower)
	vault.Name = "repl-info"
	orch.RegisterVault(vault)

	// Primary tier should return replication info.
	tid, nodes := orch.tierReplicationInfo(vaultID, primary.Chunks)
	if tid != primaryTierID {
		t.Errorf("expected tier %s, got %s", primaryTierID, tid)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "node-2" {
		t.Errorf("expected [node-2], got %v", nodes)
	}

	// Follower tier should return nothing.
	tid2, nodes2 := orch.tierReplicationInfo(vaultID, follower.Chunks)
	if tid2 != (uuid.UUID{}) {
		t.Errorf("expected zero tier ID for follower, got %s", tid2)
	}
	if len(nodes2) != 0 {
		t.Errorf("expected no nodes for follower, got %v", nodes2)
	}
}

// --- Retention action from position ---

func TestRetentionActionDerivedFromPosition(t *testing.T) {
	t.Parallel()

	tier1ID := uuid.Must(uuid.NewV7())
	tier2ID := uuid.Must(uuid.NewV7())
	tier3ID := uuid.Must(uuid.NewV7())
	policyID := uuid.Must(uuid.NewV7())

	vaultID := uuid.Must(uuid.NewV7())
	vaultCfg := config.VaultConfig{
		ID: vaultID,
	}

	cfg := &config.Config{
		RetentionPolicies: []config.RetentionPolicyConfig{
			{ID: policyID, MaxAge: func() *string { s := "1s"; return &s }()},
		},
		Tiers: []config.TierConfig{
			{ID: tier1ID, VaultID: vaultID, Position: 0},
			{ID: tier2ID, VaultID: vaultID, Position: 1},
			{ID: tier3ID, VaultID: vaultID, Position: 2},
		},
	}

	// Tier at position 0 (not last) — should be transition.
	tier1Cfg := &config.TierConfig{
		ID: tier1ID, VaultID: vaultID, Position: 0,
		RetentionRules: []config.RetentionRule{
			{RetentionPolicyID: policyID, Action: config.RetentionActionExpire}, // stored as expire
		},
	}
	rules1, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tier1Cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules1) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules1))
	}
	if rules1[0].action != config.RetentionActionTransition {
		t.Errorf("position 0 of 3: expected transition, got %v", rules1[0].action)
	}

	// Tier at position 2 (last) — should be expire.
	tier3Cfg := &config.TierConfig{
		ID: tier3ID, VaultID: vaultID, Position: 2,
		RetentionRules: []config.RetentionRule{
			{RetentionPolicyID: policyID, Action: config.RetentionActionTransition}, // stored as transition
		},
	}
	rules3, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tier3Cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules3) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules3))
	}
	if rules3[0].action != config.RetentionActionExpire {
		t.Errorf("position 2 of 3: expected expire, got %v", rules3[0].action)
	}
}

// --- Import idempotency ---

func TestImportToTierIdempotent(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "idempotent"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// First import — should succeed.
	err := orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatal(err)
	}

	// Second import with same chunk ID — idempotent skip (chunk already exists).
	err = orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(3)))
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

// --- AppendToTier ---

// tierTestForwarder records ForwardToTier calls.
type tierTestForwarder struct {
	mu    sync.Mutex
	calls []tierForwardCall
}

type tierForwardCall struct {
	NodeID  string
	VaultID uuid.UUID
	TierID  uuid.UUID
	ChunkID chunk.ChunkID
	Records []chunk.Record
}

func (f *tierTestForwarder) Forward(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}

func (f *tierTestForwarder) ForwardToTier(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, records []chunk.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, tierForwardCall{
		NodeID: nodeID, VaultID: vaultID, TierID: tierID,
		ChunkID: chunkID, Records: records,
	})
	return nil
}

func (f *tierTestForwarder) getCalls() []tierForwardCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tierForwardCall(nil), f.calls...)
}

func TestAppendToTierLeaderForwardsToFollowers(t *testing.T) {
	t.Parallel()
	fwd := &tierTestForwarder{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []config.ReplicationTarget{{NodeID: "node-2"}, {NodeID: "node-3"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "fwd-test"
	orch.RegisterVault(vault)

	rec := testRecord("hello")
	if err := orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
		t.Fatal(err)
	}

	calls := fwd.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 ForwardToTier calls (one per follower), got %d", len(calls))
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
	fwd := &tierTestForwarder{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	// Follower tier — should NOT re-forward.
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "no-reforward"
	orch.RegisterVault(vault)

	primaryChunkID := chunk.NewChunkID()
	if err := orch.AppendToTier(vaultID, tierID, primaryChunkID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	if len(fwd.getCalls()) != 0 {
		t.Error("follower should NOT forward to other nodes (prevents loops)")
	}
}

func TestAppendToTierSecondaryUsesChunkID(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "id-sync"
	orch.RegisterVault(vault)

	primaryChunkID := chunk.NewChunkID()
	if err := orch.AppendToTier(vaultID, tierID, primaryChunkID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	// The follower's active chunk should have the leader's chunk ID.
	active := tier.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk on follower")
	}
	if active.ID != primaryChunkID {
		t.Errorf("follower chunk ID = %s, want leader's %s", active.ID, primaryChunkID)
	}
}

func TestAppendToTierSecondarySkipsPostSeal(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
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
	tier := &TierInstance{
		TierID:      tierID,
		Type:        "memory",
		Chunks:      cm,
		Indexes:     im,
		Query:       query.New(cm, im, nil),
		IsFollower: true,
	}
	vault := NewVault(vaultID, tier)
	vault.Name = "skip-postseal"
	orch.RegisterVault(vault)

	primaryChunkID := chunk.NewChunkID()
	// First record fills the chunk (policy = 1 record), triggering seal on the second.
	if err := orch.AppendToTier(vaultID, tierID, primaryChunkID, testRecord("rec-1")); err != nil {
		t.Fatal(err)
	}
	if err := orch.AppendToTier(vaultID, tierID, primaryChunkID, testRecord("rec-2")); err != nil {
		t.Fatal(err)
	}

	// If post-seal were scheduled on a follower, it would queue compression
	// work that races with ImportToTier's delete-and-replace. The test just
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

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "seal-and-keep"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate active record forwarding: follower has an active chunk
	// with the primary's ID, still receiving records.
	tier.Chunks.SetNextChunkID(chunkID)
	for range 3 {
		if _, _, err := tier.Chunks.Append(testRecord("forwarded")); err != nil {
			t.Fatal(err)
		}
	}
	active := tier.Chunks.Active()
	if active == nil || active.ID != chunkID {
		t.Fatal("expected active chunk with primary's ID")
	}

	// Primary seals and sends canonical version. ImportToTier should
	// seal the active chunk and keep it (no delete-and-replace).
	err := orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatalf("ImportToTier: %v", err)
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

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
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

	// ImportToTier should replace the forwarded version with canonical.
	err := orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
	if err != nil {
		t.Fatalf("ImportToTier: %v", err)
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

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []config.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "no-forwarder"
	orch.RegisterVault(vault)

	rec := testRecord("single-node")
	if err := orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
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

	bogusVaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())

	err := orch.AppendToTier(bogusVaultID, tierID, chunk.ChunkID{}, testRecord("data"))
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

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "tier-not-found"
	orch.RegisterVault(vault)

	bogusTierID := uuid.Must(uuid.NewV7())
	err := orch.AppendToTier(vaultID, bogusTierID, chunk.ChunkID{}, testRecord("data"))
	if err == nil {
		t.Fatal("expected error for non-existent tier")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error containing 'not found', got %v", err)
	}
}

func TestImportToTierDrainsIteratorOnSkip(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "drain-on-skip"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Pre-populate a sealed chunk with this ID so ImportToTier will skip.
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

	err := orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, trackingIter)
	if err != nil {
		t.Fatalf("ImportToTier: %v", err)
	}

	if consumed != totalRecords {
		t.Errorf("expected all %d records consumed (drained), got %d", totalRecords, consumed)
	}
}

func TestAppendToTierForwardLifecycle(t *testing.T) {
	t.Parallel()
	fwd := &tierTestForwarder{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []config.ReplicationTarget{{NodeID: "node-2"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "forward-lifecycle"
	orch.RegisterVault(vault)

	// Append 3 records.
	for i := range 3 {
		rec := testRecord("rec-" + string(rune('a'+i)))
		if err := orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Verify 3 ForwardToTier calls.
	calls := fwd.getCalls()
	if len(calls) != 3 {
		t.Fatalf("expected 3 ForwardToTier calls, got %d", len(calls))
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

// ackTestTransferrer records ForwardTierAppend calls and returns a configurable error.
type ackTestTransferrer struct {
	tierAppendCalls int
	tierAppendErr   error
}

func (m *ackTestTransferrer) TransferRecords(_ context.Context, _ string, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}
func (m *ackTestTransferrer) ForwardAppend(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (m *ackTestTransferrer) ForwardTierAppend(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ []chunk.Record) error {
	m.tierAppendCalls++
	return m.tierAppendErr
}
func (m *ackTestTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil
}
func (m *ackTestTransferrer) ForwardSealTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (m *ackTestTransferrer) ForwardDeleteChunk(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (m *ackTestTransferrer) ReplicateSealedChunk(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (m *ackTestTransferrer) StreamToTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}
func TestAppendRecordWaitForReplicaReturnsTask(t *testing.T) {
	t.Parallel()
	fwd := &tierTestForwarder{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []config.ReplicationTarget{{NodeID: "node-2"}})
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
	fwd := &tierTestForwarder{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []config.ReplicationTarget{{NodeID: "node-2"}})
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
	fwd := &tierTestForwarder{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []config.ReplicationTarget{{NodeID: "node-2"}})
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

	tasks, err := orch.ingest(rec)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("expected non-empty replication tasks for WaitForReplica=true")
	}
	if tasks[0].vaultID != vaultID {
		t.Errorf("task[0].vaultID = %s, want %s", tasks[0].vaultID, vaultID)
	}
}

func TestAckAfterReplicationSuccess(t *testing.T) {
	t.Parallel()
	mock := &ackTestTransferrer{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.transferrer = mock

	tasks := []replicationTask{
		{
			vaultID: uuid.Must(uuid.NewV7()),
			tierID:  uuid.Must(uuid.NewV7()),
			chunkID: chunk.NewChunkID(),
			targets: []config.ReplicationTarget{{NodeID: "node-2"}},
		},
	}

	ack := make(chan error, 1)
	orch.ackAfterReplication(ack, tasks, testRecord("ack-ok"))

	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("expected nil ack error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ack")
	}

	if mock.tierAppendCalls != 1 {
		t.Errorf("expected 1 ForwardTierAppend call, got %d", mock.tierAppendCalls)
	}
}

func TestAckAfterReplicationFailure(t *testing.T) {
	t.Parallel()
	mock := &ackTestTransferrer{
		tierAppendErr: errors.New("replication failed"),
	}
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.transferrer = mock

	tasks := []replicationTask{
		{
			vaultID: uuid.Must(uuid.NewV7()),
			tierID:  uuid.Must(uuid.NewV7()),
			chunkID: chunk.NewChunkID(),
			targets: []config.ReplicationTarget{{NodeID: "node-2"}},
		},
	}

	ack := make(chan error, 1)
	orch.ackAfterReplication(ack, tasks, testRecord("ack-fail"))

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

// TestImportToTierReplacesIncompleteForwardedChunk verifies that ImportToTier
// replaces a forwarded chunk that has fewer records (simulating fire-and-forget
// drops) with the canonical version containing all records.
func TestImportToTierReplacesIncompleteForwardedChunk(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-2"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
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

	// ImportToTier with canonical version: all 100 records.
	err = orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(100)))
	if err != nil {
		t.Fatalf("ImportToTier: %v", err)
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
func TestTransitionLocalPreservesAllRecords(t *testing.T) {
	t.Parallel()
	const totalRecords = 5000

	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	// tier 0: large rotation policy so all 5000 fit in one chunk.
	tier0cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(totalRecords + 1),
		Now:            time.Now,
		MetaStore:      chunkmem.NewMetaStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	tier0im, _ := indexmem.NewFactory()(nil, tier0cm, nil)
	tier0 := &TierInstance{
		TierID:  tier0ID,
		Type:    "memory",
		Chunks:  tier0cm,
		Indexes: tier0im,
		Query:   query.New(tier0cm, tier0im, nil),
	}

	// tier 1: small rotation policy (500 records) — forces multiple chunks.
	tier1cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(500),
		Now:            time.Now,
		MetaStore:      chunkmem.NewMetaStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	tier1im, _ := indexmem.NewFactory()(nil, tier1cm, nil)
	tier1 := &TierInstance{
		TierID:  tier1ID,
		Type:    "memory",
		Chunks:  tier1cm,
		Indexes: tier1im,
		Query:   query.New(tier1cm, tier1im, nil),
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "stress-transition"
	orch.RegisterVault(vault)

	// Set up config loader.
	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "stress-transition",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory,
		VaultID: vaultID, Position: 0,
		Placements: []config.TierPlacement{{StorageID: config.SyntheticStorageID(nodeID), Leader: true}},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeMemory,
		VaultID: vaultID, Position: 1,
		Placements: []config.TierPlacement{{StorageID: config.SyntheticStorageID(nodeID), Leader: true}},
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Append 5000 records to tier 0.
	for i := 0; i < totalRecords; i++ {
		if _, _, err := tier0cm.Append(testRecord("bulk")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier0cm.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0cm.List()
	if len(metas) == 0 {
		t.Fatal("expected sealed chunk in tier 0")
	}
	chunkID := metas[0].ID

	// Run transition.
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0cm, tier0im)
	runner.transitionChunk(chunkID)

	// Verify: source chunk deleted.
	metasAfter, _ := tier0cm.List()
	for _, m := range metasAfter {
		if m.ID == chunkID {
			t.Error("expected source chunk to be deleted from tier 0")
		}
	}

	// Count ALL records in tier 1 (may span multiple chunks due to rotation).
	tier1Metas, _ := tier1cm.List()
	var total int64
	for _, m := range tier1Metas {
		total += m.RecordCount
	}
	// Also check active chunk if not in the list.
	active := tier1cm.Active()
	if active != nil {
		listed := false
		for _, m := range tier1Metas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			total += active.RecordCount
		}
	}
	if total != totalRecords {
		t.Errorf("expected %d records in tier 1, got %d (zero-loss requirement violated)", totalRecords, total)
	}
}

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
func TestTransitionLocalCursorErrorRetainsSource(t *testing.T) {
	t.Parallel()

	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	tier0 := newMemTier(t, tier0ID, false, nil)
	tier1 := newMemTier(t, tier1ID, false, nil)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "cursor-error"
	orch.RegisterVault(vault)

	// Build 50 test records for the cursor.
	recs := make([]chunk.Record, 50)
	for i := range recs {
		recs[i] = testRecord("cursor-rec")
	}

	readErr := errors.New("simulated disk I/O error")
	cursor := &errorCursor{
		records: recs,
		err:     readErr,
	}

	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	streamErr := runner.streamLocal(cursor, tier1ID)

	if streamErr == nil {
		t.Fatal("expected streamLocal to return an error for non-ErrNoMoreRecords cursor failure")
	}
	if !errors.Is(streamErr, readErr) {
		t.Errorf("expected error to wrap %q, got %q", readErr, streamErr)
	}

	// Verify the 50 records that were read successfully made it to tier 1.
	active := tier1.Chunks.Active()
	if active == nil {
		t.Fatal("expected records in tier 1 from partial read")
	}
	if active.RecordCount != 50 {
		t.Errorf("expected 50 records in tier 1 from partial read, got %d", active.RecordCount)
	}
}

// failingForwarder is a RecordForwarder that records calls and returns
// configurable errors. Used to verify fire-and-forget error handling.
type failingForwarder struct {
	mu        sync.Mutex
	calls     int
	returnErr error
}

func (f *failingForwarder) Forward(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}

func (f *failingForwarder) ForwardToTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID, _ []chunk.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.returnErr
}

func (f *failingForwarder) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestAppendToTierForwardingDoesNotBlockOnFullChannel verifies fire-and-forget
// semantics: AppendToTier commits the record locally and succeeds even when
// the forwarder returns errors. The local append must not be rolled back, and
// high-volume ingestion (exceeding typical queue capacity) must complete
// without error regardless of forwarder failures.
func TestAppendToTierForwardingDoesNotBlockOnFullChannel(t *testing.T) {
	t.Parallel()

	fwd := &failingForwarder{
		returnErr: errors.New("simulated network partition"),
	}

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []config.ReplicationTarget{{NodeID: "node-2"}, {NodeID: "node-3"}})
	vault := NewVault(vaultID, tier)
	vault.Name = "non-blocking"
	orch.RegisterVault(vault)

	// Append 200 records — well above typical queue capacity.
	// Every forwarder call fails, but AppendToTier must still succeed.
	const total = 200
	for i := 0; i < total; i++ {
		if err := orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, testRecord("burst")); err != nil {
			t.Fatalf("AppendToTier %d: %v", i, err)
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

	// Verify the forwarder was called for each record to each follower.
	// 200 records * 2 secondaries = 400 calls.
	expectedCalls := total * 2
	if got := fwd.callCount(); got != expectedCalls {
		t.Errorf("expected %d ForwardToTier calls (records * secondaries), got %d", expectedCalls, got)
	}
}

