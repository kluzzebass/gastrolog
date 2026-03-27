package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"log/slog"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

func newMemTier(t *testing.T, tierID uuid.UUID, isSecondary bool, secondaries []string) *TierInstance {
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
		IsSecondary:      isSecondary,
		SecondaryNodeIDs: secondaries,
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "import-id"
	orch.RegisterVault(vault)

	targetID := chunk.NewChunkID()
	err = orch.ImportToTier(context.Background(), vaultID, tierID, targetID, testIter(smallRecords(5)))
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

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

func TestListAllChunkMetasIncludesAllTiers(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

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

func TestLocalPrimaryTierIDsExcludesSecondaries(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	primaryTierID := uuid.Must(uuid.NewV7())
	secondaryTierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	primary := newMemTier(t, primaryTierID, false, nil)
	secondary := newMemTier(t, secondaryTierID, true, nil)
	vault := NewVault(vaultID, primary, secondary)
	vault.Name = "mixed-roles"
	orch.RegisterVault(vault)

	ids := orch.LocalPrimaryTierIDs()
	if !ids[primaryTierID] {
		t.Error("primary tier should be in LocalPrimaryTierIDs")
	}
	if ids[secondaryTierID] {
		t.Error("secondary tier should NOT be in LocalPrimaryTierIDs")
	}
}

// --- tierReplicationInfo ---

func TestTierReplicationInfoSkipsSecondaries(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	primaryTierID := uuid.Must(uuid.NewV7())
	secondaryTierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	primary := newMemTier(t, primaryTierID, false, []string{"node-2"})
	secondary := newMemTier(t, secondaryTierID, true, nil)
	vault := NewVault(vaultID, primary, secondary)
	vault.Name = "repl-info"
	orch.RegisterVault(vault)

	// Primary tier should return replication info.
	tid, nodes := orch.tierReplicationInfo(vaultID, primary.Chunks)
	if tid != primaryTierID {
		t.Errorf("expected tier %s, got %s", primaryTierID, tid)
	}
	if len(nodes) != 1 || nodes[0] != "node-2" {
		t.Errorf("expected [node-2], got %v", nodes)
	}

	// Secondary tier should return nothing.
	tid2, nodes2 := orch.tierReplicationInfo(vaultID, secondary.Chunks)
	if tid2 != (uuid.UUID{}) {
		t.Errorf("expected zero tier ID for secondary, got %s", tid2)
	}
	if len(nodes2) != 0 {
		t.Errorf("expected no nodes for secondary, got %v", nodes2)
	}
}

// --- Retention action from position ---

func TestRetentionActionDerivedFromPosition(t *testing.T) {
	t.Parallel()

	tier1ID := uuid.Must(uuid.NewV7())
	tier2ID := uuid.Must(uuid.NewV7())
	tier3ID := uuid.Must(uuid.NewV7())
	policyID := uuid.Must(uuid.NewV7())

	vaultCfg := config.VaultConfig{
		ID:      uuid.Must(uuid.NewV7()),
		TierIDs: []uuid.UUID{tier1ID, tier2ID, tier3ID},
	}

	cfg := &config.Config{
		RetentionPolicies: []config.RetentionPolicyConfig{
			{ID: policyID, MaxAge: func() *string { s := "1s"; return &s }()},
		},
	}

	// Tier at position 0 (not last) — should be transition.
	tier1Cfg := &config.TierConfig{
		ID: tier1ID,
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
		ID: tier3ID,
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

// --- Retention secondary forces expire ---

func TestRetentionSecondaryForcesExpire(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "secondary-expire"
	orch.RegisterVault(vault)

	// Append and seal a chunk.
	if _, _, err := tier.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier.Chunks.List()
	if len(metas) == 0 {
		t.Fatal("expected sealed chunk")
	}

	runner := &retentionRunner{
		vaultID: vaultID,
		tierID:  tierID,
		cm:      tier.Chunks,
		im:      tier.Indexes,
		rules: []retentionRule{
			{
				// RetentionPolicyFunc that matches all sealed chunks.
				policy: chunk.RetentionPolicyFunc(func(state chunk.VaultState) []chunk.ChunkID {
					var ids []chunk.ChunkID
					for _, c := range state.Chunks {
						ids = append(ids, c.ID)
					}
					return ids
				}),
				action: config.RetentionActionTransition,
			},
		},
		orch: orch,
		now:  time.Now,
		logger: func() *slog.Logger {
			return slog.Default()
		}(),
	}
	runner.isSecondary.Store(true)

	runner.sweep()

	// Chunk should be deleted (expire), not transitioned.
	metasAfter, _ := tier.Chunks.List()
	if len(metasAfter) != 0 {
		t.Errorf("expected chunk to be deleted by secondary expire, got %d chunks", len(metasAfter))
	}
}

// --- Transition guards secondary ---

func TestTransitionChunkGuardsSecondary(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "guard-secondary"
	orch.RegisterVault(vault)

	if _, _, err := tier.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier.Chunks.List()
	chunkID := metas[0].ID

	runner := &retentionRunner{
		vaultID: vaultID,
		tierID:  tierID,
		cm:      tier.Chunks,
		im:      tier.Indexes,
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}
	runner.isSecondary.Store(true)

	// Directly call transitionChunk — it should fall back to expire.
	runner.transitionChunk(chunkID)

	metasAfter, _ := tier.Chunks.List()
	if len(metasAfter) != 0 {
		t.Errorf("expected chunk deleted by guard fallback, got %d chunks", len(metasAfter))
	}
}

// --- Import idempotency ---

func TestImportToTierIdempotent(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "idempotent"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// First import — should succeed.
	err = orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
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

func TestAppendToTierPrimaryForwardsToSecondaries(t *testing.T) {
	t.Parallel()
	fwd := &tierTestForwarder{}
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []string{"node-2", "node-3"})
	vault := NewVault(vaultID, tier)
	vault.Name = "fwd-test"
	orch.RegisterVault(vault)

	rec := testRecord("hello")
	if err := orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
		t.Fatal(err)
	}

	calls := fwd.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 ForwardToTier calls (one per secondary), got %d", len(calls))
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
	orch, err := New(Config{LocalNodeID: "node-2"})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	// Secondary tier — should NOT re-forward.
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "no-reforward"
	orch.RegisterVault(vault)

	primaryChunkID := chunk.NewChunkID()
	if err := orch.AppendToTier(vaultID, tierID, primaryChunkID, testRecord("data")); err != nil {
		t.Fatal(err)
	}

	if len(fwd.getCalls()) != 0 {
		t.Error("secondary should NOT forward to other nodes (prevents loops)")
	}
}

func TestAppendToTierSecondaryUsesChunkID(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-2"})
	if err != nil {
		t.Fatal(err)
	}

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

	// The secondary's active chunk should have the primary's chunk ID.
	active := tier.Chunks.Active()
	if active == nil {
		t.Fatal("expected active chunk on secondary")
	}
	if active.ID != primaryChunkID {
		t.Errorf("secondary chunk ID = %s, want primary's %s", active.ID, primaryChunkID)
	}
}

func TestAppendToTierSecondarySkipsPostSeal(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-2"})
	if err != nil {
		t.Fatal(err)
	}

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
		IsSecondary: true,
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

	// If post-seal were scheduled on a secondary, it would queue compression
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

// --- Import keeps forwarded version on secondary (no delete-and-replace) ---

func TestImportToTierSecondarySealsActiveAndKeeps(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-2"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "seal-and-keep"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate active record forwarding: secondary has an active chunk
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
	err = orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, true, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "keep-sealed"
	orch.RegisterVault(vault)

	chunkID := chunk.NewChunkID()

	// Simulate: forwarded version is already sealed (e.g., secondary
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
	err = orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, testIter(smallRecords(5)))
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	// No forwarder set — single-node mode.

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []string{"node-2"})
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	bogusVaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())

	err = orch.AppendToTier(bogusVaultID, tierID, chunk.ChunkID{}, testRecord("data"))
	if err == nil {
		t.Fatal("expected error for non-existent vault")
	}
	if !errors.Is(err, ErrVaultNotFound) {
		t.Errorf("expected ErrVaultNotFound, got %v", err)
	}
}

func TestAppendToTierTierNotFound(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, nil)
	vault := NewVault(vaultID, tier)
	vault.Name = "tier-not-found"
	orch.RegisterVault(vault)

	bogusTierID := uuid.Must(uuid.NewV7())
	err = orch.AppendToTier(vaultID, bogusTierID, chunk.ChunkID{}, testRecord("data"))
	if err == nil {
		t.Fatal("expected error for non-existent tier")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error containing 'not found', got %v", err)
	}
}

func TestImportToTierDrainsIteratorOnSkip(t *testing.T) {
	t.Parallel()
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}

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

	err = orch.ImportToTier(context.Background(), vaultID, tierID, chunkID, trackingIter)
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []string{"node-2"})
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
func (m *ackTestTransferrer) ReplicateSealedChunk(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (m *ackTestTransferrer) StreamToTier(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}

func TestAppendRecordWaitForReplicaReturnsTask(t *testing.T) {
	t.Parallel()
	fwd := &tierTestForwarder{}
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []string{"node-2"})
	vault := NewVault(vaultID, tier)
	vault.Name = "ack-gated"
	orch.RegisterVault(vault)

	rec := testRecord("ack-me")
	rec.WaitForReplica = true

	orch.mu.RLock()
	_, _, task, err := orch.appendRecord(vaultID, rec)
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
	if len(task.secondaries) != 1 || task.secondaries[0] != "node-2" {
		t.Errorf("task.secondaries = %v, want [node-2]", task.secondaries)
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []string{"node-2"})
	vault := NewVault(vaultID, tier)
	vault.Name = "no-ack"
	orch.RegisterVault(vault)

	rec := testRecord("fire-and-forget")
	rec.WaitForReplica = false

	orch.mu.RLock()
	_, _, task, err := orch.appendRecord(vaultID, rec)
	orch.mu.RUnlock()

	if err != nil {
		t.Fatalf("appendRecord: %v", err)
	}
	if task != nil {
		t.Error("expected nil replicationTask for WaitForReplica=false")
	}

	// Fire-and-forget MUST have been called.
	calls := fwd.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 fire-and-forget forward call, got %d", len(calls))
	}
	if calls[0].NodeID != "node-2" {
		t.Errorf("forward call nodeID = %s, want node-2", calls[0].NodeID)
	}
}

func TestIngestReturnsReplicationTasks(t *testing.T) {
	t.Parallel()
	fwd := &tierTestForwarder{}
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.SetRecordForwarder(fwd)

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())
	tier := newMemTier(t, tierID, false, []string{"node-2"})
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.transferrer = mock

	tasks := []replicationTask{
		{
			vaultID:     uuid.Must(uuid.NewV7()),
			tierID:      uuid.Must(uuid.NewV7()),
			chunkID:     chunk.NewChunkID(),
			secondaries: []string{"node-2"},
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
	orch, err := New(Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	orch.transferrer = mock

	tasks := []replicationTask{
		{
			vaultID:     uuid.Must(uuid.NewV7()),
			tierID:      uuid.Must(uuid.NewV7()),
			chunkID:     chunk.NewChunkID(),
			secondaries: []string{"node-2"},
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

