package orchestrator

import (
	"context"
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

