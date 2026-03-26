package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// ---------- config loader adapter ----------

type transitionConfigLoader struct {
	store *cfgmem.Store
}

func (l *transitionConfigLoader) Load(ctx context.Context) (*config.Config, error) {
	return l.store.Load(ctx)
}

// ---------- helpers ----------

func makeRecord(raw string) chunk.Record {
	return chunk.Record{
		SourceTS: time.Now(),
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"msg": raw},
		Raw:      []byte(raw),
	}
}

func newMemoryTierInstance(t *testing.T, tierID uuid.UUID) *TierInstance {
	t.Helper()
	cm, err := chunkmem.NewFactory()(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	im, err := indexmem.NewFactory()(nil, cm, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &TierInstance{
		TierID:  tierID,
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}
}

func setupTwoTierVault(t *testing.T) (*Orchestrator, uuid.UUID, uuid.UUID, uuid.UUID, *config.Config) {
	t.Helper()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	tier0 := newMemoryTierInstance(t, tier0ID)
	tier1 := newMemoryTierInstance(t, tier1ID)

	orch, err := New(Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "test-vault"
	orch.RegisterVault(vault)

	cfg := &config.Config{
		Vaults: []config.VaultConfig{
			{ID: vaultID, Name: "test-vault", TierIDs: []uuid.UUID{tier0ID, tier1ID}},
		},
		Tiers: []config.TierConfig{
			{ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, NodeID: nodeID},
			{ID: tier1ID, Name: "warm", Type: config.TierTypeMemory, NodeID: nodeID},
		},
	}

	return orch, vaultID, tier0ID, tier1ID, cfg
}

func newTestRetentionRunner(orch *Orchestrator, vaultID, tierID uuid.UUID, cm chunk.ChunkManager, im index.IndexManager) *retentionRunner {
	return &retentionRunner{
		vaultID: vaultID,
		tierID:  tierID,
		cm:      cm,
		im:      im,
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}
}

// ---------- tests ----------

func TestTransitionSameNodeTwoTiers(t *testing.T) {
	t.Parallel()
	orch, vaultID, tier0ID, tier1ID, cfg := setupTwoTierVault(t)

	// Use a real config store so the transition can load config.
	store := cfgmem.NewStore()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(context.Background(), v)
	}
	for _, tc := range cfg.Tiers {
		_ = store.PutTier(context.Background(), tc)
	}
	orch.cfgLoader = &transitionConfigLoader{store: store}

	vault := orch.vaults[vaultID]
	tier0CM := vault.Tiers[0].Chunks

	// Ingest records into tier 0.
	for i := 0; i < 5; i++ {
		if _, _, err := tier0CM.Append(makeRecord("record-" + string(rune('A'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	// Seal the chunk.
	if err := tier0CM.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0CM.List()
	if len(metas) == 0 {
		t.Fatal("expected sealed chunk in tier 0")
	}
	chunkID := metas[0].ID

	// Run transition.
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0CM, vault.Tiers[0].Indexes)
	runner.transitionChunk(chunkID)

	// Verify: source chunk deleted.
	metasAfter, _ := tier0CM.List()
	for _, m := range metasAfter {
		if m.ID == chunkID {
			t.Error("expected source chunk to be deleted from tier 0")
		}
	}

	// Verify: records appear in tier 1.
	// Count via List() which includes the active chunk for memory managers.
	tier1CM := vault.Tiers[1].Chunks
	tier1Metas, _ := tier1CM.List()
	totalRecords := int64(0)
	for _, m := range tier1Metas {
		totalRecords += m.RecordCount
	}
	// Also check active chunk if not in the list (file managers separate active from list).
	active := tier1CM.Active()
	if active != nil {
		listed := false
		for _, m := range tier1Metas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			totalRecords += active.RecordCount
		}
	}
	if totalRecords != 5 {
		t.Errorf("expected 5 records in tier 1, got %d", totalRecords)
	}

	_ = tier1ID // used in config setup
}

func TestTransitionRecordIntegrity(t *testing.T) {
	t.Parallel()
	orch, vaultID, tier0ID, _, cfg := setupTwoTierVault(t)

	store := cfgmem.NewStore()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(context.Background(), v)
	}
	for _, tc := range cfg.Tiers {
		_ = store.PutTier(context.Background(), tc)
	}
	orch.cfgLoader = &transitionConfigLoader{store: store}

	vault := orch.vaults[vaultID]
	tier0CM := vault.Tiers[0].Chunks

	original := makeRecord("integrity-check")
	if _, _, err := tier0CM.Append(original); err != nil {
		t.Fatal(err)
	}
	if err := tier0CM.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0CM.List()
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0CM, vault.Tiers[0].Indexes)
	runner.transitionChunk(metas[0].ID)

	// Read the record from tier 1.
	tier1CM := vault.Tiers[1].Chunks
	active := tier1CM.Active()
	if active == nil || active.RecordCount != 1 {
		t.Fatal("expected 1 record in tier 1 active chunk")
	}
	cursor, err := tier1CM.OpenCursor(active.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cursor.Close() }()

	rec, _, err := cursor.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Raw) != "integrity-check" {
		t.Errorf("expected raw 'integrity-check', got %q", string(rec.Raw))
	}
	if rec.Attrs["msg"] != "integrity-check" {
		t.Errorf("expected attr msg='integrity-check', got %q", rec.Attrs["msg"])
	}
}

func TestTransitionTerminalTier(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	tier := newMemoryTierInstance(t, tierID)
	orch, err := New(Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, tier) // single tier = terminal
	vault.Name = "terminal"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "terminal", TierIDs: []uuid.UUID{tierID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tierID, Name: "only", Type: config.TierTypeMemory, NodeID: nodeID,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest and seal.
	if _, _, err := tier.Chunks.Append(makeRecord("terminal")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier.Chunks.List()
	chunkID := metas[0].ID

	runner := newTestRetentionRunner(orch, vaultID, tierID, tier.Chunks, tier.Indexes)
	runner.transitionChunk(chunkID)

	// Chunk should NOT be deleted — terminal tier, no next tier.
	metasAfter, _ := tier.Chunks.List()
	found := false
	for _, m := range metasAfter {
		if m.ID == chunkID {
			found = true
		}
	}
	if !found {
		t.Error("expected chunk to be retained on terminal tier")
	}
}

func TestTransitionEmptyChunk(t *testing.T) {
	t.Parallel()
	orch, vaultID, tier0ID, _, cfg := setupTwoTierVault(t)

	store := cfgmem.NewStore()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(context.Background(), v)
	}
	for _, tc := range cfg.Tiers {
		_ = store.PutTier(context.Background(), tc)
	}
	orch.cfgLoader = &transitionConfigLoader{store: store}

	vault := orch.vaults[vaultID]
	tier0CM := vault.Tiers[0].Chunks

	// Seal without any records.
	if err := tier0CM.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0CM.List()
	if len(metas) == 0 {
		// Memory manager may not produce a chunk on empty seal — that's fine.
		return
	}

	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0CM, vault.Tiers[0].Indexes)
	runner.transitionChunk(metas[0].ID)

	// Source should be deleted (0 records transitioned is still a success).
	metasAfter, _ := tier0CM.List()
	for _, m := range metasAfter {
		if m.ID == metas[0].ID {
			t.Error("expected empty chunk to be deleted after transition")
		}
	}
}

func TestTransitionMultipleChunks(t *testing.T) {
	t.Parallel()
	orch, vaultID, tier0ID, _, cfg := setupTwoTierVault(t)

	store := cfgmem.NewStore()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(context.Background(), v)
	}
	for _, tc := range cfg.Tiers {
		_ = store.PutTier(context.Background(), tc)
	}
	orch.cfgLoader = &transitionConfigLoader{store: store}

	vault := orch.vaults[vaultID]
	tier0CM := vault.Tiers[0].Chunks

	// Create 3 sealed chunks.
	for c := 0; c < 3; c++ {
		for i := 0; i < 2; i++ {
			if _, _, err := tier0CM.Append(makeRecord("batch")); err != nil {
				t.Fatal(err)
			}
		}
		if err := tier0CM.Seal(); err != nil {
			t.Fatal(err)
		}
	}

	metas, _ := tier0CM.List()
	if len(metas) < 3 {
		t.Fatalf("expected 3 sealed chunks, got %d", len(metas))
	}

	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0CM, vault.Tiers[0].Indexes)
	for _, m := range metas {
		runner.transitionChunk(m.ID)
	}

	// All source chunks should be deleted.
	metasAfter, _ := tier0CM.List()
	if len(metasAfter) != 0 {
		t.Errorf("expected 0 chunks in tier 0 after transition, got %d", len(metasAfter))
	}

	// Tier 1 should have 6 records (3 chunks * 2 records).
	tier1CM := vault.Tiers[1].Chunks
	tier1Metas, _ := tier1CM.List()
	total := int64(0)
	for _, m := range tier1Metas {
		total += m.RecordCount
	}
	if total != 6 {
		t.Errorf("expected 6 records in tier 1, got %d", total)
	}
}

func TestTransitionSourceChunkDeleted(t *testing.T) {
	t.Parallel()
	orch, vaultID, tier0ID, _, cfg := setupTwoTierVault(t)

	store := cfgmem.NewStore()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(context.Background(), v)
	}
	for _, tc := range cfg.Tiers {
		_ = store.PutTier(context.Background(), tc)
	}
	orch.cfgLoader = &transitionConfigLoader{store: store}

	vault := orch.vaults[vaultID]
	tier0CM := vault.Tiers[0].Chunks

	if _, _, err := tier0CM.Append(makeRecord("deleteme")); err != nil {
		t.Fatal(err)
	}
	if err := tier0CM.Seal(); err != nil {
		t.Fatal(err)
	}

	metasBefore, _ := tier0CM.List()
	chunkID := metasBefore[0].ID

	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0CM, vault.Tiers[0].Indexes)
	runner.transitionChunk(chunkID)

	// Verify deletion.
	metasAfter, _ := tier0CM.List()
	for _, m := range metasAfter {
		if m.ID == chunkID {
			t.Fatal("source chunk should be deleted after transition")
		}
	}
}

// ---------- cross-node tests (mock transferrer) ----------

type transitionFakeTransferrer struct {
	calls   []transitionTransferCall
	failErr error
}

type transitionTransferCall struct {
	nodeID  string
	vaultID uuid.UUID
	tierID  uuid.UUID
	records []chunk.Record
}

func (m *transitionFakeTransferrer) TransferRecords(_ context.Context, _ string, _ uuid.UUID, _ chunk.RecordIterator) error {
	return nil
}
func (m *transitionFakeTransferrer) ForwardAppend(_ context.Context, _ string, _ uuid.UUID, _ []chunk.Record) error {
	return nil
}
func (m *transitionFakeTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil
}
func (m *transitionFakeTransferrer) ForwardTierAppend(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, records []chunk.Record) error {
	if m.failErr != nil {
		return m.failErr
	}
	copied := make([]chunk.Record, len(records))
	for i, r := range records {
		copied[i] = r.Copy()
	}
	m.calls = append(m.calls, transitionTransferCall{
		nodeID: nodeID, vaultID: vaultID, tierID: tierID, records: copied,
	})
	return nil
}

func TestTransitionCrossNode(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	localNode := "local-node"
	remoteNode := "remote-node"

	tier0 := newMemoryTierInstance(t, tier0ID)
	orch, err := New(Config{LocalNodeID: localNode})
	if err != nil {
		t.Fatal(err)
	}

	// Only tier 0 is local; tier 1 is on a remote node.
	vault := NewVault(vaultID, tier0)
	vault.Name = "cross-node"
	orch.RegisterVault(vault)

	mock := &transitionFakeTransferrer{}
	orch.transferrer = mock

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cross-node", TierIDs: []uuid.UUID{tier0ID, tier1ID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, NodeID: localNode,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, NodeID: remoteNode,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest and seal.
	for i := 0; i < 3; i++ {
		if _, _, err := tier0.Chunks.Append(makeRecord("remote")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0.Chunks.List()
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	runner.transitionChunk(metas[0].ID)

	// Verify ForwardTierAppend was called.
	totalRecords := 0
	for _, call := range mock.calls {
		if call.nodeID != remoteNode {
			t.Errorf("expected nodeID %q, got %q", remoteNode, call.nodeID)
		}
		if call.vaultID != vaultID {
			t.Errorf("expected vaultID %s, got %s", vaultID, call.vaultID)
		}
		if call.tierID != tier1ID {
			t.Errorf("expected tierID %s, got %s", tier1ID, call.tierID)
		}
		totalRecords += len(call.records)
	}
	if totalRecords != 3 {
		t.Errorf("expected 3 records forwarded, got %d", totalRecords)
	}

	// Source chunk should be deleted.
	metasAfter, _ := tier0.Chunks.List()
	if len(metasAfter) != 0 {
		t.Error("expected source chunk deleted after cross-node transition")
	}
}

func TestTransitionCrossNodeFailure(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	localNode := "local-node"
	remoteNode := "remote-node"

	tier0 := newMemoryTierInstance(t, tier0ID)
	orch, err := New(Config{LocalNodeID: localNode})
	if err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, tier0)
	vault.Name = "fail"
	orch.RegisterVault(vault)

	mock := &transitionFakeTransferrer{failErr: context.DeadlineExceeded}
	orch.transferrer = mock

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "fail", TierIDs: []uuid.UUID{tier0ID, tier1ID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, NodeID: localNode,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, NodeID: remoteNode,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	if _, _, err := tier0.Chunks.Append(makeRecord("keep")); err != nil {
		t.Fatal(err)
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0.Chunks.List()
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	runner.transitionChunk(metas[0].ID)

	// Chunk should be RETAINED on failure.
	metasAfter, _ := tier0.Chunks.List()
	found := false
	for _, m := range metasAfter {
		if m.ID == metas[0].ID {
			found = true
		}
	}
	if !found {
		t.Error("expected chunk retained after cross-node failure")
	}
}

func TestTransitionNoTransferrer(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	localNode := "local-node"
	remoteNode := "remote-node"

	tier0 := newMemoryTierInstance(t, tier0ID)
	orch, err := New(Config{LocalNodeID: localNode})
	if err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, tier0)
	vault.Name = "no-xfer"
	orch.RegisterVault(vault)
	// No transferrer set.

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "no-xfer", TierIDs: []uuid.UUID{tier0ID, tier1ID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, NodeID: localNode,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, NodeID: remoteNode,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	if _, _, err := tier0.Chunks.Append(makeRecord("stuck")); err != nil {
		t.Fatal(err)
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0.Chunks.List()
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	runner.transitionChunk(metas[0].ID)

	// Chunk should be retained — can't reach remote node.
	metasAfter, _ := tier0.Chunks.List()
	if len(metasAfter) == 0 {
		t.Error("expected chunk retained when no transferrer available")
	}
}

func TestTransitionSweepDispatch(t *testing.T) {
	t.Parallel()
	orch, vaultID, tier0ID, _, cfg := setupTwoTierVault(t)

	store := cfgmem.NewStore()
	for _, v := range cfg.Vaults {
		_ = store.PutVault(context.Background(), v)
	}
	for _, tc := range cfg.Tiers {
		_ = store.PutTier(context.Background(), tc)
	}
	orch.cfgLoader = &transitionConfigLoader{store: store}

	vault := orch.vaults[vaultID]
	tier0CM := vault.Tiers[0].Chunks

	// Ingest, seal, and create a retention runner with a transition rule.
	if _, _, err := tier0CM.Append(makeRecord("sweep")); err != nil {
		t.Fatal(err)
	}
	if err := tier0CM.Seal(); err != nil {
		t.Fatal(err)
	}

	runner := &retentionRunner{
		vaultID: vaultID,
		tierID:  tier0ID,
		cm:      tier0CM,
		im:      vault.Tiers[0].Indexes,
		rules: []retentionRule{{
			policy: &keepNPolicy{n: 0}, // matches all sealed chunks
			action: config.RetentionActionTransition,
		}},
		orch:   orch,
		now:    time.Now,
		logger: slog.Default(),
	}

	runner.sweep()

	// Verify: source chunk deleted (transition happened via sweep dispatch).
	metasAfter, _ := tier0CM.List()
	if len(metasAfter) != 0 {
		t.Errorf("expected 0 chunks after sweep with transition, got %d", len(metasAfter))
	}

	// Verify: records in tier 1.
	tier1CM := vault.Tiers[1].Chunks
	active := tier1CM.Active()
	if active == nil || active.RecordCount != 1 {
		t.Error("expected 1 record in tier 1 after sweep transition")
	}
}

// keepNPolicy is a test-only retention policy that matches all sealed chunks
// beyond the first N.
type keepNPolicy struct{ n int }

func (p *keepNPolicy) Apply(state chunk.VaultState) []chunk.ChunkID {
	if len(state.Chunks) <= p.n {
		return nil
	}
	var ids []chunk.ChunkID
	for _, c := range state.Chunks[:len(state.Chunks)-p.n] {
		ids = append(ids, c.ID)
	}
	return ids
}

func (m *transitionFakeTransferrer) ForwardSealTier(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID) error {
	return nil
}
func (m *transitionFakeTransferrer) ReplicateSealedChunk(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
