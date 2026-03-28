package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
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
	calls       []transitionTransferCall
	streamCalls []transitionStreamCall
	failErr     error
}

type transitionTransferCall struct {
	nodeID  string
	vaultID uuid.UUID
	tierID  uuid.UUID
	records []chunk.Record
}

type transitionStreamCall struct {
	nodeID  string
	vaultID uuid.UUID
	tierID  uuid.UUID
	count   int
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

	// Verify StreamToTier was called.
	if len(mock.streamCalls) != 1 {
		t.Fatalf("expected 1 StreamToTier call, got %d", len(mock.streamCalls))
	}
	sc := mock.streamCalls[0]
	if sc.nodeID != remoteNode {
		t.Errorf("expected nodeID %q, got %q", remoteNode, sc.nodeID)
	}
	if sc.vaultID != vaultID {
		t.Errorf("expected vaultID %s, got %s", vaultID, sc.vaultID)
	}
	if sc.tierID != tier1ID {
		t.Errorf("expected tierID %s, got %s", tier1ID, sc.tierID)
	}
	if sc.count != 3 {
		t.Errorf("expected 3 records streamed, got %d", sc.count)
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

// TestTransitionCloudTierTTLSweep verifies that the retention sweep with a TTL
// policy correctly transitions cloud-backed sealed chunks to the next tier.
// Reproduces gastrolog-9umo2: 3m TTL on cloud tier, chunks sit for 10+ minutes.
func TestTransitionCloudTierTTLSweep(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nextTierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()
	cloudTier := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)
	nextTier := newMemoryTierInstance(t, nextTierID)

	orch, err := New(Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, cloudTier, nextTier)
	vault.Name = "ttl-cloud"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "ttl-cloud", TierIDs: []uuid.UUID{cloudTierID, nextTierID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, NodeID: nodeID,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile, NodeID: nodeID,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest, seal, and upload to cloud.
	const recordCount = 10
	for i := 0; i < recordCount; i++ {
		if _, _, err := cloudTier.Chunks.Append(makeRecord("ttl-cloud")); err != nil {
			t.Fatal(err)
		}
	}
	if err := cloudTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := cloudTier.Chunks.List()
	if len(metas) == 0 {
		t.Fatal("expected sealed chunk")
	}
	chunkID := metas[0].ID

	processor := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("PostSealProcess failed: %v", err)
	}

	// Create retention runner with a TTL policy (3 minutes) and a frozen
	// clock set 5 minutes in the future — the chunk should match.
	frozenNow := time.Now().Add(5 * time.Minute)

	runner := &retentionRunner{
		vaultID: vaultID,
		tierID:  cloudTierID,
		cm:      cloudTier.Chunks,
		im:      cloudTier.Indexes,
		rules: []retentionRule{{
			policy: chunk.NewTTLRetentionPolicy(3 * time.Minute),
			action: config.RetentionActionTransition,
		}},
		orch:   orch,
		now:    func() time.Time { return frozenNow },
		logger: slog.Default(),
	}

	runner.sweep()

	// Verify: cloud chunk deleted from source tier.
	metasFinal, _ := cloudTier.Chunks.List()
	for _, m := range metasFinal {
		if m.ID == chunkID {
			t.Error("expected cloud chunk to be deleted after TTL sweep transition")
		}
	}

	// Verify: records in next tier.
	nextTierMetas, _ := nextTier.Chunks.List()
	var totalRecords int64
	for _, m := range nextTierMetas {
		totalRecords += m.RecordCount
	}
	active := nextTier.Chunks.Active()
	if active != nil {
		listed := false
		for _, m := range nextTierMetas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			totalRecords += active.RecordCount
		}
	}
	if totalRecords != recordCount {
		t.Errorf("expected %d records in next tier after TTL sweep, got %d", recordCount, totalRecords)
	}
}

// TestTransitionCloudTierSecondaryDoesNotOverwriteBlob verifies that the
// secondary's PostSealProcess does NOT upload to cloud storage, preventing
// it from overwriting the primary's blob with a different-sized version.
// This was the root cause of gastrolog-9umo2: the secondary's upload changed
// the blob size, corrupting the primary's stored diskBytes and breaking all
// future cloud cursor reads (S3 416 Range Not Satisfiable).
func TestTransitionCloudTierSecondaryDoesNotOverwriteBlob(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nextTierID := uuid.Must(uuid.NewV7())
	primaryNode := "primary-node"
	secondaryNode := "secondary-node"

	cloudStore := blobstore.NewMemory()

	// Create primary cloud tier (has cloud backing).
	primaryTier := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)

	// Create secondary cloud tier — should NOT have cloud backing.
	secondaryDir := t.TempDir()
	secondaryCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            secondaryDir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		// NOTE: No CloudStore — this is the fix. Before the fix, the
		// secondary would also get CloudStore configured.
	})
	if err != nil {
		t.Fatal(err)
	}
	nextTier := newMemoryTierInstance(t, nextTierID)

	// Primary orchestrator.
	primaryOrch, err := New(Config{LocalNodeID: primaryNode})
	if err != nil {
		t.Fatal(err)
	}
	primaryVault := NewVault(vaultID, primaryTier, nextTier)
	primaryVault.Name = "overwrite-test"
	primaryOrch.RegisterVault(primaryVault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "overwrite-test", TierIDs: []uuid.UUID{cloudTierID, nextTierID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, NodeID: primaryNode,
		SecondaryNodeIDs: []string{secondaryNode},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile, NodeID: primaryNode,
	})
	primaryOrch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest records on primary, seal, and upload to cloud.
	const recordCount = 20
	for i := 0; i < recordCount; i++ {
		if _, _, err := primaryTier.Chunks.Append(makeRecord("primary-rec")); err != nil {
			t.Fatal(err)
		}
	}
	if err := primaryTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := primaryTier.Chunks.List()
	chunkID := metas[0].ID

	processor := primaryTier.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("primary PostSealProcess failed: %v", err)
	}

	// Verify primary's blob is in cloud.
	primaryMetas, _ := primaryTier.Chunks.List()
	var primaryDiskBytes int64
	for _, m := range primaryMetas {
		if m.ID == chunkID {
			primaryDiskBytes = m.DiskBytes
		}
	}
	if primaryDiskBytes == 0 {
		t.Fatal("expected non-zero diskBytes after cloud upload")
	}

	// Simulate secondary receiving the same records via replication.
	// Import the records to the secondary's chunk manager.
	recs := make([]chunk.Record, recordCount)
	for i := range recs {
		recs[i] = makeRecord("primary-rec")
	}
	secondaryCM.SetNextChunkID(chunkID)
	_, importErr := secondaryCM.ImportRecords(testIterFromRecords(recs))
	if importErr != nil {
		t.Fatalf("secondary import failed: %v", importErr)
	}

	// Run PostSealProcess on secondary — should NOT upload to cloud
	// because CloudStore is nil (the fix).
	if err := secondaryCM.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("secondary PostSealProcess failed: %v", err)
	}

	// Verify: secondary chunk is NOT cloud-backed (local only).
	secMetas, _ := secondaryCM.List()
	for _, m := range secMetas {
		if m.ID == chunkID && m.CloudBacked {
			t.Error("secondary chunk should NOT be cloud-backed")
		}
	}

	// Verify: primary can still transition from cloud (blob wasn't overwritten).
	runner := newTestRetentionRunner(primaryOrch, vaultID, cloudTierID, primaryTier.Chunks, primaryTier.Indexes)
	runner.transitionChunk(chunkID)

	// Verify: records arrived in next tier.
	nextTierMetas, _ := nextTier.Chunks.List()
	var totalRecords int64
	for _, m := range nextTierMetas {
		totalRecords += m.RecordCount
	}
	active := nextTier.Chunks.Active()
	if active != nil {
		listed := false
		for _, m := range nextTierMetas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			totalRecords += active.RecordCount
		}
	}
	if totalRecords != recordCount {
		t.Errorf("expected %d records in next tier, got %d", recordCount, totalRecords)
	}
}

func testIterFromRecords(recs []chunk.Record) chunk.RecordIterator {
	i := 0
	return func() (chunk.Record, error) {
		if i >= len(recs) {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		r := recs[i]
		i++
		return r, nil
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
func (m *transitionFakeTransferrer) StreamToTier(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, next chunk.RecordIterator) error {
	if m.failErr != nil {
		return m.failErr
	}
	var count int
	for {
		if _, err := next(); err != nil {
			break
		}
		count++
	}
	m.streamCalls = append(m.streamCalls, transitionStreamCall{
		nodeID: nodeID, vaultID: vaultID, tierID: tierID, count: count,
	})
	return nil
}

// ---------- cloud tier transition test ----------

// newCloudFileTier creates a file-backed TierInstance with cloud storage.
// Sealed chunks are uploaded to the in-memory blobstore and local files deleted,
// matching production cloud tier behavior.
func newCloudFileTier(t *testing.T, tierID uuid.UUID, vaultID uuid.UUID, store blobstore.Store) *TierInstance {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)
	return &TierInstance{
		TierID:  tierID,
		Type:    "cloud",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}
}

// TestTransitionCloudTierToNextTier verifies that sealed cloud-backed chunks
// are read back from object storage and streamed to the next tier. This is
// the exact scenario from gastrolog-9umo2: FILE → FILE → CLOUD → FILE chain
// where the cloud tier's sealed chunks never transition to tier 4.
func TestTransitionCloudTierToNextTier(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nextTierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()
	cloudTier := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)
	nextTier := newMemoryTierInstance(t, nextTierID)

	orch, err := New(Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, cloudTier, nextTier)
	vault.Name = "cloud-transition"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cloud-transition", TierIDs: []uuid.UUID{cloudTierID, nextTierID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, NodeID: nodeID,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile, NodeID: nodeID,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest records into the cloud tier.
	const recordCount = 10
	for i := 0; i < recordCount; i++ {
		if _, _, err := cloudTier.Chunks.Append(makeRecord("cloud-rec")); err != nil {
			t.Fatal(err)
		}
	}

	// Seal the chunk.
	if err := cloudTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Run PostSealProcess — this compresses, indexes, and uploads to cloud,
	// then deletes local files. The chunk moves to the cloud B+ tree index.
	metas, _ := cloudTier.Chunks.List()
	if len(metas) == 0 {
		t.Fatal("expected sealed chunk in cloud tier")
	}
	chunkID := metas[0].ID

	processor := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("PostSealProcess failed: %v", err)
	}

	// Verify the chunk is now cloud-backed.
	metasAfterUpload, _ := cloudTier.Chunks.List()
	found := false
	for _, m := range metasAfterUpload {
		if m.ID == chunkID {
			found = true
			if !m.CloudBacked {
				t.Fatal("expected chunk to be cloud-backed after PostSealProcess")
			}
		}
	}
	if !found {
		t.Fatal("cloud-backed chunk disappeared from List() after upload")
	}

	// Now run the transition — this should open a cloud cursor (range requests),
	// read records from the blobstore, and stream them to the next tier.
	runner := newTestRetentionRunner(orch, vaultID, cloudTierID, cloudTier.Chunks, cloudTier.Indexes)
	runner.transitionChunk(chunkID)

	// Verify: source chunk deleted from cloud tier.
	metasAfterTransition, _ := cloudTier.Chunks.List()
	for _, m := range metasAfterTransition {
		if m.ID == chunkID {
			t.Error("expected cloud chunk to be deleted after transition")
		}
	}

	// Verify: records appear in the next tier.
	nextTierMetas, _ := nextTier.Chunks.List()
	var totalRecords int64
	for _, m := range nextTierMetas {
		totalRecords += m.RecordCount
	}
	active := nextTier.Chunks.Active()
	if active != nil {
		listed := false
		for _, m := range nextTierMetas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			totalRecords += active.RecordCount
		}
	}
	if totalRecords != recordCount {
		t.Errorf("expected %d records in next tier, got %d", recordCount, totalRecords)
	}
}

// TestTransitionCloudTierSweepDispatch verifies that the retention sweep
// correctly picks up cloud-backed sealed chunks and transitions them.
// This tests the full sweep() path rather than calling transitionChunk directly.
func TestTransitionCloudTierSweepDispatch(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nextTierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()
	cloudTier := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)
	nextTier := newMemoryTierInstance(t, nextTierID)

	orch, err := New(Config{LocalNodeID: nodeID})
	if err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, cloudTier, nextTier)
	vault.Name = "cloud-sweep"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cloud-sweep", TierIDs: []uuid.UUID{cloudTierID, nextTierID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, NodeID: nodeID,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile, NodeID: nodeID,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest, seal, and upload to cloud.
	const recordCount = 10
	for i := 0; i < recordCount; i++ {
		if _, _, err := cloudTier.Chunks.Append(makeRecord("sweep-cloud")); err != nil {
			t.Fatal(err)
		}
	}
	if err := cloudTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := cloudTier.Chunks.List()
	if len(metas) == 0 {
		t.Fatal("expected sealed chunk")
	}
	chunkID := metas[0].ID

	processor := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("PostSealProcess failed: %v", err)
	}

	// Verify chunk is cloud-backed.
	metasAfter, _ := cloudTier.Chunks.List()
	for _, m := range metasAfter {
		if m.ID == chunkID && !m.CloudBacked {
			t.Fatal("expected cloud-backed chunk")
		}
	}

	// Create a retention runner with a "match all sealed" policy and
	// transition action. This simulates what the production retention sweep does.
	runner := &retentionRunner{
		vaultID: vaultID,
		tierID:  cloudTierID,
		cm:      cloudTier.Chunks,
		im:      cloudTier.Indexes,
		rules: []retentionRule{{
			policy: &keepNPolicy{n: 0}, // matches all sealed chunks
			action: config.RetentionActionTransition,
		}},
		orch:   orch,
		now:    time.Now,
		logger: slog.Default(),
	}

	// Run the sweep — this should find the cloud-backed chunk, open a cloud
	// cursor, stream records to the next tier, and delete the source.
	runner.sweep()

	// Verify: cloud chunk deleted.
	metasFinal, _ := cloudTier.Chunks.List()
	for _, m := range metasFinal {
		if m.ID == chunkID {
			t.Error("expected cloud chunk to be deleted after sweep transition")
		}
	}

	// Verify: records in next tier.
	nextTierMetas, _ := nextTier.Chunks.List()
	var totalRecords int64
	for _, m := range nextTierMetas {
		totalRecords += m.RecordCount
	}
	active := nextTier.Chunks.Active()
	if active != nil {
		listed := false
		for _, m := range nextTierMetas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			totalRecords += active.RecordCount
		}
	}
	if totalRecords != recordCount {
		t.Errorf("expected %d records in next tier after sweep, got %d", recordCount, totalRecords)
	}
}
