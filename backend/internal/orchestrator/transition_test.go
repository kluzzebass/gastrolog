package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"errors"
	"fmt"

	"os"
	"path/filepath"

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

// syntheticPlacements creates a Placements slice with a primary using a synthetic storage ID.
func syntheticPlacements(nodeID string) []config.TierPlacement {
	return []config.TierPlacement{{StorageID: config.SyntheticStorageID(nodeID), Leader: true}}
}

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

// newTestOrch creates an Orchestrator and registers t.Cleanup to stop the
// scheduler. Without this, leaked gocron goroutines cause massive race
// detector overhead (168 orchestrators × background cron jobs).
func newTestOrch(t *testing.T, cfg Config) *Orchestrator {
	t.Helper()
	orch, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)
	return orch
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

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "test-vault"
	orch.RegisterVault(vault)

	cfg := &config.Config{
		Vaults: []config.VaultConfig{
			{ID: vaultID, Name: "test-vault"},
		},
		Tiers: []config.TierConfig{
			{ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID), VaultID: vaultID, Position: 0},
			{ID: tier1ID, Name: "warm", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID), VaultID: vaultID, Position: 1},
		},
	}

	return orch, vaultID, tier0ID, tier1ID, cfg
}

func newTestRetentionRunner(orch *Orchestrator, vaultID, tierID uuid.UUID, cm chunk.ChunkManager, im index.IndexManager) *retentionRunner {
	return &retentionRunner{
		isLeader: true,
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
	for i := range 5 {
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
	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, tier) // single tier = terminal
	vault.Name = "terminal"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "terminal",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tierID, Name: "only", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
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
	for range 3 {
		for range 2 {
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
	orch := newTestOrch(t, Config{LocalNodeID: localNode})

	// Only tier 0 is local; tier 1 is on a remote node.
	vault := NewVault(vaultID, tier0)
	vault.Name = "cross-node"
	orch.RegisterVault(vault)

	mock := &transitionFakeTransferrer{}
	orch.transferrer = mock

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cross-node",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, Placements: syntheticPlacements(localNode),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, Placements: syntheticPlacements(remoteNode),
		VaultID: vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest and seal.
	for range 3 {
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
	orch := newTestOrch(t, Config{LocalNodeID: localNode})

	vault := NewVault(vaultID, tier0)
	vault.Name = "fail"
	orch.RegisterVault(vault)

	mock := &transitionFakeTransferrer{failErr: context.DeadlineExceeded}
	orch.transferrer = mock

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "fail",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, Placements: syntheticPlacements(localNode),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, Placements: syntheticPlacements(remoteNode),
		VaultID: vaultID, Position: 1,
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
	orch := newTestOrch(t, Config{LocalNodeID: localNode})

	vault := NewVault(vaultID, tier0)
	vault.Name = "no-xfer"
	orch.RegisterVault(vault)
	// No transferrer set.

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "no-xfer",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, Placements: syntheticPlacements(localNode),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, Placements: syntheticPlacements(remoteNode),
		VaultID: vaultID, Position: 1,
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

	rules := []retentionRule{{
		policy: &keepNPolicy{n: 0}, // matches all sealed chunks
		action: config.RetentionActionTransition,
	}}
	runner := &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
		tierID:   tier0ID,
		cm:      tier0CM,
		im:      vault.Tiers[0].Indexes,
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	runner.sweep(rules)

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
	cloudTier, _ := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)
	nextTier := newMemoryTierInstance(t, nextTierID)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, cloudTier, nextTier)
	vault.Name = "ttl-cloud"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "ttl-cloud",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest, seal, and upload to cloud.
	const recordCount = 10
	for range recordCount {
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

	rules := []retentionRule{{
		policy: chunk.NewTTLRetentionPolicy(3 * time.Minute),
		action: config.RetentionActionTransition,
	}}
	runner := &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
		tierID:   cloudTierID,
		cm:      cloudTier.Chunks,
		im:      cloudTier.Indexes,
		orch:    orch,
		now:     func() time.Time { return frozenNow },
		logger:  slog.Default(),
	}

	runner.sweep(rules)

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

// TestCloudTierLeaderPreservesCloudBacking verifies that a cloud tier leader
// built through the production code path (buildPrimaryTierInstance →
// buildTierInstanceForStorage) retains the sealed_backing parameter so that
// PostSealProcess uploads chunks to cloud storage.
//
// Regression test: buildTierInstanceForStorage previously stripped sealed_backing
// unconditionally (with the comment "always follower"), even when called for the
// leader. This caused cloud tier leaders to have CloudStore=nil, silently
// preventing all cloud uploads and breaking the entire archival lifecycle.
func TestCloudTierLeaderPreservesCloudBacking(t *testing.T) {
	t.Parallel()
	nodeID := "test-node"
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	fsID := uuid.Must(uuid.NewV7())
	csID := uuid.Must(uuid.NewV7())

	storageDir := t.TempDir()

	// Pre-create the vault/tier directory so the chunk manager factory
	// doesn't warn about missing paths.
	tierDir := filepath.Join(storageDir, "vaults", vaultID.String(), cloudTierID.String())
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Vaults: []config.VaultConfig{{
			ID:   vaultID,
			Name: "cloud-leader-test",
		}},
		Tiers: []config.TierConfig{{
			VaultID:  vaultID,
			Position: 0,
			ID:               cloudTierID,
			Name:             "cloud",
			Type:             config.TierTypeCloud,
			CloudServiceID:   &csID,
			ActiveChunkClass: 1,
			Placements: []config.TierPlacement{
				{StorageID: fsID.String(), Leader: true},
			},
		}},
		CloudServices: []config.CloudService{{
			ID:       csID,
			Name:     "test-cloud",
			Provider: "memory",
		}},
		NodeStorageConfigs: []config.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []config.FileStorage{{
				ID:           fsID,
				StorageClass: 1,
				Name:         "disk-1",
				Path:         storageDir,
			}},
		}},
	}

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file": chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file": indexfile.NewFactory(),
		},
		VaultsDir: storageDir,
		Logger:    slog.Default(),
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	defer orch.Stop()

	store := cfgmem.NewStore()
	orch.cfgLoader = &transitionConfigLoader{store: store}

	if err := orch.ApplyConfig(cfg, factories); err != nil {
		t.Fatalf("ApplyConfig failed: %v", err)
	}

	// The vault should have been created with the cloud tier.
	orch.mu.RLock()
	vault := orch.vaults[vaultID]
	orch.mu.RUnlock()
	if vault == nil {
		t.Fatal("vault not created")
	}
	if len(vault.Tiers) != 1 {
		t.Fatalf("expected 1 tier, got %d", len(vault.Tiers))
	}
	cloudTier := vault.Tiers[0]
	if cloudTier.IsFollower {
		t.Fatal("expected cloud tier to be leader, got follower")
	}

	// Ingest records, seal, and run PostSealProcess.
	const recordCount = 10
	for i := range recordCount {
		if _, _, err := cloudTier.Chunks.Append(makeRecord(fmt.Sprintf("cloud-leader-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := cloudTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, err := cloudTier.Chunks.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(metas) == 0 {
		t.Fatal("expected sealed chunk")
	}
	chunkID := metas[0].ID

	processor, ok := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
	if !ok {
		t.Fatal("chunk manager does not implement ChunkPostSealProcessor")
	}
	if err := processor.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("PostSealProcess failed: %v", err)
	}

	// Verify the chunk is cloud-backed after PostSealProcess. If sealed_backing
	// was stripped from the leader, CloudStore=nil and upload is skipped,
	// leaving CloudBacked=false.
	metas, err = cloudTier.Chunks.List()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, m := range metas {
		if m.ID == chunkID {
			found = true
			if !m.CloudBacked {
				t.Fatal("chunk is NOT cloud-backed after PostSealProcess — sealed_backing was incorrectly stripped for the leader")
			}
			if !m.Compressed {
				t.Fatal("chunk was not compressed")
			}
			break
		}
	}
	if !found {
		t.Fatal("sealed chunk not found in list")
	}
}

// TestTransitionCloudTierFollowerDoesNotOverwriteBlob verifies that the
// follower's PostSealProcess does NOT upload to cloud storage, preventing
// it from overwriting the leader's blob with a different-sized version.
// This was the root cause of gastrolog-9umo2: the follower's upload changed
// the blob size, corrupting the leader's stored diskBytes and breaking all
// future cloud cursor reads (S3 416 Range Not Satisfiable).
func TestTransitionCloudTierFollowerDoesNotOverwriteBlob(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nextTierID := uuid.Must(uuid.NewV7())
	leaderNode := "leader-node"
	followerNode := "follower-node"

	cloudStore := blobstore.NewMemory()

	// Create leader cloud tier (has cloud backing).
	primaryTier, _ := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)

	// Create follower cloud tier — should NOT have cloud backing.
	followerDir := t.TempDir()
	followerCM, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            followerDir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		// NOTE: No CloudStore — this is the fix. Before the fix, the
		// follower would also get CloudStore configured.
	})
	if err != nil {
		t.Fatal(err)
	}
	nextTier := newMemoryTierInstance(t, nextTierID)

	// Leader orchestrator.
	leaderOrch, err := New(Config{LocalNodeID: leaderNode})
	if err != nil {
		t.Fatal(err)
	}
	primaryVault := NewVault(vaultID, primaryTier, nextTier)
	primaryVault.Name = "overwrite-test"
	leaderOrch.RegisterVault(primaryVault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "overwrite-test",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud,
		VaultID: vaultID, Position: 0,
		Placements: []config.TierPlacement{
			{StorageID: config.SyntheticStorageID(leaderNode), Leader: true},
			{StorageID: config.SyntheticStorageID(followerNode), Leader: false},
		},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile,
		VaultID: vaultID, Position: 1,
		Placements: []config.TierPlacement{
			{StorageID: config.SyntheticStorageID(leaderNode), Leader: true},
		},
	})
	leaderOrch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest records on leader, seal, and upload to cloud.
	const recordCount = 20
	for range recordCount {
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

	// Verify leader's blob is in cloud.
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

	// Simulate follower receiving the same records via replication.
	// Import the records to the follower's chunk manager.
	recs := make([]chunk.Record, recordCount)
	for i := range recs {
		recs[i] = makeRecord("primary-rec")
	}
	followerCM.SetNextChunkID(chunkID)
	_, importErr := followerCM.ImportRecords(testIterFromRecords(recs))
	if importErr != nil {
		t.Fatalf("follower import failed: %v", importErr)
	}

	// Run PostSealProcess on follower — should NOT upload to cloud
	// because CloudStore is nil (the fix).
	if err := followerCM.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("follower PostSealProcess failed: %v", err)
	}

	// Verify: follower chunk is NOT cloud-backed (local only).
	followerMetas, _ := followerCM.List()
	for _, m := range followerMetas {
		if m.ID == chunkID && m.CloudBacked {
			t.Error("follower chunk should NOT be cloud-backed")
		}
	}

	// Verify: leader can still transition from cloud (blob wasn't overwritten).
	runner := newTestRetentionRunner(leaderOrch, vaultID, cloudTierID, primaryTier.Chunks, primaryTier.Indexes)
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
func (m *transitionFakeTransferrer) ForwardDeleteChunk(_ context.Context, _ string, _, _ uuid.UUID, _ chunk.ChunkID) error {
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
func newCloudFileTier(t *testing.T, tierID uuid.UUID, vaultID uuid.UUID, store blobstore.Store) (*TierInstance, string) {
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
	}, dir
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
	cloudTier, _ := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)
	nextTier := newMemoryTierInstance(t, nextTierID)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, cloudTier, nextTier)
	vault.Name = "cloud-transition"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cloud-transition",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest records into the cloud tier.
	const recordCount = 10
	for range recordCount {
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
	cloudTier, _ := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)
	nextTier := newMemoryTierInstance(t, nextTierID)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, cloudTier, nextTier)
	vault.Name = "cloud-sweep"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cloud-sweep",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeFile, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest, seal, and upload to cloud.
	const recordCount = 10
	for range recordCount {
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
	rules := []retentionRule{{
		policy: &keepNPolicy{n: 0}, // matches all sealed chunks
		action: config.RetentionActionTransition,
	}}
	runner := &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
		tierID:   cloudTierID,
		cm:      cloudTier.Chunks,
		im:      cloudTier.Indexes,
		orch:    orch,
		now:     time.Now,
		logger:  slog.Default(),
	}

	// Run the sweep — this should find the cloud-backed chunk, open a cloud
	// cursor, stream records to the next tier, and delete the source.
	runner.sweep(rules)

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

// ---------- helpers for new tests ----------

// newFileTierInstance creates a file-backed TierInstance without cloud storage.
// Returns the tier instance and its filesystem directory for post-test verification.
func newFileTierInstance(t *testing.T, tierID uuid.UUID) (*TierInstance, string) {
	t.Helper()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)
	return &TierInstance{
		TierID:  tierID,
		Type:    "file",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}, dir
}


// assertNoDirsOnDisk verifies no chunk subdirectories remain in a tier directory.
func assertNoDirsOnDisk(t *testing.T, label, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Errorf("%s: ReadDir(%s): %v", label, dir, err)
		return
	}
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			t.Errorf("%s: chunk directory %s still on disk at %s", label, e.Name(), dir)
		}
	}
}

// countAllTierRecords counts all records across both sealed and active chunks.
func countAllTierRecords(tb testing.TB, cm chunk.ChunkManager) int64 {
	tb.Helper()
	metas, _ := cm.List()
	var total int64
	for _, m := range metas {
		total += m.RecordCount
	}
	active := cm.Active()
	if active != nil {
		listed := false
		for _, m := range metas {
			if m.ID == active.ID {
				listed = true
				break
			}
		}
		if !listed {
			total += active.RecordCount
		}
	}
	return total
}

// readAllRecords reads every record from a chunk manager (all sealed + active).
func readAllRecords(t *testing.T, cm chunk.ChunkManager) []chunk.Record {
	t.Helper()
	var all []chunk.Record
	metas, _ := cm.List()

	// Collect chunk IDs to read (sealed chunks).
	ids := make([]chunk.ChunkID, 0, len(metas))
	for _, m := range metas {
		ids = append(ids, m.ID)
	}
	// Include active chunk if not already in the list.
	if active := cm.Active(); active != nil {
		found := false
		for _, m := range metas {
			if m.ID == active.ID {
				found = true
				break
			}
		}
		if !found {
			ids = append(ids, active.ID)
		}
	}

	for _, id := range ids {
		cursor, err := cm.OpenCursor(id)
		if err != nil {
			t.Fatalf("OpenCursor(%s): %v", id, err)
		}
		for {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				_ = cursor.Close()
				t.Fatalf("cursor.Next: %v", err)
			}
			all = append(all, rec.Copy())
		}
		_ = cursor.Close()
	}
	return all
}

// makeRecordWithEventID creates a record with an explicit EventID for testing preservation.
func makeRecordWithEventID(raw string, ingesterID uuid.UUID, seq uint32) chunk.Record {
	now := time.Now()
	return chunk.Record{
		SourceTS: now,
		IngestTS: now,
		EventID: chunk.EventID{
			IngesterID: ingesterID,
			IngestTS:   now,
			IngestSeq:  seq,
		},
		Attrs: chunk.Attributes{"msg": raw},
		Raw:   []byte(raw),
	}
}

// ---------- 3-tier chain transition tests ----------

// TestTransitionThreeTierChainMemory verifies that a 3-tier chain
// (memory→memory→memory) preserves exact record count with no duplication.
func TestTransitionThreeTierChainMemory(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	tier2ID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	tier0 := newMemoryTierInstance(t, tier0ID)
	tier1 := newMemoryTierInstance(t, tier1ID)
	tier2 := newMemoryTierInstance(t, tier2ID)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, tier0, tier1, tier2)
	vault.Name = "three-tier"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "three-tier",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier2ID, Name: "cold", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 2,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	const recordCount = 50
	for i := range recordCount {
		if _, _, err := tier0.Chunks.Append(makeRecord(fmt.Sprintf("chain-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Transition tier 0 → tier 1.
	metas0, _ := tier0.Chunks.List()
	if len(metas0) == 0 {
		t.Fatal("expected sealed chunk in tier 0")
	}
	runner0 := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	runner0.transitionChunk(metas0[0].ID)

	// Verify: tier 0 empty, tier 1 has all records.
	if got := countAllTierRecords(t, tier0.Chunks); got != 0 {
		t.Errorf("tier 0: expected 0 records, got %d", got)
	}
	if got := countAllTierRecords(t, tier1.Chunks); got != recordCount {
		t.Errorf("tier 1: expected %d records, got %d", recordCount, got)
	}

	// Seal tier 1, then transition tier 1 → tier 2.
	if err := tier1.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}
	metas1, _ := tier1.Chunks.List()
	if len(metas1) == 0 {
		t.Fatal("expected sealed chunk in tier 1")
	}
	runner1 := newTestRetentionRunner(orch, vaultID, tier1ID, tier1.Chunks, tier1.Indexes)
	runner1.transitionChunk(metas1[0].ID)

	// Verify final state: only tier 2 has records.
	if got := countAllTierRecords(t, tier0.Chunks); got != 0 {
		t.Errorf("tier 0: expected 0 records after full chain, got %d", got)
	}
	if got := countAllTierRecords(t, tier1.Chunks); got != 0 {
		t.Errorf("tier 1: expected 0 records after full chain, got %d", got)
	}
	if got := countAllTierRecords(t, tier2.Chunks); got != recordCount {
		t.Errorf("tier 2: expected %d records after full chain, got %d", recordCount, got)
	}
}

// TestTransitionThreeTierChainFileFileCloud verifies the production-like
// file→file→cloud chain preserves all records without N× duplication.
// This is the exact scenario from the gastrolog-1rv42 session bugs.
func TestTransitionThreeTierChainFileFileCloud(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	tier2ID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()

	tier0, tier0Dir := newFileTierInstance(t, tier0ID)
	tier1, tier1Dir := newFileTierInstance(t, tier1ID)
	tier2, _ := newCloudFileTier(t, tier2ID, vaultID, cloudStore)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, tier0, tier1, tier2)
	vault.Name = "file-file-cloud"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "file-file-cloud",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeFile, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier2ID, Name: "cold", Type: config.TierTypeCloud, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 2,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest records into tier 0 (hot file tier).
	const recordCount = 30
	for i := range recordCount {
		if _, _, err := tier0.Chunks.Append(makeRecord(fmt.Sprintf("ffc-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Run PostSealProcess on tier 0 (compress + index, no cloud upload).
	metas0, _ := tier0.Chunks.List()
	if len(metas0) == 0 {
		t.Fatal("expected sealed chunk in tier 0")
	}
	processor0 := tier0.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor0.PostSealProcess(context.Background(), metas0[0].ID); err != nil {
		t.Fatalf("tier 0 PostSealProcess: %v", err)
	}

	// Transition tier 0 → tier 1.
	runner0 := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	runner0.transitionChunk(metas0[0].ID)

	if got := countAllTierRecords(t, tier1.Chunks); got != recordCount {
		t.Fatalf("tier 1: expected %d records after tier 0→1, got %d", recordCount, got)
	}

	// Seal tier 1 and run post-seal.
	if err := tier1.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}
	metas1, _ := tier1.Chunks.List()
	if len(metas1) == 0 {
		t.Fatal("expected sealed chunk in tier 1")
	}
	processor1 := tier1.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor1.PostSealProcess(context.Background(), metas1[0].ID); err != nil {
		t.Fatalf("tier 1 PostSealProcess: %v", err)
	}

	// Transition tier 1 → tier 2 (cloud).
	runner1 := newTestRetentionRunner(orch, vaultID, tier1ID, tier1.Chunks, tier1.Indexes)
	runner1.transitionChunk(metas1[0].ID)

	if got := countAllTierRecords(t, tier2.Chunks); got != recordCount {
		t.Fatalf("tier 2 (cloud): expected %d records after tier 1→2, got %d", recordCount, got)
	}

	// Verify no duplication: tiers 0 and 1 should be empty.
	if got := countAllTierRecords(t, tier0.Chunks); got != 0 {
		t.Errorf("tier 0: expected 0 records after full chain, got %d", got)
	}
	if got := countAllTierRecords(t, tier1.Chunks); got != 0 {
		t.Errorf("tier 1: expected 0 records after full chain, got %d", got)
	}

	// Verify chunk directories removed from disk on tiers 0 and 1.
	assertNoDirsOnDisk(t, "tier 0", tier0Dir)
	assertNoDirsOnDisk(t, "tier 1", tier1Dir)

	// Seal and upload tier 2 to cloud, verify cloud-backed.
	if err := tier2.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}
	metas2, _ := tier2.Chunks.List()
	if len(metas2) == 0 {
		t.Fatal("expected sealed chunk in tier 2")
	}
	processor2 := tier2.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor2.PostSealProcess(context.Background(), metas2[0].ID); err != nil {
		t.Fatalf("tier 2 PostSealProcess: %v", err)
	}

	// Verify cloud-backed.
	metas2After, _ := tier2.Chunks.List()
	for _, m := range metas2After {
		if !m.CloudBacked {
			t.Errorf("chunk %s in tier 2 should be cloud-backed", m.ID)
		}
	}

	// Verify records readable from cloud.
	cloudRecords := readAllRecords(t, tier2.Chunks)
	if len(cloudRecords) != recordCount {
		t.Errorf("cloud tier: expected %d readable records, got %d", recordCount, len(cloudRecords))
	}
}

// ---------- EventID preservation tests ----------

// TestTransitionEventIDPreserved verifies that EventIDs survive local tier transitions.
func TestTransitionEventIDPreserved(t *testing.T) {
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

	// Ingest records with distinct EventIDs.
	ingesterID := uuid.Must(uuid.NewV7())
	const recordCount = 10
	type eventSnapshot struct {
		raw       string
		ingesterID uuid.UUID
		ingestSeq  uint32
	}
	originals := make([]eventSnapshot, recordCount)
	for i := range recordCount {
		rec := makeRecordWithEventID(fmt.Sprintf("eid-%d", i), ingesterID, uint32(i))
		originals[i] = eventSnapshot{
			raw:        fmt.Sprintf("eid-%d", i),
			ingesterID: ingesterID,
			ingestSeq:  uint32(i),
		}
		if _, _, err := tier0CM.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier0CM.Seal(); err != nil {
		t.Fatal(err)
	}

	metas, _ := tier0CM.List()
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0CM, vault.Tiers[0].Indexes)
	runner.transitionChunk(metas[0].ID)

	// Read records from tier 1 and verify EventIDs match.
	tier1Records := readAllRecords(t, vault.Tiers[1].Chunks)
	if len(tier1Records) != recordCount {
		t.Fatalf("expected %d records in tier 1, got %d", recordCount, len(tier1Records))
	}

	for i, rec := range tier1Records {
		orig := originals[i]
		if string(rec.Raw) != orig.raw {
			t.Errorf("record %d: raw mismatch: %q vs %q", i, string(rec.Raw), orig.raw)
		}
		if rec.EventID.IngesterID != orig.ingesterID {
			t.Errorf("record %d: IngesterID mismatch: %s vs %s", i, rec.EventID.IngesterID, orig.ingesterID)
		}
		if rec.EventID.IngestSeq != orig.ingestSeq {
			t.Errorf("record %d: IngestSeq mismatch: %d vs %d", i, rec.EventID.IngestSeq, orig.ingestSeq)
		}
	}
}

// TestTransitionEventIDPreservedThroughCloudTier verifies EventIDs survive
// transitions through a cloud-backed tier (the full round-trip: ingest → seal
// → cloud upload → cloud cursor read → transition to next tier).
func TestTransitionEventIDPreservedThroughCloudTier(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nextTierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()
	cloudTier, _ := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)
	nextTier := newMemoryTierInstance(t, nextTierID)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, cloudTier, nextTier)
	vault.Name = "eventid-cloud"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "eventid-cloud",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: nextTierID, Name: "local", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest records with distinct EventIDs.
	ingesterID := uuid.Must(uuid.NewV7())
	const recordCount = 15
	type snapshot struct {
		raw       string
		ingestSeq uint32
	}
	originals := make([]snapshot, recordCount)
	for i := range recordCount {
		raw := fmt.Sprintf("cloud-eid-%d", i)
		rec := makeRecordWithEventID(raw, ingesterID, uint32(i))
		originals[i] = snapshot{raw: raw, ingestSeq: uint32(i)}
		if _, _, err := cloudTier.Chunks.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := cloudTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Upload to cloud.
	metas, _ := cloudTier.Chunks.List()
	processor := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor.PostSealProcess(context.Background(), metas[0].ID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	// Transition cloud → next tier.
	runner := newTestRetentionRunner(orch, vaultID, cloudTierID, cloudTier.Chunks, cloudTier.Indexes)
	runner.transitionChunk(metas[0].ID)

	// Verify EventIDs in next tier.
	nextRecords := readAllRecords(t, nextTier.Chunks)
	if len(nextRecords) != recordCount {
		t.Fatalf("expected %d records, got %d", recordCount, len(nextRecords))
	}

	for i, rec := range nextRecords {
		orig := originals[i]
		if string(rec.Raw) != orig.raw {
			t.Errorf("record %d: raw mismatch: %q vs %q", i, string(rec.Raw), orig.raw)
		}
		if rec.EventID.IngesterID != ingesterID {
			t.Errorf("record %d: IngesterID lost after cloud transition", i)
		}
		if rec.EventID.IngestSeq != orig.ingestSeq {
			t.Errorf("record %d: IngestSeq: got %d, want %d", i, rec.EventID.IngestSeq, orig.ingestSeq)
		}
	}
}

// ---------- Record count accuracy tests ----------

// TestTransitionRecordCountAccuracy verifies that chunk metadata RecordCount
// matches the actual number of records readable via cursor at each tier stage.
func TestTransitionRecordCountAccuracy(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	tier0, tier0Dir := newFileTierInstance(t, tier0ID)
	tier1, _ := newFileTierInstance(t, tier1ID)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, tier0, tier1)
	vault.Name = "count-accuracy"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "count-accuracy",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeFile, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "warm", Type: config.TierTypeFile, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	const recordCount = 100
	for i := range recordCount {
		if _, _, err := tier0.Chunks.Append(makeRecord(fmt.Sprintf("acc-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Verify tier 0 metadata vs actual records.
	metas0, _ := tier0.Chunks.List()
	if len(metas0) != 1 {
		t.Fatalf("expected 1 sealed chunk in tier 0, got %d", len(metas0))
	}
	if metas0[0].RecordCount != recordCount {
		t.Errorf("tier 0 metadata: expected RecordCount=%d, got %d", recordCount, metas0[0].RecordCount)
	}
	tier0Actual := readAllRecords(t, tier0.Chunks)
	if int64(len(tier0Actual)) != metas0[0].RecordCount {
		t.Errorf("tier 0: metadata says %d records but cursor read %d", metas0[0].RecordCount, len(tier0Actual))
	}

	// Run post-seal then transition.
	processor0 := tier0.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor0.PostSealProcess(context.Background(), metas0[0].ID); err != nil {
		t.Fatalf("tier 0 PostSealProcess: %v", err)
	}
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	runner.transitionChunk(metas0[0].ID)

	// Seal tier 1 and verify metadata vs actual.
	if err := tier1.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}
	metas1, _ := tier1.Chunks.List()
	if len(metas1) == 0 {
		t.Fatal("expected sealed chunk in tier 1")
	}
	var metaTotal int64
	for _, m := range metas1 {
		metaTotal += m.RecordCount
	}
	tier1Actual := readAllRecords(t, tier1.Chunks)
	if int64(len(tier1Actual)) != metaTotal {
		t.Errorf("tier 1: metadata says %d records but cursor read %d", metaTotal, len(tier1Actual))
	}
	if metaTotal != recordCount {
		t.Errorf("tier 1: expected %d total records, got %d", recordCount, metaTotal)
	}

	// Verify source tier 0 chunk directories removed from disk.
	assertNoDirsOnDisk(t, "tier 0", tier0Dir)
}

// ---------- Cloud search after transition ----------

// TestTransitionCloudSearchAfterTransition verifies that records in a cloud
// tier are searchable via the query engine after transition and upload.
func TestTransitionCloudSearchAfterTransition(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()
	tier0 := newMemoryTierInstance(t, tier0ID)
	cloudTier, _ := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, tier0, cloudTier)
	vault.Name = "cloud-search"
	orch.RegisterVault(vault)

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cloud-search",
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "hot", Type: config.TierTypeMemory, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: cloudTierID, Name: "cloud", Type: config.TierTypeCloud, Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	orch.cfgLoader = &transitionConfigLoader{store: store}

	// Ingest distinct records.
	const recordCount = 20
	for i := range recordCount {
		if _, _, err := tier0.Chunks.Append(makeRecord(fmt.Sprintf("searchable-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := tier0.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	// Transition tier 0 → cloud tier.
	metas0, _ := tier0.Chunks.List()
	runner := newTestRetentionRunner(orch, vaultID, tier0ID, tier0.Chunks, tier0.Indexes)
	runner.transitionChunk(metas0[0].ID)

	// Seal the cloud tier and upload.
	if err := cloudTier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}
	metasCloud, _ := cloudTier.Chunks.List()
	if len(metasCloud) == 0 {
		t.Fatal("expected sealed chunk in cloud tier")
	}
	processor := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
	if err := processor.PostSealProcess(context.Background(), metasCloud[0].ID); err != nil {
		t.Fatalf("cloud PostSealProcess: %v", err)
	}

	// Verify: records searchable via query engine on cloud tier.
	ctx := context.Background()
	q := query.Query{}
	results, _ := cloudTier.Query.Search(ctx, q, nil)

	var searchCount int
	for rec, err := range results {
		if err != nil {
			t.Fatalf("search iteration error: %v", err)
		}
		_ = rec
		searchCount++
	}

	if searchCount != recordCount {
		t.Errorf("cloud search returned %d records, expected %d", searchCount, recordCount)
	}
}

// ---------- Cloud upload idempotency ----------

// TestTransitionCloudUploadOnlyOneBlob verifies that uploading a sealed chunk
// to cloud produces exactly one blob in the blobstore, and that the blob
// contains all records. This guards against duplicate uploads from racing nodes.
func TestTransitionCloudUploadOnlyOneBlob(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	cloudTierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()
	cloudTier, _ := newCloudFileTier(t, cloudTierID, vaultID, cloudStore)

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})

	vault := NewVault(vaultID, cloudTier)
	vault.Name = "one-blob"
	orch.RegisterVault(vault)

	// Ingest multiple chunks worth of records (3 sealed chunks).
	const recordsPerChunk = 25
	const numChunks = 3
	for c := 0; c < numChunks; c++ {
		for i := range recordsPerChunk {
			if _, _, err := cloudTier.Chunks.Append(makeRecord(fmt.Sprintf("blob-%d-%d", c, i))); err != nil {
				t.Fatal(err)
			}
		}
		if err := cloudTier.Chunks.Seal(); err != nil {
			t.Fatal(err)
		}
	}

	metas, _ := cloudTier.Chunks.List()
	if len(metas) != numChunks {
		t.Fatalf("expected %d sealed chunks, got %d", numChunks, len(metas))
	}

	processor := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
	for _, m := range metas {
		if err := processor.PostSealProcess(context.Background(), m.ID); err != nil {
			t.Fatalf("PostSealProcess(%s): %v", m.ID, err)
		}
	}

	// Count blobs in cloud store — should be exactly numChunks.
	var blobCount int
	if err := cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		blobCount++
		return nil
	}); err != nil {
		t.Fatalf("blobstore List: %v", err)
	}
	if blobCount != numChunks {
		t.Errorf("expected %d blobs in cloud store, got %d", numChunks, blobCount)
	}

	// Verify all chunks are cloud-backed and records readable.
	metasAfter, _ := cloudTier.Chunks.List()
	for _, m := range metasAfter {
		if !m.CloudBacked {
			t.Errorf("chunk %s not cloud-backed", m.ID)
		}
		if m.RecordCount != recordsPerChunk {
			t.Errorf("chunk %s: metadata RecordCount=%d, expected %d", m.ID, m.RecordCount, recordsPerChunk)
		}
	}

	// Verify all records readable.
	allRecords := readAllRecords(t, cloudTier.Chunks)
	if len(allRecords) != numChunks*recordsPerChunk {
		t.Errorf("expected %d total records, got %d", numChunks*recordsPerChunk, len(allRecords))
	}
}

// ==========================================================================
// Multi-node cluster transition tests
//
// These wire up multiple full orchestrators with in-process RemoteTransferrers,
// multi-tier vaults with leader/follower replication, rotation policies that
// create many sealed chunks, and burst ingestion to stress the transition +
// replication pipeline under realistic conditions.
// ==========================================================================

// directTransferrer implements RemoteTransferrer by calling directly into
// the target orchestrator. This is the in-process equivalent of the gRPC
// transferrer used in production — same operations, no network.
type directTransferrer struct {
	nodes map[string]*Orchestrator
}

func (d *directTransferrer) StreamToTier(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	return orch.StreamAppendToTier(ctx, vaultID, tierID, next)
}

func (d *directTransferrer) ReplicateSealedChunk(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	return orch.ImportToTier(ctx, vaultID, tierID, chunkID, next)
}

func (d *directTransferrer) ForwardSealTier(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	return orch.SealActiveTier(vaultID, tierID, chunkID)
}

func (d *directTransferrer) ForwardDeleteChunk(_ context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	return orch.DeleteChunkFromTier(vaultID, tierID, chunkID)
}

func (d *directTransferrer) ForwardTierAppend(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, records []chunk.Record) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	for _, rec := range records {
		if err := orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, rec); err != nil {
			return err
		}
	}
	return nil
}

func (d *directTransferrer) ForwardAppend(_ context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	for _, rec := range records {
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			return err
		}
	}
	return nil
}

func (d *directTransferrer) TransferRecords(ctx context.Context, nodeID string, vaultID uuid.UUID, next chunk.RecordIterator) error {
	orch, ok := d.nodes[nodeID]
	if !ok {
		return fmt.Errorf("directTransferrer: unknown node %q", nodeID)
	}
	for {
		rec, err := next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, _, err := orch.Append(vaultID, rec); err != nil {
			return err
		}
	}
}

func (d *directTransferrer) WaitVaultReady(_ context.Context, _ string, _ uuid.UUID) error {
	return nil
}

// newClusterRetentionRunner creates a retention runner with follower targets
// for proper cross-node delete forwarding.
func newClusterRetentionRunner(orch *Orchestrator, vaultID, tierID uuid.UUID, tier *TierInstance) *retentionRunner {
	return &retentionRunner{
		isLeader:        true,
		vaultID:         vaultID,
		tierID:          tierID,
		cm:              tier.Chunks,
		im:              tier.Indexes,
		orch:            orch,
		followerTargets: tier.FollowerTargets,
		now:             time.Now,
		logger:          slog.Default(),
	}
}

// clusterTestNode is one node in a multi-node cluster test.
type clusterTestNode struct {
	nodeID   string
	orch     *Orchestrator
	tiers    []*TierInstance // all tier instances on this node
	tierDirs []string        // filesystem directories, one per tier
}

// clusterHarness holds the full multi-node cluster.
type clusterHarness struct {
	nodes    map[string]*clusterTestNode
	cfgStore *cfgmem.Store
	vaultID  uuid.UUID
	tierIDs  []uuid.UUID
}

// allNodeIDs returns sorted node IDs.
func (h *clusterHarness) allNodeIDs() []string {
	ids := make([]string, 0, len(h.nodes))
	for id := range h.nodes {
		ids = append(ids, id)
	}
	return ids
}

// cursorCountRecords opens cursors on every chunk (sealed + active) and counts
// records by actually reading them. Does NOT trust ChunkMeta.RecordCount.
func cursorCountRecords(t *testing.T, cm chunk.ChunkManager) int64 {
	t.Helper()
	return int64(len(readAllRecords(t, cm)))
}

// countRecordsOnNode counts all cursor-verified records across all tiers on a node.
func (h *clusterHarness) countRecordsOnNode(t *testing.T, nodeID string) int64 {
	t.Helper()
	node := h.nodes[nodeID]
	var total int64
	for _, tier := range node.tiers {
		total += cursorCountRecords(t, tier.Chunks)
	}
	return total
}

// countRecordsOnTier counts cursor-verified records in a specific tier across ALL nodes.
func (h *clusterHarness) countRecordsOnTier(t *testing.T, tierIdx int) map[string]int64 {
	t.Helper()
	counts := make(map[string]int64)
	for nodeID, node := range h.nodes {
		if tierIdx < len(node.tiers) {
			counts[nodeID] = cursorCountRecords(t, node.tiers[tierIdx].Chunks)
		}
	}
	return counts
}

// countChunksOnTier counts sealed chunks in a specific tier across ALL nodes.
func (h *clusterHarness) countChunksOnTier(t *testing.T, tierIdx int) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for nodeID, node := range h.nodes {
		if tierIdx < len(node.tiers) {
			metas, _ := node.tiers[tierIdx].Chunks.List()
			counts[nodeID] = len(metas)
		}
	}
	return counts
}

// setupCluster creates a multi-node cluster with a shared vault using
// file-backed chunk managers with real filesystem directories.
//
// Layout:
//   - nodeIDs[0] is the leader for all tiers
//   - nodeIDs[1:] are followers for all tiers
//   - Each tier gets its own TempDir per node (real filesystem I/O)
//   - rotationRecords controls the rotation policy (e.g., 100 = seal every 100 records)
//   - The leader's tiers have FollowerTargets pointing to all followers
//   - Every node has a directTransferrer wired to all other nodes
func setupCluster(t *testing.T, nodeIDs []string, tierCount int, rotationRecords uint64) *clusterHarness {
	t.Helper()
	if len(nodeIDs) < 2 {
		t.Fatal("setupCluster needs at least 2 nodes")
	}
	leaderID := nodeIDs[0]
	vaultID := uuid.Must(uuid.NewV7())
	tierIDs := make([]uuid.UUID, tierCount)
	for i := range tierCount {
		tierIDs[i] = uuid.Must(uuid.NewV7())
	}

	// Create config store.
	store := cfgmem.NewStore()
	tierCfgs := make([]config.TierConfig, tierCount)
	for i := range tierCount {
		placements := make([]config.TierPlacement, 0, len(nodeIDs))
		placements = append(placements, config.TierPlacement{
			StorageID: config.SyntheticStorageID(leaderID), Leader: true,
		})
		for _, fid := range nodeIDs[1:] {
			placements = append(placements, config.TierPlacement{
				StorageID: config.SyntheticStorageID(fid), Leader: false,
			})
		}
		tierCfgs[i] = config.TierConfig{
			ID:         tierIDs[i],
			Name:       fmt.Sprintf("tier-%d", i),
			Type:       config.TierTypeFile,
			Placements: placements,
			VaultID:    vaultID,
			Position:   uint32(i),
		}
		_ = store.PutTier(context.Background(), tierCfgs[i])
	}
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cluster-vault",
	})

	// Build follower targets for the leader.
	followerTargets := make([]config.ReplicationTarget, 0, len(nodeIDs)-1)
	for _, fid := range nodeIDs[1:] {
		followerTargets = append(followerTargets, config.ReplicationTarget{NodeID: fid})
	}

	// Create all orchestrators with file-backed tiers.
	orchs := make(map[string]*Orchestrator)
	nodes := make(map[string]*clusterTestNode)

	for _, nid := range nodeIDs {
		orch := newTestOrch(t, Config{LocalNodeID: nid})
		orch.cfgLoader = &transitionConfigLoader{store: store}
		orchs[nid] = orch

		isLeader := nid == leaderID
		tiers := make([]*TierInstance, tierCount)
		tierDirs := make([]string, tierCount)
		for i := range tierCount {
			dir := t.TempDir()
			tierDirs[i] = dir
			cm, cmErr := chunkfile.NewManager(chunkfile.Config{
				Dir:            dir,
				Now:            time.Now,
				RotationPolicy: chunk.NewRecordCountPolicy(rotationRecords),
			})
			if cmErr != nil {
				t.Fatal(cmErr)
			}
			im := indexfile.NewManager(dir, nil, nil)
			tier := &TierInstance{
				TierID:  tierIDs[i],
				Type:    "file",
				Chunks:  cm,
				Indexes: im,
				Query:   query.New(cm, im, nil),
			}
			if isLeader {
				tier.FollowerTargets = followerTargets
			} else {
				tier.IsFollower = true
				tier.LeaderNodeID = leaderID
			}
			tiers[i] = tier
		}

		vault := NewVault(vaultID, tiers...)
		vault.Name = "cluster-vault"
		orch.RegisterVault(vault)

		nodes[nid] = &clusterTestNode{
			nodeID:   nid,
			orch:     orch,
			tiers:    tiers,
			tierDirs: tierDirs,
		}
	}

	// Wire directTransferrer: each node can reach all other nodes.
	for _, nid := range nodeIDs {
		remotes := make(map[string]*Orchestrator)
		for _, other := range nodeIDs {
			if other != nid {
				remotes[other] = orchs[other]
			}
		}
		orchs[nid].SetRemoteTransferrer(&directTransferrer{nodes: remotes})
	}

	t.Cleanup(func() {
		// Close file managers BEFORE t.TempDir cleanup removes their directories.
		// Stop orchestrators first (stops schedulers), then close chunk managers.
		for _, n := range nodes {
			n.orch.Stop()
		}
		for _, n := range nodes {
			for _, tier := range n.tiers {
				_ = tier.Chunks.Close()
			}
		}
	})

	return &clusterHarness{
		nodes:    nodes,
		cfgStore: store,
		vaultID:  vaultID,
		tierIDs:  tierIDs,
	}
}

// assertTierDirEmpty verifies that a tier's filesystem directory contains no
// chunk subdirectories on ANY node. This goes below the chunk manager API —
// it checks the actual filesystem to catch silent delete failures, leaked
// directories, and stale files.
func (h *clusterHarness) assertTierDirEmpty(t *testing.T, tierIdx int) {
	t.Helper()
	for _, nid := range h.allNodeIDs() {
		node := h.nodes[nid]
		dir := node.tierDirs[tierIdx]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Errorf("tier %d on %s: ReadDir(%s): %v", tierIdx, nid, dir, err)
			continue
		}
		// Filter to only chunk directories (chunk IDs are 26-char base32).
		// Skip .lock, index files, and other manager artifacts.
		var chunkDirs []string
		for _, e := range entries {
			if e.IsDir() && len(e.Name()) == 26 {
				chunkDirs = append(chunkDirs, e.Name())
			}
		}
		if len(chunkDirs) > 0 {
			t.Errorf("tier %d on %s: %d chunk directories still on disk: %v",
				tierIdx, nid, len(chunkDirs), chunkDirs)
		}
	}
}

// listChunkDirsOnNode returns the chunk directory names in a tier dir on a node.
func (h *clusterHarness) listChunkDirsOnNode(t *testing.T, nodeID string, tierIdx int) []string {
	t.Helper()
	dir := h.nodes[nodeID].tierDirs[tierIdx]
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			dirs = append(dirs, e.Name())
		}
	}
	return dirs
}

// TestClusterTransitionBurstNoOrphans creates a 4-node cluster with 2 tiers,
// bursts 10K records with a 100-record rotation policy (100 sealed chunks),
// transitions all chunks from tier 0 → tier 1, and verifies:
//   - All records arrive in tier 1 on the LEADER
//   - Source tier 0 is empty on ALL nodes
//   - No records are lost or duplicated
//   - Record count matches on the leader
func TestClusterTransitionBurstNoOrphans(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "follower-1", "follower-2", "follower-3"}, 2, 100)

	leaderNode := h.nodes["leader"]
	tier0 := leaderNode.tiers[0]

	// Burst ingest 10K records into tier 0 on the leader.
	const totalRecords = 10_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		rec := chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "burst-%d", i),
		}
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, rec); err != nil {
			t.Fatalf("AppendToTier failed at record %d: %v", i, err)
		}
	}

	// Seal the last active chunk if any records remain.
	if active := tier0.Chunks.Active(); active != nil && active.RecordCount > 0 {
		if err := tier0.Chunks.Seal(); err != nil {
			t.Fatalf("final seal: %v", err)
		}
	}

	// Count sealed chunks — should be ~100 (totalRecords/rotationRecords).
	metas0, _ := tier0.Chunks.List()
	if len(metas0) < 50 {
		t.Fatalf("expected many sealed chunks from rotation, got %d", len(metas0))
	}
	t.Logf("tier 0 leader: %d sealed chunks", len(metas0))

	// Verify total records in tier 0 on leader (cursor-verified, not metadata).
	tier0LeaderCount := cursorCountRecords(t, tier0.Chunks)
	if tier0LeaderCount != totalRecords {
		t.Fatalf("tier 0 leader: expected %d records, got %d", totalRecords, tier0LeaderCount)
	}

	// Transition ALL sealed chunks from tier 0 → tier 1.
	runner := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[0], tier0)
	for _, m := range metas0 {
		runner.transitionChunk(m.ID)
	}
	// Also transition any chunk that was active when we listed.
	if active := tier0.Chunks.Active(); active != nil && active.RecordCount > 0 {
		if err := tier0.Chunks.Seal(); err != nil {
			t.Fatalf("post-transition seal: %v", err)
		}
		metas0Extra, _ := tier0.Chunks.List()
		for _, m := range metas0Extra {
			runner.transitionChunk(m.ID)
		}
	}

	// ---- Verify: tier 0 is EMPTY on ALL nodes (cursor-verified) ----
	for _, nodeID := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nodeID].tiers[0].Chunks)
		if count != 0 {
			metas, _ := h.nodes[nodeID].tiers[0].Chunks.List()
			t.Errorf("tier 0 on %s: cursor read %d records (should be 0, %d sealed chunks remain)",
				nodeID, count, len(metas))
		}
	}

	// ---- Verify: tier 0 chunk directories removed from disk on ALL nodes ----
	h.assertTierDirEmpty(t, 0)

	// ---- Verify: tier 1 on leader has ALL records (cursor-verified) ----
	tier1LeaderCount := cursorCountRecords(t, leaderNode.tiers[1].Chunks)
	if tier1LeaderCount != totalRecords {
		t.Errorf("tier 1 leader: cursor read %d records, expected %d", tier1LeaderCount, totalRecords)
	}

	// ---- Verify: no net duplication across entire cluster ----
	// Total records across all tiers on the leader should equal totalRecords.
	leaderTotal := h.countRecordsOnNode(t, "leader")
	if leaderTotal != totalRecords {
		t.Errorf("leader total across all tiers: expected %d, got %d", totalRecords, leaderTotal)
	}
}

// TestClusterTransitionThreeTierChainBurst creates a 4-node cluster with
// 3 tiers and bursts 10K records through the full tier chain with 100-record
// rotation. Verifies no orphans on any node and exact record preservation.
func TestClusterTransitionThreeTierChainBurst(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 3, 100)

	leaderNode := h.nodes["leader"]
	tier0 := leaderNode.tiers[0]

	const totalRecords = 10_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "chain3-%d", i),
		}); err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
	}

	// Seal and transition tier 0 → tier 1.
	if active := tier0.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = tier0.Chunks.Seal()
	}
	metas0, _ := tier0.Chunks.List()
	t.Logf("tier 0: %d sealed chunks to transition", len(metas0))
	runner0 := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[0], tier0)
	for _, m := range metas0 {
		runner0.transitionChunk(m.ID)
	}

	// Verify tier 0 empty on leader (cursor-verified).
	if got := cursorCountRecords(t, tier0.Chunks); got != 0 {
		t.Fatalf("tier 0 leader should be empty after transition, cursor read %d", got)
	}

	// Tier 1 should have all records. Seal and transition tier 1 → tier 2.
	tier1 := leaderNode.tiers[1]
	tier1Count := cursorCountRecords(t, tier1.Chunks)
	if tier1Count != totalRecords {
		t.Fatalf("tier 1 leader: expected %d, got %d", totalRecords, tier1Count)
	}

	if active := tier1.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = tier1.Chunks.Seal()
	}
	metas1, _ := tier1.Chunks.List()
	t.Logf("tier 1: %d sealed chunks to transition", len(metas1))
	runner1 := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[1], tier1)
	for _, m := range metas1 {
		runner1.transitionChunk(m.ID)
	}

	// ---- Final state: ONLY tier 2 on leader has records ----
	for tierIdx := range 3 {
		counts := h.countRecordsOnTier(t, tierIdx)
		leaderCount := counts["leader"]
		expected := int64(0)
		if tierIdx == 2 {
			expected = totalRecords
		}
		if leaderCount != expected {
			t.Errorf("tier %d leader: expected %d records, got %d", tierIdx, expected, leaderCount)
		}
	}

	// ---- Verify tier 0 empty on ALL nodes (cursor-verified) ----
	for _, nid := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nid].tiers[0].Chunks)
		if count != 0 {
			t.Errorf("tier 0 on %s: cursor read %d records after full chain (should be 0)", nid, count)
		}
	}

	// ---- Verify tier 0 AND tier 1 chunk directories gone from disk ----
	h.assertTierDirEmpty(t, 0)
	h.assertTierDirEmpty(t, 1)

	// ---- Verify no net duplication on leader (cursor-verified) ----
	leaderTotal := h.countRecordsOnNode(t, "leader")
	if leaderTotal != totalRecords {
		t.Errorf("leader total: expected %d across all tiers, got %d", totalRecords, leaderTotal)
	}
}

// TestClusterTransitionEventIDPreservedAcrossNodes verifies that EventIDs
// survive transitions in a multi-node cluster with replication.
func TestClusterTransitionEventIDPreservedAcrossNodes(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 2, 100)

	leaderNode := h.nodes["leader"]
	tier0 := leaderNode.tiers[0]

	// Ingest records with distinct EventIDs.
	ingesterID := uuid.Must(uuid.NewV7())
	const totalRecords = 5_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		rec := chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "eid-cluster-%d", i),
			EventID: chunk.EventID{
				IngesterID: ingesterID,
				IngestTS:   ts,
				IngestSeq:  uint32(i),
			},
		}
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, rec); err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
	}

	// Seal and transition.
	if active := tier0.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = tier0.Chunks.Seal()
	}
	metas0, _ := tier0.Chunks.List()
	runner := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[0], tier0)
	for _, m := range metas0 {
		runner.transitionChunk(m.ID)
	}

	// Read records from tier 1 on leader and verify EventIDs.
	tier1Records := readAllRecords(t, leaderNode.tiers[1].Chunks)
	if len(tier1Records) != totalRecords {
		t.Fatalf("tier 1: expected %d records, got %d", totalRecords, len(tier1Records))
	}

	// Verify every record has a valid EventID (not zero).
	var zeroEventIDs int
	var wrongIngester int
	for _, rec := range tier1Records {
		if rec.EventID == (chunk.EventID{}) {
			zeroEventIDs++
		} else if rec.EventID.IngesterID != ingesterID {
			wrongIngester++
		}
	}
	if zeroEventIDs > 0 {
		t.Errorf("%d records lost their EventID after cluster transition", zeroEventIDs)
	}
	if wrongIngester > 0 {
		t.Errorf("%d records have wrong IngesterID after cluster transition", wrongIngester)
	}
}

// TestClusterTransitionLargeBurst ingests a large burst (10K records) through
// the serialized AppendToTier path and verifies no data loss after transition.
// The burst creates ~100 sealed chunks via the 100-record rotation policy.
//
// NOTE: concurrent Append through the file chunk manager's attr.log writer
// is not safe (see gastrolog-3l7ow findings). Production serializes through
// the digest loop. This test uses sequential ingestion to match that model.
func TestClusterTransitionLargeBurst(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 2, 100)

	leaderNode := h.nodes["leader"]
	tier0 := leaderNode.tiers[0]

	const totalRecords = 10_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		rec := chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "burst-%d", i),
		}
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Seal remaining active chunk.
	if active := tier0.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = tier0.Chunks.Seal()
	}

	// Count what we have before transition (cursor-verified).
	tier0Count := cursorCountRecords(t, tier0.Chunks)
	if tier0Count != totalRecords {
		t.Fatalf("tier 0 after concurrent burst: cursor read %d, expected %d", tier0Count, totalRecords)
	}

	metas0, _ := tier0.Chunks.List()
	t.Logf("tier 0: %d sealed chunks from concurrent burst", len(metas0))

	// Transition all chunks.
	runner := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[0], tier0)
	for _, m := range metas0 {
		runner.transitionChunk(m.ID)
	}

	// Verify: tier 0 empty on ALL nodes, tier 1 has all records on leader (all cursor-verified).
	for _, nid := range h.allNodeIDs() {
		remaining := cursorCountRecords(t, h.nodes[nid].tiers[0].Chunks)
		if remaining != 0 {
			t.Errorf("tier 0 on %s: cursor read %d records after transition (should be 0)", nid, remaining)
		}
	}

	// Verify: tier 0 chunk directories gone from disk on ALL nodes.
	h.assertTierDirEmpty(t, 0)

	tier1Count := cursorCountRecords(t, leaderNode.tiers[1].Chunks)
	if tier1Count != totalRecords {
		t.Errorf("tier 1 leader: cursor read %d records, expected %d (lost %d)", tier1Count, totalRecords, totalRecords-tier1Count)
	}
}

// TestClusterTransitionNoChunksLeftBehindOnFollowers verifies that after
// transition, the source tier's sealed chunks are cleaned up on follower nodes
// (via deleteFromFollowers), not just on the leader.
func TestClusterTransitionNoChunksLeftBehindOnFollowers(t *testing.T) {
	t.Parallel()
	h := setupCluster(t, []string{"leader", "f1", "f2", "f3"}, 2, 100)

	leaderNode := h.nodes["leader"]
	tier0Leader := leaderNode.tiers[0]

	// Ingest records — rotation at 100 creates multiple sealed chunks.
	const totalRecords = 1_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "follower-cleanup-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if active := tier0Leader.Chunks.Active(); active != nil && active.RecordCount > 0 {
		_ = tier0Leader.Chunks.Seal()
	}

	// Capture chunk IDs before transition.
	metas0, _ := tier0Leader.Chunks.List()
	originalChunkIDs := make(map[chunk.ChunkID]bool)
	for _, m := range metas0 {
		originalChunkIDs[m.ID] = true
	}
	t.Logf("tier 0: %d sealed chunks to transition", len(metas0))

	// Transition all chunks.
	runner := newClusterRetentionRunner(leaderNode.orch, h.vaultID, h.tierIDs[0], tier0Leader)
	for _, m := range metas0 {
		runner.transitionChunk(m.ID)
	}

	// ---- Verify: NO original chunks exist on ANY node in tier 0 ----
	for _, nid := range h.allNodeIDs() {
		node := h.nodes[nid]
		tier0CM := node.tiers[0].Chunks
		metas, _ := tier0CM.List()
		for _, m := range metas {
			if originalChunkIDs[m.ID] {
				t.Errorf("tier 0 on %s: chunk %s still exists after transition (should be deleted)",
					nid, m.ID)
			}
		}
		// Also check active chunk.
		if active := tier0CM.Active(); active != nil && originalChunkIDs[active.ID] {
			t.Errorf("tier 0 on %s: chunk %s is still active after transition", nid, active.ID)
		}
	}

	// ---- Verify: tier 0 has 0 cursor-readable records on ALL nodes ----
	for _, nid := range h.allNodeIDs() {
		count := cursorCountRecords(t, h.nodes[nid].tiers[0].Chunks)
		if count != 0 {
			t.Errorf("tier 0 on %s: cursor read %d records (should be 0)", nid, count)
		}
	}

	// ---- Verify: no chunk directories on disk for tier 0 on ANY node ----
	h.assertTierDirEmpty(t, 0)
}

// ==========================================================================
// Multi-node drain tests
// ==========================================================================

// waitForDrainJob polls the scheduler until the drain job completes or times out.
// Uses ListJobs which returns snapshots — no race with the scheduler goroutine.
func waitForDrainJob(t *testing.T, orch *Orchestrator, vaultID uuid.UUID, timeout time.Duration) {
	t.Helper()
	jobName := "drain:" + vaultID.String()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// ListJobs returns snapshot copies under the scheduler's lock.
		for _, j := range orch.Scheduler().ListJobs() {
			if j.Name != jobName {
				continue
			}
			snap := j.Snapshot()
			if snap.Progress != nil && snap.Progress.Status == JobStatusCompleted {
				return
			}
			if snap.Progress != nil && snap.Progress.Status == JobStatusFailed {
				t.Fatalf("drain job failed: %s", snap.Progress.Error)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("drain job did not complete within %s", timeout)
}

// TestClusterDrainVaultRecordsArriveOnDestination drains a file-backed vault
// from node-A to node-B via directTransferrer. Verifies:
//   - All records arrive on node-B (cursor-verified)
//   - Source vault unregistered on node-A
//   - Source chunk directories removed from disk
func TestClusterDrainVaultRecordsArriveOnDestination(t *testing.T) {
	t.Parallel()

	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())

	// Config store — both nodes share the same vault/tier config.
	store := cfgmem.NewStore()
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tierID, Name: "hot", Type: config.TierTypeFile,
		VaultID: vaultID, Position: 0,
		Placements: []config.TierPlacement{
			{StorageID: config.SyntheticStorageID("node-A"), Leader: true},
		},
	})
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "drain-test",
	})
	_ = store.PutFilter(context.Background(), config.FilterConfig{
		ID: uuid.Must(uuid.NewV7()), Name: "catch-all", Expression: "*",
	})

	// Create node-A (source) with file-backed tier.
	dirA := t.TempDir()
	orchA, err := New(Config{
		LocalNodeID:  "node-A",
		ConfigLoader: &transitionConfigLoader{store: store},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmA, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dirA,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	imA := indexfile.NewManager(dirA, nil, nil)
	tierA := &TierInstance{TierID: tierID, Type: "file", Chunks: cmA, Indexes: imA, Query: query.New(cmA, imA, nil)}
	orchA.RegisterVault(NewVault(vaultID, tierA))

	// Create node-B (destination) with file-backed tier.
	dirB := t.TempDir()
	orchB, err := New(Config{
		LocalNodeID:  "node-B",
		ConfigLoader: &transitionConfigLoader{store: store},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmB, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dirB,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	imB := indexfile.NewManager(dirB, nil, nil)
	tierB := &TierInstance{TierID: tierID, Type: "file", Chunks: cmB, Indexes: imB, Query: query.New(cmB, imB, nil)}
	orchB.RegisterVault(NewVault(vaultID, tierB))

	// Wire directTransferrer between the two nodes.
	orchA.SetRemoteTransferrer(&directTransferrer{nodes: map[string]*Orchestrator{"node-B": orchB}})
	orchB.SetRemoteTransferrer(&directTransferrer{nodes: map[string]*Orchestrator{"node-A": orchA}})

	t.Cleanup(func() {
		orchA.Stop()
		orchB.Stop()
		_ = cmA.Close()
		_ = cmB.Close()
	})

	// Ingest 1K records on node-A (10 sealed chunks).
	const totalRecords = 1_000
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range totalRecords {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := orchA.Append(vaultID, chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			Raw:      fmt.Appendf(nil, "drain-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Verify records on node-A before drain.
	preCount := cursorCountRecords(t, cmA)
	if preCount != totalRecords {
		t.Fatalf("node-A pre-drain: cursor read %d, expected %d", preCount, totalRecords)
	}

	// Start drain from node-A to node-B.
	if err := orchA.DrainVault(context.Background(), vaultID, "node-B"); err != nil {
		t.Fatalf("DrainVault: %v", err)
	}

	// Wait for drain to complete.
	waitForDrainJob(t, orchA, vaultID, 30*time.Second)

	// ---- Verify: node-B has all records (cursor-verified) ----
	destCount := cursorCountRecords(t, cmB)
	if destCount != totalRecords {
		t.Errorf("node-B: cursor read %d records, expected %d (lost %d)", destCount, totalRecords, totalRecords-destCount)
	}

	// ---- Verify: node-A vault unregistered ----
	if orchA.VaultExists(vaultID) {
		t.Error("node-A: vault should be unregistered after drain")
	}

	// ---- Verify: node-A chunk directories removed from disk ----
	entries, err := os.ReadDir(dirA)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dirA, err)
	}
	var chunkDirs []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 26 {
			chunkDirs = append(chunkDirs, e.Name())
		}
	}
	if len(chunkDirs) > 0 {
		t.Errorf("node-A: %d chunk directories still on disk after drain: %v", len(chunkDirs), chunkDirs)
	}
}

// --- Memory budget enforcement ---

func TestMemoryBudgetEnforcementTransitionsChunks(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	// Memory tier with 500-byte budget. Each record is ~100 bytes.
	// With budget/10 = 50 bytes per chunk, each chunk holds ~1 record.
	memCM, _ := chunkmem.NewFactory()(map[string]string{"budgetBytes": "500"}, nil)
	memIM, _ := indexmem.NewFactory()(nil, memCM, nil)

	// File tier as destination.
	dir := t.TempDir()
	fileCM, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000)})
	if err != nil {
		t.Fatal(err)
	}
	fileIM := indexfile.NewManager(dir, nil, nil)

	store := cfgmem.NewStore()
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "mem", Type: config.TierTypeMemory,
		Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "file", Type: config.TierTypeFile,
		Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 1,
	})
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "budget-test",
	})

	orch := newTestOrch(t, Config{
		LocalNodeID:  nodeID,
		ConfigLoader: &transitionConfigLoader{store: store},
	})

	memTier := &TierInstance{
		TierID: tier0ID, Type: "memory",
		Chunks: memCM, Indexes: memIM, Query: query.New(memCM, memIM, nil),
	}
	fileTier := &TierInstance{
		TierID: tier1ID, Type: "file",
		Chunks: fileCM, Indexes: fileIM, Query: query.New(fileCM, fileIM, nil),
	}
	orch.RegisterVault(NewVault(vaultID, memTier, fileTier))

	// Ingest records until well over budget.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 50 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = orch.AppendToTier(vaultID, tier0ID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: make([]byte, 100),
		})
	}

	// Seal any active chunk.
	if active := memCM.Active(); active != nil && active.RecordCount > 0 {
		_ = memCM.Seal()
	}

	memBefore := memCM.(*chunkmem.Manager).TotalBytes()
	if memBefore <= 500 {
		t.Fatalf("expected memory tier to exceed 500-byte budget, got %d", memBefore)
	}

	// Run retention sweep — should enforce budget and transition excess.
	orch.retentionSweepAll()

	memAfter := memCM.(*chunkmem.Manager).TotalBytes()
	if memAfter >= memBefore {
		t.Errorf("expected budget enforcement to reduce memory usage: before=%d, after=%d", memBefore, memAfter)
	}

	// Verify records arrived at the file tier.
	fileMetas, _ := fileCM.List()
	fileActive := fileCM.Active()
	var fileRecords int64
	for _, m := range fileMetas {
		fileRecords += m.RecordCount
	}
	if fileActive != nil {
		fileRecords += fileActive.RecordCount
	}
	if fileRecords == 0 {
		t.Error("expected records to transition to file tier")
	}
}

func TestMemoryBudgetEnforcementTerminalTierNoTransition(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	nodeID := "test-node"

	// Memory tier with tiny budget, NO next tier. Budget enforcement
	// should not panic or lose data — chunks stay even if over budget.
	memCM, _ := chunkmem.NewFactory()(map[string]string{"budgetBytes": "100"}, nil)
	memIM, _ := indexmem.NewFactory()(nil, memCM, nil)

	store := cfgmem.NewStore()
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tierID, Name: "mem-terminal", Type: config.TierTypeMemory,
		Placements: syntheticPlacements(nodeID),
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "terminal-budget",
	})

	orch := newTestOrch(t, Config{
		LocalNodeID:  nodeID,
		ConfigLoader: &transitionConfigLoader{store: store},
	})

	tier := &TierInstance{
		TierID: tierID, Type: "memory",
		Chunks: memCM, Indexes: memIM, Query: query.New(memCM, memIM, nil),
	}
	orch.RegisterVault(NewVault(vaultID, tier))

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 20 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: make([]byte, 50),
		})
	}
	if active := memCM.Active(); active != nil && active.RecordCount > 0 {
		_ = memCM.Seal()
	}

	beforeMetas, _ := memCM.List()
	beforeCount := len(beforeMetas)

	// Run retention sweep — budget enforcement tries to transition but
	// there's no next tier. Chunks should survive (logged as warning, not lost).
	orch.retentionSweepAll()

	afterMetas, _ := memCM.List()
	if len(afterMetas) != beforeCount {
		t.Errorf("terminal tier: chunks should survive when no next tier exists: before=%d, after=%d",
			beforeCount, len(afterMetas))
	}
}

func TestMemoryBudgetEnforcementOnlyRunsOnLeader(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	tier0ID := uuid.Must(uuid.NewV7())
	tier1ID := uuid.Must(uuid.NewV7())
	leaderNode := "leader"
	followerNode := "follower"

	// Create two orchestrators: leader and follower.
	// Only the leader should enforce the budget.
	memCMLeader, _ := chunkmem.NewFactory()(map[string]string{"budgetBytes": "100"}, nil)
	memIMLeader, _ := indexmem.NewFactory()(nil, memCMLeader, nil)
	memCMFollower, _ := chunkmem.NewFactory()(map[string]string{"budgetBytes": "100"}, nil)
	memIMFollower, _ := indexmem.NewFactory()(nil, memCMFollower, nil)

	fileDirLeader := t.TempDir()
	fileCMLeader, _ := chunkfile.NewManager(chunkfile.Config{Dir: fileDirLeader, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000)})
	fileIMLeader := indexfile.NewManager(fileDirLeader, nil, nil)

	fileDirFollower := t.TempDir()
	fileCMFollower, _ := chunkfile.NewManager(chunkfile.Config{Dir: fileDirFollower, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000)})
	fileIMFollower := indexfile.NewManager(fileDirFollower, nil, nil)

	store := cfgmem.NewStore()
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier0ID, Name: "mem", Type: config.TierTypeMemory,
		Placements: []config.TierPlacement{
			{StorageID: config.SyntheticStorageID(leaderNode), Leader: true},
			{StorageID: config.SyntheticStorageID(followerNode)},
		},
		VaultID: vaultID, Position: 0,
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tier1ID, Name: "file", Type: config.TierTypeFile,
		Placements: syntheticPlacements(leaderNode),
		VaultID: vaultID, Position: 1,
	})
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "budget-leader-only",
	})

	orchLeader := newTestOrch(t, Config{
		LocalNodeID:  leaderNode,
		ConfigLoader: &transitionConfigLoader{store: store},
	})
	orchFollower := newTestOrch(t, Config{
		LocalNodeID:  followerNode,
		ConfigLoader: &transitionConfigLoader{store: store},
	})

	leaderMemTier := &TierInstance{
		TierID: tier0ID, Type: "memory",
		Chunks: memCMLeader, Indexes: memIMLeader, Query: query.New(memCMLeader, memIMLeader, nil),
	}
	leaderFileTier := &TierInstance{
		TierID: tier1ID, Type: "file",
		Chunks: fileCMLeader, Indexes: fileIMLeader, Query: query.New(fileCMLeader, fileIMLeader, nil),
	}
	orchLeader.RegisterVault(NewVault(vaultID, leaderMemTier, leaderFileTier))

	followerMemTier := &TierInstance{
		TierID: tier0ID, Type: "memory", IsFollower: true, LeaderNodeID: leaderNode,
		Chunks: memCMFollower, Indexes: memIMFollower, Query: query.New(memCMFollower, memIMFollower, nil),
	}
	followerFileTier := &TierInstance{
		TierID: tier1ID, Type: "file", IsFollower: true, LeaderNodeID: leaderNode,
		Chunks: fileCMFollower, Indexes: fileIMFollower, Query: query.New(fileCMFollower, fileIMFollower, nil),
	}
	orchFollower.RegisterVault(NewVault(vaultID, followerMemTier, followerFileTier))

	// Ingest on both — both exceed budget.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 20 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		rec := chunk.Record{IngestTS: ts, WriteTS: ts, Raw: make([]byte, 50)}
		_ = orchLeader.AppendToTier(vaultID, tier0ID, chunk.ChunkID{}, rec)
		_ = orchFollower.AppendToTier(vaultID, tier0ID, chunk.ChunkID{}, rec)
	}
	if active := memCMLeader.Active(); active != nil && active.RecordCount > 0 {
		_ = memCMLeader.Seal()
	}
	if active := memCMFollower.Active(); active != nil && active.RecordCount > 0 {
		_ = memCMFollower.Seal()
	}

	leaderBefore := memCMLeader.(*chunkmem.Manager).TotalBytes()
	followerBefore := memCMFollower.(*chunkmem.Manager).TotalBytes()

	// Run sweep on both.
	orchLeader.retentionSweepAll()
	orchFollower.retentionSweepAll()

	leaderAfter := memCMLeader.(*chunkmem.Manager).TotalBytes()
	followerAfter := memCMFollower.(*chunkmem.Manager).TotalBytes()

	// Leader should have drained excess.
	if leaderAfter >= leaderBefore {
		t.Errorf("leader should enforce budget: before=%d, after=%d", leaderBefore, leaderAfter)
	}
	// Follower should NOT drain (not leader).
	if followerAfter != followerBefore {
		t.Errorf("follower should not enforce budget: before=%d, after=%d", followerBefore, followerAfter)
	}
}

// TestExplicitStorageLeaderGetsRotationPolicy verifies that a tier built via
// buildTierInstanceForStorage (explicit placement path) applies the rotation
// policy from config. Regression test for a gap where applyRotationPolicy was
// only called in buildTierInstance but not buildTierInstanceForStorage.
func TestExplicitStorageLeaderGetsRotationPolicy(t *testing.T) {
	t.Parallel()
	nodeID := "test-node"
	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	fsID := uuid.Must(uuid.NewV7())
	policyID := uuid.Must(uuid.NewV7())

	storageDir := t.TempDir()
	tierDir := filepath.Join(storageDir, "vaults", vaultID.String(), tierID.String())
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}

	maxRecords := int64(3)
	cfg := &config.Config{
		Vaults: []config.VaultConfig{{
			ID:   vaultID,
			Name: "rotation-test",
		}},
		Tiers: []config.TierConfig{{
			VaultID:          vaultID,
			Position:         0,
			ID:               tierID,
			Name:             "file",
			Type:             config.TierTypeFile,
			RotationPolicyID: &policyID,
			Placements: []config.TierPlacement{
				{StorageID: fsID.String(), Leader: true},
			},
		}},
		RotationPolicies: []config.RotationPolicyConfig{{
			ID:         policyID,
			Name:       "3-records",
			MaxRecords: &maxRecords,
		}},
		NodeStorageConfigs: []config.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []config.FileStorage{{
				ID:           fsID,
				StorageClass: 1,
				Name:         "disk-1",
				Path:         storageDir,
			}},
		}},
	}

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file": chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file": indexfile.NewFactory(),
		},
		VaultsDir: storageDir,
		Logger:    slog.Default(),
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	defer orch.Stop()

	store := cfgmem.NewStore()
	orch.cfgLoader = &transitionConfigLoader{store: store}

	if err := orch.ApplyConfig(cfg, factories); err != nil {
		t.Fatalf("ApplyConfig failed: %v", err)
	}

	orch.mu.RLock()
	vault := orch.vaults[vaultID]
	orch.mu.RUnlock()
	if vault == nil {
		t.Fatal("vault not created")
	}
	if len(vault.Tiers) != 1 {
		t.Fatalf("expected 1 tier, got %d", len(vault.Tiers))
	}

	tier := vault.Tiers[0]

	// Ingest enough records to trigger rotation (maxRecords=3).
	for i := range 5 {
		if _, _, err := tier.Chunks.Append(makeRecord(fmt.Sprintf("rot-%d", i))); err != nil {
			t.Fatalf("append[%d]: %v", i, err)
		}
	}

	// If the rotation policy was applied, we should have at least 2 chunks
	// (3 records in the first, 2 in the second).
	metas, err := tier.Chunks.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) < 2 {
		t.Fatalf("expected at least 2 chunks from rotation (maxRecords=3, 5 appended), got %d — rotation policy not applied via buildTierInstanceForStorage", len(metas))
	}
}
