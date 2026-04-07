package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

// fakeChunkManager implements chunk.ChunkManager for testing.
type fakeChunkManager struct{}

func (f *fakeChunkManager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	return chunk.ChunkID{}, 0, nil
}
func (f *fakeChunkManager) Seal() error              { return nil }
func (f *fakeChunkManager) Active() *chunk.ChunkMeta { return nil }
func (f *fakeChunkManager) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	return chunk.ChunkMeta{}, nil
}
func (f *fakeChunkManager) List() ([]chunk.ChunkMeta, error)                        { return nil, nil }
func (f *fakeChunkManager) OpenCursor(id chunk.ChunkID) (chunk.RecordCursor, error) { return nil, nil }
func (f *fakeChunkManager) FindStartPosition(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *fakeChunkManager) FindIngestStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *fakeChunkManager) FindSourceStartPosition(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *fakeChunkManager) ReadWriteTimestamps(id chunk.ChunkID, positions []uint64) ([]time.Time, error) {
	return nil, nil
}
func (f *fakeChunkManager) SetRotationPolicy(policy chunk.RotationPolicy) {}
func (f *fakeChunkManager) CheckRotation() *string                        { return nil }
func (f *fakeChunkManager) Delete(id chunk.ChunkID) error                 { return nil }
func (f *fakeChunkManager) ImportRecords(chunk.RecordIterator) (chunk.ChunkMeta, error) { return chunk.ChunkMeta{}, nil }
func (f *fakeChunkManager) ScanAttrs(_ chunk.ChunkID, _ uint64, _ func(time.Time, chunk.Attributes) bool) error {
	return nil
}
func (f *fakeChunkManager) SetNextChunkID(_ chunk.ChunkID) {}
func (f *fakeChunkManager) Close() error                   { return nil }

// fakeIndexManager implements index.IndexManager for testing.
type fakeIndexManager struct{}

func (f *fakeIndexManager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return nil
}
func (f *fakeIndexManager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenAttrKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKeyIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenAttrValueIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrValueIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenAttrKVIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKVIndexEntry], error) {
	return nil, nil
}
func (f *fakeIndexManager) OpenKVKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.KVKeyIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *fakeIndexManager) OpenKVValueIndex(chunkID chunk.ChunkID) (*index.Index[index.KVValueIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *fakeIndexManager) OpenKVIndex(chunkID chunk.ChunkID) (*index.Index[index.KVIndexEntry], index.KVIndexStatus, error) {
	return nil, index.KVComplete, nil
}
func (f *fakeIndexManager) IndexesComplete(chunkID chunk.ChunkID) (bool, error) {
	return true, nil
}
func (f *fakeIndexManager) DeleteIndexes(chunkID chunk.ChunkID) error { return nil }
func (f *fakeIndexManager) FindIngestStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}
func (f *fakeIndexManager) FindSourceStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}
func (f *fakeIndexManager) OpenJSONPathIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPathIndexEntry], index.JSONIndexStatus, error) {
	return nil, index.JSONComplete, nil
}
func (f *fakeIndexManager) OpenJSONPVIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPVIndexEntry], index.JSONIndexStatus, error) {
	return nil, index.JSONComplete, nil
}
func (f *fakeIndexManager) LoadIngestEntries(chunkID chunk.ChunkID) ([]index.TSEntry, error) {
	return nil, index.ErrIndexNotFound
}
func (f *fakeIndexManager) LoadSourceEntries(chunkID chunk.ChunkID) ([]index.TSEntry, error) {
	return nil, index.ErrIndexNotFound
}
func (f *fakeIndexManager) IndexSizes(chunkID chunk.ChunkID) map[string]int64 {
	return map[string]int64{}
}
func (f *fakeIndexManager) BuildAdapter() chunk.ChunkIndexBuilder { return nil }

// testVaultCfg creates a VaultConfig + TierConfig pair for tests.
// tierType is the tier type (e.g., config.TierTypeMemory or "test").
func testVaultCfg(vaultID uuid.UUID, tierType config.TierType) (config.VaultConfig, config.TierConfig) {
	tierID := uuid.Must(uuid.NewV7())
	return config.VaultConfig{
			ID:      vaultID,
			Enabled: true,
		}, config.TierConfig{
			ID:      tierID,
			Name:    "tier-" + vaultID.String()[:8],
			Type:    tierType,
			VaultID: vaultID,
		}
}

// fakeIngester implements Ingester for testing.
type fakeIngester struct{}

func (f *fakeIngester) Run(ctx context.Context, out chan<- IngestMessage) error {
	<-ctx.Done()
	return nil
}

func TestApplyConfigNil(t *testing.T) {
	orch := newTestOrch(t, Config{})
	err := orch.ApplyConfig(nil, Factories{})
	if err != nil {
		t.Errorf("expected nil error for nil config, got %v", err)
	}
}

// TestApplyConfigVaultWithNoLocalTiers is the regression test for
// gastrolog-264pk. Before the fix, ApplyConfig (the startup path) would
// silently skip registering any vault whose buildTierInstances returned
// zero local tiers — which happens on a node that isn't a placement
// target for any of the vault's tiers (e.g. a voteless node that joined
// the cluster as a non-tier-member, or a snapshot-restored node where
// placements are reapplied via post-snapshot log replay rather than the
// initial ApplyConfig). The vault then never made it into the
// orchestrator, and any subsequent NotifyTierPut firing handleTierPut
// would fail with "vault not found" — and since handleVaultPut never
// fires for snapshot-restored vaults, the cluster ends up in a permanent
// stuck state. AddVault (the runtime path) registers empty vaults
// correctly; initVault must do the same. This test asserts the parity.
func TestApplyConfigVaultWithNoLocalTiers(t *testing.T) {
	t.Parallel()
	// Local node is "node-1". Build a vault whose only tier is placed
	// exclusively on "node-2" — buildTierInstances should return zero
	// local tiers, but the vault must still be registered so a later
	// AddTierToVault call can succeed.
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(_ map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(_ map[string]string, _ chunk.ChunkManager, _ *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}

	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	cfg := &config.Config{
		Vaults: []config.VaultConfig{{ID: vaultID, Enabled: true}},
		Tiers: []config.TierConfig{{
			ID:         tierID,
			Name:       "remote-only",
			Type:       config.TierTypeMemory,
			VaultID:    vaultID,
			Placements: syntheticPlacements("node-2"), // NOT node-1
		}},
	}

	if err := orch.ApplyConfig(cfg, factories); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	// The vault MUST be registered, even though buildTierInstances
	// returned zero local tiers for it.
	if !slices.Contains(orch.ListVaults(), vaultID) {
		t.Fatalf("vault %s should be registered after ApplyConfig even with zero local tiers", vaultID)
	}
}

func TestApplyConfigVaults(t *testing.T) {
	orch := newTestOrch(t, Config{})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}

	vault1ID := uuid.Must(uuid.NewV7())
	vault2ID := uuid.Must(uuid.NewV7())
	vc1, tc1 := testVaultCfg(vault1ID, config.TierTypeMemory)
	vc2, tc2 := testVaultCfg(vault2ID, config.TierTypeMemory)

	cfg := &config.Config{
		Vaults: []config.VaultConfig{vc1, vc2},
		Tiers:  []config.TierConfig{tc1, tc2},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify vaults were registered.
	keys := orch.ListVaults()
	if len(keys) != 2 {
		t.Errorf("expected 2 vaults, got %d", len(keys))
	}
	if orch.ChunkManager(vault1ID) == nil || orch.ChunkManager(vault2ID) == nil {
		t.Error("expected both vaults to have chunk managers")
	}
	if orch.IndexManager(vault1ID) == nil || orch.IndexManager(vault2ID) == nil {
		t.Error("expected both vaults to have index managers")
	}
	if orch.QueryEngine(vault1ID) == nil || orch.QueryEngine(vault2ID) == nil {
		t.Error("expected both vaults to have query engines")
	}
}

func TestApplyConfigIngesters(t *testing.T) {
	orch := newTestOrch(t, Config{})

	factories := Factories{
		IngesterTypes: map[string]IngesterRegistration{
			"test": {Factory: func(id uuid.UUID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				return &fakeIngester{}, nil
			}},
		},
	}

	recv1ID := uuid.Must(uuid.NewV7())
	recv2ID := uuid.Must(uuid.NewV7())
	cfg := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: recv1ID, Type: "test", Enabled: true},
			{ID: recv2ID, Type: "test", Enabled: true},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(orch.ingesters) != 2 {
		t.Errorf("expected 2 ingesters, got %d", len(orch.ingesters))
	}
}

func TestApplyConfigUnknownChunkManagerType(t *testing.T) {
	orch := newTestOrch(t, Config{})

	vaultID := uuid.Must(uuid.NewV7())
	vc, tc := testVaultCfg(vaultID, config.TierTypeMemory)
	cfg := &config.Config{
		Vaults: []config.VaultConfig{vc},
		Tiers:  []config.TierConfig{tc},
	}

	// Vault init failure is non-fatal (vault skipped), so no error returned.
	err := orch.ApplyConfig(cfg, Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{},
		IndexManagers: map[string]index.ManagerFactory{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orch.ChunkManager(vaultID) != nil {
		t.Error("vault with unknown chunk manager type should not be registered")
	}
}

func TestApplyConfigUnknownIndexManagerType(t *testing.T) {
	orch := newTestOrch(t, Config{})

	vaultID := uuid.Must(uuid.NewV7())
	vc, tc := testVaultCfg(vaultID, config.TierTypeMemory)
	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{}, // missing "memory"
	}

	cfg := &config.Config{
		Vaults: []config.VaultConfig{vc},
		Tiers:  []config.TierConfig{tc},
	}

	// Vault init failure is non-fatal (vault skipped), so no error returned.
	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orch.ChunkManager(vaultID) != nil {
		t.Error("vault with unknown index manager type should not be registered")
	}
}

func TestApplyConfigUnknownIngesterType(t *testing.T) {
	orch := newTestOrch(t, Config{})

	cfg := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: uuid.Must(uuid.NewV7()), Enabled: true},
		},
	}

	err := orch.ApplyConfig(cfg, Factories{
		IngesterTypes: map[string]IngesterRegistration{},
	})
	if err == nil {
		t.Error("expected error for unknown ingester type")
	}
}

func TestApplyConfigDuplicateVaultID(t *testing.T) {
	orch := newTestOrch(t, Config{})

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}

	dupID := uuid.Must(uuid.NewV7())
	vc1, tc1 := testVaultCfg(dupID, config.TierTypeMemory)
	vc2 := vc1 // duplicate ID, same tier
	cfg := &config.Config{
		Vaults: []config.VaultConfig{vc1, vc2},
		Tiers:  []config.TierConfig{tc1},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error for duplicate vault ID")
	}
}

func TestApplyConfigDuplicateIngesterID(t *testing.T) {
	orch := newTestOrch(t, Config{})

	factories := Factories{
		IngesterTypes: map[string]IngesterRegistration{
			"test": {Factory: func(id uuid.UUID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				return &fakeIngester{}, nil
			}},
		},
	}

	dupIngID := uuid.Must(uuid.NewV7())
	cfg := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: dupIngID, Enabled: true},
			{ID: dupIngID, Enabled: true}, // duplicate
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error for duplicate ingester ID")
	}
}

func TestApplyConfigChunkManagerFactoryError(t *testing.T) {
	orch := newTestOrch(t, Config{})

	vaultID := uuid.Must(uuid.NewV7())
	vc, tc := testVaultCfg(vaultID, config.TierTypeMemory)
	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return nil, errors.New("factory error")
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return &fakeIndexManager{}, nil
			},
		},
	}

	cfg := &config.Config{
		Vaults: []config.VaultConfig{vc},
		Tiers:  []config.TierConfig{tc},
	}

	// Vault init failure is non-fatal — node stays up, vault is skipped.
	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orch.ChunkManager(vaultID) != nil {
		t.Error("failed vault should not be registered")
	}
}

func TestApplyConfigIndexManagerFactoryError(t *testing.T) {
	orch := newTestOrch(t, Config{})

	vaultID := uuid.Must(uuid.NewV7())
	vc, tc := testVaultCfg(vaultID, config.TierTypeMemory)
	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				return nil, errors.New("factory error")
			},
		},
	}

	cfg := &config.Config{
		Vaults: []config.VaultConfig{vc},
		Tiers:  []config.TierConfig{tc},
	}

	// Vault init failure is non-fatal — node stays up, vault is skipped.
	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orch.ChunkManager(vaultID) != nil {
		t.Error("failed vault should not be registered")
	}
}

func TestApplyConfigIngesterFactoryError(t *testing.T) {
	orch := newTestOrch(t, Config{})

	factories := Factories{
		IngesterTypes: map[string]IngesterRegistration{
			"test": {Factory: func(id uuid.UUID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				return nil, errors.New("factory error")
			}},
		},
	}

	cfg := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: uuid.Must(uuid.NewV7()), Enabled: true},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err == nil {
		t.Error("expected error from ingester factory")
	}
}

func TestApplyConfigParamsPassedToIngesterFactory(t *testing.T) {
	orch := newTestOrch(t, Config{})

	var receivedParams map[string]string
	factories := Factories{
		IngesterTypes: map[string]IngesterRegistration{
			"test": {Factory: func(id uuid.UUID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				receivedParams = params
				return &fakeIngester{}, nil
			}},
		},
	}

	cfg := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: uuid.Must(uuid.NewV7()), Type: "test", Enabled: true, Params: map[string]string{
				"host": "localhost",
				"port": "514",
			}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedParams["host"] != "localhost" {
		t.Errorf("expected host=localhost, got %s", receivedParams["host"])
	}
	if receivedParams["port"] != "514" {
		t.Errorf("expected port=514, got %s", receivedParams["port"])
	}
}

func TestApplyConfigParamsPassedToVaultFactories(t *testing.T) {
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	var cmReceivedParams map[string]string
	var imReceivedParams map[string]string

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				cmReceivedParams = params
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				imReceivedParams = params
				return &fakeIndexManager{}, nil
			},
		},
	}

	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	storageID := uuid.Must(uuid.NewV7())

	cfg := &config.Config{
		Vaults: []config.VaultConfig{
			{ID: vaultID, Enabled: true},
		},
		Tiers: []config.TierConfig{
			{ID: tierID, Name: "local", Type: config.TierTypeFile, StorageClass: 1, VaultID: vaultID, Position: 0},
		},
		NodeStorageConfigs: []config.NodeStorageConfig{
			{NodeID: "node-1", FileStorages: []config.FileStorage{
				{ID: storageID, StorageClass: 1, Name: "fast", Path: "/data/chunks"},
			}},
		},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify dir param: <storage-path>/vaults/<vault-id>/<tier-id>
	expectedDir := "/data/chunks/vaults/" + vaultID.String() + "/" + tierID.String()
	if cmReceivedParams["dir"] != expectedDir {
		t.Errorf("chunk manager: expected dir=%s, got %s", expectedDir, cmReceivedParams["dir"])
	}
	if imReceivedParams["dir"] != expectedDir {
		t.Errorf("index manager: expected dir=%s, got %s", expectedDir, imReceivedParams["dir"])
	}
}

func TestApplyConfigIndexManagerReceivesChunkManager(t *testing.T) {
	orch := newTestOrch(t, Config{})

	expectedCM := &fakeChunkManager{}
	var receivedCM chunk.ChunkManager

	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return expectedCM, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": func(params map[string]string, cm chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
				receivedCM = cm
				return &fakeIndexManager{}, nil
			},
		},
	}

	vaultID := uuid.Must(uuid.NewV7())
	vc, tc := testVaultCfg(vaultID, config.TierTypeMemory)
	cfg := &config.Config{
		Vaults: []config.VaultConfig{vc},
		Tiers:  []config.TierConfig{tc},
	}

	err := orch.ApplyConfig(cfg, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedCM != expectedCM {
		t.Error("index manager factory did not receive the correct chunk manager")
	}
}
