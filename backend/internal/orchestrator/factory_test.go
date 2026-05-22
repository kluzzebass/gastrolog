package orchestrator

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"log/slog"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
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
func (f *fakeChunkManager) FindIngestEntryIndex(_ chunk.ChunkID, _ time.Time) (uint64, bool, error) {
	return 0, false, nil
}
func (f *fakeChunkManager) HasLocalContent(_ chunk.ChunkID) bool { return true }
func (f *fakeChunkManager) ScanActiveByIngestTS(_ chunk.ChunkID, _ func(time.Time, chunk.Attributes) bool) error {
	return chunk.ErrChunkNotFound
}
func (f *fakeChunkManager) ScanActiveIngestTS(_ chunk.ChunkID, _ func(int64) bool) error {
	return chunk.ErrChunkNotFound
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
func (f *fakeChunkManager) ImportRecords(chunk.ChunkID, chunk.RecordIterator) (chunk.ChunkMeta, error) {
	return chunk.ChunkMeta{}, nil
}
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
func (f *fakeIndexManager) FindIngestEntryIndex(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	return 0, false, index.ErrIndexNotFound
}
func (f *fakeIndexManager) FindSourceEntryIndex(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
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

// testVaultCfg creates a VaultConfig for tests.
// vaultType is the storage shape (e.g., system.VaultTypeMemory or "test").
func testVaultCfg(vaultID glid.GLID, vaultType system.VaultType) system.VaultConfig {
	return system.VaultConfig{
		ID:      vaultID,
		Name:    "vault-" + vaultID.String()[:8],
		Enabled: true,
		Type:    vaultType,
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

// TestApplyConfigVaultWithNoLocalInstance is the regression test for
// gastrolog-264pk. Before the fix, ApplyConfig (the startup path) would
// silently skip registering any vault whose buildVaultInstances returned
// zero local instances — which happens on a node that isn't a placement
// target for any of the vault's instances (e.g. a node that joined
// the cluster as a non-instance-member, or a snapshot-restored node where
// placements are reapplied via post-snapshot log replay rather than the
// initial ApplyConfig). The vault then never made it into the
// orchestrator, and any subsequent notification firing handleInstancePut
// would fail with "vault not found" — and since handleVaultPut never
// fires for snapshot-restored vaults, the cluster ends up in a permanent
// stuck state. AddVault (the runtime path) registers empty vaults
// correctly; initVault must do the same. This test asserts the parity.
func TestApplyConfigVaultWithNoLocalInstance(t *testing.T) {
	t.Parallel()
	// Local node is "node-1". Build a vault whose only instance is placed
	// exclusively on "node-2" — buildVaultInstances should return zero
	// local instances, but the vault must still be registered so a later
	// AddVaultInstance call can succeed.
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

	vaultID := glid.New()
	cfg := &system.Config{
		Vaults: []system.VaultConfig{{ID: vaultID, Enabled: true}},
	}

	if err := orch.ApplyConfig(&system.System{Config: *cfg}, factories); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	// The vault MUST be registered, even though buildVaultInstances
	// returned zero local instances for it.
	if !slices.Contains(orch.ListVaults(), vaultID) {
		t.Fatalf("vault %s should be registered after ApplyConfig even with zero local instances", vaultID)
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

	vault1ID := glid.New()
	vault2ID := glid.New()
	vc1 := testVaultCfg(vault1ID, system.VaultTypeMemory)
	vc2 := testVaultCfg(vault2ID, system.VaultTypeMemory)

	cfg := &system.Config{
		Vaults: []system.VaultConfig{vc1, vc2},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
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
			"test": {Factory: func(id glid.GLID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				return &fakeIngester{}, nil
			}},
		},
	}

	recv1ID := glid.New()
	recv2ID := glid.New()
	cfg := &system.Config{
		Ingesters: []system.IngesterConfig{
			{ID: recv1ID, Type: "test", Enabled: true},
			{ID: recv2ID, Type: "test", Enabled: true},
		},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(orch.ingesters) != 2 {
		t.Errorf("expected 2 ingesters, got %d", len(orch.ingesters))
	}
}

// TestApplyConfigIngesterEligibility covers the cold-start eligibility gate
// in applyIngester. The runtime path (app/dispatch.go shouldRunIngester) and
// the cold-start path must agree, or AllNodes ingesters with a non-empty
// NodeIDs list (typical legacy shape: NodeIDs holds the creator's node)
// only start on the creator node after a cluster restart.
func TestApplyConfigIngesterEligibility(t *testing.T) {
	const localNode = "node-B"

	factories := Factories{
		IngesterTypes: map[string]IngesterRegistration{
			"test": {Factory: func(id glid.GLID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				return &fakeIngester{}, nil
			}},
		},
	}

	tests := []struct {
		name      string
		allNodes  bool
		nodeIDs   []string
		wantStart bool
	}{
		{name: "all_nodes overrides legacy NodeIDs from another node", allNodes: true, nodeIDs: []string{"node-A"}, wantStart: true},
		{name: "all_nodes with empty NodeIDs", allNodes: true, nodeIDs: nil, wantStart: true},
		{name: "all_nodes with local node in NodeIDs", allNodes: true, nodeIDs: []string{localNode}, wantStart: true},
		{name: "node-pinned to other node", allNodes: false, nodeIDs: []string{"node-A"}, wantStart: false},
		{name: "node-pinned to local node", allNodes: false, nodeIDs: []string{localNode}, wantStart: true},
		{name: "empty NodeIDs (legacy: match all)", allNodes: false, nodeIDs: nil, wantStart: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orch := newTestOrch(t, Config{LocalNodeID: localNode})

			ingID := glid.New()
			cfg := &system.Config{
				Ingesters: []system.IngesterConfig{{
					ID:       ingID,
					Type:     "test",
					Enabled:  true,
					AllNodes: tc.allNodes,
					NodeIDs:  tc.nodeIDs,
				}},
			}

			if err := orch.ApplyConfig(&system.System{Config: *cfg}, factories); err != nil {
				t.Fatalf("ApplyConfig: %v", err)
			}

			_, got := orch.ingesters[ingID]
			if got != tc.wantStart {
				t.Errorf("ingester registered = %v, want %v", got, tc.wantStart)
			}
		})
	}
}

func TestApplyConfigUnknownChunkManagerType(t *testing.T) {
	orch := newTestOrch(t, Config{})

	vaultID := glid.New()
	vc := testVaultCfg(vaultID, system.VaultTypeMemory)
	cfg := &system.Config{
		Vaults: []system.VaultConfig{vc},
	}

	// Vault init failure is non-fatal (vault skipped), so no error returned.
	err := orch.ApplyConfig(&system.System{Config: *cfg}, Factories{
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

	vaultID := glid.New()
	vc := testVaultCfg(vaultID, system.VaultTypeMemory)
	factories := Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": func(params map[string]string, _ *slog.Logger) (chunk.ChunkManager, error) {
				return &fakeChunkManager{}, nil
			},
		},
		IndexManagers: map[string]index.ManagerFactory{}, // missing "memory"
	}

	cfg := &system.Config{
		Vaults: []system.VaultConfig{vc},
	}

	// Vault init failure is non-fatal (vault skipped), so no error returned.
	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orch.ChunkManager(vaultID) != nil {
		t.Error("vault with unknown index manager type should not be registered")
	}
}

func TestApplyConfigUnknownIngesterType(t *testing.T) {
	orch := newTestOrch(t, Config{})

	cfg := &system.Config{
		Ingesters: []system.IngesterConfig{
			{ID: glid.New(), Enabled: true},
		},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, Factories{
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

	dupID := glid.New()
	vc1 := testVaultCfg(dupID, system.VaultTypeMemory)
	vc2 := vc1 // duplicate ID, same instance
	cfg := &system.Config{
		Vaults: []system.VaultConfig{vc1, vc2},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
	if err == nil {
		t.Error("expected error for duplicate vault ID")
	}
}

func TestApplyConfigDuplicateIngesterID(t *testing.T) {
	orch := newTestOrch(t, Config{})

	factories := Factories{
		IngesterTypes: map[string]IngesterRegistration{
			"test": {Factory: func(id glid.GLID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				return &fakeIngester{}, nil
			}},
		},
	}

	dupIngID := glid.New()
	cfg := &system.Config{
		Ingesters: []system.IngesterConfig{
			{ID: dupIngID, Enabled: true},
			{ID: dupIngID, Enabled: true}, // duplicate
		},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
	if err == nil {
		t.Error("expected error for duplicate ingester ID")
	}
}

func TestApplyConfigChunkManagerFactoryError(t *testing.T) {
	orch := newTestOrch(t, Config{})

	vaultID := glid.New()
	vc := testVaultCfg(vaultID, system.VaultTypeMemory)
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

	cfg := &system.Config{
		Vaults: []system.VaultConfig{vc},
	}

	// Vault init failure is non-fatal — node stays up, vault is skipped.
	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if orch.ChunkManager(vaultID) != nil {
		t.Error("failed vault should not be registered")
	}
}

func TestApplyConfigIndexManagerFactoryError(t *testing.T) {
	orch := newTestOrch(t, Config{})

	vaultID := glid.New()
	vc := testVaultCfg(vaultID, system.VaultTypeMemory)
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

	cfg := &system.Config{
		Vaults: []system.VaultConfig{vc},
	}

	// Vault init failure is non-fatal — node stays up, vault is skipped.
	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
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
			"test": {Factory: func(id glid.GLID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				return nil, errors.New("factory error")
			}},
		},
	}

	cfg := &system.Config{
		Ingesters: []system.IngesterConfig{
			{ID: glid.New(), Enabled: true},
		},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
	if err == nil {
		t.Error("expected error from ingester factory")
	}
}

func TestApplyConfigParamsPassedToIngesterFactory(t *testing.T) {
	orch := newTestOrch(t, Config{})

	var receivedParams map[string]string
	factories := Factories{
		IngesterTypes: map[string]IngesterRegistration{
			"test": {Factory: func(id glid.GLID, params map[string]string, logger *slog.Logger) (Ingester, error) {
				receivedParams = params
				return &fakeIngester{}, nil
			}},
		},
	}

	cfg := &system.Config{
		Ingesters: []system.IngesterConfig{
			{ID: glid.New(), Type: "test", Enabled: true, Params: map[string]string{
				"host": "localhost",
				"port": "514",
			}},
		},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
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

	// Vault and instance share the same ID.
	vaultID := glid.New()
	storageID := glid.New()

	sys := &system.System{
		Config: system.Config{
			Vaults: []system.VaultConfig{
				{ID: vaultID, Name: "vault", Enabled: true, Type: system.VaultTypeFile, StorageClass: 1},
			},
		},
		Runtime: system.Runtime{
			NodeStorageConfigs: []system.NodeStorageConfig{{
				NodeID: "node-1",
				FileStorages: []system.FileStorage{{
					ID: storageID, StorageClass: 1, Name: "fast", Path: "/data/chunks",
				}},
			}},
			VaultPlacements: map[glid.GLID][]system.VaultPlacement{
				vaultID: {{StorageID: storageID.String()}},
			},
		},
	}

	err := orch.ApplyConfig(sys, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify dir param: <storage-path>/vaults/<vault-id>/<vault-id>
	expectedDir := "/data/chunks/vaults/" + vaultID.String() + "/" + vaultID.String()
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

	vaultID := glid.New()
	vc := testVaultCfg(vaultID, system.VaultTypeMemory)
	cfg := &system.Config{
		Vaults: []system.VaultConfig{vc},
	}

	err := orch.ApplyConfig(&system.System{Config: *cfg}, factories)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedCM != expectedCM {
		t.Error("index manager factory did not receive the correct chunk manager")
	}
}

// --- gastrolog-292yi: all nodes in all instance Raft groups ---

// TestBuildVaultRaftMembers_AllClusterNodes verifies that buildVaultRaftMembers
// returns every cluster node as a Raft member, regardless of storage placement.
func TestBuildVaultRaftMembers_AllClusterNodes(t *testing.T) {
	t.Parallel()

	node1 := glid.New()
	node2 := glid.New()
	node3 := glid.New()

	clusterNodes := []system.NodeConfig{
		{ID: node1, Name: "node-1"},
		{ID: node2, Name: "node-2"},
		{ID: node3, Name: "node-3"},
	}

	orch := newTestOrch(t, Config{LocalNodeID: node1.String()})

	factories := Factories{
		NodeAddressResolver: func(nodeID string) (string, bool) {
			return nodeID + ":7946", true
		},
	}

	members := orch.buildVaultRaftMembers(clusterNodes, factories)

	if len(members) != 3 {
		t.Fatalf("expected 3 members (all cluster nodes), got %d", len(members))
	}

	// Verify all node IDs are present.
	memberIDs := make(map[hraft.ServerID]bool)
	for _, m := range members {
		memberIDs[m.ID] = true
	}
	for _, node := range clusterNodes {
		if !memberIDs[hraft.ServerID(node.ID.String())] {
			t.Errorf("node %s missing from members", node.ID)
		}
	}
}

// TestBuildVaultRaftMembers_UnresolvableNodeSkipped verifies that nodes whose
// address can't be resolved are excluded from the member list.
func TestBuildVaultRaftMembers_UnresolvableNodeSkipped(t *testing.T) {
	t.Parallel()

	node1 := glid.New()
	node2 := glid.New()

	clusterNodes := []system.NodeConfig{
		{ID: node1, Name: "node-1"},
		{ID: node2, Name: "node-2"},
	}

	orch := newTestOrch(t, Config{LocalNodeID: node1.String()})

	// Only node-1 is resolvable.
	factories := Factories{
		NodeAddressResolver: func(nodeID string) (string, bool) {
			if nodeID == node1.String() {
				return "10.0.0.1:7946", true
			}
			return "", false
		},
	}

	members := orch.buildVaultRaftMembers(clusterNodes, factories)

	if len(members) != 1 {
		t.Fatalf("expected 1 member (only resolvable node), got %d", len(members))
	}
	if string(members[0].ID) != node1.String() {
		t.Errorf("expected member %s, got %s", node1, members[0].ID)
	}
}

// TestBuildVaultRaftMembers_NilResolver verifies that a nil NodeAddressResolver
// returns no members.
func TestBuildVaultRaftMembers_NilResolver(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{})
	members := orch.buildVaultRaftMembers(
		[]system.NodeConfig{{ID: glid.New()}},
		Factories{},
	)
	if len(members) != 0 {
		t.Fatalf("expected 0 members with nil resolver, got %d", len(members))
	}
}

// TestBuildVaultRaftMembers_EmptyNodes verifies that empty node list returns nil.
func TestBuildVaultRaftMembers_EmptyNodes(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{})
	members := orch.buildVaultRaftMembers(nil, Factories{
		NodeAddressResolver: func(string) (string, bool) { return "addr", true },
	})
	if members != nil {
		t.Fatalf("expected nil members for empty nodes, got %v", members)
	}
}

// --- gastrolog-4zy8a: vault-ctl group membership must follow cluster growth ---

// allResolveFactories returns a Factories whose NodeAddressResolver maps every
// node ID to a deterministic "<id>:7946" address. Used by the
// RefreshVaultCtlMembers tests below.
func allResolveFactories() Factories {
	return Factories{
		NodeAddressResolver: func(nodeID string) (string, bool) {
			return nodeID + ":7946", true
		},
	}
}

// TestRefreshVaultCtlMembers_FansOutToEveryLocalVault verifies that the
// orchestrator updates the desired-member set on every registered vault when
// the cluster node list changes. Without this, vault-ctl Raft groups stay
// frozen at bootstrap membership and a scaled-in node loops forever in
// pre-vote campaigns rejected with "node is not in configuration".
func TestRefreshVaultCtlMembers_FansOutToEveryLocalVault(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultA := glid.New()
	vaultB := glid.New()
	orch.RegisterVault(&Vault{ID: vaultA})
	orch.RegisterVault(&Vault{ID: vaultB})

	clusterNodes := []system.NodeConfig{
		{ID: glid.New(), Name: "node-1"},
		{ID: glid.New(), Name: "node-2"},
		{ID: glid.New(), Name: "node-3"},
	}

	orch.RefreshVaultCtlMembers(clusterNodes, allResolveFactories())

	for _, id := range []glid.GLID{vaultA, vaultB} {
		desired := orch.vaultCtlLeaders.desired.Get(id)
		if len(desired) != 3 {
			t.Fatalf("vault %s: expected 3 desired members, got %d", id, len(desired))
		}
	}
}

// TestRefreshVaultCtlMembers_GrowsExistingDesiredSet verifies the scale-out
// scenario: a vault was registered when the cluster had N nodes, then the
// cluster grew. The next refresh must include all N+M nodes.
func TestRefreshVaultCtlMembers_GrowsExistingDesiredSet(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	orch.RegisterVault(&Vault{ID: vaultID})

	// Initial cluster: 3 nodes.
	initial := []system.NodeConfig{
		{ID: glid.New(), Name: "node-1"},
		{ID: glid.New(), Name: "node-2"},
		{ID: glid.New(), Name: "node-3"},
	}
	orch.RefreshVaultCtlMembers(initial, allResolveFactories())

	if got := len(orch.vaultCtlLeaders.desired.Get(vaultID)); got != 3 {
		t.Fatalf("after initial refresh: expected 3 members, got %d", got)
	}

	// Scale out: add 2 more nodes.
	scaled := append([]system.NodeConfig(nil), initial...)
	scaled = append(scaled,
		system.NodeConfig{ID: glid.New(), Name: "node-4"},
		system.NodeConfig{ID: glid.New(), Name: "node-5"},
	)
	orch.RefreshVaultCtlMembers(scaled, allResolveFactories())

	if got := len(orch.vaultCtlLeaders.desired.Get(vaultID)); got != 5 {
		t.Fatalf("after scale-out refresh: expected 5 members, got %d", got)
	}
}

// TestRefreshVaultCtlMembers_PartialResolutionSkips verifies that when one
// or more cluster nodes aren't yet resolvable (e.g. their cluster-Raft
// address hasn't propagated), the refresh is a no-op rather than passing a
// partial set to the reconciler — which would otherwise RemoveServer the
// missing entries. The next NotifyNodeConfigPut retries.
func TestRefreshVaultCtlMembers_PartialResolutionSkips(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()
	orch.RegisterVault(&Vault{ID: vaultID})

	// Seed a baseline desired set first.
	baseline := []system.NodeConfig{
		{ID: glid.New(), Name: "node-1"},
		{ID: glid.New(), Name: "node-2"},
	}
	orch.RefreshVaultCtlMembers(baseline, allResolveFactories())

	// Now refresh with 3 nodes but only 2 resolvable. Refresh must skip
	// (leaving baseline intact), not write a 2-member set that omits the
	// unresolvable third node.
	expanded := []system.NodeConfig{
		{ID: glid.New(), Name: "node-1"},
		{ID: glid.New(), Name: "node-2"},
		{ID: glid.New(), Name: "node-3-not-yet-in-raft"},
	}
	partial := Factories{
		NodeAddressResolver: func(nodeID string) (string, bool) {
			if nodeID == expanded[2].ID.String() {
				return "", false
			}
			return nodeID + ":7946", true
		},
	}
	orch.RefreshVaultCtlMembers(expanded, partial)

	if got := len(orch.vaultCtlLeaders.desired.Get(vaultID)); got != 2 {
		t.Fatalf("partial resolution should leave baseline (2) intact, got %d", got)
	}
}

// TestRefreshVaultCtlMembers_NoVaults verifies the no-vault case is a safe
// no-op (e.g. a coordinator-only node, or a node before any vault has been
// built locally).
func TestRefreshVaultCtlMembers_NoVaults(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	clusterNodes := []system.NodeConfig{
		{ID: glid.New(), Name: "node-1"},
		{ID: glid.New(), Name: "node-2"},
	}
	orch.RefreshVaultCtlMembers(clusterNodes, allResolveFactories())
	// Nothing to assert — should not panic and is a safe no-op.
}

// TestRefreshVaultCtlMembers_NilResolverNoOp verifies the single-node /
// non-cluster path: a nil resolver produces zero members, and the function
// returns without touching any vault's desired set.
func TestRefreshVaultCtlMembers_NilResolverNoOp(t *testing.T) {
	t.Parallel()

	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	vaultID := glid.New()
	orch.RegisterVault(&Vault{ID: vaultID})

	orch.RefreshVaultCtlMembers(
		[]system.NodeConfig{{ID: glid.New()}},
		Factories{}, // nil NodeAddressResolver → buildVaultRaftMembers returns nil
	)

	if got := len(orch.vaultCtlLeaders.desired.Get(vaultID)); got != 0 {
		t.Fatalf("single-node mode should leave desired set empty, got %d", got)
	}
}
