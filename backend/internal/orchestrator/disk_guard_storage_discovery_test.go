package orchestrator

// Coverage for the DISCOVERY half of gastrolog-9akebz: refreshVaultDiskGuards
// converging storageDiskGuard entries from system.FileStorage (the storage
// entity's own DiskFreeWarn/DiskFreeFloor, not a vault-level field) and
// linking each vault's guard entry to its LOCAL placements' storage IDs.
// disk_guard_test.go exercises the guard's own API directly (SetStorageGuard
// / SetVaultGuard); this file drives the same path through a real
// system.Config + Runtime, the shape refreshVaultDiskGuards actually reads on
// every scheduler tick — proving the config→guard wiring itself, not just
// the guard's internal bookkeeping.

import (
	"context"
	"strings"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// TestRefreshVaultDiskGuardsRegistersStorageFromConfig pins the end-to-end
// discovery wiring: a NodeStorageConfig's FileStorage carries its own
// DiskFreeWarn/DiskFreeFloor, and a vault's placement on that storage links
// the vault's guard entry to it — so evaluateStorages' verdict, derived
// purely from config + a live statfs sample, protects the vault.
func TestRefreshVaultDiskGuardsRegistersStorageFromConfig(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	storageID := glid.New()
	const nodeID = "node-1"

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "on-disk",
			Enabled: true,
			Type:    system.VaultTypeFile,
		}},
	}
	rt := system.Runtime{
		VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {
				{StorageID: storageID.String(), Leader: true},
			},
		},
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []system.FileStorage{{
				ID:            storageID,
				Path:          "volA",
				DiskFreeWarn:  "50%",
				DiskFreeFloor: "40%",
			}},
		}},
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	// Path resolution against vaultsDir is tested on its own
	// (TestRefreshStorageGuardsResolvesRelativePathAgainstVaultsDir); disable
	// it here so "volA" stays the literal fake-sampler key this test's
	// assertions depend on.
	orch.vaultsDir = ""
	orch.setSystemLoader(testSystemLoaderWithRuntime{cfg: cfg, rt: rt})
	orch.diskGuard.sample = func(path string) (uint64, uint64, error) {
		if path == "volA" {
			return 30 * gib, 100 * gib, nil // 30% free — below the storage's 40% floor
		}
		return 0, 0, errNoSuchVolume
	}

	ctx := context.Background()
	orch.refreshVaultDiskGuards(ctx)
	orch.diskGuard.evaluateStorages(orch.alerts)

	if !orch.diskGuard.vaultStorageProtected(vaultID) {
		t.Fatal("vault must be storage-protected: its placement's storage carries a 40% floor and free is 30%")
	}
}

// TestRefreshVaultDiskGuardsPublishesPlacementsAndClass pins the
// placements-on-storage discovery wiring for gastrolog-3cobq4: a vault's
// config placement on a storage surfaces in that storage's snapshot
// (config-derived, per the storage inspector brief), and the storage's
// configured class passes through too.
func TestRefreshVaultDiskGuardsPublishesPlacementsAndClass(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	storageID := glid.New()
	const nodeID = "node-1"

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "on-disk",
			Enabled: true,
			Type:    system.VaultTypeFile,
		}},
	}
	rt := system.Runtime{
		VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {
				{StorageID: storageID.String(), Leader: true},
			},
		},
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []system.FileStorage{{
				ID:           storageID,
				Path:         "volA",
				StorageClass: 3,
			}},
		}},
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	// Path resolution against vaultsDir is tested on its own
	// (TestRefreshStorageGuardsResolvesRelativePathAgainstVaultsDir); disable
	// it here so "volA" stays the literal fake-sampler key this test's
	// assertions depend on.
	orch.vaultsDir = ""
	orch.setSystemLoader(testSystemLoaderWithRuntime{cfg: cfg, rt: rt})
	orch.diskGuard.sample = func(path string) (uint64, uint64, error) {
		if path == "volA" {
			return 50 * gib, 100 * gib, nil
		}
		return 0, 0, errNoSuchVolume
	}

	orch.refreshVaultDiskGuards(context.Background())
	orch.diskGuard.evaluateStorages(orch.alerts)

	snaps := orch.diskGuard.storageSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("want 1 storage snapshot, got %d", len(snaps))
	}
	s := snaps[0]
	if s.StorageClass != 3 {
		t.Fatalf("storage class = %d, want 3", s.StorageClass)
	}
	if len(s.PlacedVaultIDs) != 1 || s.PlacedVaultIDs[0] != vaultID {
		t.Fatalf("placed vaults = %v, want [%s]", s.PlacedVaultIDs, vaultID)
	}
}

// TestRefreshVaultDiskGuardsPlacementsClearWhenVaultRemoved pins the
// no-strand contract for placements specifically: a vault's placement
// disappearing from config (the vault is deleted, or its placement moves
// elsewhere) must clear it from the storage's placed-vault list on the very
// next refresh — the storage entry itself is retained (still hosted here),
// only the placement linkage changes.
func TestRefreshVaultDiskGuardsPlacementsClearWhenVaultRemoved(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	storageID := glid.New()
	const nodeID = "node-1"

	rt := system.Runtime{
		VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {
				{StorageID: storageID.String(), Leader: true},
			},
		},
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []system.FileStorage{{
				ID:   storageID,
				Path: "volA",
			}},
		}},
	}
	loader := &testSystemLoaderWithRuntime{
		cfg: &system.Config{
			Vaults: []system.VaultConfig{{
				ID:      vaultID,
				Name:    "on-disk",
				Enabled: true,
				Type:    system.VaultTypeFile,
			}},
		},
		rt: rt,
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	// Path resolution against vaultsDir is tested on its own
	// (TestRefreshStorageGuardsResolvesRelativePathAgainstVaultsDir); disable
	// it here so "volA" stays the literal fake-sampler key this test's
	// assertions depend on.
	orch.vaultsDir = ""
	orch.setSystemLoader(loader)
	orch.diskGuard.sample = func(path string) (uint64, uint64, error) {
		if path == "volA" {
			return 50 * gib, 100 * gib, nil
		}
		return 0, 0, errNoSuchVolume
	}

	orch.refreshVaultDiskGuards(context.Background())
	snaps := orch.diskGuard.storageSnapshots()
	if len(snaps) != 1 || len(snaps[0].PlacedVaultIDs) != 1 {
		t.Fatal("precondition: storage must start with the vault's placement")
	}

	// The vault is deleted from config entirely.
	loader.cfg = &system.Config{}
	orch.refreshVaultDiskGuards(context.Background())

	snaps = orch.diskGuard.storageSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("the storage itself must remain (still locally hosted), got %d snapshots", len(snaps))
	}
	if len(snaps[0].PlacedVaultIDs) != 0 {
		t.Fatalf("removed vault's placement must clear from the storage's list, got %v", snaps[0].PlacedVaultIDs)
	}
}

// TestRefreshVaultDiskGuardsStorageRemovalReleasesNoStrand pins the
// no-strand contract at the DISCOVERY layer (not just the guard's own
// retainStorageGuards API): a storage removed from NodeStorageConfigs
// between ticks — the operator deleted it, or it moved off this node —
// must release every vault's derived protect flag on the very next
// refresh, exactly like retainVaultGuards' vault-removal precedent.
func TestRefreshVaultDiskGuardsStorageRemovalReleasesNoStrand(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	storageID := glid.New()
	const nodeID = "node-1"

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "on-disk",
			Enabled: true,
			Type:    system.VaultTypeFile,
		}},
	}
	rt := system.Runtime{
		VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {
				{StorageID: storageID.String(), Leader: true},
			},
		},
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []system.FileStorage{{
				ID:            storageID,
				Path:          "volA",
				DiskFreeWarn:  "50%",
				DiskFreeFloor: "40%",
			}},
		}},
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeID})
	// Path resolution against vaultsDir is tested on its own
	// (TestRefreshStorageGuardsResolvesRelativePathAgainstVaultsDir); disable
	// it here so "volA" stays the literal fake-sampler key this test's
	// assertions depend on.
	orch.vaultsDir = ""
	loader := &testSystemLoaderWithRuntime{cfg: cfg, rt: rt}
	orch.setSystemLoader(loader)
	orch.diskGuard.sample = func(path string) (uint64, uint64, error) {
		if path == "volA" {
			return 30 * gib, 100 * gib, nil
		}
		return 0, 0, errNoSuchVolume
	}

	ctx := context.Background()
	orch.refreshVaultDiskGuards(ctx)
	orch.diskGuard.evaluateStorages(orch.alerts)
	if !orch.diskGuard.vaultStorageProtected(vaultID) {
		t.Fatal("precondition: vault must start storage-protected")
	}

	// The storage is removed from config entirely (deleted, or its
	// NodeStorageConfig no longer lists it).
	loader.rt = system.Runtime{
		NodeStorageConfigs: []system.NodeStorageConfig{{NodeID: nodeID}},
	}
	orch.refreshVaultDiskGuards(ctx)
	if orch.diskGuard.vaultStorageProtected(vaultID) {
		t.Fatal("removing the storage from config must release the vault's protect flag on the next refresh — no strand")
	}
}

var errNoSuchVolume = &noSuchVolumeError{}

type noSuchVolumeError struct{}

func (*noSuchVolumeError) Error() string { return "no such volume" }

// testSystemLoaderWithRuntime is testSystemLoader plus a settable Runtime —
// refreshVaultDiskGuards reads both sys.Config (vaults, retention policies)
// and sys.Runtime (NodeStorageConfigs), and this test needs to mutate the
// latter BETWEEN refreshes to simulate a storage disappearing from config.
type testSystemLoaderWithRuntime struct {
	cfg *system.Config
	rt  system.Runtime
}

func (l testSystemLoaderWithRuntime) Load(_ context.Context) (*system.System, error) {
	if l.cfg == nil {
		return nil, nil
	}
	return &system.System{Config: *l.cfg, Runtime: l.rt}, nil
}

// TestRefreshVaultDiskGuardsResolvesNodeDisplayName pins a review finding on
// gastrolog-9akebz: operator-facing text (alarms, the local admission
// detail) must name the node, not its raw GLID — the same fallback
// contract as placementManager.nameOrID. refreshVaultDiskGuards resolves
// the local node's display name from Runtime.Nodes ONCE per refresh (config
// already loaded, off the admission hot path) and threads it into
// SetStorageGuard, so both the alarm text and vaultStorageProtectDetail
// read the name.
func TestRefreshVaultDiskGuardsResolvesNodeDisplayName(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	storageID := glid.New()
	nodeGLID := glid.New()
	nodeIDStr := nodeGLID.String()
	const nodeDisplay = "friendly-node-name"

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "on-disk",
			Enabled: true,
			Type:    system.VaultTypeFile,
		}},
	}
	// NodeConfig.ID and NodeStorageConfig.NodeID are looked up independently
	// (nodeDisplayName scans the former, NodeIDForStorage/localNodeID match
	// the latter) — both must agree with orch.LocalNodeID to exercise "this
	// is the local node, resolve its display name" end to end.
	rt := system.Runtime{
		VaultPlacements: map[glid.GLID][]system.VaultPlacement{
			vaultID: {
				{StorageID: storageID.String(), Leader: true},
			},
		},
		Nodes: []system.NodeConfig{{ID: nodeGLID, Name: nodeDisplay}},
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeIDStr,
			FileStorages: []system.FileStorage{{
				ID:            storageID,
				Name:          "my-storage",
				Path:          "volA",
				DiskFreeWarn:  "50%",
				DiskFreeFloor: "40%",
			}},
		}},
	}

	orch := newTestOrch(t, Config{LocalNodeID: nodeIDStr})
	// Path resolution tested separately; disable it here so "volA" stays
	// literal.
	orch.vaultsDir = ""
	orch.setSystemLoader(testSystemLoaderWithRuntime{cfg: cfg, rt: rt})
	orch.diskGuard.sample = func(path string) (uint64, uint64, error) {
		if path == "volA" {
			return 30 * gib, 100 * gib, nil // below the storage's 40% floor
		}
		return 0, 0, errNoSuchVolume
	}

	ctx := context.Background()
	orch.refreshVaultDiskGuards(ctx)
	orch.diskGuard.evaluateStorages(orch.alerts)

	if !orch.diskGuard.vaultStorageProtected(vaultID) {
		t.Fatal("precondition: vault must be storage-protected")
	}
	detail := orch.diskGuard.vaultStorageProtectDetail(vaultID)
	if !strings.Contains(detail, nodeDisplay) {
		t.Fatalf("detail must name the node (%q), not its raw ID, got: %q", nodeDisplay, detail)
	}
	if strings.Contains(detail, nodeIDStr) {
		t.Fatalf("detail must not leak the raw node GLID once a display name is known, got: %q", detail)
	}
}

// TestNodeDisplayNameFallsBackToID pins the fallback half: an unknown node
// (no matching NodeConfig, or a NodeConfig with an empty Name) resolves to
// the raw ID — never an empty string — matching nameOrID's contract.
func TestNodeDisplayNameFallsBackToID(t *testing.T) {
	t.Parallel()
	known := glid.New()
	unknown := "some-node-id"

	nodes := []system.NodeConfig{{ID: known, Name: "known-name"}, {ID: glid.New(), Name: ""}}

	if got := nodeDisplayName(nodes, known.String()); got != "known-name" {
		t.Fatalf("nodeDisplayName(known) = %q, want %q", got, "known-name")
	}
	if got := nodeDisplayName(nodes, unknown); got != unknown {
		t.Fatalf("nodeDisplayName(unknown) = %q, want the raw id %q", got, unknown)
	}
	if got := nodeDisplayName(nil, unknown); got != unknown {
		t.Fatalf("nodeDisplayName(nil nodes) = %q, want the raw id %q", got, unknown)
	}
}
