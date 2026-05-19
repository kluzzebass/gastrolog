// Cross-replica read fan-out: remoteVaultsByNodeFiltered emits one
// entry per placement member of every remote vault, so the coordinator
// opens a stream to every replica. Cross-replica record duplicates are
// collapsed by dedupWindow at the merge boundary
// (gastrolog-6bt8s/gastrolog-hshgl).
//
// Pre-fan-out the function routed each vault to its placement leader
// alone, so a single follower holding records the leader hadn't yet
// received would be invisible to queries. This test pins the new
// behaviour against accidental regressions.

package server

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

func TestRemoteVaultsByNodeFanOutsToAllPlacementMembers(t *testing.T) {
	t.Parallel()

	store := sysmem.NewStore()
	ctx := context.Background()

	// Three nodes — local + two peers — each with a file storage that
	// hosts a placement for the same vault.
	const local = "node-local"
	const peerA = "node-a"
	const peerB = "node-b"

	storageLocal := glid.New()
	storageA := glid.New()
	storageB := glid.New()

	if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID: local,
		FileStorages: []system.FileStorage{{
			ID: storageLocal, Name: "ls", StorageClass: 0, Path: "/tmp/local",
		}},
	}); err != nil {
		t.Fatalf("SetNodeStorageConfig local: %v", err)
	}
	if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID: peerA,
		FileStorages: []system.FileStorage{{
			ID: storageA, Name: "as", StorageClass: 0, Path: "/tmp/a",
		}},
	}); err != nil {
		t.Fatalf("SetNodeStorageConfig peerA: %v", err)
	}
	if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID: peerB,
		FileStorages: []system.FileStorage{{
			ID: storageB, Name: "bs", StorageClass: 0, Path: "/tmp/b",
		}},
	}); err != nil {
		t.Fatalf("SetNodeStorageConfig peerB: %v", err)
	}

	// Vault placed on peerA (leader) and peerB (follower) — local is
	// NOT a placement member, so the entire vault is "remote" from
	// local's perspective.
	vaultID := glid.New()
	placements := []system.VaultPlacement{
		{StorageID: storageA.String(), Leader: true},
		{StorageID: storageB.String(), Leader: false},
	}
	if err := store.PutVault(ctx, system.VaultConfig{
		ID:           vaultID,
		Name:         "fanout-vault",
		Type:         system.VaultTypeFile,
		StorageClass: 0,
		Placements:   placements,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, vaultID, placements); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	qs := &QueryServer{cfgStore: store, localNodeID: local}

	got := qs.remoteVaultsByNodeFiltered(ctx, nil, nil)

	// Both peers should appear, each mapped to the vault ID. Pre-fan-out
	// only peerA (the leader) would have appeared.
	if len(got) != 2 {
		t.Fatalf("expected 2 byNode entries (one per placement member), got %d: %v", len(got), got)
	}
	for _, peer := range []string{peerA, peerB} {
		vaults, ok := got[peer]
		if !ok {
			t.Errorf("peer %s missing from byNode (fan-out reads must hit every placement member)", peer)
			continue
		}
		if len(vaults) != 1 || vaults[0] != vaultID {
			t.Errorf("peer %s: expected single entry for %s, got %v", peer, vaultID, vaults)
		}
	}
	if _, self := got[local]; self {
		t.Errorf("local node must not appear in remote byNode map")
	}
}

func TestRemoteVaultsByNodeSkipsLocalVaults(t *testing.T) {
	t.Parallel()

	store := sysmem.NewStore()
	ctx := context.Background()

	const local = "node-local"
	const peer = "node-peer"
	storageLocal := glid.New()
	storagePeer := glid.New()

	if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       local,
		FileStorages: []system.FileStorage{{ID: storageLocal, Name: "ls", StorageClass: 0, Path: "/tmp/local"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       peer,
		FileStorages: []system.FileStorage{{ID: storagePeer, Name: "ps", StorageClass: 0, Path: "/tmp/peer"}},
	}); err != nil {
		t.Fatal(err)
	}

	vaultID := glid.New()
	placements := []system.VaultPlacement{
		{StorageID: storageLocal.String(), Leader: true},
		{StorageID: storagePeer.String(), Leader: false},
	}
	if err := store.PutVault(ctx, system.VaultConfig{
		ID: vaultID, Name: "skip-local", Type: system.VaultTypeFile, StorageClass: 0, Placements: placements,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetVaultPlacements(ctx, vaultID, placements); err != nil {
		t.Fatal(err)
	}

	qs := &QueryServer{cfgStore: store, localNodeID: local}

	// Mark the vault as locally-handled — the search node queries its
	// engine directly for these, so remote routing must skip them
	// entirely (avoids double-counting + redundant RPCs).
	localVaults := map[glid.GLID]bool{vaultID: true}
	got := qs.remoteVaultsByNodeFiltered(ctx, nil, localVaults)
	if len(got) != 0 {
		t.Errorf("vault marked local must produce no remote entries, got %v", got)
	}
}
