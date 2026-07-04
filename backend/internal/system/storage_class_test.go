package system_test

// Regression coverage for gastrolog-2bv1x: StorageIDForNode must never fall
// back to a mismatched storage class for file vaults. Follower placement
// (eligibleStorages/storageEligible) was always strict; the leader path went
// through this fallback and could land on the wrong disk class.

import (
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func nodeStorages(nodeID string, classes ...uint32) system.NodeStorageConfig {
	nsc := system.NodeStorageConfig{NodeID: nodeID}
	for _, c := range classes {
		nsc.FileStorages = append(nsc.FileStorages, system.FileStorage{
			ID:           glid.New(),
			StorageClass: c,
		})
	}
	return nsc
}

func TestStorageIDForNodeStrictClassMatch(t *testing.T) {
	t.Parallel()
	const nodeID = "node-1"
	nsc := nodeStorages(nodeID, 1, 2)
	nscs := []system.NodeStorageConfig{nsc}

	fileVault := func(class uint32) system.VaultConfig {
		return system.VaultConfig{Type: system.VaultTypeFile, StorageClass: class}
	}

	// Exact match picks the matching storage, not FileStorages[0].
	if got := system.StorageIDForNode(nodeID, fileVault(2), nscs); got != nsc.FileStorages[1].ID.String() {
		t.Fatalf("class 2 = %q, want the class-2 storage %q", got, nsc.FileStorages[1].ID.String())
	}

	// No matching class: empty, NOT a silent fallback to another class.
	if got := system.StorageIDForNode(nodeID, fileVault(7), nscs); got != "" {
		t.Fatalf("mismatched class = %q, want \"\" (no silent fallback)", got)
	}

	// Node without any storage config: file vaults get no synthetic ID.
	if got := system.StorageIDForNode("node-unknown", fileVault(1), nscs); got != "" {
		t.Fatalf("unknown node = %q, want \"\" for file vault", got)
	}
}

func TestStorageIDForNodeMemoryVaultFallbacks(t *testing.T) {
	t.Parallel()
	const nodeID = "node-1"
	nsc := nodeStorages(nodeID, 1)
	memVault := system.VaultConfig{Type: system.VaultTypeMemory}

	// Memory vaults have no class requirement: any storage works.
	got := system.StorageIDForNode(nodeID, memVault, []system.NodeStorageConfig{nsc})
	if got != nsc.FileStorages[0].ID.String() {
		t.Fatalf("memory vault = %q, want first storage", got)
	}

	// No storage config at all: synthetic ID keeps memory vaults placeable.
	got = system.StorageIDForNode("node-bare", memVault, nil)
	if got != system.SyntheticStorageID("node-bare") {
		t.Fatalf("memory vault on bare node = %q, want synthetic", got)
	}
}
