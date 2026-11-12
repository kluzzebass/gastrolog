package server

import (
	"strings"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
)

// A node's former-home copies can be the only copies of a vault's sealed
// chunks until the new homes pull them, and they are served only from the
// node's configured storages. Dropping any storage of that node then is
// refused; a vault still placed on the node is the placement check's case.
func TestStorageRemovalRefusal_FormerHomeCopiesStillNeeded(t *testing.T) {
	keep := system.FileStorage{ID: glid.New(), Name: "keep", Path: "storage/keep", StorageClass: 1}
	drop := system.FileStorage{ID: glid.New(), Name: "drop", Path: "storage/drop", StorageClass: 1}
	prev := &system.NodeStorageConfig{NodeID: "n1", FileStorages: []system.FileStorage{keep, drop}}
	next := system.NodeStorageConfig{NodeID: "n1", FileStorages: []system.FileStorage{keep}}
	moved := system.VaultConfig{ID: glid.New(), Name: "moved-away"}
	elsewhere := []system.VaultPlacement{{StorageID: glid.New().String(), Leader: true}}
	vaults := []system.VaultConfig{moved}
	placements := map[glid.GLID][]system.VaultPlacement{moved.ID: elsewhere}
	losses := []orchestrator.ChunkHolderLoss{{VaultID: moved.ID, VaultName: moved.Name, Chunks: 3}}

	err := storageRemovalRefusal(prev, next, vaults, placements, losses)
	if err == nil || !strings.Contains(err.Error(), "moved-away") {
		t.Fatalf("dropping a storage while a former home's copies are needed must be refused naming the vault, got %v", err)
	}
	if err := storageRemovalRefusal(prev, next, vaults, placements, nil); err != nil {
		t.Fatalf("with no dependent chunks the removal must pass, got %v", err)
	}
	if err := storageRemovalRefusal(prev, *prev, vaults, placements, losses); err != nil {
		t.Fatalf("a config that drops nothing must pass whatever depends on the node, got %v", err)
	}
}

func TestStorageRemovalRefusal_PlacedStorage(t *testing.T) {
	placed := system.FileStorage{ID: glid.New(), Name: "placed", Path: "storage/placed", StorageClass: 1}
	spare := system.FileStorage{ID: glid.New(), Name: "spare", Path: "storage/spare", StorageClass: 2}
	prev := &system.NodeStorageConfig{NodeID: "n1", FileStorages: []system.FileStorage{placed, spare}}
	v := system.VaultConfig{ID: glid.New(), Name: "lives-here"}
	vaults := []system.VaultConfig{v}
	placements := map[glid.GLID][]system.VaultPlacement{v.ID: {{StorageID: placed.ID.String(), Leader: true}}}

	dropPlaced := system.NodeStorageConfig{NodeID: "n1", FileStorages: []system.FileStorage{spare}}
	if err := storageRemovalRefusal(prev, dropPlaced, vaults, placements, nil); err == nil || !strings.Contains(err.Error(), "lives-here") {
		t.Fatalf("dropping the storage a vault is placed on must be refused naming the vault, got %v", err)
	}

	dropSpare := system.NodeStorageConfig{NodeID: "n1", FileStorages: []system.FileStorage{placed}}
	if err := storageRemovalRefusal(prev, dropSpare, vaults, placements, nil); err != nil {
		t.Fatalf("dropping an unplaced storage must pass, got %v", err)
	}

	reclassified := placed
	reclassified.StorageClass = 7
	reclass := system.NodeStorageConfig{NodeID: "n1", FileStorages: []system.FileStorage{reclassified, spare}}
	if err := storageRemovalRefusal(prev, reclass, vaults, placements, nil); err != nil {
		t.Fatalf("reclassifying a placed storage keeps its path and must pass, got %v", err)
	}

	// The vault is still placed on this node: its losses are the placement
	// check's concern, not a reason to refuse dropping the spare storage.
	losses := []orchestrator.ChunkHolderLoss{{VaultID: v.ID, VaultName: v.Name, Chunks: 1}}
	if err := storageRemovalRefusal(prev, dropSpare, vaults, placements, losses); err != nil {
		t.Fatalf("a vault still placed on the node does not block dropping another storage, got %v", err)
	}
}
