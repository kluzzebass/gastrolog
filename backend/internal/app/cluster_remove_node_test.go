package app

import (
	"context"
	"strings"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Tests for the orphan-refusal gate. The gate is structured as a pure
// helper (vaultsOrphanedByRemoval) over a system.Store so it can be
// unit-tested without standing up a real cluster.

func TestVaultsOrphanedByRemoval_EmptyStore(t *testing.T) {
	t.Parallel()
	store := sysmem.NewStore()
	if got := vaultsOrphanedByRemoval(context.Background(), store, "node-A"); len(got) != 0 {
		t.Fatalf("expected no orphans for empty store, got %v", got)
	}
}

func TestVaultsOrphanedByRemoval_RF1_SoleHolderRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	// One vault, RF=1, only placement on node-A. Removing node-A would
	// orphan it.
	target := "node-A"
	vaultID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "solo", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, vaultID, []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID(target), Leader: true},
	}); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	orphans := vaultsOrphanedByRemoval(ctx, store, target)
	if len(orphans) != 1 || orphans[0].ID != vaultID {
		t.Fatalf("expected exactly one orphan (vault %s), got %v", vaultID, orphans)
	}
	if orphans[0].Name != "solo" {
		t.Fatalf("orphan name: got %q, want %q", orphans[0].Name, "solo")
	}
}

func TestVaultsOrphanedByRemoval_RF2_OtherHolderSurvives(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	// Vault placed on both node-A (leader) and node-B (follower).
	// Removing node-A still leaves node-B as a holder — not orphaned.
	vaultID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "ha", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, vaultID, []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID("node-A"), Leader: true},
		{StorageID: system.SyntheticStorageID("node-B"), Leader: false},
	}); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	if got := vaultsOrphanedByRemoval(ctx, store, "node-A"); len(got) != 0 {
		t.Fatalf("expected no orphans when surviving holder exists, got %v", got)
	}
}

func TestVaultsOrphanedByRemoval_RF3_AllPlacementsHealthy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	vaultID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "triple", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, vaultID, []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID("node-A"), Leader: true},
		{StorageID: system.SyntheticStorageID("node-B"), Leader: false},
		{StorageID: system.SyntheticStorageID("node-C"), Leader: false},
	}); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	for _, target := range []string{"node-A", "node-B", "node-C"} {
		if got := vaultsOrphanedByRemoval(ctx, store, target); len(got) != 0 {
			t.Fatalf("expected no orphans for %s in RF=3, got %v", target, got)
		}
	}
}

// TestVaultsOrphanedByRemoval_MixedVaults verifies that orphan
// detection is per-vault — a single store with one orphan-prone vault
// and one HA vault correctly returns only the orphan.
func TestVaultsOrphanedByRemoval_MixedVaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	soloID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: soloID, Name: "solo", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault solo: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, soloID, []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID("node-A"), Leader: true},
	}); err != nil {
		t.Fatalf("SetVaultPlacements solo: %v", err)
	}

	haID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: haID, Name: "ha", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault ha: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, haID, []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID("node-A"), Leader: true},
		{StorageID: system.SyntheticStorageID("node-B"), Leader: false},
	}); err != nil {
		t.Fatalf("SetVaultPlacements ha: %v", err)
	}

	orphans := vaultsOrphanedByRemoval(ctx, store, "node-A")
	if len(orphans) != 1 || orphans[0].ID != soloID {
		t.Fatalf("expected only solo (%s) orphaned, got %v", soloID, orphans)
	}
}

// TestVaultsOrphanedByRemoval_NoPlacements verifies that a vault with
// no placements at all (post-deletion residue, mid-bootstrap state) is
// not counted as orphaned — there's nothing to lose.
func TestVaultsOrphanedByRemoval_NoPlacements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	vaultID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "ghost", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	// Intentionally omit SetVaultPlacements.

	if got := vaultsOrphanedByRemoval(ctx, store, "node-A"); len(got) != 0 {
		t.Fatalf("vault with no placements should not register as orphan, got %v", got)
	}
}

// TestVaultsOrphanedByRemoval_FileStorageResolution verifies the
// helper resolves file-storage placements (not just synthetic memory
// placements) — the production case where vaults are placed on
// per-node file storages registered via NodeStorageConfig.
func TestVaultsOrphanedByRemoval_FileStorageResolution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()

	target := "node-A"
	storageID := glid.New()
	if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID: target,
		FileStorages: []system.FileStorage{
			{ID: storageID, StorageClass: 1, Name: "fs-A", Path: "/data"},
		},
	}); err != nil {
		t.Fatalf("SetNodeStorageConfig: %v", err)
	}
	vaultID := glid.New()
	if err := store.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "file-solo", Type: system.VaultTypeFile, StorageClass: 1}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := store.SetVaultPlacements(ctx, vaultID, []system.VaultPlacement{
		{StorageID: storageID.String(), Leader: true},
	}); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	orphans := vaultsOrphanedByRemoval(ctx, store, target)
	if len(orphans) != 1 || orphans[0].ID != vaultID {
		t.Fatalf("expected file-storage vault %s to register as orphan, got %v", vaultID, orphans)
	}
}

// TestOrphanRefusalError_FormatLists asserts the operator-actionable
// shape of the error message: includes the count, every affected
// vault's name + ID, and the documented escape hatch (`--force`).
func TestOrphanRefusalError_FormatLists(t *testing.T) {
	t.Parallel()
	id1, id2 := glid.New(), glid.New()
	err := orphanRefusalError("node-A", []orphanedVault{
		{ID: id1, Name: "alpha"},
		{ID: id2, Name: "beta"},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	for _, want := range []string{"node-A", "2 vault", "alpha", "beta", id1.String(), id2.String(), "--force", "data loss"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error missing %q in:\n%s", want, msg)
		}
	}
}
