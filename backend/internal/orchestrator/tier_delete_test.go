package orchestrator

import (
	"gastrolog/internal/glid"
	"os"
	"testing"

	"gastrolog/internal/chunk"

)

// TestRemoveTierFromVaultPreservesData verifies that RemoveTierFromVault is
// non-destructive: it unregisters the tier instance but leaves chunks and
// the tier directory intact, so placement flaps don't wipe data.
// See gastrolog-4vz40.
func TestRemoveTierFromVaultPreservesData(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	tier, dir := newFileTierInstance(t, tierID)
	if _, _, err := tier.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, tier)
	vault.Name = "remove-preserves"
	orch.RegisterVault(vault)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("tier directory should exist before removal: %v", err)
	}

	if !orch.RemoveTierFromVault(vaultID, tierID) {
		t.Fatal("RemoveTierFromVault returned false")
	}

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("tier directory must survive non-destructive removal, got: %v", err)
	}
}

// TestDeleteTierFromVaultCleansTierDirectory verifies that DeleteTierFromVault
// removes the tier's data directory entirely — not just the chunk subdirs.
// Regression test for gastrolog-42j4n: orphaned tier directories accumulate
// on disk after tier deletion.
func TestDeleteTierFromVaultCleansTierDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	tier, dir := newFileTierInstance(t, tierID)
	if _, _, err := tier.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, tier)
	vault.Name = "delete-test"
	orch.RegisterVault(vault)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("tier directory should exist before deletion: %v", err)
	}

	if !orch.DeleteTierFromVault(vaultID, tierID) {
		t.Fatal("DeleteTierFromVault returned false")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("tier directory should be removed after DeleteTierFromVault, got: %v", err)
	}
}

// TestDeleteTierFromVaultCleansEmptyTierDirectory verifies that even an
// empty tier (no chunks appended) has its directory removed on deletion.
func TestDeleteTierFromVaultCleansEmptyTierDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	tier, dir := newFileTierInstance(t, tierID)

	vault := NewVault(vaultID, tier)
	vault.Name = "empty-delete-test"
	orch.RegisterVault(vault)

	if !orch.DeleteTierFromVault(vaultID, tierID) {
		t.Fatal("DeleteTierFromVault returned false")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("empty tier directory should be removed, got: %v", err)
	}
}

// avoid unused warning if testRecord isn't imported yet
var _ = chunk.Record{}
