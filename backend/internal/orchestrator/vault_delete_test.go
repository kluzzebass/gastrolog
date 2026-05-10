package orchestrator

import (
	"gastrolog/internal/glid"
	"os"
	"testing"

	"gastrolog/internal/chunk"
)

// TestRemoveInstanceFromVaultPreservesData verifies that RemoveVaultInstance is
// non-destructive: it unregisters the vault instance but leaves chunks and
// the vault directory intact, so placement flaps don't wipe data.
// See gastrolog-4vz40.
func TestRemoveInstanceFromVaultPreservesData(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()

	vaultInst, dir := newFileInstance(t, vaultID)
	if _, _, err := vaultInst.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, vaultInst)
	vault.Name = "remove-preserves"
	orch.RegisterVault(vault)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("vault directory should exist before removal: %v", err)
	}

	if !orch.RemoveVaultInstance(vaultID) {
		t.Fatal("RemoveVaultInstance returned false")
	}

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("vault directory must survive non-destructive removal, got: %v", err)
	}
}

// TestDeleteInstanceFromVaultCleansVaultDirectory verifies that DeleteVaultInstance
// removes the vault's data directory entirely — not just the chunk subdirs.
// Regression test for gastrolog-42j4n: orphaned instance directories accumulate
// on disk after instance deletion.
func TestDeleteInstanceFromVaultCleansVaultDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()

	vaultInst, dir := newFileInstance(t, vaultID)
	if _, _, err := vaultInst.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := vaultInst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, vaultInst)
	vault.Name = "delete-test"
	orch.RegisterVault(vault)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("vault directory should exist before deletion: %v", err)
	}

	if !orch.DeleteVaultInstance(vaultID) {
		t.Fatal("DeleteVaultInstance returned false")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("vault directory should be removed after DeleteVaultInstance, got: %v", err)
	}
}

// TestDeleteInstanceFromVaultCleansEmptyVaultDirectory verifies that even an
// empty instance (no chunks appended) has its directory removed on deletion.
func TestDeleteInstanceFromVaultCleansEmptyVaultDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	vaultID := glid.New()

	vaultInst, dir := newFileInstance(t, vaultID)

	vault := NewVault(vaultID, vaultInst)
	vault.Name = "empty-delete-test"
	orch.RegisterVault(vault)

	if !orch.DeleteVaultInstance(vaultID) {
		t.Fatal("DeleteVaultInstance returned false")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("empty vault directory should be removed, got: %v", err)
	}
}

// avoid unused warning if testRecord isn't imported yet
var _ = chunk.Record{}
