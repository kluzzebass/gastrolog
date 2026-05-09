package orchestrator

import (
	"gastrolog/internal/glid"
	"os"
	"testing"

	"gastrolog/internal/chunk"
)

// TestRemoveTierFromVaultPreservesData verifies that RemoveVaultInstance is
// non-destructive: it unregisters the inst instance but leaves chunks and
// the inst directory intact, so placement flaps don't wipe data.
// See gastrolog-4vz40.
func TestRemoveTierFromVaultPreservesData(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	inst, dir := newFileTierInstance(t, tierID)
	if _, _, err := inst.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := inst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, inst)
	vault.Name = "remove-preserves"
	orch.RegisterVault(vault)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("inst directory should exist before removal: %v", err)
	}

	if !orch.RemoveVaultInstance(vaultID) {
		t.Fatal("RemoveVaultInstance returned false")
	}

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("inst directory must survive non-destructive removal, got: %v", err)
	}
}

// TestDeleteTierFromVaultCleansTierDirectory verifies that DeleteVaultInstance
// removes the inst's data directory entirely — not just the chunk subdirs.
// Regression test for gastrolog-42j4n: orphaned inst directories accumulate
// on disk after inst deletion.
func TestDeleteTierFromVaultCleansTierDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	inst, dir := newFileTierInstance(t, tierID)
	if _, _, err := inst.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := inst.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, inst)
	vault.Name = "delete-test"
	orch.RegisterVault(vault)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("inst directory should exist before deletion: %v", err)
	}

	if !orch.DeleteVaultInstance(vaultID) {
		t.Fatal("DeleteVaultInstance returned false")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("inst directory should be removed after DeleteVaultInstance, got: %v", err)
	}
}

// TestDeleteTierFromVaultCleansEmptyTierDirectory verifies that even an
// empty inst (no chunks appended) has its directory removed on deletion.
func TestDeleteTierFromVaultCleansEmptyTierDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := glid.New()
	vaultID := glid.New()

	inst, dir := newFileTierInstance(t, tierID)

	vault := NewVault(vaultID, inst)
	vault.Name = "empty-delete-test"
	orch.RegisterVault(vault)

	if !orch.DeleteVaultInstance(vaultID) {
		t.Fatal("DeleteVaultInstance returned false")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("empty inst directory should be removed, got: %v", err)
	}
}

// avoid unused warning if testRecord isn't imported yet
var _ = chunk.Record{}
