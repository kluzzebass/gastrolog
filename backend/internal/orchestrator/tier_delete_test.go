package orchestrator

import (
	"os"
	"testing"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// TestRemoveTierFromVaultCleansTierDirectory verifies that RemoveTierFromVault
// removes the tier's data directory entirely — not just the chunk subdirs.
// Regression test for gastrolog-42j4n: orphaned tier directories accumulate
// on disk after tier deletion.
func TestRemoveTierFromVaultCleansTierDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	tier, dir := newFileTierInstance(t, tierID)
	// Append and seal a chunk so the tier has data on disk.
	if _, _, err := tier.Chunks.Append(testRecord("data")); err != nil {
		t.Fatal(err)
	}
	if err := tier.Chunks.Seal(); err != nil {
		t.Fatal(err)
	}

	vault := NewVault(vaultID, tier)
	vault.Name = "delete-test"
	orch.RegisterVault(vault)

	// Sanity check: tier directory exists before deletion.
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("tier directory should exist before deletion: %v", err)
	}

	if !orch.RemoveTierFromVault(vaultID, tierID) {
		t.Fatal("RemoveTierFromVault returned false")
	}

	// After removal, the tier's data directory must be gone — not just
	// empty. An empty directory is still garbage that accumulates over time.
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("tier directory should be removed after RemoveTierFromVault, got: %v", err)
	}
}

// TestRemoveTierFromVaultCleansEmptyTierDirectory verifies that even an
// empty tier (no chunks appended) has its directory removed on deletion.
func TestRemoveTierFromVaultCleansEmptyTierDirectory(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	tierID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	tier, dir := newFileTierInstance(t, tierID)

	vault := NewVault(vaultID, tier)
	vault.Name = "empty-delete-test"
	orch.RegisterVault(vault)

	if !orch.RemoveTierFromVault(vaultID, tierID) {
		t.Fatal("RemoveTierFromVault returned false")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("empty tier directory should be removed, got: %v", err)
	}
}

// avoid unused warning if testRecord isn't imported yet
var _ = chunk.Record{}
