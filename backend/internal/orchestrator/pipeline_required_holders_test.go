package orchestrator

// Coverage at the orchestrator wiring seam: the ChunkRequiredHolders closure
// buildPipelineVaultSpec hands to chunking must report ok=false when the
// placement lookup comes back empty — a placed vault
// always has at least one member, so empty means the lookup failed (config
// load error, vault dropped from config) and the chunking release/purge gates
// must fail closed rather than reading empty as "no holders required".

import (
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestChunkRequiredHoldersReportsUnresolvedOnEmptyLookup(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})

	buildSpec := func() func() ([]string, bool) {
		t.Helper()
		spec, err := orch.buildPipelineVaultSpec(vaultID, true, vaultctlfsm.New(), nil, func() bool { return true }, true, chunking.ManifestRotationPolicy{})
		if err != nil {
			t.Fatalf("buildPipelineVaultSpec: %v", err)
		}
		if spec.ChunkRequiredHolders == nil {
			t.Fatal("home spec missing ChunkRequiredHolders")
		}
		return spec.ChunkRequiredHolders
	}

	// Vault absent from config (lookup fails): unresolved, never "no holders".
	orch.setSystemLoader(testSystemLoader{cfg: &system.Config{}})
	if ids, ok := buildSpec()(); ok || len(ids) != 0 {
		t.Fatalf("empty lookup = (%v, %v), want unresolved", ids, ok)
	}

	// Vault placed on two nodes: resolved with both members.
	storageA, storageB := glid.New(), glid.New()
	orch.setSystemLoader(testSystemLoaderWithRuntime{
		cfg: &system.Config{
			Vaults: []system.VaultConfig{{
				ID:   vaultID,
				Name: "placed",
				Type: system.VaultTypeFile,
			}},
		},
		rt: system.Runtime{
			VaultPlacements: map[glid.GLID][]system.VaultPlacement{
				vaultID: {
					{StorageID: storageA.String(), Leader: true},
					{StorageID: storageB.String()},
				},
			},
			NodeStorageConfigs: []system.NodeStorageConfig{
				{NodeID: "node-A", FileStorages: []system.FileStorage{{ID: storageA}}},
				{NodeID: "node-B", FileStorages: []system.FileStorage{{ID: storageB}}},
			},
		},
	})
	ids, ok := buildSpec()()
	if !ok || len(ids) != 2 {
		t.Fatalf("placed lookup = (%v, %v), want both members resolved", ids, ok)
	}
}
