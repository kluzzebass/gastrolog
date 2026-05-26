package orchestrator

import (
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestSequencedVaultDiagnostics(t *testing.T) {
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	vaultID := glid.New()

	v := &Vault{
		ID:         vaultID,
		Name:       "seq",
		Enabled:    true,
		WriteModel: system.VaultWriteModelSequenced,
	}
	orch.mu.Lock()
	orch.vaults[vaultID] = v
	v.spool = orch.createVaultSpoolStore(v)
	orch.mu.Unlock()

	wireTestSeqAllocator(orch, vaultID)
	store := orch.vaultSpoolStore(vaultID)
	store.mu.Lock()
	store.ingestH = 12
	store.mu.Unlock()
	store.setMaterializationWatermark(8)
	store.setConvergenceWatermark(7)

	sub, err := orch.vaultCtlSubFSM(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	result := sub.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishFence(10, time.Now())})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("publish fence: %v", err)
	}

	diag, err := orch.SequencedVaultDiagnostics(vaultID)
	if err != nil {
		t.Fatal(err)
	}
	if diag.WriteModel != system.VaultWriteModelSequenced {
		t.Fatalf("write model = %q", diag.WriteModel)
	}
	if diag.IngestHighWatermark != 12 {
		t.Fatalf("H = %d, want 12", diag.IngestHighWatermark)
	}
	if diag.FenceHighWatermark != 10 {
		t.Fatalf("F_n = %d, want 10", diag.FenceHighWatermark)
	}
	if diag.MaterializationWatermark != 8 {
		t.Fatalf("M_r = %d, want 8", diag.MaterializationWatermark)
	}
	if diag.ConvergenceWatermark != 7 {
		t.Fatalf("C_r = %d, want 7", diag.ConvergenceWatermark)
	}
	if len(diag.Fences.Records) != 1 {
		t.Fatalf("fences = %d, want 1", len(diag.Fences.Records))
	}
}

func TestSequencedVaultDiagnosticsRejectsChunkAppend(t *testing.T) {
	orch := newTestOrch(t, Config{})
	vaultID := glid.New()
	orch.mu.Lock()
	orch.vaults[vaultID] = &Vault{ID: vaultID, WriteModel: system.VaultWriteModelChunkAppend}
	orch.mu.Unlock()

	_, err := orch.SequencedVaultDiagnostics(vaultID)
	if err == nil {
		t.Fatal("expected error for chunk_append vault")
	}
}
