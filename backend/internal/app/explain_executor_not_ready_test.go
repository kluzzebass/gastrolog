package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
)

// A peer asked to plan several vaults leaves out one that is not ready and
// plans the rest, as the coordinator's own plan leaves out a vault it leads
// that is not ready. The vault is planned once it is ready.
func TestExplainExecutorLeavesOutVaultNotReady(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	orch, readyVault := newExecutorVault(t, t0, 3, 0)

	v := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(10000)})
	if _, _, err := v.CM.Append(chunk.Record{IngestTS: t0, WriteTS: t0, Raw: []byte("held")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	heldVault := glid.New()
	var ready atomic.Bool
	orch.RegisterVault(orchestrator.NewVault(heldVault, &orchestrator.VaultInstance{
		VaultID:           heldVault,
		Type:              "memory",
		Chunks:            v.CM,
		Indexes:           v.IM,
		Query:             v.QE,
		ManifestReadFacet: orchestrator.ManifestReadFacet{IsFSMReady: ready.Load},
	}))

	exec := newExplainExecutor(orch, "peer")
	plannedVaults := func() map[glid.GLID]bool {
		t.Helper()
		plans, _, err := exec(context.Background(), []glid.GLID{readyVault, heldVault}, "")
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		out := map[glid.GLID]bool{}
		for _, p := range plans {
			out[glid.FromBytes(p.GetVaultId())] = true
		}
		return out
	}

	if got := plannedVaults(); !got[readyVault] || got[heldVault] {
		t.Fatalf("planned vaults while one is not ready = %v, want only %s", got, readyVault)
	}
	ready.Store(true)
	if got := plannedVaults(); !got[readyVault] || !got[heldVault] {
		t.Fatalf("planned vaults once ready = %v, want both %s and %s", got, readyVault, heldVault)
	}
}
