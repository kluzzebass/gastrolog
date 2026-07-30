package server

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// TestHistogramFullyLocal_RequiresLeadership pins the leadership gate on
// the local-only histogram path. Selecting that path from local replica
// membership instead would let a follower-only node skip the cross-node
// fan-out and serve the histogram from purely local data. Followers
// receive only sealed chunks via replication — the active (un-sealed)
// chunk lives only on the leader and is never replicated — so the
// follower-only view drops every record currently in the active chunk,
// cutting the histogram off at the last sealed chunk's IngestEnd instead
// of running up to "now".
//
// histogramFullyLocal therefore requires local LEADERSHIP of every
// queried vault, so a follower node falls back to the leader-engine +
// remote-merge path that includes the leader's active chunk.
func TestHistogramFullyLocal_RequiresLeadership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: "node-1"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Stop() })

	leaderVaultID := glid.New()
	followerVaultID := glid.New()

	orch.RegisterVault(orchestrator.NewVault(leaderVaultID, mustVaultInstance(t, leaderVaultID, false)))
	orch.RegisterVault(orchestrator.NewVault(followerVaultID, mustVaultInstance(t, followerVaultID, true)))

	store := sysmem.NewStore()
	for _, vid := range []glid.GLID{leaderVaultID, followerVaultID} {
		if err := store.PutVault(ctx, system.VaultConfig{ID: vid, Name: "v-" + vid.String()}); err != nil {
			t.Fatalf("PutVault: %v", err)
		}
	}

	qs := NewQueryServer(orch, store, nil, "node-1", nil, nil, 0, 0, 0, nil)

	now := time.Now()

	leaderQ := query.Query{
		Start:    now.Add(-time.Hour),
		End:      now,
		BoolExpr: vaultEqualExpr(leaderVaultID),
	}
	if !qs.histogramFullyLocal(ctx, leaderQ) {
		t.Errorf("histogramFullyLocal(leader-only vault) = false; want true (this node leads every queried vault)")
	}

	followerQ := query.Query{
		Start:    now.Add(-time.Hour),
		End:      now,
		BoolExpr: vaultEqualExpr(followerVaultID),
	}
	if qs.histogramFullyLocal(ctx, followerQ) {
		t.Errorf("histogramFullyLocal(follower-only vault) = true; want false (active chunk lives on remote leader, must fan out)")
	}
}

func mustVaultInstance(t *testing.T, vaultID glid.GLID, isFollower bool) *orchestrator.VaultInstance {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		Now:            time.Now,
		MetaStore:      chunkmem.NewMetaStore(),
	})
	if err != nil {
		t.Fatalf("chunkmem.NewManager: %v", err)
	}
	im, err := indexmem.NewFactory()(nil, cm, nil)
	if err != nil {
		t.Fatalf("indexmem factory: %v", err)
	}
	return &orchestrator.VaultInstance{
		VaultID:    vaultID,
		Type:       "memory",
		Chunks:     cm,
		Indexes:    im,
		Query:      query.New(cm, im, nil),
		IsFollower: isFollower,
	}
}

func vaultEqualExpr(id glid.GLID) querylang.Expr {
	return &querylang.PredicateExpr{
		Kind:  querylang.PredKV,
		Op:    querylang.OpEq,
		Key:   "vault_id",
		Value: id.String(),
	}
}
