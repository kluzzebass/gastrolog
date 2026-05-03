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

// TestHistogramFullyLocal_RequiresLeadership is the regression for
// gastrolog-2g334. The bug: histogramFullyLocal used LocalReplicaTierIDs
// which includes follower tiers, so a node that's only a follower for
// a vault's tiers would skip the cross-node fan-out and serve the
// histogram from purely local data. Followers receive only sealed chunks
// via replication — the active (un-sealed) chunk lives only on the
// leader and is never replicated. The follower-only view drops every
// record currently in the active chunk, producing an empty right edge
// where the histogram cuts off at the last sealed chunk's IngestEnd
// instead of running up to "now".
//
// The fix gates the local-only path on local LEADERSHIP of every queried
// tier, so a follower node correctly falls back to the leader-engine +
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
	leaderTierID := glid.New()
	followerTierID := glid.New()

	orch.RegisterVault(orchestrator.NewVault(leaderVaultID, mustTierInstance(t, leaderTierID, false)))
	orch.RegisterVault(orchestrator.NewVault(followerVaultID, mustTierInstance(t, followerTierID, true)))

	store := sysmem.NewStore()
	for _, vid := range []glid.GLID{leaderVaultID, followerVaultID} {
		if err := store.PutVault(ctx, system.VaultConfig{ID: vid, Name: "v-" + vid.String()}); err != nil {
			t.Fatalf("PutVault: %v", err)
		}
	}
	for _, tc := range []struct {
		tierID  glid.GLID
		vaultID glid.GLID
	}{
		{leaderTierID, leaderVaultID},
		{followerTierID, followerVaultID},
	} {
		if err := store.PutTier(ctx, system.TierConfig{
			ID: tc.tierID, Name: "tier-" + tc.tierID.String(), Type: system.TierTypeMemory,
			VaultID: tc.vaultID, Position: 0,
		}); err != nil {
			t.Fatalf("PutTier: %v", err)
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
		t.Errorf("histogramFullyLocal(leader-only vault) = false; want true (this node leads every queried tier)")
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

func mustTierInstance(t *testing.T, tierID glid.GLID, isFollower bool) *orchestrator.TierInstance {
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
	return &orchestrator.TierInstance{
		TierID:     tierID,
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
