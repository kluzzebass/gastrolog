package server_test

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// Each node reads its own config store. A write on the coordinator reaches
// the other nodes only when replication delivers it, so a test can hold a
// node stale and observe what it does with a view that lags the cluster.
func TestMultiNode_ConfigStoresArePerNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithManualReplication())

	// Setup converged before replication went manual: every node knows
	// every vault.
	for _, id := range []string{"coord", "data-1", "data-2"} {
		vaults, err := h.store(t, id).ListVaults(ctx)
		if err != nil || len(vaults) != 3 {
			t.Fatalf("%s sees %d vaults after setup (err %v), want 3", id, len(vaults), err)
		}
	}

	routeID := glid.New()
	if err := h.cfgStore.PutRoute(ctx, system.RouteConfig{
		ID:           routeID,
		Name:         "held-back",
		Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
		Destinations: []glid.GLID{h.nodes["coord"].vaultID}, Enabled: true,
	}); err != nil {
		t.Fatalf("PutRoute: %v", err)
	}
	if r, _ := h.store(t, "coord").GetRoute(ctx, routeID); r == nil {
		t.Fatal("the writer must see its own write")
	}
	for _, id := range []string{"data-1", "data-2"} {
		if r, _ := h.store(t, id).GetRoute(ctx, routeID); r != nil {
			t.Fatalf("%s saw the route before delivery: the stores are not per node", id)
		}
	}

	h.deliverAll()
	for _, id := range []string{"data-1", "data-2"} {
		if r, _ := h.store(t, id).GetRoute(ctx, routeID); r == nil {
			t.Fatalf("%s does not see the route after delivery", id)
		}
	}
}

// Without the option the harness converges on every write, which is what the
// rest of the suite assumes.
func TestMultiNode_ConfigReplicatesImmediatelyByDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := setupMultiNode(t, []string{"coord", "data-1"})
	routeID := glid.New()
	if err := h.cfgStore.PutRoute(ctx, system.RouteConfig{
		ID:           routeID,
		Name:         "now",
		Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
		Destinations: []glid.GLID{h.nodes["coord"].vaultID}, Enabled: true,
	}); err != nil {
		t.Fatalf("PutRoute: %v", err)
	}
	if r, _ := h.store(t, "data-1").GetRoute(ctx, routeID); r == nil {
		t.Fatal("immediate mode: data-1 must see the coordinator's write")
	}
}
