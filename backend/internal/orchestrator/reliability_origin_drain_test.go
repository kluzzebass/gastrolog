package orchestrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/system"
)

// The last route to a vault going away stops new records reaching it, but the
// records a non-home node already acked live only in that node's staging
// until the homes collect them. Every way the last route can go must leave
// them deliverable, including across a restart of the origin, and the origin
// must let go of the vault once nothing of it is left to deliver.
func TestOrchPipeline_OriginDeliversStagedRecordsAfterLastRouteGoes(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline durability test")
	}

	cases := []struct {
		name    string
		change  func(h *orchRelHarness, route system.RouteConfig)
		restart bool
	}{
		{name: "route deleted", change: (*orchRelHarness).deleteRoute},
		{name: "route disabled", change: (*orchRelHarness).disableRoute},
		{name: "route retargeted", change: (*orchRelHarness).retargetRouteToDefaultVault},
		{name: "route deleted and origin restarted", change: (*orchRelHarness).deleteRoute, restart: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newOrchRelHarness(t, 4,
				withExtraVault([]int{0, 1, 2}),
				withMatchAllRoute(1),
				withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
			)
			v := h.vaults[1]
			origin := h.nodeIDs[3] // not a home for v

			// Homes cannot pull from the paused origin, so every acked record
			// is still published-but-uncollected staging there when the route
			// goes.
			h.pausePeer(origin)
			bodies := h.submitIngestRecords(origin, 2*pipelineChunkMaxRecords, "origin-drain")

			tc.change(h, h.onlyRoute())
			h.reloadRoutesOnLiveNodes()
			if tc.restart {
				h.stopNode(origin)
				h.startNode(origin)
			}
			h.unpausePeer(origin)

			h.waitSealedRecords(v, h.nodeIDs[0], int64(len(bodies)))
			got := make(map[string]bool, len(bodies))
			for _, raw := range h.searchRecords(v, h.nodeIDs[0]) {
				got[string(raw)] = true
			}
			for body := range bodies {
				if !got[body] {
					t.Fatalf("acked record %q is not in vault %s after the last route went", body, v.label)
				}
			}

			h.waitOriginLetsGo(v, origin)
		})
	}
}

// onlyRoute returns the harness's single route.
func (h *orchRelHarness) onlyRoute() system.RouteConfig {
	h.t.Helper()
	routes, err := h.cfgStore.ListRoutes(context.Background())
	if err != nil {
		h.t.Fatalf("list routes: %v", err)
	}
	if len(routes) != 1 {
		h.t.Fatalf("want exactly one route, have %d", len(routes))
	}
	return routes[0]
}

func (h *orchRelHarness) deleteRoute(route system.RouteConfig) {
	h.t.Helper()
	if err := h.cfgStore.DeleteRoute(context.Background(), route.ID); err != nil {
		h.t.Fatalf("delete route %s: %v", route.Name, err)
	}
}

func (h *orchRelHarness) disableRoute(route system.RouteConfig) {
	h.t.Helper()
	route.Enabled = false
	if err := h.cfgStore.PutRoute(context.Background(), route); err != nil {
		h.t.Fatalf("disable route %s: %v", route.Name, err)
	}
}

func (h *orchRelHarness) retargetRouteToDefaultVault(route system.RouteConfig) {
	h.t.Helper()
	route.Destinations = []glid.GLID{h.vaults[0].id}
	if err := h.cfgStore.PutRoute(context.Background(), route); err != nil {
		h.t.Fatalf("retarget route %s: %v", route.Name, err)
	}
}

// reloadRoutesOnLiveNodes delivers a route change to every running node, as
// the config FSM's route notification does.
func (h *orchRelHarness) reloadRoutesOnLiveNodes() {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if n == nil || n.orch == nil {
			continue
		}
		if err := n.orch.ReloadFilters(context.Background()); err != nil {
			h.t.Fatalf("%s: reload routes: %v", n.label, err)
		}
	}
}

// waitOriginLetsGo waits until the origin holds no staging for the vault and
// no longer runs its pipeline: the drain is over and nothing keeps the
// registration alive.
func (h *orchRelHarness) waitOriginLetsGo(v vaultSpec, originID string) {
	h.t.Helper()
	n := h.nodes[originID]
	root := h.pipelineVaultRoot(originID, v)
	what := fmt.Sprintf("origin %s releasing vault %s", n.label, v.label)
	h.waitProgress(what, 50*time.Millisecond, func() (string, bool) {
		working, _ := paths.ListSegmentIDs(paths.WorkingDir(root))
		completed, _ := paths.ListSegmentIDs(paths.CompletedDir(root))
		registered := n.orch.RunsPipelineForTest(v.id)
		return fmt.Sprintf("working=%d completed=%d registered=%v", len(working), len(completed), registered),
			len(working) == 0 && len(completed) == 0 && !registered
	}, func() { h.dumpPipelineState(v) })
}
