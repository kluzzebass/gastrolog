package server

import (
	"context"
	"testing"
	"time"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// putLiveNode seeds a NodeConfig with State=Live. buildIngesterAlive
// now intersects its FSM alive map with the set of nodes whose
// EffectiveState() is NodeStateLive (gastrolog-2kzb4), so tests that
// rely on a node-ID appearing in the result must register that node
// with the cfg store first.
func putLiveNode(t *testing.T, cfgStore *sysmem.Store, name string) glid.GLID {
	t.Helper()
	id := glid.New()
	if err := cfgStore.PutNode(context.Background(), system.NodeConfig{
		ID: id, Name: name, State: system.NodeStateLive, StateSince: time.Now(),
	}); err != nil {
		t.Fatalf("PutNode %s: %v", name, err)
	}
	return id
}

// TestBuildIngesterAlive covers the WatchSystemStatus-push payload that the
// inspector now reads from instead of polling ListIngesters. The handler
// must surface every configured ingester ID and reflect the FSM alive map,
// including the "no node has reported alive yet" case (empty/absent map).
func TestBuildIngesterAlive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)

	// Both nodes Live so the post-2kzb4 filter doesn't drop them.
	nodeA := putLiveNode(t, cfgStore, "node-A")
	nodeB := putLiveNode(t, cfgStore, "node-B")
	nodeAStr := nodeA.String()
	nodeBStr := nodeB.String()

	aliveID := glid.New()
	if err := cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: aliveID, Name: "alive", Type: "syslog", Enabled: true,
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	if err := cfgStore.SetIngesterAlive(ctx, aliveID, nodeAStr, true); err != nil {
		t.Fatalf("SetIngesterAlive: %v", err)
	}
	if err := cfgStore.SetIngesterAlive(ctx, aliveID, nodeBStr, true); err != nil {
		t.Fatalf("SetIngesterAlive: %v", err)
	}

	silentID := glid.New()
	if err := cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: silentID, Name: "silent", Type: "syslog", Enabled: true,
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	// Deliberately no SetIngesterAlive call for silentID.

	srv := NewLifecycleServer(orch, nil, nil, cfgStore, nodeAStr, "", nil, nil, nil)

	got := srv.buildIngesterAlive(ctx)
	byID := make(map[glid.GLID]*apiv1.IngesterAlive)
	for _, ia := range got {
		byID[glid.FromBytes(ia.Id)] = ia
	}

	live, ok := byID[aliveID]
	if !ok {
		t.Fatal("alive ingester missing from buildIngesterAlive result")
	}
	if !live.NodeStatus[nodeAStr] || !live.NodeStatus[nodeBStr] {
		t.Errorf("alive map = %v, want both nodes true", live.NodeStatus)
	}

	silent, ok := byID[silentID]
	if !ok {
		t.Fatal("silent ingester missing from buildIngesterAlive result")
	}
	if len(silent.NodeStatus) != 0 {
		t.Errorf("silent ingester alive map = %v, want empty", silent.NodeStatus)
	}
}

// TestBuildIngesterAlive_FiltersOfflineNodes is the gastrolog-2kzb4
// regression: an FSM alive entry for a node that is no longer Live
// must be dropped from the returned proto, so the inspector doesn't
// keep reporting "ingester running on a node that's clearly offline."
//
// Setup: two nodes registered, only node-A is Live. The ingester's
// FSM alive map says both are running. After filter: only node-A
// remains in NodeStatus.
func TestBuildIngesterAlive_FiltersOfflineNodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)

	nodeA := putLiveNode(t, cfgStore, "node-A")
	nodeAStr := nodeA.String()
	// node-B is Unreachable — registered in cfg store but NOT Live.
	nodeB := glid.New()
	if err := cfgStore.PutNode(ctx, system.NodeConfig{
		ID: nodeB, Name: "node-B", State: system.NodeStateUnreachable, StateSince: time.Now(),
	}); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}
	nodeBStr := nodeB.String()

	ingID := glid.New()
	if err := cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "chatty", Type: "syslog", Enabled: true,
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	// FSM still records B as alive (lingering from before B went offline).
	if err := cfgStore.SetIngesterAlive(ctx, ingID, nodeAStr, true); err != nil {
		t.Fatalf("SetIngesterAlive A: %v", err)
	}
	if err := cfgStore.SetIngesterAlive(ctx, ingID, nodeBStr, true); err != nil {
		t.Fatalf("SetIngesterAlive B: %v", err)
	}

	srv := NewLifecycleServer(orch, nil, nil, cfgStore, nodeAStr, "", nil, nil, nil)

	got := srv.buildIngesterAlive(ctx)
	if len(got) != 1 {
		t.Fatalf("expected 1 ingester row, got %d", len(got))
	}
	ns := got[0].NodeStatus
	if _, present := ns[nodeBStr]; present {
		t.Errorf("offline node-B still present in alive map: %v", ns)
	}
	if !ns[nodeAStr] {
		t.Errorf("live node-A missing or false in alive map: %v", ns)
	}
}

// TestBuildIngesterAlive_OperatorStatesAlsoFiltered verifies that the
// filter rejects every non-Live state, not just Unreachable.
// Maintenance / Draining / Decommissioning all imply "this node is
// not currently expected to be running the ingester" — leaving any
// of them in the alive map would mislead the inspector the same way
// Unreachable did.
func TestBuildIngesterAlive_OperatorStatesAlsoFiltered(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, state := range []system.NodeState{
		system.NodeStateMaintenance,
		system.NodeStateDraining,
		system.NodeStateDecommissioning,
	} {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			cfgStore := sysmem.NewStore()
			orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, LocalNodeID: "node-A"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(orch.Close)

			nodeA := putLiveNode(t, cfgStore, "node-A")
			nodeAStr := nodeA.String()
			nodeB := glid.New()
			now := time.Now()
			// Some operator states require a transition path; go via
			// Draining for Decommissioning (the legal pre-state).
			if state == system.NodeStateDecommissioning {
				if err := cfgStore.PutNode(ctx, system.NodeConfig{
					ID: nodeB, Name: "node-B", State: system.NodeStateDraining, StateSince: now,
				}); err != nil {
					t.Fatalf("PutNode draining: %v", err)
				}
				if err := cfgStore.SetNodeState(ctx, nodeB, system.NodeStateDecommissioning, now); err != nil {
					t.Fatalf("SetNodeState Decommissioning: %v", err)
				}
			} else {
				if err := cfgStore.PutNode(ctx, system.NodeConfig{
					ID: nodeB, Name: "node-B", State: state, StateSince: now,
				}); err != nil {
					t.Fatalf("PutNode %s: %v", state, err)
				}
			}
			nodeBStr := nodeB.String()

			ingID := glid.New()
			if err := cfgStore.PutIngester(ctx, system.IngesterConfig{
				ID: ingID, Name: "ing", Type: "syslog", Enabled: true,
			}); err != nil {
				t.Fatalf("PutIngester: %v", err)
			}
			if err := cfgStore.SetIngesterAlive(ctx, ingID, nodeAStr, true); err != nil {
				t.Fatalf("SetIngesterAlive A: %v", err)
			}
			if err := cfgStore.SetIngesterAlive(ctx, ingID, nodeBStr, true); err != nil {
				t.Fatalf("SetIngesterAlive B: %v", err)
			}

			srv := NewLifecycleServer(orch, nil, nil, cfgStore, nodeAStr, "", nil, nil, nil)

			got := srv.buildIngesterAlive(ctx)
			if len(got) != 1 {
				t.Fatalf("expected 1 ingester row, got %d", len(got))
			}
			ns := got[0].NodeStatus
			if _, present := ns[nodeBStr]; present {
				t.Errorf("state %s: node still present in alive map: %v", state, ns)
			}
			if !ns[nodeAStr] {
				t.Errorf("state %s: live node missing or false: %v", state, ns)
			}
		})
	}
}
