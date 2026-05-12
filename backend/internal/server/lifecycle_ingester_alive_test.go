package server

import (
	"context"
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

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

	aliveID := glid.New()
	if err := cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: aliveID, Name: "alive", Type: "syslog", Enabled: true,
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	if err := cfgStore.SetIngesterAlive(ctx, aliveID, "node-A", true); err != nil {
		t.Fatalf("SetIngesterAlive: %v", err)
	}
	if err := cfgStore.SetIngesterAlive(ctx, aliveID, "node-B", true); err != nil {
		t.Fatalf("SetIngesterAlive: %v", err)
	}

	silentID := glid.New()
	if err := cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: silentID, Name: "silent", Type: "syslog", Enabled: true,
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	// Deliberately no SetIngesterAlive call for silentID.

	srv := NewLifecycleServer(orch, nil, nil, cfgStore, "node-A", "", nil, nil, nil)

	got := srv.buildIngesterAlive(ctx)
	byID := make(map[glid.GLID]*apiv1.IngesterAlive)
	for _, ia := range got {
		byID[glid.FromBytes(ia.Id)] = ia
	}

	live, ok := byID[aliveID]
	if !ok {
		t.Fatal("alive ingester missing from buildIngesterAlive result")
	}
	if !live.NodeStatus["node-A"] || !live.NodeStatus["node-B"] {
		t.Errorf("alive map = %v, want node-A and node-B both true", live.NodeStatus)
	}

	silent, ok := byID[silentID]
	if !ok {
		t.Fatal("silent ingester missing from buildIngesterAlive result")
	}
	if len(silent.NodeStatus) != 0 {
		t.Errorf("silent ingester alive map = %v, want empty", silent.NodeStatus)
	}
}
