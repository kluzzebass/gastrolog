package server_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// The protocol level in a node record is the node's own report of its
// binary. Renaming the node keeps it, and the system view shows it.
func TestRenamingANodeKeepsItsProtocolLevel(t *testing.T) {
	t.Parallel()
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	if err := cfgStore.PutNode(ctx, system.NodeConfig{ID: id, Name: "before", ProtocolLevel: 3}); err != nil {
		t.Fatal(err)
	}

	resp, err := client.PutNodeConfig(ctx, connect.NewRequest(&gastrologv1.PutNodeConfigRequest{
		Config: &gastrologv1.NodeConfig{Id: id.Bytes(), Name: "after"},
	}))
	if err != nil {
		t.Fatalf("PutNodeConfig: %v", err)
	}

	got, err := cfgStore.GetNode(ctx, id)
	if err != nil || got == nil || got.Name != "after" || got.ProtocolLevel != 3 {
		t.Fatalf("node record after rename = %+v (%v), want name after at protocol level 3", got, err)
	}
	var shown *gastrologv1.NodeConfig
	for _, n := range resp.Msg.GetSystem().GetNodeConfigs() {
		if glid.FromBytes(n.GetId()) == id {
			shown = n
		}
	}
	if shown == nil || shown.GetProtocolLevel() != 3 {
		t.Fatalf("system view shows %+v, want protocol level 3", shown)
	}
}
