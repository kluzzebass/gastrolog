package memory

import (
	"context"
	"gastrolog/internal/glid"
	"testing"

	"gastrolog/internal/system"
	"gastrolog/internal/system/storetest"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	storetest.TestStore(t, func(t *testing.T) system.Store {
		return NewStore()
	})
}

// DeleteNode atomically sweeps every FSM map that references the deleted
// node ID — IngesterAlive flag entries, the NodeStorageConfig, and any
// IngesterAssignment pointing at the node. Deleting only the `nodes` map
// entry leaves references that surface as inspector badges reading "10/3"
// forever after a cluster scale-down.
func TestDeleteNodeSweepsReferences(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewStore()

	deletedNodeID := glid.New()
	deletedNodeStr := deletedNodeID.String()
	survivingNodeID := glid.New()
	survivingNodeStr := survivingNodeID.String()
	ingester1 := glid.New()
	ingester2 := glid.New()

	if err := s.PutNode(ctx, system.NodeConfig{ID: deletedNodeID, Name: "deleted"}); err != nil {
		t.Fatalf("put deleted node: %v", err)
	}
	if err := s.PutNode(ctx, system.NodeConfig{ID: survivingNodeID, Name: "survivor"}); err != nil {
		t.Fatalf("put surviving node: %v", err)
	}

	// Alive flags on both ingesters, both nodes. Surviving node must remain after the sweep.
	if err := s.SetIngesterAlive(ctx, ingester1, deletedNodeStr, true); err != nil {
		t.Fatalf("alive ing1/deleted: %v", err)
	}
	if err := s.SetIngesterAlive(ctx, ingester1, survivingNodeStr, true); err != nil {
		t.Fatalf("alive ing1/surviving: %v", err)
	}
	// ingester2 lives only on the about-to-be-deleted node — the whole
	// nested map should collapse after DeleteNode (no surviving
	// alive entries → no zombie ingester key).
	if err := s.SetIngesterAlive(ctx, ingester2, deletedNodeStr, true); err != nil {
		t.Fatalf("alive ing2/deleted: %v", err)
	}

	// NodeStorageConfig for the deleted node.
	if err := s.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       deletedNodeStr,
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 1}},
	}); err != nil {
		t.Fatalf("set nsc deleted: %v", err)
	}
	if err := s.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID:       survivingNodeStr,
		FileStorages: []system.FileStorage{{ID: glid.New(), StorageClass: 1}},
	}); err != nil {
		t.Fatalf("set nsc surviving: %v", err)
	}

	// IngesterAssignment: ingester1 assigned to deleted, ingester2 to surviving.
	if err := s.SetIngesterAssignment(ctx, ingester1, deletedNodeStr); err != nil {
		t.Fatalf("set assignment ing1: %v", err)
	}
	if err := s.SetIngesterAssignment(ctx, ingester2, survivingNodeStr); err != nil {
		t.Fatalf("set assignment ing2: %v", err)
	}

	// Sanity: pre-delete state has every reference.
	sys, err := s.Load(ctx)
	if err != nil || sys == nil {
		t.Fatalf("pre-delete load: %v", err)
	}
	if len(sys.Runtime.IngesterAlive[ingester1]) != 2 {
		t.Fatalf("pre-delete ing1 alive len = %d, want 2", len(sys.Runtime.IngesterAlive[ingester1]))
	}

	// THE FIX UNDER TEST.
	if err := s.DeleteNode(ctx, deletedNodeID); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Post-delete: no surviving reference to the deleted node.
	sys, err = s.Load(ctx)
	if err != nil || sys == nil {
		t.Fatalf("post-delete load: %v", err)
	}

	// nodes
	for _, n := range sys.Runtime.Nodes {
		if n.ID == deletedNodeID {
			t.Errorf("deleted node still present in Nodes")
		}
	}

	// IngesterAlive: deleted-node key gone from each remaining ingester;
	// the ingester whose only alive entry was the deleted node is collapsed entirely.
	if remaining, ok := sys.Runtime.IngesterAlive[ingester1]; ok {
		if _, hasDeleted := remaining[deletedNodeStr]; hasDeleted {
			t.Errorf("ing1 still references deleted node in IngesterAlive: %v", remaining)
		}
		if _, hasSurviving := remaining[survivingNodeStr]; !hasSurviving {
			t.Errorf("ing1 lost surviving node entry: %v", remaining)
		}
	}
	if _, ok := sys.Runtime.IngesterAlive[ingester2]; ok {
		t.Errorf("ing2 IngesterAlive entry should be collapsed (no surviving nodes), got %+v", sys.Runtime.IngesterAlive[ingester2])
	}

	// NodeStorageConfig: deleted node's NSC gone, surviving stays.
	var nscNodes []string
	for _, nsc := range sys.Runtime.NodeStorageConfigs {
		nscNodes = append(nscNodes, nsc.NodeID)
	}
	for _, n := range nscNodes {
		if n == deletedNodeStr {
			t.Errorf("NodeStorageConfig for deleted node still present: %v", nscNodes)
		}
	}
	if len(nscNodes) != 1 || nscNodes[0] != survivingNodeStr {
		t.Errorf("expected only surviving NSC, got %v", nscNodes)
	}

	// IngesterAssignment: ingester1 (assigned to deleted) is gone;
	// ingester2 (assigned to surviving) is preserved.
	if _, ok := sys.Runtime.IngesterAssignment[ingester1]; ok {
		t.Errorf("ing1 assignment should be cleared (pointed at deleted node), got %v", sys.Runtime.IngesterAssignment[ingester1])
	}
	if got, ok := sys.Runtime.IngesterAssignment[ingester2]; !ok || got != survivingNodeStr {
		t.Errorf("ing2 assignment should be preserved (%q), got %q (ok=%v)", survivingNodeStr, got, ok)
	}
}

func TestStoreIsolation(t *testing.T) {
	t.Parallel()
	s := NewStore()
	ctx := context.Background()

	ingesterID := glid.New()
	if err := s.PutIngester(ctx, system.IngesterConfig{
		ID: ingesterID, Type: "test", Params: map[string]string{"key": "value"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// Load and modify.
	got, err := s.GetIngester(ctx, ingesterID)
	if err != nil {
		t.Fatalf("GetIngester: %v", err)
	}
	modifiedID := glid.New()
	got.ID = modifiedID
	got.Params["key"] = "modified"

	// Get again should return unmodified copy.
	got2, err := s.GetIngester(ctx, ingesterID)
	if err != nil {
		t.Fatalf("GetIngester: %v", err)
	}
	if got2.ID != ingesterID {
		t.Errorf("expected ID %v, got %v", ingesterID, got2.ID)
	}
	if got2.Params["key"] != "value" {
		t.Errorf("expected Params[key] %q, got %q", "value", got2.Params["key"])
	}
}
