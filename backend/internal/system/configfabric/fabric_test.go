package configfabric

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// In Immediate mode a write on one node is visible on every node before the
// write returns; in Manual mode it is visible only on the writer until the
// fabric delivers it, and delivery is per node.
func TestFabricModelsReplication(t *testing.T) {
	ctx := context.Background()
	f := New(3)
	id := glid.New()
	if err := f.Node(0).Store().PutVault(ctx, system.VaultConfig{ID: id, Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if v, _ := f.Node(i).Store().GetVault(ctx, id); v == nil {
			t.Fatalf("immediate mode: node %d does not see the vault", i)
		}
	}

	f.SetMode(Manual)
	id2 := glid.New()
	if err := f.Node(1).Store().PutVault(ctx, system.VaultConfig{ID: id2, Name: "w", Type: system.VaultTypeMemory}); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.Node(1).Store().GetVault(ctx, id2); v == nil {
		t.Fatal("the writer must see its own write")
	}
	for _, i := range []int{0, 2} {
		if v, _ := f.Node(i).Store().GetVault(ctx, id2); v != nil {
			t.Fatalf("manual mode: node %d saw the write before delivery", i)
		}
		if f.Lag(i) != 1 {
			t.Fatalf("node %d lag = %d, want 1", i, f.Lag(i))
		}
	}
	f.Deliver(2)
	if v, _ := f.Node(2).Store().GetVault(ctx, id2); v == nil {
		t.Fatal("node 2 must see the write after delivery")
	}
	if v, _ := f.Node(0).Store().GetVault(ctx, id2); v != nil {
		t.Fatal("delivery to node 2 must not reach node 0")
	}
	f.DeliverAll()
	if v, _ := f.Node(0).Store().GetVault(ctx, id2); v == nil || f.Lag(0) != 0 {
		t.Fatal("node 0 must see the write after DeliverAll")
	}
}

// A stale node that writes applies its backlog first, in log order, so its
// own view is consistent when its write lands; its write still carries
// whatever it computed from the stale view.
func TestStaleWriterCatchesUpInOrder(t *testing.T) {
	ctx := context.Background()
	f := New(2)
	f.SetMode(Manual)
	a, b := glid.New(), glid.New()
	_ = f.Node(0).Store().PutVault(ctx, system.VaultConfig{ID: a, Name: "a", Type: system.VaultTypeMemory})
	_ = f.Node(1).Store().PutVault(ctx, system.VaultConfig{ID: b, Name: "b", Type: system.VaultTypeMemory})
	for i := range 2 {
		if va, _ := f.Node(i).Store().GetVault(ctx, a); va == nil {
			t.Fatalf("node %d lacks a after writing", i)
		}
	}
	if vb, _ := f.Node(0).Store().GetVault(ctx, b); vb != nil {
		t.Fatal("node 0 must not see b until delivery")
	}
	// The FSM's error is the write's error, on the writer and everywhere else.
	err := f.Node(0).Store().SetNodeState(ctx, glid.New(), system.NodeStateDraining, timeNow())
	if err == nil {
		t.Fatal("setting state on an unknown node must fail")
	}
}

func timeNow() time.Time { return time.Now() }
