// Outer FSM (vaultraft) tests for the fan-out placement extension.
// gastrolog-4cxw0: confirm OpVaultChunkFSM correctly routes the new
// placement commands to per-vault sub-FSMs, and that the cross-vault
// snapshot/restore preserves placement state.

package vaultraft

import (
	"bytes"
	"io"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestFSM_PlacementCommandsRouteToVaultSubFSM(t *testing.T) {
	t.Parallel()
	f := NewFSM()
	vaultID := glid.New()
	cid := testChunkID(11)
	now := time.Now().Truncate(time.Nanosecond)

	// Create a fan-out chunk via the outer FSM with an initial
	// Receiving set.
	wire := vaultctlfsm.MarshalCreateChunkWithReceiving(cid, now, now, now,
		[]string{"node-A", "node-B"})
	if err := f.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultID, wire)}); err != nil {
		t.Fatalf("apply create: %v", err)
	}

	sub := f.VaultFSM(vaultID)
	if sub == nil {
		t.Fatal("vault sub-FSM missing")
	}
	p := sub.Placement(cid)
	if p == nil {
		t.Fatal("placement not stamped at create time")
	}
	if !slices.Equal(p.Receiving, []string{"node-A", "node-B"}) {
		t.Errorf("Receiving = %v, want [node-A node-B]", p.Receiving)
	}

	// Add a third receiver via the outer FSM.
	addWire := vaultctlfsm.MarshalAddReceiving(cid, "node-C")
	if err := f.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultID, addWire)}); err != nil {
		t.Fatalf("apply add: %v", err)
	}
	p = sub.Placement(cid)
	if !slices.Equal(p.Receiving, []string{"node-A", "node-B", "node-C"}) {
		t.Errorf("after add: Receiving = %v", p.Receiving)
	}
}

func TestFSM_PlacementSurvivesOuterSnapshotRoundtrip(t *testing.T) {
	t.Parallel()
	src := NewFSM()

	// Two vaults to exercise the per-vault sub-FSM snapshot fan-out.
	vaultA, vaultB := glid.New(), glid.New()
	if bytes.Compare(vaultA[:], vaultB[:]) > 0 {
		vaultA, vaultB = vaultB, vaultA
	}

	cidA := testChunkID(20)
	cidB := testChunkID(21)
	now := time.Now().Truncate(time.Nanosecond)

	createA := vaultctlfsm.MarshalCreateChunkWithReceiving(cidA, now, now, now,
		[]string{"node-A", "node-B", "node-C"})
	createB := vaultctlfsm.MarshalCreateChunkWithReceiving(cidB, now, now, now,
		[]string{"node-A", "node-B"})
	beginRemoval := vaultctlfsm.MarshalBeginHoldingRemoval(cidA, "node-A", []string{"node-B", "node-C"})
	ack := vaultctlfsm.MarshalAckPull(cidA, "node-A", "node-B")

	for _, w := range [][]byte{createA, beginRemoval, ack} {
		if err := src.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultA, w)}); err != nil {
			t.Fatalf("apply on vault A: %v", err)
		}
	}
	if err := src.Apply(&hraft.Log{Data: MarshalVaultChunkCommand(vaultB, createB)}); err != nil {
		t.Fatalf("apply on vault B: %v", err)
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := NewFSM()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Vault A: half-drained PendingPulls.
	subA := dst.VaultFSM(vaultA)
	if subA == nil {
		t.Fatal("vault A sub-FSM missing after restore")
	}
	pA := subA.Placement(cidA)
	if pA == nil {
		t.Fatal("placement A missing after restore")
	}
	if !slices.Equal(pA.Receiving, []string{"node-A", "node-B", "node-C"}) {
		t.Errorf("vault A Receiving = %v", pA.Receiving)
	}
	if pA.PendingPulls == nil || pA.PendingPulls["node-A"] == nil {
		t.Fatalf("vault A PendingPulls dropped: %v", pA.PendingPulls)
	}
	if pA.PendingPulls["node-A"]["node-B"] {
		t.Error("acked toNode survived snapshot")
	}
	if !pA.PendingPulls["node-A"]["node-C"] {
		t.Error("non-acked toNode missing after restore")
	}

	// Vault B: untouched placement.
	subB := dst.VaultFSM(vaultB)
	if subB == nil {
		t.Fatal("vault B sub-FSM missing after restore")
	}
	pB := subB.Placement(cidB)
	if pB == nil {
		t.Fatal("placement B missing after restore")
	}
	if !slices.Equal(pB.Receiving, []string{"node-A", "node-B"}) {
		t.Errorf("vault B Receiving = %v", pB.Receiving)
	}
	if pB.PendingPulls != nil {
		t.Errorf("vault B PendingPulls = %v, want nil", pB.PendingPulls)
	}
}
