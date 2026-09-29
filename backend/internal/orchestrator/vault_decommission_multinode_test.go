package orchestrator_test

// Deleting a vault must retire its control-plane Raft group from every
// node's shared WAL. Vault-ctl groups span every cluster node, so a delete
// that only stops the group leaves each node's WAL carrying the group's
// registration, stable keys and log entries forever — and a restart replays
// them back into memory, the slow leak this pins shut.

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/raftgroup"
)

func TestOrchClusterVaultDeleteDecommissionsCtlGroupEverywhere(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault-delete acceptance test with node restarts")
	}

	h := newOrchRelHarness(t, 4)
	v := h.vaults[0]
	h.waitForAllReady()

	// Give the control-plane group real replicated state on every node.
	now := time.Now()
	for range 3 {
		if err := h.appendOnLeaderForVault(v, chunk.Record{
			SourceTS: now, IngestTS: now, Raw: []byte("doomed"),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// The vault-deleted notification reaches every node and force-removes
	// the vault there; emulate that fan-out directly (the dispatcher leg is
	// covered by app-level tests). Delete from config first so a restart
	// below cannot re-ensure the group from a config the delete had already
	// left.
	if err := h.cfgStore.DeleteVault(context.Background(), v.id, false); err != nil {
		t.Fatalf("cfgStore.DeleteVault: %v", err)
	}
	for _, id := range h.nodeIDs {
		if err := h.nodes[id].orch.ForceRemoveVault(v.id); err != nil {
			t.Fatalf("%s: ForceRemoveVault: %v", h.nodes[id].label, err)
		}
	}

	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	for _, id := range h.nodeIDs {
		if g := h.nodes[id].groupMgr.GetGroup(gid); g != nil {
			t.Fatalf("%s: control-plane group still running after vault delete", h.nodes[id].label)
		}
	}

	// Restart every node: WAL replay is where a stop-without-drop
	// resurrects the group.
	for _, id := range h.nodeIDs {
		h.stopNode(id)
	}
	for _, id := range h.nodeIDs {
		h.startNode(id)
	}

	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		gs := n.wal.GroupStore(gid)
		if last, err := gs.LastIndex(); err != nil || last != 0 {
			t.Errorf("%s: deleted vault's ctl group resurrected from WAL: LastIndex=%d err=%v, want 0",
				n.label, last, err)
		}
		if term, err := gs.GetUint64([]byte("CurrentTerm")); err == nil && term != 0 {
			t.Errorf("%s: deleted vault's ctl group kept CurrentTerm=%d across restart, want gone",
				n.label, term)
		}
	}
}

// UnregisterVault is reassignment, not deletion: the vault moves away while
// its control-plane group lives on, so the group's WAL state must survive —
// a node whose state is wrongly dropped would rejoin its peers voting from
// blank term and vote history.
func TestOrchUnregisterVaultKeepsCtlGroupWALState(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault reassignment acceptance test")
	}

	h := newOrchRelHarness(t, 4)
	v := h.vaults[0]
	h.waitForAllReady()

	now := time.Now()
	for range 3 {
		if err := h.appendOnLeaderForVault(v, chunk.Record{
			SourceTS: now, IngestTS: now, Raw: []byte("moving"),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	leaving := h.nodes[h.nodeIDs[3]]
	if err := leaving.orch.UnregisterVault(v.id); err != nil {
		t.Fatalf("UnregisterVault: %v", err)
	}

	if g := leaving.groupMgr.GetGroup(gid); g != nil {
		t.Fatalf("%s: control-plane group still running after unregister", leaving.label)
	}
	if last, err := leaving.wal.GroupStore(gid).LastIndex(); err != nil || last == 0 {
		t.Fatalf("%s: unregister dropped the ctl group's WAL state: LastIndex=%d err=%v, want > 0",
			leaving.label, last, err)
	}
}
