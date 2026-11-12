package orchestrator_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftlevel"
	"gastrolog/internal/vaultraft"

	"google.golang.org/protobuf/encoding/protowire"
)

// TestVaultCtlGatesCommandAboveActiveLevel runs a four-node vault-ctl group
// whose nodes know a level-2 command type (the table puts the vault-ctl noop
// at level 2). Proposals of it are refused at the proposing node and, for a
// proposer that skips its own gate, at the leader, until the cluster's active
// level reaches 2; after that every node's proposals apply. The leader also
// refuses to propose an entry it cannot decode itself.
func TestVaultCtlGatesCommandAboveActiveLevel(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("four-node vault-ctl raft group")
	}
	noopAt2 := raftlevel.DefaultTable().With(
		raftlevel.Case((&gastrologv1.VaultRaftCommand{}).ProtoReflect().Descriptor(), "noop"), 2)
	h := newOrchRelHarness(t, 4, withProtocolLevels(noopAt2))
	ctx := context.Background()
	gid := raftgroup.VaultControlPlaneGroupID(h.vaultID)

	leader := h.waitForVaultCtlLeader()
	var follower *orchRelNode
	for _, id := range h.nodeIDs {
		if h.nodes[id] != leader {
			follower = h.nodes[id]
			break
		}
	}
	if err := h.cfgStore.SetProtocolLevel(ctx, 1); err != nil {
		t.Fatal(err)
	}

	for _, n := range []*orchRelNode{follower, leader} {
		if err := n.orch.ApplyVaultControlPlane(h.vaultID, vaultraft.MarshalNoop()); !errors.Is(err, raftlevel.ErrCommandAboveClusterLevel) {
			t.Fatalf("%s: proposal at active level 1 = %v, want ErrCommandAboveClusterLevel", n.label, err)
		}
	}

	// A proposer without the gate: the follower forwards straight to the
	// leader, which refuses before anything reaches the log.
	leaderGroup := leader.groupMgr.GetGroup(gid)
	followerGroup := follower.groupMgr.GetGroup(gid)
	ungated := cluster.NewVaultApplyForwarder(followerGroup.Raft, gid, nil, follower.peerConns, cluster.RaftApplyTimeout)
	unknown := protowire.AppendBytes(protowire.AppendTag(nil, 999, protowire.BytesType), nil)
	for _, tc := range []struct {
		name string
		data []byte
		want error
	}{
		{"above the active level", vaultraft.MarshalNoop(), raftlevel.ErrCommandAboveClusterLevel},
		{"unknown command type", unknown, raftlevel.ErrUnknownCommand},
		{"undecodable bytes", []byte{0xFF}, raftlevel.ErrUndecodableCommand},
	} {
		if err := leaderGroup.Raft.Barrier(cluster.RaftApplyTimeout).Error(); err != nil {
			t.Fatal(err)
		}
		before := leaderGroup.Raft.LastIndex()
		err := ungated.Apply(tc.data)
		if err == nil || !strings.Contains(err.Error(), tc.want.Error()) {
			t.Fatalf("%s: forwarded to the leader = %v, want a refusal saying %q", tc.name, err, tc.want)
		}
		if got := leaderGroup.Raft.LastIndex(); got != before {
			t.Fatalf("%s: leader log grew from %d to %d", tc.name, before, got)
		}
	}

	if err := h.cfgStore.SetProtocolLevel(ctx, 2); err != nil {
		t.Fatal(err)
	}
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if err := n.orch.ApplyVaultControlPlane(h.vaultID, vaultraft.MarshalNoop()); err != nil {
			t.Fatalf("%s: proposal at active level 2: %v", n.label, err)
		}
	}
	if err := ungated.Apply(vaultraft.MarshalNoop()); err != nil {
		t.Fatalf("forwarded proposal at active level 2: %v", err)
	}
}
