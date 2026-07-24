package app

import (
	"context"
	"io"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
)

// This file exercises the event-driven learner promoter against REAL
// hashicorp/raft membership changes: a fresh node is added as a Nonvoter
// learner to an established 4-voter group, catches up by replicating the
// log, and is promoted to Voter by the promoter engine — driven by a
// trigger, never a cron. Covers both group families: cluster-ctl (via the
// production clusterCtlPromotionGroup over a real-raft-backed membership)
// and vault-ctl (via the production vaultCtlPromotionGroup over a real
// raftgroup.Group). The peer-stats reader stands in for the NodeStats
// broadcast fabric, reporting the learner's real applied index — exactly
// what its broadcast would carry.
//
// These stand up real Raft instances and wait out election + replication
// timing, so they are -short-skipped like the other real-raft tests.

// noopFSM is a minimal FSM: every apply advances the raft log/applied
// index (which is all the promoter's catch-up comparison needs) but stores
// nothing.
type noopFSM struct{}

func (noopFSM) Apply(*hraft.Log) any                 { return nil }
func (noopFSM) Snapshot() (hraft.FSMSnapshot, error) { return noopSnapshot{}, nil }
func (noopFSM) Restore(rc io.ReadCloser) error       { return rc.Close() }

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink hraft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                              {}

// inmemRaftNode is one node in an in-process Raft cluster.
type inmemRaftNode struct {
	id    string
	raft  *hraft.Raft
	trans *hraft.InmemTransport
}

func newInmemRaftNode(t *testing.T, id string) *inmemRaftNode {
	t.Helper()
	_, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(id), 500*time.Millisecond)

	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(id)
	cfg.HeartbeatTimeout = 200 * time.Millisecond
	cfg.ElectionTimeout = 200 * time.Millisecond
	cfg.LeaderLeaseTimeout = 100 * time.Millisecond
	cfg.CommitTimeout = 20 * time.Millisecond
	cfg.LogOutput = io.Discard

	store := hraft.NewInmemStore()
	snap := hraft.NewInmemSnapshotStore()
	r, err := hraft.NewRaft(cfg, noopFSM{}, store, store, snap, trans)
	if err != nil {
		t.Fatalf("NewRaft %s: %v", id, err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })
	return &inmemRaftNode{id: id, raft: r, trans: trans}
}

// interconnect wires every node's transport to every other so Raft RPCs
// can flow between them.
func interconnect(nodes ...*inmemRaftNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.trans.Connect(hraft.ServerAddress(b.id), b.trans)
			}
		}
	}
}

func waitState(t *testing.T, r *hraft.Raft, want hraft.RaftState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.State() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node did not reach state %v within %s (now %v)", want, timeout, r.State())
}

// suffrageOf returns the learner's suffrage in the leader's configuration,
// or "" if absent.
func suffrageOf(r *hraft.Raft, id string) string {
	f := r.GetConfiguration()
	if f.Error() != nil {
		return ""
	}
	for _, s := range f.Configuration().Servers {
		if string(s.ID) == id {
			return s.Suffrage.String()
		}
	}
	return ""
}

// establishClusterWithLearner bootstraps a leader, adds voters up to
// voterCount total, then adds learnerID as a Nonvoter, applies enough
// entries for the learner to replicate, and returns the leader node and
// learner node. Fails the test on any timeout.
func establishClusterWithLearner(t *testing.T, voterCount int, learnerID string) (leader, learner *inmemRaftNode) {
	t.Helper()

	voters := make([]*inmemRaftNode, voterCount)
	for i := range voters {
		voters[i] = newInmemRaftNode(t, string(rune('1'+i))+"-voter")
	}
	learner = newInmemRaftNode(t, learnerID)
	all := append(append([]*inmemRaftNode{}, voters...), learner)
	interconnect(all...)

	// Bootstrap the first voter alone, then grow the voter set.
	leader = voters[0]
	boot := hraft.Configuration{Servers: []hraft.Server{
		{ID: hraft.ServerID(leader.id), Address: hraft.ServerAddress(leader.id), Suffrage: hraft.Voter},
	}}
	if err := leader.raft.BootstrapCluster(boot).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	waitState(t, leader.raft, hraft.Leader, 5*time.Second)

	for _, v := range voters[1:] {
		if err := leader.raft.AddVoter(hraft.ServerID(v.id), hraft.ServerAddress(v.id), 0, 5*time.Second).Error(); err != nil {
			t.Fatalf("AddVoter %s: %v", v.id, err)
		}
	}
	// Add the fresh node as a learner (Nonvoter).
	if err := leader.raft.AddNonvoter(hraft.ServerID(learner.id), hraft.ServerAddress(learner.id), 0, 5*time.Second).Error(); err != nil {
		t.Fatalf("AddNonvoter %s: %v", learner.id, err)
	}

	// Apply a batch of entries so there is a log for the learner to catch
	// up on.
	for i := 0; i < 20; i++ {
		if err := leader.raft.Apply([]byte("entry"), 5*time.Second).Error(); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	// Wait for the learner to replicate up to (near) the leader's index.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if learner.raft.AppliedIndex()+vaultCtlLearnerCatchupTolerance >= leader.raft.AppliedIndex() {
			return leader, learner
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("learner did not catch up: learner=%d leader=%d",
		learner.raft.AppliedIndex(), leader.raft.AppliedIndex())
	return leader, learner
}

// clusterCtlLivePeerStats reports the learner's live applied index in the
// top-level RaftAppliedIndex field — exactly what the learner's NodeStats
// broadcast carries for the cluster-ctl group. Reading it live from the
// learner's raft models genuine catch-up progress arriving over the
// broadcast fabric.
type clusterCtlLivePeerStats struct {
	learnerID string
	learner   *hraft.Raft
}

func (s *clusterCtlLivePeerStats) Get(senderID string) *gastrologv1.NodeStats {
	if senderID != s.learnerID {
		return nil
	}
	return &gastrologv1.NodeStats{RaftAppliedIndex: s.learner.AppliedIndex()}
}

// vaultCtlLivePeerStats is the per-vault analogue: the learner's live
// applied index in a VaultStats entry for the vault under test.
type vaultCtlLivePeerStats struct {
	learnerID string
	learner   *hraft.Raft
	vaultID   glid.GLID
}

func (s *vaultCtlLivePeerStats) Get(senderID string) *gastrologv1.NodeStats {
	if senderID != s.learnerID {
		return nil
	}
	return &gastrologv1.NodeStats{
		Vaults: []*gastrologv1.VaultStats{vaultStatsByID(s.vaultID, s.learner.AppliedIndex())},
	}
}

// driveUntilVoter starts the promoter and triggers it until the learner is
// a Voter in the leader's configuration, or the deadline passes.
func driveUntilVoter(t *testing.T, p *learnerPromoter, leaderRaft *hraft.Raft, learnerID string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.trigger()
		if suffrageOf(leaderRaft, learnerID) == "Voter" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("learner %s not promoted to Voter (suffrage=%q)", learnerID, suffrageOf(leaderRaft, learnerID))
}

// TestMultiNode_ClusterCtlLearnerPromotedEventDriven: a fresh node added as
// a Nonvoter to an established 4-voter cluster-ctl group is promoted to
// Voter by the event-driven promoter once caught up.
func TestMultiNode_ClusterCtlLearnerPromotedEventDriven(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up a real 5-node raft group and waits out election + replication timing; -short skips")
	}
	learnerID := "fresh-node"
	leader, learner := establishClusterWithLearner(t, 4, learnerID)

	membership := realRaftMembership{r: leader.raft}
	ps := &clusterCtlLivePeerStats{learnerID: learnerID, learner: learner.raft}
	p := newClusterCtlLearnerPromoter(membership, ps, quietLogger())

	driveUntilVoter(t, p, leader.raft, learnerID)
}

// TestMultiNode_VaultCtlLearnerPromotedEventDriven: same scenario against a
// vault-ctl group, driving the production vaultCtlPromotionGroup over a
// real raftgroup.Group.
func TestMultiNode_VaultCtlLearnerPromotedEventDriven(t *testing.T) {
	if testing.Short() {
		t.Skip("stands up a real 5-node raft group and waits out election + replication timing; -short skips")
	}
	learnerID := "fresh-node"
	leader, learner := establishClusterWithLearner(t, 4, learnerID)

	vaultID := glid.New()
	g := &vaultCtlPromotionGroup{
		vaultID:   vaultID,
		group:     &raftgroup.Group{Raft: leader.raft},
		peerState: &vaultCtlLivePeerStats{learnerID: learnerID, learner: learner.raft, vaultID: vaultID},
		logger:    quietLogger(),
	}
	p := newLearnerPromoter("vault-ctl", func() []promotionGroup { return []promotionGroup{g} }, quietLogger())

	driveUntilVoter(t, p, leader.raft, learnerID)
}

// realRaftMembership adapts a live *hraft.Raft to the raftMembership seam
// the cluster-ctl promoter consumes (in production this is cluster.Server).
type realRaftMembership struct{ r *hraft.Raft }

func (m realRaftMembership) IsLeader() bool { return m.r.State() == hraft.Leader }

func (m realRaftMembership) Servers() ([]cluster.RaftServer, error) {
	f := m.r.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	var out []cluster.RaftServer
	for _, s := range f.Configuration().Servers {
		out = append(out, cluster.RaftServer{
			ID:       string(s.ID),
			Address:  string(s.Address),
			Suffrage: s.Suffrage.String(),
		})
	}
	return out, nil
}

func (m realRaftMembership) AddVoter(id, addr string, timeout time.Duration) error {
	return m.r.AddVoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, timeout).Error()
}

func (m realRaftMembership) LocalStats() map[string]string { return m.r.Stats() }
