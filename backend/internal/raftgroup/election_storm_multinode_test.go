package raftgroup

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/waittest"

	hraft "github.com/hashicorp/raft"
)

// managerLiveness reports one GroupManager's liveness counters to a stats
// collector, as the node's Raft liveness adapter does.
type managerLiveness struct{ m *GroupManager }

func (managerLiveness) WALAppendTotals() (uint64, uint64) { return 0, 0 }
func (managerLiveness) TakeWALAppendMax() uint64          { return 0 }
func (l managerLiveness) RaftLiveness() (uint64, uint64, uint64) {
	return l.m.Liveness().Snapshot()
}

type stormLogSpy struct {
	mu    sync.Mutex
	storm int
}

func (s *stormLogSpy) Enabled(context.Context, slog.Level) bool { return true }
func (s *stormLogSpy) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "Raft election storm") {
		s.mu.Lock()
		s.storm++
		s.mu.Unlock()
	}
	return nil
}
func (s *stormLogSpy) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *stormLogSpy) WithGroup(string) slog.Handler      { return s }
func (s *stormLogSpy) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storm
}

// waitForLeaderEverywhere waits until every node's instance of every group
// knows a leader.
func waitForLeaderEverywhere(t *testing.T, nodes []*managerTestNode, groupIDs []string) {
	t.Helper()
	waittest.For(t, "every node to see a leader in every group", func() bool {
		for _, n := range nodes {
			for _, gid := range groupIDs {
				if addr, _ := n.manager.GetGroup(gid).Raft.LeaderWithID(); addr == "" {
					return false
				}
			}
		}
		return true
	})
}

// Creating vaults starts one vault-ctl group each, and each group's initial
// leader election must not read as an election storm on any node. Repeated
// elections in an established group still must.
func TestElectionStorm_FourNodes_NewGroupsCalmChurnStorms(t *testing.T) {
	if testing.Short() {
		t.Skip("spins up real 4-node raft groups and waits out election timing; -short skips")
	}
	ids := []string{"node-1", "node-2", "node-3", "node-4"}
	nodes := makeManagerCluster(t, ids)
	members := make([]hraft.Server, len(nodes))
	for i, n := range nodes {
		members[i] = hraft.Server{ID: hraft.ServerID(ids[i]), Address: n.transport.LocalAddr()}
	}

	spies := make([]*stormLogSpy, len(nodes))
	collectors := make([]*cluster.StatsCollector, len(nodes))
	for i, n := range nodes {
		spies[i] = &stormLogSpy{}
		collectors[i] = cluster.NewStatsCollector(cluster.StatsCollectorConfig{
			RaftLiveness: managerLiveness{n.manager},
			Logger:       slog.New(spies[i]),
			NodeID:       ids[i],
			NodeNameFn:   func() string { return ids[i] },
		})
	}
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tickAll := func() {
		clock = clock.Add(5 * time.Second)
		for _, c := range collectors {
			c.CollectLocalTick(clock)
		}
	}
	tickAll()

	// More groups than nodes, back to back inside one stats window: counted
	// initial elections would put at least three on some node.
	const groupCount = 12
	groupIDs := make([]string, groupCount)
	for g := range groupIDs {
		groupIDs[g] = VaultControlPlaneGroupID(glid.New())
		for i, n := range nodes {
			if _, err := n.manager.CreateGroup(GroupConfig{
				GroupID: groupIDs[g], FSM: &counterFSM{}, SeedMembers: members,
			}); err != nil {
				t.Fatalf("%s CreateGroup %s: %v", ids[i], groupIDs[g], err)
			}
		}
	}
	waitForLeaderEverywhere(t, nodes, groupIDs)
	tickAll()

	for i, spy := range spies {
		if got := spy.count(); got != 0 {
			e, _, _ := nodes[i].manager.Liveness().Snapshot()
			t.Errorf("%s logged %d election storms after creating %d groups (counted elections %d)",
				ids[i], got, groupCount, e)
		}
	}

	// Churn: commanded elections in one established group, steered so one
	// node starts at least three of them inside a single window.
	const target = 0
	gid := groupIDs[0]
	start, _, _ := nodes[target].manager.Liveness().Snapshot()
	for attempt := 0; ; attempt++ {
		if e, _, _ := nodes[target].manager.Liveness().Snapshot(); e-start >= 3 {
			break
		}
		if attempt == 20 {
			t.Fatalf("%s started fewer than 3 elections after %d leadership transfers", ids[target], attempt)
		}
		leaderIdx := -1
		for i, n := range nodes {
			if n.manager.GetGroup(gid).Raft.State() == hraft.Leader {
				leaderIdx = i
			}
		}
		if leaderIdx < 0 {
			waitForLeaderEverywhere(t, nodes, []string{gid})
			continue
		}
		to := target
		if leaderIdx == target {
			to = (target + 1) % len(nodes)
		}
		f := nodes[leaderIdx].manager.GetGroup(gid).Raft.LeadershipTransferToServer(
			hraft.ServerID(ids[to]), nodes[to].transport.LocalAddr())
		if err := f.Error(); err != nil {
			t.Logf("leadership transfer %s -> %s: %v", ids[leaderIdx], ids[to], err)
		}
		waitForLeaderEverywhere(t, nodes, []string{gid})
	}
	tickAll()

	if got := spies[target].count(); got != 1 {
		e, _, _ := nodes[target].manager.Liveness().Snapshot()
		t.Fatalf("%s logged %d election storms after starting %d elections in an established group, want 1",
			ids[target], got, e-start)
	}
}
