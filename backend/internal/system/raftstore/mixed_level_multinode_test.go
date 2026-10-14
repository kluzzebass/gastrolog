package raftstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftlevel"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// oldCodecFSM is the cluster-ctl FSM as a binary at a given protocol level
// sees the log: a command type above that level decodes as an unknown field,
// exactly as a newer binary's command type decodes in an older binary.
type oldCodecFSM struct {
	*raftfsm.FSM
	level *atomic.Uint32
	table raftlevel.Table
}

func (f oldCodecFSM) Apply(l *hraft.Log) any {
	var cmd gastrologv1.SystemCommand
	if proto.Unmarshal(l.Data, &cmd) == nil {
		if level, _, err := f.table.SystemCommandLevel(&cmd); err == nil && level > f.level.Load() {
			l = &hraft.Log{Index: l.Index, Term: l.Term, Type: l.Type, Data: unknownCommandType()}
		}
	}
	return f.FSM.Apply(l)
}

// levelNode is one member of the mixed-level cluster.
type levelNode struct {
	id        string
	addr      hraft.ServerAddress
	level     atomic.Uint32 // the protocol level of the binary the node runs
	fsm       *raftfsm.FSM
	raft      *hraft.Raft
	store     *Store
	transport *hraft.InmemTransport
	logs      *hraft.InmemStore
	stable    *hraft.InmemStore
	snaps     *hraft.InmemSnapshotStore
	stopped   chan error // unapplicable-hook calls on this node
	raiser    atomic.Pointer[raftlevel.Raiser]
}

type levelCluster struct {
	t       *testing.T
	nodes   []*levelNode
	table   raftlevel.Table
	raised  chan uint32
	elected chan hraft.Observation // leadership changes on any node
	ctx     context.Context
}

func (c *levelCluster) leader() (*levelNode, error) {
	for _, n := range c.nodes {
		if n.raft != nil && n.raft.State() == hraft.Leader {
			return n, nil
		}
	}
	return nil, errors.New("no cluster-ctl leader")
}

// clusterForwarder hands a follower's command to the current leader's
// ApplyRaw, as the ForwardApply RPC does.
type clusterForwarder struct{ c *levelCluster }

func (f clusterForwarder) Forward(_ context.Context, data []byte) (uint64, error) {
	l, err := f.c.leader()
	if err != nil {
		return 0, err
	}
	return l.store.ApplyRaw(data)
}

// raiseRecorder reports each committed raise, and checks at that moment that
// every node record carries at least the new level.
type raiseRecorder struct {
	*Store
	c *levelCluster
}

func (r raiseRecorder) SetProtocolLevel(ctx context.Context, level uint32) error {
	nodes, err := r.ListNodes(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if n.ProtocolLevel < level {
			return fmt.Errorf("raise to %d while node %s reports %d", level, n.ID, n.ProtocolLevel)
		}
	}
	if err := r.Store.SetProtocolLevel(ctx, level); err != nil {
		return err
	}
	r.c.raised <- level
	return nil
}

func raftTestConfig(id string) *hraft.Config {
	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(id)
	conf.LogOutput = io.Discard
	conf.HeartbeatTimeout = 100 * time.Millisecond
	conf.ElectionTimeout = 100 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	return conf
}

// start runs the node's Raft with an FSM for a binary at binaryLevel, over
// the node's persistent log, stable and snapshot stores.
func (c *levelCluster) start(n *levelNode, binaryLevel uint32) {
	t := c.t
	n.level.Store(binaryLevel)
	n.fsm = raftfsm.New(
		raftfsm.WithBinaryLevel(binaryLevel),
		raftfsm.WithOnUnapplicable(func(_ string, _ uint64, reason error) { n.stopped <- reason }),
		raftfsm.WithOnApply(func(note raftfsm.Notification) {
			if note.Kind == raftfsm.NotifyNodeConfigPut || note.Kind == raftfsm.NotifyNodeConfigDeleted {
				if r := n.raiser.Load(); r != nil {
					r.Wake()
				}
			}
		}),
	)
	r, err := hraft.NewRaft(raftTestConfig(n.id), oldCodecFSM{FSM: n.fsm, level: &n.level, table: c.table},
		n.logs, n.stable, n.snaps, n.transport)
	if err != nil {
		t.Fatalf("NewRaft %s: %v", n.id, err)
	}
	n.raft = r
	n.store = New(r, n.fsm, 5*time.Second)
	n.store.gate = n.store.gate.WithTable(c.table)
	n.store.SetForwarder(clusterForwarder{c})

	raiser := raftlevel.NewRaiser(raiseRecorder{Store: n.store, c: c},
		func() bool { return r.State() == hraft.Leader },
		func() ([]string, error) {
			f := r.GetConfiguration()
			if err := f.Error(); err != nil {
				return nil, err
			}
			ids := make([]string, 0, len(f.Configuration().Servers))
			for _, s := range f.Configuration().Servers {
				ids = append(ids, string(s.ID))
			}
			return ids, nil
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	n.raiser.Store(raiser)
	leaderCh := make(chan hraft.Observation, 4)
	isLeaderChange := func(o *hraft.Observation) bool {
		_, ok := o.Data.(hraft.LeaderObservation)
		return ok
	}
	r.RegisterObserver(hraft.NewObserver(leaderCh, true, isLeaderChange))
	r.RegisterObserver(hraft.NewObserver(c.elected, false, isLeaderChange))
	ctx, cancel := context.WithCancel(c.ctx)
	done := make(chan struct{})
	go func() { raiser.Run(ctx, leaderCh); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func (c *levelCluster) stop(n *levelNode) {
	if err := n.raft.Shutdown().Error(); err != nil {
		c.t.Fatalf("shutdown %s: %v", n.id, err)
	}
	n.raft = nil
}

// connect gives n a fresh transport at its address, wired to every peer.
func (c *levelCluster) connect(n *levelNode) {
	_, n.transport = hraft.NewInmemTransport(n.addr)
	for _, p := range c.nodes {
		if p == n || p.transport == nil {
			continue
		}
		n.transport.Connect(p.addr, p.transport)
		p.transport.Connect(n.addr, n.transport)
	}
}

func newLevelCluster(t *testing.T, levels ...uint32) *levelCluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := &levelCluster{
		t: t, table: rotationPolicyAtLevel2, ctx: ctx,
		raised:  make(chan uint32, 8),
		elected: make(chan hraft.Observation, 64),
	}
	for range levels {
		n := &levelNode{
			id:      glid.New().String(),
			logs:    hraft.NewInmemStore(),
			stable:  hraft.NewInmemStore(),
			snaps:   hraft.NewInmemSnapshotStore(),
			stopped: make(chan error, 16),
		}
		n.addr = hraft.ServerAddress(n.id)
		c.nodes = append(c.nodes, n)
	}
	for _, n := range c.nodes {
		c.connect(n)
	}
	var servers []hraft.Server
	for _, n := range c.nodes {
		servers = append(servers, hraft.Server{ID: hraft.ServerID(n.id), Address: n.addr})
	}
	// Every node runs the binary that knows the level-2 command type; what
	// each one reports is what the test moves.
	for _, n := range c.nodes {
		c.start(n, 2)
	}
	t.Cleanup(func() {
		cancel()
		for _, n := range c.nodes {
			if n.raft != nil {
				_ = n.raft.Shutdown().Error()
			}
		}
	})
	if err := c.nodes[0].raft.BootstrapCluster(hraft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	c.waitForLeader()
	for i, n := range c.nodes {
		c.report(n, levels[i])
	}
	return c
}

func (c *levelCluster) waitForLeader() {
	c.t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		if _, err := c.leader(); err == nil {
			return
		}
		select {
		case <-deadline:
			c.t.Fatal("no cluster-ctl leader elected")
		case <-c.elected:
		}
	}
}

// report writes the node's record with the protocol level its binary
// reports, as startup does.
func (c *levelCluster) report(n *levelNode, level uint32) {
	c.t.Helper()
	n.level.Store(level)
	id, err := glid.ParseAny(n.id)
	if err != nil {
		c.t.Fatal(err)
	}
	if err := n.store.PutNode(c.ctx, system.NodeConfig{ID: id, Name: n.id, ProtocolLevel: level}); err != nil {
		c.t.Fatalf("report level %d from %s: %v", level, n.id, err)
	}
}

func (c *levelCluster) awaitRaise(want uint32) {
	c.t.Helper()
	select {
	case got := <-c.raised:
		if got != want {
			c.t.Fatalf("raised to %d, want %d", got, want)
		}
	case <-time.After(30 * time.Second):
		c.t.Fatalf("active level never raised to %d", want)
	}
}

// propose writes a level-2 command from node n, after a barrier so n's view
// of the active level is current.
func (c *levelCluster) propose(n *levelNode) error {
	if err := n.store.Barrier(c.ctx); err != nil {
		c.t.Fatalf("barrier on %s: %v", n.id, err)
	}
	return n.store.PutRotationPolicy(c.ctx, newRotationPolicy())
}

func (c *levelCluster) requireNoStops() {
	c.t.Helper()
	for _, n := range c.nodes {
		select {
		case reason := <-n.stopped:
			c.t.Fatalf("node %s met an entry it cannot apply: %v", n.id, reason)
		default:
		}
	}
}

// TestMixedLevelClusterGatesNewCommandType runs a four-node cluster-ctl group
// through a rolling upgrade. One node reports protocol level 2 and knows a
// level-2 command type; proposals of that type are refused, at the proposer
// and at the leader, until every member reports level 2, and are accepted
// after. No node ever meets an entry it cannot apply.
func TestMixedLevelClusterGatesNewCommandType(t *testing.T) {
	if testing.Short() {
		t.Skip("four-node real-raft rolling upgrade")
	}
	c := newLevelCluster(t, 2, 1, 1, 1)
	c.awaitRaise(1)

	leader, err := c.leader()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range c.nodes {
		if err := c.propose(n); !errors.Is(err, raftlevel.ErrCommandAboveClusterLevel) {
			t.Fatalf("proposal from %s with members at level 1 = %v, want ErrCommandAboveClusterLevel", n.id, err)
		}
	}
	// The leader refuses it too, for a proposer that skipped its own gate.
	data := validRotationPolicy(t)
	before := leader.raft.LastIndex()
	if _, err := leader.store.ApplyRaw(data); !errors.Is(err, raftlevel.ErrCommandAboveClusterLevel) {
		t.Fatalf("leader ApplyRaw = %v, want ErrCommandAboveClusterLevel", err)
	}
	if got := leader.raft.LastIndex(); got != before {
		t.Fatalf("leader log grew from %d to %d", before, got)
	}

	// Two more members upgrade; one is still at level 1.
	c.report(c.nodes[1], 2)
	c.report(c.nodes[2], 2)
	if err := c.propose(c.nodes[1]); !errors.Is(err, raftlevel.ErrCommandAboveClusterLevel) {
		t.Fatalf("proposal with one member at level 1 = %v, want ErrCommandAboveClusterLevel", err)
	}

	c.report(c.nodes[3], 2)
	c.awaitRaise(2)
	for _, n := range c.nodes {
		if err := c.propose(n); err != nil {
			t.Fatalf("proposal from %s with every member at level 2: %v", n.id, err)
		}
	}
	c.requireNoStops()
	for _, n := range c.nodes {
		if err := n.store.Barrier(c.ctx); err != nil {
			t.Fatal(err)
		}
		policies, err := n.store.ListRotationPolicies(c.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(policies) != len(c.nodes) {
			t.Fatalf("node %s holds %d policies, want %d", n.id, len(policies), len(c.nodes))
		}
	}
}

// TestBinaryBelowActiveLevelRefusesToStart restarts a member on a binary
// below the cluster's committed active level. The node stops on the entry
// that raised the level, naming both levels, and does not apply it.
func TestBinaryBelowActiveLevelRefusesToStart(t *testing.T) {
	if testing.Short() {
		t.Skip("four-node real-raft cluster and a member restart")
	}
	c := newLevelCluster(t, 2, 2, 2, 2)
	c.awaitRaise(2)
	c.requireNoStops()

	var old *levelNode
	for _, n := range c.nodes {
		if n.raft.State() != hraft.Leader {
			old = n
			break
		}
	}
	c.stop(old)
	c.connect(old)
	c.start(old, 1)

	select {
	case reason := <-old.stopped:
		if !errors.Is(reason, raftlevel.ErrBinaryBelowActiveLevel) {
			t.Fatalf("restarted node stopped with %v, want ErrBinaryBelowActiveLevel", reason)
		}
		if msg := reason.Error(); !contains(msg, "level is 1") || !contains(msg, "raised to 2") {
			t.Fatalf("refusal does not name both levels: %s", msg)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a binary below the active level started without stopping")
	}
	if got := old.fsm.ActiveProtocolLevel(); got >= 2 {
		t.Fatalf("restarted node's active level = %d: the raise to 2 must not apply", got)
	}
}

func validRotationPolicy(t *testing.T) []byte {
	t.Helper()
	cfg := newRotationPolicy()
	data, err := proto.Marshal(&gastrologv1.SystemCommand{Command: &gastrologv1.SystemCommand_PutRotationPolicy{
		PutRotationPolicy: &gastrologv1.PutRotationPolicyCommand{Id: cfg.ID.ToProto(), Name: cfg.Name, MaxAge: cfg.MaxAge},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
