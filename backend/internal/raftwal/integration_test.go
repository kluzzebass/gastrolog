package raftwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// counterFSM is a trivial FSM that counts applied log entries. The counter is
// atomic because tests read it while raft applies on its own goroutine.
type counterFSM struct {
	count atomic.Uint64
}

func (f *counterFSM) Apply(log *hraft.Log) interface{} {
	f.count.Add(1)
	return nil
}

// applied reports how many entries this FSM has processed, counting the ones a
// restored snapshot stands in for.
func (f *counterFSM) applied() uint64 {
	return f.count.Load()
}

func (f *counterFSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &counterSnapshot{count: f.count.Load()}, nil
}

func (f *counterFSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	var buf [8]byte
	if _, err := io.ReadFull(rc, buf[:]); err != nil {
		return err
	}
	f.count.Store(binary.LittleEndian.Uint64(buf[:]))
	return nil
}

type counterSnapshot struct{ count uint64 }

func (s *counterSnapshot) Persist(sink hraft.SnapshotSink) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], s.count)
	if _, err := sink.Write(buf[:]); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *counterSnapshot) Release() {}

// TestWALBackedRaftElectionAndApply boots a single-node Raft using the WAL
// as both LogStore and StableStore, applies entries, and verifies the FSM
// processes them.
func TestWALBackedRaftElectionAndApply(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("test-group")
	fsm := &counterFSM{}

	conf := hraft.DefaultConfig()
	conf.LocalID = "node-1"
	conf.HeartbeatTimeout = 200 * time.Millisecond
	conf.ElectionTimeout = 200 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond
	conf.LogOutput = io.Discard

	snap := hraft.NewInmemSnapshotStore()
	_, trans := hraft.NewInmemTransportWithTimeout("node-1", 1*time.Second)

	r, err := hraft.NewRaft(conf, fsm, gs, gs, snap, trans)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Shutdown().Error() }()

	// Bootstrap single-node cluster.
	boot := hraft.Configuration{
		Servers: []hraft.Server{{
			ID:      "node-1",
			Address: trans.LocalAddr(),
		}},
	}
	if err := r.BootstrapCluster(boot).Error(); err != nil {
		t.Fatal(err)
	}

	// Wait for leader.
	deadline := time.Now().Add(5 * time.Second)
	for r.State() != hraft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Apply 50 log entries.
	for i := range 50 {
		f := r.Apply([]byte{byte(i)}, 2*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}

	if fsm.applied() != 50 {
		t.Fatalf("FSM count = %d, want 50", fsm.applied())
	}

	// Verify the log store has the entries.
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first == 0 || last == 0 {
		t.Fatalf("first=%d last=%d, expected non-zero", first, last)
	}

	// Verify stable store has term info.
	term, _ := gs.GetUint64([]byte("CurrentTerm"))
	if term == 0 {
		t.Fatal("CurrentTerm should be non-zero after election")
	}
}

// TestWALBackedRaftSnapshotAndRestore verifies that snapshot + restore works
// with the WAL backend. After a snapshot, old log entries are deleted via
// DeleteRange and the FSM state is restored on "restart" (new Raft instance).
func TestWALBackedRaftSnapshotAndRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("spins up a real raft instance and waits out election + snapshot timing; -short skips")
	}
	t.Parallel()
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("snap-test")
	fsm := &counterFSM{}

	conf := hraft.DefaultConfig()
	conf.LocalID = "node-1"
	conf.HeartbeatTimeout = 200 * time.Millisecond
	conf.ElectionTimeout = 200 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond
	conf.SnapshotThreshold = 10
	conf.TrailingLogs = 5
	conf.LogOutput = io.Discard

	snapStore, err := hraft.NewFileSnapshotStore(dir, 2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, trans := hraft.NewInmemTransportWithTimeout("node-1", 1*time.Second)

	r, err := hraft.NewRaft(conf, fsm, gs, gs, snapStore, trans)
	if err != nil {
		t.Fatal(err)
	}

	boot := hraft.Configuration{
		Servers: []hraft.Server{{ID: "node-1", Address: trans.LocalAddr()}},
	}
	_ = r.BootstrapCluster(boot).Error()

	deadline := time.Now().Add(5 * time.Second)
	for r.State() != hraft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Apply enough entries to trigger a snapshot.
	for i := range 30 {
		f := r.Apply([]byte{byte(i)}, 2*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}

	// Force a snapshot.
	if err := r.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if fsm.applied() != 30 {
		t.Fatalf("FSM count = %d, want 30", fsm.applied())
	}

	// After the snapshot's truncation, first index should have advanced.
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	t.Logf("after snapshot: first=%d last=%d (trailing=%d)", first, last, conf.TrailingLogs)

	_ = r.Shutdown().Error()
}

// --- Multi-node fixture ---
//
// The cluster tests below share one shape: N hraft nodes, each with its own
// shared WAL serving as LogStore and StableStore, wired over InmemTransport.

const (
	// raftTestGroup is the one Raft group these clusters run.
	raftTestGroup = "shard"
	// raftTestTrailingLogs is how much log a node keeps after a snapshot. It
	// is small so a lagging follower falls out of the retained window fast.
	raftTestTrailingLogs = 8
	raftApplyTimeout     = 30 * time.Second
	raftRetryBudget      = 60 * time.Second
)

// raftNode is one cluster member: a raft instance over its own WAL.
type raftNode struct {
	id      hraft.ServerID
	addr    hraft.ServerAddress
	dir     string
	snapDir string
	wal     *WAL
	gs      *GroupStore
	fsm     *counterFSM
	snaps   *hraft.FileSnapshotStore
	trans   *hraft.InmemTransport
	raft    *hraft.Raft
}

type raftCluster struct {
	t     *testing.T
	nodes []*raftNode
	boot  hraft.Configuration
}

// raftTestWALConfig sizes segments and the scavenge budget so rotation,
// truncation and reclamation all run within a few hundred entries.
func raftTestWALConfig() Config {
	return Config{SegmentTargetSize: 4096, ScavengeMaxLiveBytes: 1024}
}

func raftTestConfig(id hraft.ServerID) *hraft.Config {
	conf := hraft.DefaultConfig()
	conf.LocalID = id
	conf.HeartbeatTimeout = 1 * time.Second
	conf.ElectionTimeout = 1 * time.Second
	conf.LeaderLeaseTimeout = 500 * time.Millisecond
	conf.CommitTimeout = 20 * time.Millisecond
	// Above anything these workloads reach, so the periodic snapshotter never
	// fires and the forced snapshots are the only truncation points.
	conf.SnapshotThreshold = 8192
	conf.TrailingLogs = raftTestTrailingLogs
	conf.LogOutput = io.Discard
	return conf
}

// newRaftCluster boots nodeCount fully connected nodes and bootstraps them
// into one voting configuration.
func newRaftCluster(t *testing.T, nodeCount int) *raftCluster {
	t.Helper()
	c := &raftCluster{t: t, nodes: make([]*raftNode, nodeCount)}
	servers := make([]hraft.Server, nodeCount)
	for i := range c.nodes {
		id := hraft.ServerID(fmt.Sprintf("node-%d", i+1))
		dir := t.TempDir()
		w, err := Open(dir, raftTestWALConfig())
		if err != nil {
			t.Fatalf("%s: open WAL: %v", id, err)
		}
		addr, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(id), 5*time.Second)
		n := &raftNode{
			id: id, addr: addr, dir: dir, snapDir: t.TempDir(),
			wal: w, gs: w.GroupStore(raftTestGroup), fsm: &counterFSM{}, trans: trans,
		}
		c.nodes[i] = n
		// Both cleanups read the node's current stack, which a restart
		// replaces; raft is registered second so it shuts down first.
		t.Cleanup(func() { _ = n.wal.Close() })
		t.Cleanup(func() {
			if n.raft != nil {
				_ = n.raft.Shutdown().Error()
			}
		})
		servers[i] = hraft.Server{ID: id, Address: addr}
	}
	for _, a := range c.nodes {
		for _, b := range c.nodes {
			if a != b {
				a.trans.Connect(b.addr, b.trans)
			}
		}
	}
	c.boot = hraft.Configuration{Servers: servers}
	for _, n := range c.nodes {
		c.startRaft(n, true)
	}
	return c
}

// startRaft builds a raft instance on n's current WAL-backed store and FSM.
// bootstrap is for a cluster with no persisted state; a restarted node reads
// its configuration back from its own log and snapshots.
func (c *raftCluster) startRaft(n *raftNode, bootstrap bool) {
	c.t.Helper()
	snaps, err := hraft.NewFileSnapshotStore(n.snapDir, 2, io.Discard)
	if err != nil {
		c.t.Fatalf("%s: snapshot store: %v", n.id, err)
	}
	n.snaps = snaps
	r, err := hraft.NewRaft(raftTestConfig(n.id), n.fsm, n.gs, n.gs, snaps, n.trans)
	if err != nil {
		c.t.Fatalf("%s: new raft: %v", n.id, err)
	}
	n.raft = r
	if bootstrap {
		if err := r.BootstrapCluster(c.boot).Error(); err != nil {
			c.t.Fatalf("%s: bootstrap: %v", n.id, err)
		}
	}
}

// leader resolves the current leader through VerifyLeader futures: a follower
// answers ErrNotLeader from its main loop immediately, so a scan costs one
// round-trip per node and never sleeps. Leadership can move across a
// snapshot, so callers re-resolve every time.
func (c *raftCluster) leader() *raftNode {
	c.t.Helper()
	deadline := time.Now().Add(raftRetryBudget)
	for {
		for _, n := range c.nodes {
			if n.raft.State() != hraft.Leader {
				continue
			}
			if err := n.raft.VerifyLeader().Error(); err == nil {
				return n
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatal("no leader emerged")
		}
		runtime.Gosched()
	}
}

// follower returns some node that is not the leader.
func (c *raftCluster) follower() *raftNode {
	c.t.Helper()
	ld := c.leader()
	for _, n := range c.nodes {
		if n != ld {
			return n
		}
	}
	c.t.Fatal("no follower in cluster")
	return nil
}

// apply commits one entry and returns its log index, re-resolving the leader
// if leadership moved between the resolution and the call.
func (c *raftCluster) apply(data []byte) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(raftRetryBudget)
	for {
		f := c.leader().raft.Apply(data, raftApplyTimeout)
		if err := f.Error(); err == nil {
			return f.Index()
		} else if time.Now().After(deadline) {
			c.t.Fatalf("apply: %v", err)
		}
	}
}

// snapshot forces a user snapshot, which truncates the log on that node only.
func (c *raftCluster) snapshot(n *raftNode) {
	c.t.Helper()
	if err := n.raft.Snapshot().Error(); err != nil {
		c.t.Fatalf("%s: snapshot: %v", n.id, err)
	}
}

func (c *raftCluster) shutdownAll() {
	c.t.Helper()
	for _, n := range c.nodes {
		if err := n.raft.Shutdown().Error(); err != nil {
			c.t.Errorf("%s: shutdown: %v", n.id, err)
		}
	}
}

// isolate cuts n off in both directions: InmemTransport.Disconnect drops only
// the caller's route, so each peer has to drop its own route to n as well.
func (c *raftCluster) isolate(n *raftNode) {
	n.trans.DisconnectAll()
	for _, o := range c.nodes {
		if o != n {
			o.trans.Disconnect(n.addr)
		}
	}
}

// rejoin restores the routes to and from n.
func (c *raftCluster) rejoin(n *raftNode) {
	for _, o := range c.nodes {
		if o != n {
			n.trans.Connect(o.addr, o.trans)
			o.trans.Connect(n.addr, n.trans)
		}
	}
}

// waitApplied spins until n's raft reports index as applied. The deadline is a
// safety net, not a decision gate: progress is read from state, and the loop
// yields instead of sleeping.
func (c *raftCluster) waitApplied(n *raftNode, index uint64) {
	c.t.Helper()
	deadline := time.Now().Add(raftRetryBudget)
	for n.raft.AppliedIndex() < index {
		if time.Now().After(deadline) {
			c.t.Fatalf("%s: applied index %d never reached %d", n.id, n.raft.AppliedIndex(), index)
		}
		runtime.Gosched()
	}
}

// waitFSM spins until n's FSM has accounted for count entries, whether by
// applying them or by restoring a snapshot that stands in for them.
func (c *raftCluster) waitFSM(n *raftNode, count uint64) {
	c.t.Helper()
	deadline := time.Now().Add(raftRetryBudget)
	for n.fsm.applied() < count {
		if time.Now().After(deadline) {
			c.t.Fatalf("%s: FSM applied %d, never reached %d", n.id, n.fsm.applied(), count)
		}
		runtime.Gosched()
	}
}

// segmentsRotated reports how many segments the WAL has opened, so a bounded
// segment census can be read as reclamation having unlinked the rest rather
// than as a WAL that never grew. Sequences are dense from one and every
// segment the writer opens is registered in the live-bytes map, so the highest
// key counts them.
func segmentsRotated(t *testing.T, w *WAL) int {
	t.Helper()
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()
	highest := 0
	for seq := range w.segLive {
		if seq > highest {
			highest = seq
		}
	}
	return highest
}

// newestSnapshotIndex reports the log index of n's most recent snapshot, or 0
// when it holds none.
func newestSnapshotIndex(t *testing.T, n *raftNode) uint64 {
	t.Helper()
	list, err := n.snaps.List()
	if err != nil {
		t.Fatalf("%s: list snapshots: %v", n.id, err)
	}
	if len(list) == 0 {
		return 0
	}
	return list[0].Index
}

// Four raft nodes, each on its own shared WAL, under sustained apply +
// forced-snapshot cycles: post-snapshot truncation drains old segments and
// reclamation keeps every node's WAL directory bounded while the cluster
// keeps electing and applying.
func TestFourNodeRaftSustainedTruncationReclaims(t *testing.T) {
	if testing.Short() {
		t.Skip("drives a four-node raft cluster through 20 apply + snapshot rounds")
	}
	t.Parallel()

	const (
		nodeCount    = 4
		rounds       = 20
		perRound     = 50
		payloadBytes = 200
		// A node that truncates but never reclaims ends this workload holding
		// around sixty 4 KiB segments.
		maxSegments = 6
	)

	c := newRaftCluster(t, nodeCount)

	payload := make([]byte, payloadBytes)
	for range rounds {
		for range perRound {
			c.apply(payload)
		}
		// A user snapshot truncates the log only on the node that runs it,
		// so every node takes one: this is what drives DeleteRange into all
		// four WALs.
		for _, n := range c.nodes {
			c.snapshot(n)
		}
		// The cluster still commits after the truncation.
		c.apply(payload)
	}

	// Quiesce raft so the segment census is not racing further appends.
	c.shutdownAll()

	for _, n := range c.nodes {
		// Order the census after the reclamation pass the last truncation
		// kicked off: passes run on the writer after batch waiters return.
		syncBarrier(t, n.wal)

		first, err := n.gs.FirstIndex()
		if err != nil {
			t.Fatalf("%s: FirstIndex: %v", n.id, err)
		}
		last, err := n.gs.LastIndex()
		if err != nil {
			t.Fatalf("%s: LastIndex: %v", n.id, err)
		}
		segments := segmentFileCount(t, n.dir)
		t.Logf("%s: applied=%d first=%d last=%d segments=%d", n.id, n.fsm.applied(), first, last, segments)

		// Premise: this node's log really was truncated, so a bounded
		// segment count means reclamation ran, not that nothing was written.
		if first <= 1 {
			t.Errorf("%s: FirstIndex = %d, want a truncated log", n.id, first)
		}
		if n.fsm.applied() == 0 {
			t.Errorf("%s: FSM applied nothing", n.id)
		}
		if segments > maxSegments {
			t.Errorf("%s: %d data-bearing segments, want <= %d", n.id, segments, maxSegments)
		}
		assertLiveBytesInvariant(t, n.wal, "final")

		var lg hraft.Log
		if err := n.gs.GetLog(last, &lg); err != nil {
			t.Errorf("%s: GetLog(%d): %v", n.id, last, err)
		}
	}
}

// A follower that falls behind the leader's retained log window catches up by
// InstallSnapshot, and hashicorp/raft compacts the whole log it had in one
// DeleteRange — a different truncation shape from the incremental one a local
// snapshot produces. The WAL must still reclaim behind it, keep its live-bytes
// accounting straight, serve reads, and replay to the same bounds on restart.
func TestFourNodeRaftInstallSnapshotFollowerReclaims(t *testing.T) {
	if testing.Short() {
		t.Skip("isolates a follower from a four-node raft cluster and waits out InstallSnapshot catch-up")
	}
	t.Parallel()

	const (
		nodeCount    = 4
		warmupRounds = 3
		perRound     = 50
		lagApplies   = 60
		payloadBytes = 200
		// The follower rotates through roughly ten 4 KiB segments here. What
		// survives the catch-up is the head segment plus the one or two the
		// entries hraft retains below the installed snapshot index pin.
		maxSegments = 6
	)

	c := newRaftCluster(t, nodeCount)
	payload := make([]byte, payloadBytes)

	// Warm up with local truncation on every node, so the follower enters the
	// isolation phase on a WAL that has already truncated and reclaimed.
	for range warmupRounds {
		for range perRound {
			c.apply(payload)
		}
		for _, n := range c.nodes {
			c.snapshot(n)
		}
		c.apply(payload)
	}

	lagger := c.follower()
	ld := c.leader()
	c.isolate(lagger)
	// The leader's log at isolation bounds what the lagger can hold: an append
	// still in flight when the routes dropped carries nothing past this index,
	// and an isolated node grows no log of its own (pre-vote keeps it from
	// winning an election, so it never appends).
	frozen, err := ld.gs.LastIndex()
	if err != nil {
		t.Fatalf("%s: LastIndex: %v", ld.id, err)
	}
	laggerSnapBefore := newestSnapshotIndex(t, lagger)

	// Drive the connected majority past the lagger and truncate it away.
	for range lagApplies {
		c.apply(payload)
	}
	for _, n := range c.nodes {
		if n != lagger {
			c.snapshot(n)
		}
	}

	// Premise: no connected node can still serve AppendEntries from the
	// lagger's log tail, so catch-up has to go through InstallSnapshot.
	// Without this the test degrades into ordinary log replication.
	laggerLast, err := lagger.gs.LastIndex()
	if err != nil {
		t.Fatalf("%s: LastIndex: %v", lagger.id, err)
	}
	t.Logf("%s isolated at last=%d (bound %d), snapshot=%d",
		lagger.id, laggerLast, frozen, laggerSnapBefore)
	for _, n := range c.nodes {
		if n == lagger {
			continue
		}
		first, err := n.gs.FirstIndex()
		if err != nil {
			t.Fatalf("%s: FirstIndex: %v", n.id, err)
		}
		t.Logf("%s: FirstIndex=%d", n.id, first)
		if first <= frozen {
			t.Fatalf("%s: FirstIndex = %d, want past the lagger's reachable tail %d", n.id, first, frozen)
		}
	}

	// Reconnect and push one more entry through, then wait on the lagger's own
	// state until it has caught up.
	c.rejoin(lagger)
	marker := c.apply(payload)
	c.waitApplied(lagger, marker)
	c.waitFSM(lagger, c.leader().fsm.applied())

	// The lagger never ran a snapshot while isolated and its log stopped at
	// the bound above, so a snapshot indexed past it can only have arrived
	// over the wire.
	shipped := newestSnapshotIndex(t, lagger)
	if shipped <= frozen {
		t.Fatalf("%s: newest snapshot index = %d, want one installed by the leader (> %d)",
			lagger.id, shipped, frozen)
	}

	// Quiesce the lagger for the census: reclamation runs on the WAL writer,
	// and a segment count taken against live appends means nothing.
	c.isolate(lagger)
	if err := lagger.raft.Shutdown().Error(); err != nil {
		t.Fatalf("%s: shutdown: %v", lagger.id, err)
	}
	syncBarrier(t, lagger.wal)

	first, err := lagger.gs.FirstIndex()
	if err != nil {
		t.Fatalf("%s: FirstIndex: %v", lagger.id, err)
	}
	last, err := lagger.gs.LastIndex()
	if err != nil {
		t.Fatalf("%s: LastIndex: %v", lagger.id, err)
	}
	segments := segmentFileCount(t, lagger.dir)
	rotated := segmentsRotated(t, lagger.wal)
	t.Logf("%s after catch-up: applied=%d first=%d last=%d installed=%d segments=%d rotated=%d",
		lagger.id, lagger.fsm.applied(), first, last, shipped, segments, rotated)

	if last <= shipped {
		t.Errorf("%s: LastIndex = %d, want appends past the installed snapshot %d", lagger.id, last, shipped)
	}
	// Premise for the census below: the WAL rotated through more segments than
	// it now holds, so a small count means they were unlinked.
	if rotated <= maxSegments {
		t.Errorf("%s: only %d segments ever opened, so a census of %d proves nothing about reclamation",
			lagger.id, rotated, segments)
	}
	if segments > maxSegments {
		t.Errorf("%s: %d data-bearing segments, want <= %d", lagger.id, segments, maxSegments)
	}
	assertLiveBytesInvariant(t, lagger.wal, "after install-snapshot catch-up")

	// The compacted log is gapped: hashicorp/raft compacts up to
	// TrailingLogs back from the log head it had, which is below the installed
	// snapshot index, and the leader resumes appending above that index. Both
	// ends stay readable and the hole reads as missing.
	var lg hraft.Log
	if err := lagger.gs.GetLog(last, &lg); err != nil {
		t.Errorf("%s: GetLog(%d): %v", lagger.id, last, err)
	}
	if err := lagger.gs.GetLog(first, &lg); err != nil {
		t.Errorf("%s: GetLog(%d): %v", lagger.id, first, err)
	}
	if first > frozen {
		t.Errorf("%s: FirstIndex = %d, want the retained tail of the pre-catch-up log (<= %d)",
			lagger.id, first, frozen)
	}
	if err := lagger.gs.GetLog(frozen+1, &lg); !errors.Is(err, hraft.ErrLogNotFound) {
		t.Errorf("%s: GetLog(%d) in the compacted hole = %v, want ErrLogNotFound", lagger.id, frozen+1, err)
	}

	// Restart the lagger's WAL stack. Replay sees the surviving DeleteRange
	// masks against segments reclamation has already unlinked, so the rebuilt
	// index is the only authority for the group's bounds.
	if err := lagger.wal.Close(); err != nil {
		t.Fatalf("%s: close WAL: %v", lagger.id, err)
	}
	w2, err := Open(lagger.dir, raftTestWALConfig())
	if err != nil {
		t.Fatalf("%s: reopen WAL: %v", lagger.id, err)
	}
	lagger.wal = w2
	lagger.gs = w2.GroupStore(raftTestGroup)

	replayFirst, err := lagger.gs.FirstIndex()
	if err != nil {
		t.Fatalf("%s: FirstIndex after replay: %v", lagger.id, err)
	}
	replayLast, err := lagger.gs.LastIndex()
	if err != nil {
		t.Fatalf("%s: LastIndex after replay: %v", lagger.id, err)
	}
	if replayFirst != first || replayLast != last {
		t.Fatalf("%s: replayed bounds first=%d last=%d, want first=%d last=%d",
			lagger.id, replayFirst, replayLast, first, last)
	}
	assertLiveBytesInvariant(t, w2, "after replay")
	if err := lagger.gs.GetLog(replayLast, &lg); err != nil {
		t.Errorf("%s: GetLog(%d) after replay: %v", lagger.id, replayLast, err)
	}

	// The reopened store carries the node back into the cluster: raft restores
	// from its installed snapshot, replays the log tail into a fresh FSM, and
	// commits new entries.
	lagger.fsm = &counterFSM{}
	c.startRaft(lagger, false)
	c.rejoin(lagger)
	marker2 := c.apply(payload)
	c.waitApplied(lagger, marker2)
	c.waitFSM(lagger, c.leader().fsm.applied())

	c.shutdownAll()
	syncBarrier(t, lagger.wal)

	finalFirst, err := lagger.gs.FirstIndex()
	if err != nil {
		t.Fatalf("%s: FirstIndex after restart: %v", lagger.id, err)
	}
	finalLast, err := lagger.gs.LastIndex()
	if err != nil {
		t.Fatalf("%s: LastIndex after restart: %v", lagger.id, err)
	}
	segments = segmentFileCount(t, lagger.dir)
	t.Logf("%s after restart: applied=%d first=%d last=%d segments=%d",
		lagger.id, lagger.fsm.applied(), finalFirst, finalLast, segments)

	if finalLast < marker2 {
		t.Errorf("%s: LastIndex = %d, want the post-restart marker %d", lagger.id, finalLast, marker2)
	}
	if finalFirst == 0 || finalFirst > finalLast {
		t.Errorf("%s: bounds first=%d last=%d", lagger.id, finalFirst, finalLast)
	}
	if segments > maxSegments {
		t.Errorf("%s: %d data-bearing segments after restart, want <= %d", lagger.id, segments, maxSegments)
	}
	assertLiveBytesInvariant(t, lagger.wal, "after restart")
	if err := lagger.gs.GetLog(finalLast, &lg); err != nil {
		t.Errorf("%s: GetLog(%d) after restart: %v", lagger.id, finalLast, err)
	}
}
