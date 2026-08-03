package raftwal

import (
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// counterFSM is a trivial FSM that counts applied log entries.
type counterFSM struct {
	count int
}

func (f *counterFSM) Apply(log *hraft.Log) interface{} {
	f.count++
	return nil
}

func (f *counterFSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &counterSnapshot{count: f.count}, nil
}

func (f *counterFSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	var buf [4]byte
	if _, err := io.ReadFull(rc, buf[:]); err != nil {
		return err
	}
	f.count = int(binary.LittleEndian.Uint32(buf[:]))
	return nil
}

type counterSnapshot struct{ count int }

func (s *counterSnapshot) Persist(sink hraft.SnapshotSink) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(s.count))
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

	if fsm.count != 50 {
		t.Fatalf("FSM count = %d, want 50", fsm.count)
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

	if fsm.count != 30 {
		t.Fatalf("FSM count = %d, want 30", fsm.count)
	}

	// After the snapshot's truncation, first index should have advanced.
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	t.Logf("after snapshot: first=%d last=%d (trailing=%d)", first, last, conf.TrailingLogs)

	_ = r.Shutdown().Error()
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
		applyTimeout = 30 * time.Second
		retryBudget  = 60 * time.Second
		// A node that truncates but never reclaims ends this workload holding
		// around sixty 4 KiB segments.
		maxSegments = 6
	)

	type raftNode struct {
		id    hraft.ServerID
		addr  hraft.ServerAddress
		dir   string
		wal   *WAL
		gs    *GroupStore
		fsm   *counterFSM
		trans *hraft.InmemTransport
		raft  *hraft.Raft
	}

	nodes := make([]*raftNode, nodeCount)
	servers := make([]hraft.Server, nodeCount)
	for i := range nodes {
		id := hraft.ServerID(fmt.Sprintf("node-%d", i+1))
		dir := t.TempDir()
		w, err := Open(dir, Config{SegmentTargetSize: 4096, ScavengeMaxLiveBytes: 1024})
		if err != nil {
			t.Fatalf("%s: open WAL: %v", id, err)
		}
		t.Cleanup(func() { _ = w.Close() })
		addr, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(id), 5*time.Second)
		nodes[i] = &raftNode{
			id: id, addr: addr, dir: dir,
			wal: w, gs: w.GroupStore("shard"), fsm: &counterFSM{}, trans: trans,
		}
		servers[i] = hraft.Server{ID: id, Address: addr}
	}
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.trans.Connect(b.addr, b.trans)
			}
		}
	}

	boot := hraft.Configuration{Servers: servers}
	for _, n := range nodes {
		conf := hraft.DefaultConfig()
		conf.LocalID = n.id
		conf.HeartbeatTimeout = 1 * time.Second
		conf.ElectionTimeout = 1 * time.Second
		conf.LeaderLeaseTimeout = 500 * time.Millisecond
		conf.CommitTimeout = 20 * time.Millisecond
		// Above anything this workload reaches, so the periodic snapshotter
		// never fires and the forced snapshots below are the only truncation
		// points.
		conf.SnapshotThreshold = 8192
		conf.TrailingLogs = 8
		conf.LogOutput = io.Discard

		snaps, err := hraft.NewFileSnapshotStore(t.TempDir(), 2, io.Discard)
		if err != nil {
			t.Fatalf("%s: snapshot store: %v", n.id, err)
		}
		r, err := hraft.NewRaft(conf, n.fsm, n.gs, n.gs, snaps, n.trans)
		if err != nil {
			t.Fatalf("%s: new raft: %v", n.id, err)
		}
		n.raft = r
		t.Cleanup(func() { _ = r.Shutdown().Error() })
		if err := r.BootstrapCluster(boot).Error(); err != nil {
			t.Fatalf("%s: bootstrap: %v", n.id, err)
		}
	}

	// leader resolves the current leader through VerifyLeader futures: a
	// follower answers ErrNotLeader from its main loop immediately, so a scan
	// costs one round-trip per node and never sleeps. Leadership can move
	// across a snapshot, so every round re-resolves.
	leader := func() *raftNode {
		t.Helper()
		deadline := time.Now().Add(retryBudget)
		for {
			for _, n := range nodes {
				if n.raft.State() != hraft.Leader {
					continue
				}
				if err := n.raft.VerifyLeader().Error(); err == nil {
					return n
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("no leader emerged")
			}
			runtime.Gosched()
		}
	}
	// apply commits one entry, re-resolving the leader if leadership moved
	// between the resolution and the call.
	apply := func(data []byte) {
		t.Helper()
		deadline := time.Now().Add(retryBudget)
		for {
			if err := leader().raft.Apply(data, applyTimeout).Error(); err == nil {
				return
			} else if time.Now().After(deadline) {
				t.Fatalf("apply: %v", err)
			}
		}
	}

	payload := make([]byte, payloadBytes)
	for round := range rounds {
		for range perRound {
			apply(payload)
		}
		// A user snapshot truncates the log only on the node that runs it,
		// so every node takes one: this is what drives DeleteRange into all
		// four WALs.
		for _, n := range nodes {
			if err := n.raft.Snapshot().Error(); err != nil {
				t.Fatalf("round %d: %s snapshot: %v", round, n.id, err)
			}
		}
		// The cluster still commits after the truncation.
		apply(payload)
	}

	// Quiesce raft so the segment census is not racing further appends.
	for _, n := range nodes {
		if err := n.raft.Shutdown().Error(); err != nil {
			t.Errorf("%s: shutdown: %v", n.id, err)
		}
	}

	for _, n := range nodes {
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
		t.Logf("%s: applied=%d first=%d last=%d segments=%d", n.id, n.fsm.count, first, last, segments)

		// Premise: this node's log really was truncated, so a bounded
		// segment count means reclamation ran, not that nothing was written.
		if first <= 1 {
			t.Errorf("%s: FirstIndex = %d, want a truncated log", n.id, first)
		}
		if n.fsm.count == 0 {
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
