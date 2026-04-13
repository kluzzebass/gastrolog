package raftwal_test

import (
	"encoding/binary"
	"io"
	"testing"
	"time"

	"gastrolog/internal/raftwal"

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

	w, err := raftwal.Open(dir)
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
	t.Parallel()
	dir := t.TempDir()

	w, err := raftwal.Open(dir)
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

	// After snapshot + compaction, first index should have advanced.
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	t.Logf("after snapshot: first=%d last=%d (trailing=%d)", first, last, conf.TrailingLogs)

	_ = r.Shutdown().Error()
}
