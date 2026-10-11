package vaultraft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"

	hraft "github.com/hashicorp/raft"
)

// readyCounter counts ready callbacks and closes fired on the first.
type readyCounter struct {
	n     atomic.Int64
	once  sync.Once
	fired chan struct{}
}

func newReadyCounter() *readyCounter { return &readyCounter{fired: make(chan struct{})} }

func (c *readyCounter) callback() {
	c.n.Add(1)
	c.once.Do(func() { close(c.fired) })
}

func requireFired(t *testing.T, c *readyCounter, what string) {
	t.Helper()
	select {
	case <-c.fired:
	case <-time.After(waittest.Failsafe):
		t.Fatalf("%s: ready callback never fired", what)
	}
}

// Each kind of first entry makes the FSM ready and fires its ready callback
// exactly once; later entries never fire it again.
func TestFSMReadyFiresOnceOnFirstEntry(t *testing.T) {
	snapshotOf := func(t *testing.T) io.ReadCloser {
		src := NewFSM()
		vaultID := glid.New()
		now := time.Now()
		if got := src.Apply(&hraft.Log{Index: 7, Data: MarshalVaultChunkCommand(vaultID,
			vaultctlfsm.MarshalCreateChunk(testChunkID(1), now, now, now))}); got != nil {
			t.Fatalf("apply to snapshot source: %v", got)
		}
		snap, err := src.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
			t.Fatal(err)
		}
		return io.NopCloser(&buf)
	}
	for _, tc := range []struct {
		name  string
		first func(t *testing.T, f *FSM)
	}{
		{"configuration entry", func(_ *testing.T, f *FSM) { f.StoreConfiguration(1, hraft.Configuration{}) }},
		{"command", func(_ *testing.T, f *FSM) { f.Apply(&hraft.Log{Index: 1, Data: MarshalNoop()}) }},
		{"snapshot restore", func(t *testing.T, f *FSM) {
			if err := f.Restore(snapshotOf(t)); err != nil {
				t.Fatalf("Restore: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFSM()
			c := newReadyCounter()
			f.SetOnReady(c.callback)
			if f.Ready() || c.n.Load() != 0 {
				t.Fatalf("fresh FSM: ready=%v callbacks=%d, want not ready and none", f.Ready(), c.n.Load())
			}

			tc.first(t, f)
			if !f.Ready() {
				t.Fatal("FSM not ready after its first entry")
			}
			if got := c.n.Load(); got != 1 {
				t.Fatalf("ready callbacks after the first entry = %d, want 1", got)
			}

			f.StoreConfiguration(20, hraft.Configuration{})
			f.Apply(&hraft.Log{Index: 21, Data: MarshalNoop()})
			if err := f.Restore(snapshotOf(t)); err != nil {
				t.Fatalf("second Restore: %v", err)
			}
			if got := c.n.Load(); got != 1 {
				t.Fatalf("ready callbacks after later entries = %d, want 1", got)
			}
		})
	}
}

// A callback registered after the FSM became ready runs at registration; once
// the one-shot has fired, a replacement callback never runs.
func TestFSMReadyCallbackRegisteredLate(t *testing.T) {
	f := NewFSM()
	f.StoreConfiguration(1, hraft.Configuration{})

	late := newReadyCounter()
	f.SetOnReady(late.callback)
	if got := late.n.Load(); got != 1 {
		t.Fatalf("callback registered on a ready FSM ran %d times, want 1", got)
	}

	replacement := newReadyCounter()
	f.SetOnReady(replacement.callback)
	f.Apply(&hraft.Log{Index: 2, Data: MarshalNoop()})
	if got := replacement.n.Load(); got != 0 {
		t.Fatalf("replacement callback ran %d times after the one-shot fired, want 0", got)
	}
}

// Registration racing the first entry fires the callback exactly once,
// whichever lands first.
func TestFSMReadyRegistrationRacingFirstEntry(t *testing.T) {
	for i := range 500 {
		f := NewFSM()
		c := newReadyCounter()
		var wg sync.WaitGroup
		wg.Go(func() { f.SetOnReady(c.callback) })
		wg.Go(func() { f.StoreConfiguration(1, hraft.Configuration{}) })
		wg.Go(func() { f.Apply(&hraft.Log{Index: 2, Data: MarshalNoop()}) })
		wg.Wait()
		if got := c.n.Load(); got != 1 {
			t.Fatalf("iteration %d: ready callbacks = %d, want exactly 1", i, got)
		}
	}
}

type readyNode struct {
	id        string
	fsm       *FSM
	ready     *readyCounter
	store     *hraft.InmemStore
	snaps     *hraft.InmemSnapshotStore
	transport *hraft.InmemTransport
	raft      *hraft.Raft
}

func (n *readyNode) start(t *testing.T) {
	t.Helper()
	n.fsm = NewFSM()
	n.ready = newReadyCounter()
	n.fsm.SetOnReady(n.ready.callback)
	_, n.transport = hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(n.id), 500*time.Millisecond)
	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(n.id)
	cfg.LogOutput = io.Discard
	cfg.HeartbeatTimeout = harnessHeartbeatTimeout
	cfg.ElectionTimeout = harnessElectionTimeout
	cfg.LeaderLeaseTimeout = harnessLeaseTimeout
	cfg.CommitTimeout = 20 * time.Millisecond
	r, err := hraft.NewRaft(cfg, n.fsm, n.store, n.store, n.snaps, n.transport)
	if err != nil {
		t.Fatalf("%s: NewRaft: %v", n.id, err)
	}
	n.raft = r
}

func connectAll(nodes []*readyNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.transport.Connect(b.transport.LocalAddr(), b.transport)
			}
		}
	}
}

// A fresh four-node vault-ctl group, seeded symmetrically and given no
// command, becomes ready on every node: the bootstrap configuration is the
// first entry each FSM applies. A node restarted over its log with a fresh
// FSM becomes ready again on replay.
func TestFSMReadyOnFreshGroupWithoutCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("real four-node raft election")
	}
	nodes := make([]*readyNode, 4)
	servers := make([]hraft.Server, len(nodes))
	for i := range nodes {
		id := fmt.Sprintf("node-%d", i+1)
		nodes[i] = &readyNode{id: id, store: hraft.NewInmemStore(), snaps: hraft.NewInmemSnapshotStore()}
		servers[i] = hraft.Server{ID: hraft.ServerID(id), Address: hraft.ServerAddress(id)}
	}
	for _, n := range nodes {
		n.start(t)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown().Error()
		}
	})
	connectAll(nodes)
	for _, n := range nodes {
		if err := n.raft.BootstrapCluster(hraft.Configuration{Servers: servers}).Error(); err != nil {
			t.Fatalf("%s: bootstrap: %v", n.id, err)
		}
	}

	for _, n := range nodes {
		requireFired(t, n.ready, n.id)
		if !n.fsm.Ready() {
			t.Errorf("%s: ready callback fired but Ready() is false", n.id)
		}
		if got := len(n.fsm.Vaults()); got != 0 {
			t.Errorf("%s: %d vault sub-FSMs, want none: no command was applied", n.id, got)
		}
	}

	restarted := nodes[3]
	if err := restarted.raft.Shutdown().Error(); err != nil {
		t.Fatalf("shutdown %s: %v", restarted.id, err)
	}
	restarted.start(t)
	connectAll(nodes)
	requireFired(t, restarted.ready, restarted.id+" after restart")

	for _, n := range nodes {
		if got := n.ready.n.Load(); got != 1 {
			t.Errorf("%s: ready callbacks = %d, want 1", n.id, got)
		}
	}
}
