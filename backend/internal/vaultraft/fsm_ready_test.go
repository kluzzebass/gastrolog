package vaultraft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"

	hraft "github.com/hashicorp/raft"
)

// requireReady waits for the FSM's first applied entry.
func requireReady(t *testing.T, f *FSM, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waittest.Failsafe)
	defer cancel()
	if err := f.ApplyWait().Wait(ctx, 1); err != nil {
		t.Fatalf("%s: FSM never became ready: %v", what, err)
	}
}

// Each kind of first entry makes the FSM ready, and later entries keep it so.
func TestFSMReadyOnFirstEntry(t *testing.T) {
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
			if f.Ready() {
				t.Fatal("fresh FSM reports ready before any entry")
			}

			tc.first(t, f)
			if !f.Ready() {
				t.Fatal("FSM not ready after its first entry")
			}

			f.StoreConfiguration(20, hraft.Configuration{})
			f.Apply(&hraft.Log{Index: 21, Data: MarshalNoop()})
			if err := f.Restore(snapshotOf(t)); err != nil {
				t.Fatalf("second Restore: %v", err)
			}
			if !f.Ready() {
				t.Fatal("FSM not ready after later entries")
			}
		})
	}
}

type readyNode struct {
	id        string
	fsm       *FSM
	store     *hraft.InmemStore
	snaps     *hraft.InmemSnapshotStore
	transport *hraft.InmemTransport
	raft      *hraft.Raft
}

func (n *readyNode) start(t *testing.T) {
	t.Helper()
	n.fsm = NewFSM()
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
		requireReady(t, n.fsm, n.id)
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
	requireReady(t, restarted.fsm, restarted.id+" after restart")
}
