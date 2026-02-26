package raftstore

import (
	"io"
	"testing"
	"time"

	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/config/storetest"

	hraft "github.com/hashicorp/raft"
)

// newTestRaft creates a single-node in-memory raft instance that becomes
// leader immediately. No cluster, no network — just raft's log + FSM
// machinery for persistence testing.
func newTestRaft(t *testing.T) (*hraft.Raft, *raftfsm.FSM) {
	t.Helper()

	fsm := raftfsm.New()

	conf := hraft.DefaultConfig()
	conf.LocalID = "test-node"
	conf.LogOutput = io.Discard
	// Tight timeouts so single-node election is near-instant.
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapStore := hraft.NewInmemSnapshotStore()
	_, transport := hraft.NewInmemTransport("test-node")

	r, err := hraft.NewRaft(conf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	t.Cleanup(func() {
		future := r.Shutdown()
		if err := future.Error(); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	// Bootstrap as single voter so this node becomes leader.
	boot := hraft.Configuration{
		Servers: []hraft.Server{
			{ID: "test-node", Address: transport.LocalAddr()},
		},
	}
	if err := r.BootstrapCluster(boot).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}

	// Wait for leadership.
	select {
	case <-r.LeaderCh():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leadership")
	}

	return r, fsm
}

func TestConformance(t *testing.T) {
	storetest.TestStore(t, func(t *testing.T) config.Store {
		r, fsm := newTestRaft(t)
		return New(r, fsm, 5*time.Second)
	})
}

func TestApplyBadData(t *testing.T) {
	r, fsm := newTestRaft(t)
	s := New(r, fsm, 5*time.Second)

	// Apply garbage through raft — FSM returns an unmarshal error
	// which surfaces via future.Response().
	future := s.raft.Apply([]byte("not a valid protobuf"), s.applyTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("unexpected raft-level error: %v", err)
	}
	resp := future.Response()
	if resp == nil {
		t.Fatal("expected error response from FSM, got nil")
	}
	if _, ok := resp.(error); !ok {
		t.Fatalf("expected error, got %T: %v", resp, resp)
	}
}
