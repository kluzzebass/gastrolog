package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// TestPeerState_ReceivedAt returns a timestamp per known peer, including
// expired ones — staleness is the caller's concern (Prometheus computes
// the age gauge at scrape time).
func TestPeerState_ReceivedAt(t *testing.T) {
	ps := NewPeerState(time.Nanosecond) // entries expire on read, but ReceivedAt is independent
	t0 := time.Now().Add(-time.Hour)
	t1 := time.Now()
	ps.Update("stale", &gastrologv1.NodeStats{}, t0)
	ps.Update("fresh", &gastrologv1.NodeStats{}, t1)

	got := ps.ReceivedAt()
	if !got["stale"].Equal(t0) {
		t.Errorf("stale timestamp: got %v want %v", got["stale"], t0)
	}
	if !got["fresh"].Equal(t1) {
		t.Errorf("fresh timestamp: got %v want %v", got["fresh"], t1)
	}
	if _, ok := got["never"]; ok {
		t.Errorf("ReceivedAt included an unknown peer")
	}
}

// TestPeerJobState_ReceivedAt mirrors the PeerState test for the jobs cache.
func TestPeerJobState_ReceivedAt(t *testing.T) {
	pjs := NewPeerJobState(time.Nanosecond)
	t0 := time.Now().Add(-time.Minute)
	pjs.Update("n", []*gastrologv1.Job{{Id: []byte("j1")}}, t0)

	got := pjs.ReceivedAt()
	if !got["n"].Equal(t0) {
		t.Errorf("timestamp: got %v want %v", got["n"], t0)
	}
}

// TestPeerState_ReceivedAt_Empty verifies a fresh holder reports no peers.
func TestPeerState_ReceivedAt_Empty(t *testing.T) {
	ps := NewPeerState(time.Second)
	if got := ps.ReceivedAt(); len(got) != 0 {
		t.Errorf("fresh PeerState.ReceivedAt: got %v, want empty map", got)
	}
	pjs := NewPeerJobState(time.Second)
	if got := pjs.ReceivedAt(); len(got) != 0 {
		t.Errorf("fresh PeerJobState.ReceivedAt: got %v, want empty map", got)
	}
}
