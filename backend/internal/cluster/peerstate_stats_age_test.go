package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// StatsReceivedAt is what the UI dates a broadcast value by. It has to agree
// with Get on the TTL: an age for stats that Get no longer serves would date
// a value the operator is not shown, and stats without an age would be shown
// as a live reading.
func TestPeerState_StatsReceivedAt_AgreesWithGet(t *testing.T) {
	ps := NewPeerState(testStatsTTL, 0)
	now := time.Now()

	fresh := now.Add(-testStatsTTL / 2)
	ps.Update("node-b", &gastrologv1.NodeStats{NodeName: "b"}, fresh)
	if ps.Get("node-b") == nil {
		t.Fatal("premise: Get must serve stats received within the TTL")
	}
	if got := ps.StatsReceivedAt("node-b"); !got.Equal(fresh) {
		t.Fatalf("StatsReceivedAt = %v, want the Update instant %v", got, fresh)
	}

	stale := now.Add(-testStatsTTL - time.Second)
	ps.Update("node-c", &gastrologv1.NodeStats{NodeName: "c"}, stale)
	if ps.Get("node-c") != nil {
		t.Fatal("premise: Get must not serve stats past the TTL")
	}
	if got := ps.StatsReceivedAt("node-c"); !got.IsZero() {
		t.Fatalf("StatsReceivedAt = %v for expired stats, want zero", got)
	}

	if got := ps.StatsReceivedAt("never-heard"); !got.IsZero() {
		t.Fatalf("StatsReceivedAt = %v for an unknown node, want zero", got)
	}
	if got := ps.StatsTTL(); got != testStatsTTL {
		t.Fatalf("StatsTTL = %v, want %v", got, testStatsTTL)
	}
}
