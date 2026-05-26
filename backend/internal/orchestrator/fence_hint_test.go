package orchestrator

import (
	"testing"
	"time"
)

func TestFenceHintArbitratorAcceptsMonotonic(t *testing.T) {
	t.Parallel()
	a := newFenceHintArbitrator()
	now := time.Unix(0, 1).UTC()

	if !a.Ingest(FenceHint{NodeID: "a", H: 10, ObservedAt: now}) {
		t.Fatal("first hint rejected")
	}
	if !a.Ingest(FenceHint{NodeID: "a", H: 20, ObservedAt: now.Add(time.Second)}) {
		t.Fatal("monotonic hint rejected")
	}
	if got := a.EffectiveH(); got != 20 {
		t.Fatalf("EffectiveH = %d, want 20", got)
	}
}

func TestFenceHintArbitratorRejectsRegressivePerNode(t *testing.T) {
	t.Parallel()
	a := newFenceHintArbitrator()
	now := time.Now().UTC()
	a.Ingest(FenceHint{NodeID: "a", H: 50, ObservedAt: now})
	if a.Ingest(FenceHint{NodeID: "a", H: 40, ObservedAt: now.Add(time.Second)}) {
		t.Fatal("regressive per-node hint accepted")
	}
}

func TestFenceHintArbitratorRejectsStaleOverlap(t *testing.T) {
	t.Parallel()
	a := newFenceHintArbitrator()
	now := time.Unix(0, 2).UTC()
	a.Ingest(FenceHint{NodeID: "fast", H: 100, ObservedAt: now})
	if a.Ingest(FenceHint{NodeID: "slow", H: 80, ObservedAt: now.Add(time.Second)}) {
		t.Fatal("stale overlapping hint accepted")
	}
	if got := a.EffectiveH(); got != 100 {
		t.Fatalf("EffectiveH = %d, want 100", got)
	}
}

func TestFenceHintArbitratorLeaderNotHolderUsesHints(t *testing.T) {
	t.Parallel()
	a := newFenceHintArbitrator()
	now := time.Now().UTC()
	a.Ingest(FenceHint{NodeID: "replica-1", H: 30, ObservedAt: now})
	a.Ingest(FenceHint{NodeID: "replica-2", H: 45, ObservedAt: now})
	if got := a.EffectiveH(); got != 45 {
		t.Fatalf("leader-not-holder EffectiveH = %d, want 45", got)
	}
}
