package orchestrator

import (
	"maps"
	"sync"
	"time"
)

// FenceHint is ephemeral replica evidence for fence trigger evaluation.
// Only the vault-ctl leader consumes hints; publication remains durable on Raft.
type FenceHint struct {
	NodeID     string
	H          uint64 // ingest high watermark (seq axis)
	ObservedAt time.Time
}

// fenceHintArbitrator ingests replica hints and suppresses stale overlaps.
type fenceHintArbitrator struct {
	mu          sync.Mutex
	byNode      map[string]FenceHint
	clusterHigh uint64
}

func newFenceHintArbitrator() *fenceHintArbitrator {
	return &fenceHintArbitrator{byNode: make(map[string]FenceHint)}
}

// Ingest records hint evidence. Returns false when the hint is rejected as
// regressive for its node or stale relative to newer cluster progress.
func (a *fenceHintArbitrator) Ingest(h FenceHint) bool {
	if h.NodeID == "" || h.H == 0 {
		return false
	}
	if h.ObservedAt.IsZero() {
		h.ObservedAt = time.Now().UTC()
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if prev, ok := a.byNode[h.NodeID]; ok && h.H < prev.H {
		return false
	}
	if h.H < a.clusterHigh {
		return false
	}
	a.byNode[h.NodeID] = h
	if h.H > a.clusterHigh {
		a.clusterHigh = h.H
	}
	return true
}

// EffectiveH returns the highest accepted hint H across replicas.
func (a *fenceHintArbitrator) EffectiveH() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.clusterHigh
}

// ClusterHigh is an alias for EffectiveH (test + operator clarity).
func (a *fenceHintArbitrator) ClusterHigh() uint64 {
	return a.EffectiveH()
}

func (a *fenceHintArbitrator) snapshot() map[string]FenceHint {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]FenceHint, len(a.byNode))
	maps.Copy(out, a.byNode)
	return out
}
