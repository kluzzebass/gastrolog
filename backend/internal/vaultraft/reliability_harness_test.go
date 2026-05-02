package vaultraft

import (
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftwal"
	tierfsm "gastrolog/internal/tier/raftfsm"

	hraft "github.com/hashicorp/raft"
)

// reliabilityNode bundles all state for a single in-process node in the
// reliability harness.
type reliabilityNode struct {
	id        string
	walDir    string
	groupName string

	mu        sync.Mutex
	fsm       *FSM
	raft      *hraft.Raft
	transport *hraft.InmemTransport
	wal       *raftwal.WAL
	store     *raftwal.GroupStore
	snap      *hraft.InmemSnapshotStore
}

// reliabilityHarness is an in-process multi-node cluster running a real
// vaultraft.FSM on each node backed by raftwal + hraft's InmemTransport.
//
// Use for scenarios that must exercise the full Raft/FSM stack: restart
// survival, leader failover, partition heal, mid-apply crash. For scenarios
// that only need multiple orchestrators but not real Raft, use
// server.setupMultiNode (TierTypeMemory, no Raft).
type reliabilityHarness struct {
	t         *testing.T
	nodeIDs   []string
	nodes     map[string]*reliabilityNode
	groupName string
}

const (
	harnessGroupName        = "vault-ctl-reliability"
	harnessElectionTimeout  = 300 * time.Millisecond
	harnessHeartbeatTimeout = 300 * time.Millisecond
	harnessLeaseTimeout     = 150 * time.Millisecond
	harnessLeaderWait       = 5 * time.Second
	harnessConvergeWait     = 5 * time.Second
)

// newReliabilityHarness boots an N-node cluster, bootstraps the first node,
// and waits for a leader. All nodes start connected. Cleanup is automatic
// via t.Cleanup.
func newReliabilityHarness(t *testing.T, n int) *reliabilityHarness {
	t.Helper()
	if n < 1 {
		t.Fatal("reliability harness requires n >= 1")
	}

	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("node-%d", i+1)
	}

	h := &reliabilityHarness{
		t:         t,
		nodeIDs:   ids,
		nodes:     make(map[string]*reliabilityNode, n),
		groupName: harnessGroupName,
	}

	for _, id := range ids {
		node := &reliabilityNode{
			id:        id,
			walDir:    filepath.Join(t.TempDir(), "wal-"+id),
			groupName: h.groupName,
		}
		h.nodes[id] = node
	}

	h.startAllNodes()
	h.bootstrap(ids[0])
	h.waitForLeader()
	t.Cleanup(h.shutdown)
	return h
}

// startAllNodes opens WALs, builds FSMs, creates transports, wires them,
// and constructs the hraft.Raft instances. Idempotent across restart —
// calling again after Stop reuses the WAL dir (persistent state survives).
func (h *reliabilityHarness) startAllNodes() {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		h.startNode(id)
	}
	h.wireTransports()
}

func (h *reliabilityHarness) startNode(id string) {
	h.t.Helper()
	n := h.nodes[id]
	n.mu.Lock()
	defer n.mu.Unlock()

	wal, err := raftwal.Open(n.walDir)
	if err != nil {
		h.t.Fatalf("%s: open wal: %v", id, err)
	}
	n.wal = wal
	n.store = wal.GroupStore(n.groupName)
	n.fsm = NewFSM()
	_, trans := hraft.NewInmemTransportWithTimeout(hraft.ServerAddress(id), 500*time.Millisecond)
	n.transport = trans
	n.snap = hraft.NewInmemSnapshotStore()

	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(id)
	cfg.LogOutput = io.Discard
	cfg.HeartbeatTimeout = harnessHeartbeatTimeout
	cfg.ElectionTimeout = harnessElectionTimeout
	cfg.LeaderLeaseTimeout = harnessLeaseTimeout
	cfg.CommitTimeout = 20 * time.Millisecond
	cfg.SnapshotThreshold = 8192
	cfg.TrailingLogs = 512

	r, err := hraft.NewRaft(cfg, n.fsm, n.store, n.store, n.snap, n.transport)
	if err != nil {
		h.t.Fatalf("%s: NewRaft: %v", id, err)
	}
	n.raft = r
}

// wireTransports connects every pair of live transports both ways. Call
// after any node start/restart to include the new node in the mesh.
func (h *reliabilityHarness) wireTransports() {
	for _, a := range h.nodeIDs {
		na := h.nodes[a]
		if na.transport == nil {
			continue
		}
		for _, b := range h.nodeIDs {
			if a == b {
				continue
			}
			nb := h.nodes[b]
			if nb.transport == nil {
				continue
			}
			na.transport.Connect(hraft.ServerAddress(b), nb.transport)
			nb.transport.Connect(hraft.ServerAddress(a), na.transport)
		}
	}
}

func (h *reliabilityHarness) bootstrap(nodeID string) {
	h.t.Helper()
	servers := make([]hraft.Server, len(h.nodeIDs))
	for i, id := range h.nodeIDs {
		servers[i] = hraft.Server{
			ID:      hraft.ServerID(id),
			Address: hraft.ServerAddress(id),
		}
	}
	n := h.nodes[nodeID]
	if err := n.raft.BootstrapCluster(hraft.Configuration{Servers: servers}).Error(); err != nil {
		h.t.Fatalf("bootstrap %s: %v", nodeID, err)
	}
}

// waitForLeader blocks until any node reports itself as leader (or times
// out). Returns the leader's node ID.
func (h *reliabilityHarness) waitForLeader() string {
	h.t.Helper()
	deadline := time.Now().Add(harnessLeaderWait)
	for time.Now().Before(deadline) {
		if id := h.leaderID(); id != "" {
			return id
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatal("no leader elected within timeout")
	return ""
}

// leaderID returns the ID of the current leader among live nodes, or ""
// if none. Only considers nodes that are live (not Stopped).
func (h *reliabilityHarness) leaderID() string {
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		n.mu.Lock()
		r := n.raft
		n.mu.Unlock()
		if r == nil {
			continue
		}
		if r.State() == hraft.Leader {
			return id
		}
	}
	return ""
}

// leader returns the current leader node (blocks up to harnessLeaderWait).
func (h *reliabilityHarness) leader() *reliabilityNode {
	h.t.Helper()
	return h.nodes[h.waitForLeader()]
}

// applyTierCreate submits a CmdCreateChunk to the vault FSM via the current
// leader. Used by scenarios that want to populate FSM state.
func (h *reliabilityHarness) applyTierCreate(tierID glid.GLID, chunkID chunk.ChunkID, at time.Time) {
	h.t.Helper()
	leader := h.leader()
	wire := tierfsm.MarshalCreateChunk(chunkID, at, at, at)
	cmd := MarshalTierCommand(tierID, wire)
	fut := leader.raft.Apply(cmd, 2*time.Second)
	if err := fut.Error(); err != nil {
		h.t.Fatalf("apply tier create: %v", err)
	}
	if r, ok := fut.Response().(error); ok && r != nil {
		h.t.Fatalf("apply tier create FSM error: %v", r)
	}
}

// stopNode shuts down a node's Raft and WAL (persistent state stays on
// disk). Use for restart scenarios: stopNode + startNode(sameID) +
// wireTransports reloads from WAL.
func (h *reliabilityHarness) stopNode(id string) {
	h.t.Helper()
	n := h.nodes[id]
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			h.t.Fatalf("%s: shutdown raft: %v", id, err)
		}
		n.raft = nil
	}
	if n.wal != nil {
		if err := n.wal.Close(); err != nil {
			h.t.Fatalf("%s: close wal: %v", id, err)
		}
		n.wal = nil
		n.store = nil
	}
	n.transport = nil
	n.fsm = nil
}

// restartNode stops the node, then reopens it from the same WAL dir.
// Rewires transports so the restarted node rejoins the mesh.
func (h *reliabilityHarness) restartNode(id string) {
	h.t.Helper()
	h.stopNode(id)
	h.startNode(id)
	h.wireTransports()
}

// disconnect severs both directions of the transport between a and b,
// simulating a partial network partition.
func (h *reliabilityHarness) disconnect(a, b string) {
	h.t.Helper()
	na := h.nodes[a]
	nb := h.nodes[b]
	if na.transport != nil {
		na.transport.Disconnect(hraft.ServerAddress(b))
	}
	if nb.transport != nil {
		nb.transport.Disconnect(hraft.ServerAddress(a))
	}
}

// reconnect restores the transport pair.
func (h *reliabilityHarness) reconnect(a, b string) {
	h.t.Helper()
	na := h.nodes[a]
	nb := h.nodes[b]
	if na.transport != nil && nb.transport != nil {
		na.transport.Connect(hraft.ServerAddress(b), nb.transport)
		nb.transport.Connect(hraft.ServerAddress(a), na.transport)
	}
}

// shutdown closes every node. Called via t.Cleanup.
func (h *reliabilityHarness) shutdown() {
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		n.mu.Lock()
		r := n.raft
		w := n.wal
		n.raft = nil
		n.wal = nil
		n.mu.Unlock()
		if r != nil {
			_ = r.Shutdown().Error()
		}
		if w != nil {
			_ = w.Close()
		}
	}
}

// --- Divergence assertions ---

// tierFSMFingerprint produces a deterministic, comparable snapshot of a
// tier sub-FSM's state: sorted chunk IDs with their seal/compressed state,
// sorted transition receipts, sorted tombstone IDs. Two fingerprints that
// string-equal represent identical replicated state.
func tierFSMFingerprint(t *tierfsm.FSM) string {
	entries := t.List()
	ids := make([]chunk.ChunkID, len(entries))
	byID := make(map[chunk.ChunkID]tierfsm.ManifestEntry, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
		byID[e.ID] = e
	}
	slices.SortFunc(ids, func(a, b chunk.ChunkID) int {
		for i := range a {
			if a[i] != b[i] {
				return int(a[i]) - int(b[i])
			}
		}
		return 0
	})
	var sb fingerprintBuilder
	for _, id := range ids {
		e := byID[id]
		sb.writef("chunk=%x sealed=%t compressed=%t ret=%t stream=%t archived=%t\n",
			id[:], e.Sealed, e.Compressed, e.RetentionPending, e.TransitionStreamed, e.Archived)
	}
	return sb.String()
}

// vaultFSMFingerprint deterministically encodes every tier sub-FSM in the
// vault FSM. Two vault FSMs with equal fingerprints have converged.
func vaultFSMFingerprint(f *FSM) string {
	f.tierMu.Lock()
	ids := make([]glid.GLID, 0, len(f.tiers))
	for id := range f.tiers {
		ids = append(ids, id)
	}
	f.tierMu.Unlock()
	slices.SortFunc(ids, compareGLID)

	var sb fingerprintBuilder
	for _, id := range ids {
		sb.writef("tier=%x\n", id[:])
		f.tierMu.Lock()
		sub := f.tiers[id]
		f.tierMu.Unlock()
		if sub != nil {
			sb.write(tierFSMFingerprint(sub))
		}
	}
	return sb.String()
}

// assertAllFSMsConverged polls until every live node (1) has an
// AppliedIndex at or past the leader's LastIndex (log has been replayed
// through the FSM) and (2) has an FSM fingerprint matching the leader's.
//
// The AppliedIndex check matters after restart: NewRaft returns before the
// replay goroutine finishes re-applying the WAL to the FSM. Without it, we
// can observe "all FSMs empty, therefore converged" immediately after a
// crash+restart, before replay has done its work.
func (h *reliabilityHarness) assertAllFSMsConverged() {
	h.t.Helper()
	deadline := time.Now().Add(harnessConvergeWait)
	var lastPrints map[string]string
	for time.Now().Before(deadline) {
		leaderID := h.leaderID()
		if leaderID == "" {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		leaderNode := h.nodes[leaderID]
		leaderLast := leaderNode.raft.LastIndex()
		if leaderNode.raft.AppliedIndex() < leaderLast {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		leaderPrint := vaultFSMFingerprint(leaderNode.fsm)
		lastPrints = map[string]string{leaderID: leaderPrint}

		converged := true
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			n.mu.Lock()
			fsm := n.fsm
			r := n.raft
			n.mu.Unlock()
			if fsm == nil || r == nil {
				continue
			}
			if r.AppliedIndex() < leaderLast {
				converged = false
				lastPrints[id] = fmt.Sprintf("<behind: applied=%d leaderLast=%d>",
					r.AppliedIndex(), leaderLast)
				continue
			}
			p := vaultFSMFingerprint(fsm)
			lastPrints[id] = p
			if p != leaderPrint {
				converged = false
			}
		}
		if converged {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	h.t.Fatalf("FSMs did not converge within %s. Fingerprints:\n%s",
		harnessConvergeWait, formatPrints(lastPrints))
}

func formatPrints(m map[string]string) string {
	var sb fingerprintBuilder
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		sb.writef("=== %s ===\n%s\n", id, m[id])
	}
	return sb.String()
}

// fingerprintBuilder wraps a strings.Builder with Printf/Write helpers and
// satisfies the gofmt-ignored "import strings for one Printf" pattern.
type fingerprintBuilder struct {
	parts []string
}

func (b *fingerprintBuilder) writef(format string, a ...any) {
	b.parts = append(b.parts, fmt.Sprintf(format, a...))
}

func (b *fingerprintBuilder) write(s string) {
	b.parts = append(b.parts, s)
}

func (b *fingerprintBuilder) String() string {
	n := 0
	for _, p := range b.parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range b.parts {
		out = append(out, p...)
	}
	return string(out)
}
