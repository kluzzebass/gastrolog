package vaultraft

import (
	"errors"
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
	"gastrolog/internal/vaultraft/vaultctlfsm"

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
// server.setupMultiNode (VaultTypeMemory, no Raft).
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

	// Harness convergence waits are progress-based (see waitProgress), not
	// wall-clock-bounded: the awaited work (election, log replication, FSM
	// apply) is CPU-bound in-process activity, so under multi-suite
	// contention a fixed wall-clock budget buys an arbitrary fraction of the
	// compute — TestReliability_ConcurrentWrites_NoDivergence starved and
	// failed at 10.5s under full-suite load despite passing solo in ~1s. A
	// wait fails only when its observed progress metric has not changed for
	// this stall window. The window sits well above any legitimate quiet
	// period in this harness — the raft config above uses 300ms
	// election/heartbeat timeouts, so even several stalled election rounds
	// under contention fit comfortably inside it.
	harnessStallWindow = 60 * time.Second
	// harnessHardBackstop bounds total wait time even while progress
	// trickles, so a livelocked metric (one that keeps changing without ever
	// converging — election churn, oscillating counts) still fails with
	// diagnostics instead of hanging the package. Steady progress toward the
	// goal finishes far inside this; only a true wedge or livelock reaches it.
	harnessHardBackstop = 5 * time.Minute
)

// progressPoint is one observed change of a wait's progress metric, kept for
// the trajectory report on failure.
type progressPoint struct {
	elapsed time.Duration
	state   string
}

// pollUntilStall polls sample every interval until it reports done, or its
// progress string stops changing for harnessStallWindow, or
// harnessHardBackstop elapses. It never touches *testing.T, so it is safe to
// call from any goroutine — unlike waitProgress below, which Fatalfs and so
// must only run on the goroutine executing the test (applyWithLeaderRetry in
// reliability_test.go runs inside writer goroutines and builds on this
// directly for that reason).
//
// sample returns (progress, done): done=true ends the wait successfully;
// progress is an opaque human-readable snapshot of the awaited metric — ANY
// change resets the stall clock and is appended to the returned trajectory.
// Returns whether sample reported done, a human-readable reason when it
// didn't, and the full trajectory for diagnostics.
func pollUntilStall(interval time.Duration, sample func() (string, bool)) (done bool, reason string, trajectory []progressPoint) {
	start := time.Now()
	state, ok := sample()
	trajectory = []progressPoint{{0, state}}
	if ok {
		return true, "", trajectory
	}
	lastChange := start
	for {
		time.Sleep(interval)
		next, ok := sample()
		now := time.Now()
		if ok {
			trajectory = append(trajectory, progressPoint{now.Sub(start), next})
			return true, "", trajectory
		}
		if next != state {
			state = next
			lastChange = now
			trajectory = append(trajectory, progressPoint{now.Sub(start), next})
		}
		stalledFor := now.Sub(lastChange)
		if stalledFor >= harnessStallWindow {
			return false, fmt.Sprintf("progress stalled (no change for %s; stall window %s)",
				stalledFor.Round(time.Millisecond), harnessStallWindow), trajectory
		}
		if now.Sub(start) >= harnessHardBackstop {
			return false, fmt.Sprintf("hit hard backstop %s while still changing (livelock?)", harnessHardBackstop), trajectory
		}
	}
}

// formatTrajectory renders a progress trajectory for a failed wait. Long
// trajectories are elided in the middle — the head shows the starting state,
// the tail shows where progress stopped.
func formatTrajectory(points []progressPoint) string {
	const headKeep, tailKeep = 8, 24
	var b []byte
	appendPoint := func(p progressPoint) {
		b = append(b, []byte(fmt.Sprintf("  +%-10s %s\n", p.elapsed.Round(time.Millisecond), p.state))...)
	}
	if len(points) <= headKeep+tailKeep {
		for _, p := range points {
			appendPoint(p)
		}
		return string(b)
	}
	for _, p := range points[:headKeep] {
		appendPoint(p)
	}
	b = append(b, []byte(fmt.Sprintf("  ... %d changes elided ...\n", len(points)-headKeep-tailKeep))...)
	for _, p := range points[len(points)-tailKeep:] {
		appendPoint(p)
	}
	return string(b)
}

// waitProgress is the harness's convergence-wait primitive for waits that
// run on the main test goroutine (it calls h.t.Fatalf on failure — unsafe
// from any other goroutine; see pollUntilStall). onFail (optional) runs
// scenario diagnostics before the Fatalf.
func (h *reliabilityHarness) waitProgress(what string, interval time.Duration, sample func() (string, bool), onFail func()) {
	h.t.Helper()
	start := time.Now()
	done, reason, trajectory := pollUntilStall(interval, sample)
	if done {
		// Slow-but-successful waits log their trajectory so a wait that
		// nearly stalled (legitimate quiet period approaching the window)
		// is visible without failing.
		if elapsed := time.Since(start); elapsed >= harnessStallWindow/2 {
			h.t.Logf("%s: converged after %s (%d progress changes)\n%s",
				what, elapsed.Round(time.Millisecond), len(trajectory), formatTrajectory(trajectory))
		}
		return
	}
	if onFail != nil {
		onFail()
	}
	h.t.Fatalf("%s: %s after %s\nprogress trajectory (%d changes):\n%s",
		what, reason, time.Since(start).Round(time.Millisecond), len(trajectory), formatTrajectory(trajectory))
}

// newReliabilityHarness boots an N-node cluster, bootstraps the first node,
// and waits for a leader. All nodes start connected. Cleanup is automatic
// via t.Cleanup.
func newReliabilityHarness(t *testing.T, n int) *reliabilityHarness {
	t.Helper()
	if testing.Short() {
		t.Skip("reliability matrix: real multi-node raft cluster, failover/convergence timing; -short skips (see `just backend test-reliability`)")
	}
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

// waitForLeader blocks until any node reports itself as leader, or the
// progress wait stalls. Returns the leader's node ID. Election activity
// (raft state/term changing on any node) counts as progress, so a slow-but-
// live election under CPU contention is never mistaken for a stall.
func (h *reliabilityHarness) waitForLeader() string {
	h.t.Helper()
	var leader string
	h.waitProgress("leader election", 20*time.Millisecond, func() (string, bool) {
		if id := h.leaderID(); id != "" {
			leader = id
			return "", true
		}
		var views []string
		for _, id := range h.nodeIDs {
			n := h.nodes[id]
			n.mu.Lock()
			r := n.raft
			n.mu.Unlock()
			if r == nil {
				views = append(views, id+"=down")
				continue
			}
			views = append(views, fmt.Sprintf("%s=%s@t%s", id, r.State(), r.Stats()["term"]))
		}
		return fmt.Sprintf("%v", views), false
	}, nil)
	return leader
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

// leader returns the current leader node (blocks until progress stalls; see
// waitForLeader).
func (h *reliabilityHarness) leader() *reliabilityNode {
	h.t.Helper()
	return h.nodes[h.waitForLeader()]
}

// applyInstanceCreate submits a CmdCreateChunk to the vault FSM via the
// current leader, retrying across leadership transitions. At the harness's
// aggressive 300ms election timeouts, leadership can move mid-apply — either
// spontaneously when a loaded machine starves heartbeats (hraft returns
// ErrLeadershipLost for a mid-commit step-down; observed under full-suite
// runs) or when a scenario forces a transfer. These are documented
// retryable transients, not quorum failures; re-applying the same command
// bytes is safe because the FSM apply is convergent (applyCreate overwrites
// with identical values). A real quorum loss still fails: the retries stall
// out against the shared progress-wait window.
func (h *reliabilityHarness) applyInstanceCreate(vaultID glid.GLID, chunkID chunk.ChunkID, at time.Time) {
	h.t.Helper()
	wire := vaultctlfsm.MarshalCreateChunk(chunkID, at, at, at)
	h.applyCommand(MarshalVaultChunkCommand(vaultID, wire), "apply instance create")
}

// applyCommand submits pre-marshalled command bytes via whichever node
// currently leads, with the same transient-retry policy as
// applyInstanceCreate. Stopped nodes can't win elections, so following
// h.leader() never resurrects a downed leader in failover scenarios.
// Progress-based: retries across leadership transitions are themselves
// expected under contention, so this only fails when the same (leader,
// error) pair repeats for the stall window — genuinely no forward motion —
// rather than a fixed retry deadline.
func (h *reliabilityHarness) applyCommand(cmd []byte, what string) {
	h.t.Helper()
	h.waitProgress(what, 50*time.Millisecond, func() (string, bool) {
		leader := h.leader()
		fut := leader.raft.Apply(cmd, 2*time.Second)
		err := fut.Error()
		if err == nil {
			if r, ok := fut.Response().(error); ok && r != nil {
				h.t.Fatalf("%s FSM error: %v", what, r)
			}
			return "", true
		}
		if !isLeadershipTransient(err) {
			h.t.Fatalf("%s: %v", what, err)
		}
		return fmt.Sprintf("leader=%s err=%v", leader.id, err), false
	}, nil)
}

// isLeadershipTransient reports whether an Apply error only means "the
// leader moved" — retry against the new leader — as opposed to a real
// commit failure.
func isLeadershipTransient(err error) bool {
	return errors.Is(err, hraft.ErrLeadershipLost) ||
		errors.Is(err, hraft.ErrNotLeader) ||
		errors.Is(err, hraft.ErrLeadershipTransferInProgress) ||
		errors.Is(err, hraft.ErrEnqueueTimeout)
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

// instanceFSMFingerprint produces a deterministic, comparable snapshot of
// an vault sub-FSM's state: sorted chunk IDs with their seal/compressed
// state, sorted transition receipts, sorted tombstone IDs. Two fingerprints
// that string-equal represent identical replicated state.
func instanceFSMFingerprint(t *vaultctlfsm.FSM) string {
	entries := t.List()
	ids := make([]chunk.ChunkID, len(entries))
	byID := make(map[chunk.ChunkID]vaultctlfsm.ManifestEntry, len(entries))
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
		sb.writef("chunk=%x sealed=%t ret=%t archived=%t\n",
			id[:], e.IsSealed(), e.RetentionPending, e.Archived)
	}
	return sb.String()
}

// vaultFSMFingerprint deterministically encodes every vault sub-FSM in
// the vault FSM. Two vault FSMs with equal fingerprints have converged.
func vaultFSMFingerprint(f *FSM) string {
	f.mu.Lock()
	ids := make([]glid.GLID, 0, len(f.vaults))
	for id := range f.vaults {
		ids = append(ids, id)
	}
	f.mu.Unlock()
	slices.SortFunc(ids, glid.Compare)

	var sb fingerprintBuilder
	for _, id := range ids {
		sb.writef("vault=%x\n", id[:])
		f.mu.Lock()
		sub := f.vaults[id]
		f.mu.Unlock()
		if sub != nil {
			sb.write(instanceFSMFingerprint(sub))
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
	var lastPrints map[string]string
	h.waitProgress("FSM convergence", 20*time.Millisecond, func() (string, bool) {
		leaderID := h.leaderID()
		if leaderID == "" {
			return "no-leader", false
		}
		leaderNode := h.nodes[leaderID]
		leaderLast := leaderNode.raft.LastIndex()
		if leaderNode.raft.AppliedIndex() < leaderLast {
			return fmt.Sprintf("leader=%s applied=%d/%d", leaderID, leaderNode.raft.AppliedIndex(), leaderLast), false
		}
		leaderPrint := vaultFSMFingerprint(leaderNode.fsm)
		lastPrints = map[string]string{leaderID: leaderPrint}

		converged := true
		var views []string
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
				views = append(views, fmt.Sprintf("%s=behind(%d/%d)", id, r.AppliedIndex(), leaderLast))
				continue
			}
			p := vaultFSMFingerprint(fsm)
			lastPrints[id] = p
			if p == leaderPrint {
				views = append(views, id+"=match")
			} else {
				converged = false
				views = append(views, id+"=diverged")
			}
		}
		return fmt.Sprintf("%v", views), converged
	}, func() {
		h.t.Logf("FSM fingerprints at stall:\n%s", formatPrints(lastPrints))
	})
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
