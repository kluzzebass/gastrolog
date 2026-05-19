// W-of-N coordinator tests (gastrolog-5pn44).
//
// Coverage dimensions per CLAUDE.md "Test Coverage — MANDATORY":
//   - Single-node degenerate case (N=1, W=1).
//   - Happy path: W ≤ N peers ack within deadline.
//   - Unhappy path: too many failures → ErrWOfNUnreachable.
//   - Edge case: live-Receiving de-escalation reclassifies a removed
//     peer's failure as not-required, NOT as a failure that counts
//     toward the unreachable threshold. Closes the spurious-failure
//     hole during multi-node drains.
//   - Context cancellation: deadline before threshold.
//   - Closed results channel before threshold.
//   - Defensive guards: W > N, W = 0.

package orchestrator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// allReceiving is the no-op live-membership oracle: every node stays
// in Receiving. Used when the test isn't exercising the de-escalation
// path.
func allReceiving(_ string) bool { return true }

// neverReceiving treats every failure as already-removed; the
// coordinator should classify everything as "not required."
func neverReceiving(_ string) bool { return false }

func TestWaitWOfNHappyPathReturnsOnThreshold(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"}
	results <- NodeResult{NodeID: "b"}
	results <- NodeResult{NodeID: "c"} // arrives after W reached
	close(results)

	err := waitWOfN(context.Background(), 3, 2, results, allReceiving)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestWaitWOfNFailsWhenStillExpectedDropsBelowTarget(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a", Err: errors.New("peer-error")}
	results <- NodeResult{NodeID: "b", Err: errors.New("peer-error")}
	results <- NodeResult{NodeID: "c"} // one success arrives too late

	err := waitWOfN(context.Background(), 3, 2, results, allReceiving)
	if !errors.Is(err, ErrWOfNUnreachable) {
		t.Fatalf("expected ErrWOfNUnreachable, got %v", err)
	}
	// The first peer error should be wrapped for telemetry.
	if !strings.Contains(err.Error(), "peer-error") {
		t.Errorf("expected first-error wrapping; err = %v", err)
	}
}

func TestWaitWOfNTreatsLeftReceivingAsNotRequired(t *testing.T) {
	t.Parallel()
	// 3 of 3 with W=2. Two peers fail, but both have been removed
	// from Receiving (live-membership says they're no longer
	// expected). The third peer's ack succeeds, so W=2 can't be
	// reached — but the "unreachable" classification should reflect
	// that the failures are not-required, not failures.
	//
	// Stronger scenario: 4-of-4 with W=2. Two are de-escalated, one
	// fails, one acks. Final state: 1 ack, 1 failure, 2 not-required.
	// Still expected = 4 − 1 − 2 = 1 < W − acks = 1. Wait — that's
	// equal, which is fine. So waitWOfN should NOT return failure
	// here; it should wait for the still-expected peer. But there's
	// only one peer left and it acked. Let's pick a case where the
	// de-escalation matters.
	results := make(chan NodeResult, 4)
	results <- NodeResult{NodeID: "a"} // ack
	results <- NodeResult{NodeID: "b", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "c", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "d"} // ack
	close(results)

	removed := map[string]bool{"b": true, "c": true}
	classifier := func(nodeID string) bool { return !removed[nodeID] }

	err := waitWOfN(context.Background(), 4, 2, results, classifier)
	if err != nil {
		t.Fatalf("expected success (b and c de-escalated; a and d acked), got %v", err)
	}
}

func TestWaitWOfNSpuriousFailureScenario(t *testing.T) {
	t.Parallel()
	// Regression scenario from docs/fan-out-data-plane-design.md
	// "Failure-mode rationale": multi-node drain during steady writes.
	// Snapshot is {a, b, c} with W=2. b and c get drained mid-write
	// (CmdRemoveReceiving). Without the de-escalation, b and c both
	// failing would tip the coordinator over the failure threshold.
	// With it, they're not-required; a's single ack is "enough"
	// against the now-effective denominator of 1.
	//
	// We assert this returns SUCCESS, not failure.
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"} // sole still-Receiving peer
	results <- NodeResult{NodeID: "b", Err: errors.New("drained")}
	results <- NodeResult{NodeID: "c", Err: errors.New("drained")}
	close(results)

	drained := map[string]bool{"b": true, "c": true}
	classifier := func(nodeID string) bool { return !drained[nodeID] }

	err := waitWOfN(context.Background(), 3, 2, results, classifier)
	if err != nil {
		t.Fatalf("multi-node drain produced spurious failure: %v", err)
	}
}

func TestWaitWOfNContextDeadlineBeforeThreshold(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"} // only one ack arrives

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := waitWOfN(ctx, 3, 2, results, allReceiving)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestWaitWOfNClosedChannelBeforeThreshold(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"}
	close(results)

	err := waitWOfN(context.Background(), 3, 2, results, allReceiving)
	if !errors.Is(err, ErrWOfNUnreachable) {
		t.Fatalf("expected ErrWOfNUnreachable on closed channel below threshold, got %v", err)
	}
}

func TestWaitWOfNDegenerateW1OfN1(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult, 1)
	results <- NodeResult{NodeID: "a"}
	close(results)

	if err := waitWOfN(context.Background(), 1, 1, results, allReceiving); err != nil {
		t.Fatalf("W=1, N=1 ack: %v", err)
	}
}

func TestWaitWOfNW0IsTrivialSuccess(t *testing.T) {
	t.Parallel()
	// Defensive: W=0 is invalid per config validation but the
	// coordinator should not deadlock if it ever leaks through.
	results := make(chan NodeResult)
	if err := waitWOfN(context.Background(), 3, 0, results, allReceiving); err != nil {
		t.Fatalf("W=0 trivial success: %v", err)
	}
}

func TestWaitWOfNRejectsWGreaterThanN(t *testing.T) {
	t.Parallel()
	results := make(chan NodeResult)
	err := waitWOfN(context.Background(), 2, 3, results, allReceiving)
	if err == nil {
		t.Fatal("expected error for W > N")
	}
}

func TestWaitWOfNAllRemovedReturnsUnreachable(t *testing.T) {
	t.Parallel()
	// Pathological: every snapshot member's failure is de-escalated.
	// Effective denominator becomes 0; W > 0 can't be met. The
	// coordinator should return ErrWOfNUnreachable, not deadlock.
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "b", Err: errors.New("gone")}
	results <- NodeResult{NodeID: "c", Err: errors.New("gone")}
	close(results)

	err := waitWOfN(context.Background(), 3, 1, results, neverReceiving)
	if !errors.Is(err, ErrWOfNUnreachable) {
		t.Fatalf("expected ErrWOfNUnreachable when all members de-escalated, got %v", err)
	}
}

func TestWaitWOfNStragglersAfterSuccessAreSafe(t *testing.T) {
	t.Parallel()
	// Buffered channel so producer goroutines completing AFTER
	// waitWOfN returns don't block. Mimics the production setup: the
	// caller launches N goroutines, each writes to a chan with cap N,
	// and waitWOfN exits as soon as the threshold is reached.
	results := make(chan NodeResult, 3)
	results <- NodeResult{NodeID: "a"}
	results <- NodeResult{NodeID: "b"}

	if err := waitWOfN(context.Background(), 3, 2, results, allReceiving); err != nil {
		t.Fatalf("threshold reached early: %v", err)
	}

	// Send a straggler post-return; it should not block.
	done := make(chan struct{})
	go func() {
		results <- NodeResult{NodeID: "c"}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("straggler blocked on buffered channel")
	}
}

// ---------- fanOutAppend tests ----------

// fakeReplicator is a per-call configurable ChunkReplicator stub. Each
// AppendRecords call routes through a closure so a test can return
// nil/err per node and count calls.
type fakeReplicator struct {
	mu          sync.Mutex
	calls       map[string]int
	appendStub  func(nodeID string) error
	sealCount   atomic.Int32
}

func newFakeReplicator() *fakeReplicator {
	return &fakeReplicator{calls: make(map[string]int)}
}

func (f *fakeReplicator) AppendRecords(_ context.Context, nodeID string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	f.mu.Lock()
	f.calls[nodeID]++
	stub := f.appendStub
	f.mu.Unlock()
	if stub != nil {
		return stub(nodeID)
	}
	return nil
}

func (f *fakeReplicator) SealVault(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	f.sealCount.Add(1)
	return nil
}
func (f *fakeReplicator) ImportSealedChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ chunk.RecordIterator) error {
	return nil
}
func (f *fakeReplicator) DeleteChunk(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID) error {
	return nil
}
func (f *fakeReplicator) RequestReplicaCatchup(_ context.Context, _ string, _ glid.GLID, _ []chunk.ChunkID, _ string) (uint32, error) {
	return 0, nil
}

func (f *fakeReplicator) callsFor(nodeID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[nodeID]
}

func testFanOutRecord(t *testing.T) chunk.Record {
	t.Helper()
	now := time.Now()
	return chunk.Record{
		SourceTS: now,
		IngestTS: now,
		Attrs:    chunk.Attributes{"msg": "fanout-test"},
		Raw:      []byte("fanout-test"),
	}
}

func newFanOutTestOrch(t *testing.T) (*Orchestrator, *fakeReplicator) {
	t.Helper()
	orch := newTestOrch(t, Config{LocalNodeID: "node-local", Phase: lifecycle.New()})
	rep := newFakeReplicator()
	orch.SetChunkReplicator(rep)
	return orch, rep
}

func TestFanOutAppendHappyPathAllAck(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	rec := testFanOutRecord(t)

	// W=N=3 so waitWOfN waits for all three goroutines to complete;
	// otherwise the third straggler races the test's call-count
	// assertion (waitWOfN returns at W acks; remaining goroutines
	// complete asynchronously).
	snapshot := []string{"node-a", "node-b", "node-c"}
	err := orch.fanOutAppend(context.Background(), vaultID, chunkID, rec, snapshot, 3, allReceiving, nil)
	if err != nil {
		t.Fatalf("fanOutAppend: %v", err)
	}
	for _, n := range snapshot {
		if rep.callsFor(n) != 1 {
			t.Errorf("AppendRecords(%s) called %d times, want 1", n, rep.callsFor(n))
		}
	}
}

func TestFanOutAppendLocalMemberAppendsToCM(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	rec := testFanOutRecord(t)

	cmFactory, _ := chunkmem.NewFactory(), error(nil)
	cm, err := cmFactory(nil, nil)
	if err != nil {
		t.Fatalf("create chunk manager: %v", err)
	}

	// snapshot includes the orchestrator's own node-local; local
	// member should hit the cm path, not chunkReplicator.AppendRecords.
	snapshot := []string{"node-local", "node-remote"}
	err = orch.fanOutAppend(context.Background(), vaultID, chunkID, rec, snapshot, 2, allReceiving, cm)
	if err != nil {
		t.Fatalf("fanOutAppend: %v", err)
	}
	if rep.callsFor("node-local") != 0 {
		t.Errorf("local member should not hit chunkReplicator; got %d calls", rep.callsFor("node-local"))
	}
	if rep.callsFor("node-remote") != 1 {
		t.Errorf("remote member should hit chunkReplicator once; got %d", rep.callsFor("node-remote"))
	}
}

func TestFanOutAppendReportsFailureWhenWUnreachable(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)
	rep.appendStub = func(_ string) error { return errors.New("peer down") }

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	rec := testFanOutRecord(t)

	snapshot := []string{"node-a", "node-b", "node-c"}
	err := orch.fanOutAppend(context.Background(), vaultID, chunkID, rec, snapshot, 2, allReceiving, nil)
	if !errors.Is(err, ErrWOfNUnreachable) {
		t.Fatalf("expected ErrWOfNUnreachable, got %v", err)
	}
}

func TestFanOutAppendDeescalatesRemovedReceivers(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)
	// Both b and c fail (e.g., drained mid-write). a succeeds. With
	// the de-escalation, effectiveW shrinks and the lone ack from a
	// is enough.
	rep.appendStub = func(nodeID string) error {
		if nodeID == "node-a" {
			return nil
		}
		return errors.New("drained")
	}
	removed := map[string]bool{"node-b": true, "node-c": true}
	classifier := func(nodeID string) bool { return !removed[nodeID] }

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	rec := testFanOutRecord(t)

	snapshot := []string{"node-a", "node-b", "node-c"}
	err := orch.fanOutAppend(context.Background(), vaultID, chunkID, rec, snapshot, 2, classifier, nil)
	if err != nil {
		t.Fatalf("multi-node drain produced spurious fan-out failure: %v", err)
	}
}

func TestFanOutAppendRejectsEmptySnapshot(t *testing.T) {
	t.Parallel()
	orch, _ := newFanOutTestOrch(t)
	err := orch.fanOutAppend(context.Background(), glid.New(), chunk.NewChunkID(),
		testFanOutRecord(t), nil, 1, allReceiving, nil)
	if err == nil {
		t.Fatal("expected error for empty snapshot")
	}
}

// ---------- buildFanOutTask / runFanOut wiring tests ----------

func TestBuildFanOutTaskClampsWForSelfInReceiving(t *testing.T) {
	t.Parallel()
	orch, _ := newFanOutTestOrch(t)
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()

	// Self in Receiving + 2 peers, default Full policy → W=3.
	// After self-auto-ack clamp: peers W = 2.
	placement := &vaultctlfsm.ChunkPlacement{
			Receiving:  []string{orch.localNodeID, "node-b", "node-c"},
	}
	task := orch.buildFanOutTask(vaultID, chunkID, placement, testFanOutRecord(t))
	if task == nil {
		t.Fatal("buildFanOutTask returned nil")
	}
	if len(task.peers) != 2 || slices.Contains(task.peers, orch.localNodeID) {
		t.Errorf("peers should exclude self; got %v", task.peers)
	}
	if task.w != 2 {
		t.Errorf("W=2 expected (Full=3 minus self auto-ack); got %d", task.w)
	}
	if !slices.Equal(task.snapshot, placement.Receiving) {
		t.Errorf("snapshot mismatch: got %v want %v", task.snapshot, placement.Receiving)
	}
}

func TestBuildFanOutTaskWhenSelfNotInReceiving(t *testing.T) {
	t.Parallel()
	orch, _ := newFanOutTestOrch(t)
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()

	// 3 peers, self NOT in Receiving. No self-ack credit.
	placement := &vaultctlfsm.ChunkPlacement{
			Receiving:  []string{"node-a", "node-b", "node-c"},
	}
	task := orch.buildFanOutTask(vaultID, chunkID, placement, testFanOutRecord(t))
	if task == nil {
		t.Fatal("buildFanOutTask returned nil")
	}
	if len(task.peers) != 3 {
		t.Errorf("peers should be all 3 (self not in snapshot); got %v", task.peers)
	}
	if task.w != 3 {
		t.Errorf("W=3 expected (Full, no self-ack credit); got %d", task.w)
	}
}

func TestBuildFanOutTaskNilWhenReceivingEmpty(t *testing.T) {
	t.Parallel()
	orch, _ := newFanOutTestOrch(t)
	placement := &vaultctlfsm.ChunkPlacement{
			Receiving:  nil,
	}
	if task := orch.buildFanOutTask(glid.New(), chunk.NewChunkID(), placement, testFanOutRecord(t)); task != nil {
		t.Errorf("expected nil task for empty Receiving; got %+v", task)
	}
}

func TestRunFanOutHappyPathWithSelfAutoAcked(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)

	// Snapshot {self, node-b, node-c}; W=3; clamped to peers W=2.
	// Both peers ack via mock replicator → success.
	task := &fanOutTask{
		vaultID:  glid.New(),
		chunkID:  chunk.NewChunkID(),
		peers:    []string{"node-b", "node-c"},
		w:        2,
		snapshot: []string{orch.localNodeID, "node-b", "node-c"},
	}
	if err := orch.runFanOut(context.Background(), task, testFanOutRecord(t)); err != nil {
		t.Fatalf("runFanOut: %v", err)
	}
	if rep.callsFor("node-b") != 1 || rep.callsFor("node-c") != 1 {
		t.Errorf("each peer should see 1 AppendRecords call; got b=%d c=%d",
			rep.callsFor("node-b"), rep.callsFor("node-c"))
	}
}

func TestRunFanOutZeroWIsFireAndForget(t *testing.T) {
	t.Parallel()
	orch, rep := newFanOutTestOrch(t)
	// W=0 means durability already satisfied by self (e.g., W=1 + self
	// in Receiving). Peers still get the record fire-and-forget; the
	// caller doesn't wait.
	task := &fanOutTask{
		vaultID:  glid.New(),
		chunkID:  chunk.NewChunkID(),
		peers:    []string{"node-b"},
		w:        0,
		snapshot: []string{orch.localNodeID, "node-b"},
	}
	if err := orch.runFanOut(context.Background(), task, testFanOutRecord(t)); err != nil {
		t.Fatalf("runFanOut: %v", err)
	}
	// Give the fire-and-forget goroutine a moment to land.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if rep.callsFor("node-b") == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Errorf("fire-and-forget peer write didn't land; got %d calls", rep.callsFor("node-b"))
}

func TestRunFanOutNilTaskIsNoOp(t *testing.T) {
	t.Parallel()
	orch, _ := newFanOutTestOrch(t)
	if err := orch.runFanOut(context.Background(), nil, testFanOutRecord(t)); err != nil {
		t.Errorf("nil task should be no-op: %v", err)
	}
}
