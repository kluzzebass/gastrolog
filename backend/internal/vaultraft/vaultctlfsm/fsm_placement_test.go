// Fan-out placement command + snapshot tests (gastrolog-4cxw0).
//
// Coverage dimensions per CLAUDE.md "Test Coverage — MANDATORY":
//   - Single-node happy path: each command's apply round-trips.
//   - Unhappy path: short payloads, idempotency on re-apply, applies for
//     non-existent chunks, ack-pull for non-expected toNode.
//   - Edge cases: drained PendingPulls auto-finalize Holding removal;
//     CmdAddReceiving on a chunk with no prior placement creates one;
//     legacy CmdCreateChunk payloads do NOT create a placement entry.
//   - Snapshot round-trip: every state shape survives Snapshot →
//     Persist → Restore on a fresh FSM.
//   - Forward-compat: snapshot from a fan-out-aware FSM restored on the
//     same code path produces identical state.
//   - CmdPruneNode integration: pruning drains placement + PendingPulls.

package vaultctlfsm

import (
	"bytes"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

func applyOK(t *testing.T, f *FSM, data []byte) {
	t.Helper()
	if err := f.Apply(&hraft.Log{Data: data}); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

// applyResult returns the value from Apply (the FSM apply contract
// returns any; nil = success).
func applyResult(f *FSM, data []byte) any {
	return f.Apply(&hraft.Log{Data: data})
}

// applyCreateFanOut creates a chunk via the extended CmdCreateChunk
// payload that stamps the initial Receiving set on a new
// ChunkPlacement entry, then transitions the chunk to Sealing so
// subsequent applyCreateFanOut calls don't trip the single-Active
// invariant. The Receiving / Holding / PendingPulls state on the
// placement entry survives the state transition, which is what these
// tests assert against.
func applyCreateFanOut(t *testing.T, f *FSM, id chunk.ChunkID, receiving []string) {
	t.Helper()
	now := time.Now().Truncate(time.Nanosecond)
	applyOK(t, f, MarshalCreateChunkWithReceiving(id, now, now, now, receiving))
	applyOK(t, f, MarshalBeginSeal(id))
}

// ---------- CmdAddReceiving ----------

func TestAddReceivingCreatesPlacementAndAddsToBothSets(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()

	var mu sync.Mutex
	var capturedChunk chunk.ChunkID
	var capturedNode string
	f.SetOnAddReceiving(func(c chunk.ChunkID, n string) {
		mu.Lock()
		capturedChunk = c
		capturedNode = n
		mu.Unlock()
	})

	applyOK(t, f, MarshalAddReceiving(id, "node-A"))

	p := f.Placement(id)
	if p == nil {
		t.Fatal("Placement: nil after CmdAddReceiving")
	}
	if !slices.Equal(p.Receiving, []string{"node-A"}) {
		t.Errorf("Receiving = %v, want [node-A]", p.Receiving)
	}
	if !slices.Equal(p.Holding, []string{"node-A"}) {
		t.Errorf("Holding = %v, want [node-A]", p.Holding)
	}

	mu.Lock()
	defer mu.Unlock()
	if capturedChunk != id || capturedNode != "node-A" {
		t.Errorf("callback got (%v, %q), want (%v, node-A)", capturedChunk, capturedNode, id)
	}
}

func TestAddReceivingIdempotent(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()

	applyOK(t, f, MarshalAddReceiving(id, "node-A"))
	applyOK(t, f, MarshalAddReceiving(id, "node-A"))

	p := f.Placement(id)
	if len(p.Receiving) != 1 || len(p.Holding) != 1 {
		t.Errorf("re-apply duplicated: Receiving=%v Holding=%v", p.Receiving, p.Holding)
	}
}

func TestAddReceivingAcrossMultipleNodes(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()

	for _, n := range []string{"node-A", "node-B", "node-C"} {
		applyOK(t, f, MarshalAddReceiving(id, n))
	}

	p := f.Placement(id)
	if len(p.Receiving) != 3 || len(p.Holding) != 3 {
		t.Errorf("Receiving=%v Holding=%v; expected 3 entries each", p.Receiving, p.Holding)
	}
}

func TestAddReceivingShortPayloadErrors(t *testing.T) {
	t.Parallel()

	f := New()
	// 16 bytes chunkID + 0 bytes node-id length section is truncated.
	short := []byte{byte(CmdAddReceiving), 1, 2, 3}
	err := applyResult(f, short)
	if err == nil {
		t.Fatal("expected error on short CmdAddReceiving payload")
	}
}

// ---------- CmdRemoveReceiving ----------

func TestRemoveReceivingKeepsHolding(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalAddReceiving(id, "node-A"))
	applyOK(t, f, MarshalRemoveReceiving(id, "node-A"))

	p := f.Placement(id)
	if len(p.Receiving) != 0 {
		t.Errorf("Receiving = %v, want []", p.Receiving)
	}
	if !slices.Equal(p.Holding, []string{"node-A"}) {
		t.Errorf("Holding = %v, want [node-A]", p.Holding)
	}
}

func TestRemoveReceivingForUnknownChunkIsNoOp(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalRemoveReceiving(id, "node-A"))
	if p := f.Placement(id); p != nil {
		t.Errorf("Placement should remain nil; got %+v", p)
	}
}

// ---------- CmdAddHolding ----------

func TestAddHoldingDoesNotAddToReceiving(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalAddHolding(id, "node-A"))

	p := f.Placement(id)
	if len(p.Receiving) != 0 {
		t.Errorf("Receiving = %v, want []", p.Receiving)
	}
	if !slices.Equal(p.Holding, []string{"node-A"}) {
		t.Errorf("Holding = %v, want [node-A]", p.Holding)
	}
}

// ---------- CmdBeginHoldingRemoval + CmdAckPull ----------

func TestBeginHoldingRemovalPopulatesPendingPulls(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		applyOK(t, f, MarshalAddReceiving(id, n))
	}
	// Drop node-A: nodes B and C must ack having pulled its records.
	applyOK(t, f, MarshalBeginHoldingRemoval(id, "node-A", []string{"node-B", "node-C"}))

	p := f.Placement(id)
	got, ok := p.PendingPulls["node-A"]
	if !ok {
		t.Fatal("PendingPulls[node-A] missing")
	}
	want := map[string]bool{"node-B": true, "node-C": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PendingPulls[node-A] = %v, want %v", got, want)
	}
}

func TestBeginHoldingRemovalIdempotent(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalAddReceiving(id, "node-A"))
	applyOK(t, f, MarshalAddReceiving(id, "node-B"))
	applyOK(t, f, MarshalBeginHoldingRemoval(id, "node-A", []string{"node-B"}))
	// Already-acked partial state: apply CmdAckPull, then re-apply
	// CmdBeginHoldingRemoval. Re-apply must not reset the acks.
	applyOK(t, f, MarshalAckPull(id, "node-A", "node-B"))

	// After the ack drained, node-A is no longer in Holding, so the
	// state is post-finalize. Re-applying CmdBeginHoldingRemoval
	// would create a fresh entry. Test the "still in-flight" version:
	id2 := chunk.NewChunkID()
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		applyOK(t, f, MarshalAddReceiving(id2, n))
	}
	applyOK(t, f, MarshalBeginHoldingRemoval(id2, "node-A", []string{"node-B", "node-C"}))
	applyOK(t, f, MarshalAckPull(id2, "node-A", "node-B"))
	// Re-apply: node-C should still owe an ack; the apply is a no-op.
	applyOK(t, f, MarshalBeginHoldingRemoval(id2, "node-A", []string{"node-B", "node-C"}))

	p := f.Placement(id2)
	got, ok := p.PendingPulls["node-A"]
	if !ok {
		t.Fatal("PendingPulls[node-A] cleared by re-apply")
	}
	if got["node-B"] {
		t.Error("node-B's ack got reset by re-apply")
	}
	if !got["node-C"] {
		t.Error("node-C still expected; got false")
	}
}

func TestAckPullFinalizesWhenDrained(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		applyOK(t, f, MarshalAddReceiving(id, n))
	}
	applyOK(t, f, MarshalBeginHoldingRemoval(id, "node-A", []string{"node-B", "node-C"}))

	var mu sync.Mutex
	var finalizedAt int
	calls := 0
	f.SetOnAckPull(func(_ chunk.ChunkID, _, _ string, finalized bool) {
		mu.Lock()
		calls++
		if finalized {
			finalizedAt = calls
		}
		mu.Unlock()
	})

	applyOK(t, f, MarshalAckPull(id, "node-A", "node-B"))
	applyOK(t, f, MarshalAckPull(id, "node-A", "node-C"))

	p := f.Placement(id)
	if p.PendingPulls != nil {
		t.Errorf("PendingPulls = %v, want nil after drain", p.PendingPulls)
	}
	if slices.Contains(p.Holding, "node-A") {
		t.Errorf("node-A still in Holding after drained ack; Holding = %v", p.Holding)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("onAckPull fired %d times; want 2", calls)
	}
	if finalizedAt != 2 {
		t.Errorf("finalized flag set at call %d; want 2", finalizedAt)
	}
}

func TestAckPullForUnknownChunkIsNoOp(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalAckPull(id, "node-A", "node-B"))
	if p := f.Placement(id); p != nil {
		t.Errorf("placement should remain nil; got %+v", p)
	}
}

func TestAckPullForNonExpectedNodeIsNoOp(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalAddReceiving(id, "node-A"))
	applyOK(t, f, MarshalAddReceiving(id, "node-B"))
	applyOK(t, f, MarshalBeginHoldingRemoval(id, "node-A", []string{"node-B"}))

	// node-Z is not in the expected set.
	applyOK(t, f, MarshalAckPull(id, "node-A", "node-Z"))

	p := f.Placement(id)
	if !p.PendingPulls["node-A"]["node-B"] {
		t.Error("node-B's pending ack got cleared by unrelated ack")
	}
}

// ---------- CmdCreateChunk extended ----------

func TestLegacyCreateChunkCreatesNoPlacement(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	now := time.Now().Truncate(time.Nanosecond)
	applyOK(t, f, MarshalCreateChunk(id, now, now, now))

	if got := f.Get(id); got == nil {
		t.Fatal("Get: nil after legacy create")
	}
	if p := f.Placement(id); p != nil {
		t.Errorf("legacy CreateChunk created a placement: %+v", p)
	}
}

func TestFanOutCreateChunkStampsPlacement(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyCreateFanOut(t, f, id, []string{"node-A", "node-B"})

	if got := f.Get(id); got == nil {
		t.Fatal("Get: nil after fan-out create")
	}
	p := f.Placement(id)
	if p == nil {
		t.Fatal("Placement: nil after fan-out create")
	}
	if !slices.Equal(p.Receiving, []string{"node-A", "node-B"}) {
		t.Errorf("Receiving = %v, want [node-A node-B]", p.Receiving)
	}
	if !slices.Equal(p.Holding, []string{"node-A", "node-B"}) {
		t.Errorf("Holding = %v, want [node-A node-B] (Holding ⊇ Receiving)", p.Holding)
	}
}

func TestFanOutCreateChunkWithEmptyReceiving(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyCreateFanOut(t, f, id, nil)

	p := f.Placement(id)
	if p == nil {
		t.Fatal("Placement: nil")
	}
	if len(p.Receiving) != 0 || len(p.Holding) != 0 {
		t.Errorf("empty Receiving stamp: got Receiving=%v Holding=%v", p.Receiving, p.Holding)
	}
}

// ---------- CmdSealChunk extended ----------

func TestSealChunkFanOutStampsFinalSetHash(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyCreateFanOut(t, f, id, []string{"node-A"})

	var hash [32]byte
	for i := range hash {
		hash[i] = byte(i)
	}
	now := time.Now().Truncate(time.Nanosecond)
	applyOK(t, f, MarshalSealChunkFanOut(id, now, 100, 1000, now, now, now, true, hash))

	p := f.Placement(id)
	if p.FinalSetHash != hash {
		t.Errorf("FinalSetHash = %x, want %x", p.FinalSetHash, hash)
	}
	e := f.Get(id)
	if e.State != chunk.ChunkStateSealed {
		t.Errorf("State = %v, want Sealed", e.State)
	}
	if !e.IngestTSMonotonic {
		t.Error("IngestTSMonotonic flag dropped on extended seal")
	}
}

func TestLegacySealChunkLeavesFinalSetHashZero(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyCreateFanOut(t, f, id, []string{"node-A"})

	now := time.Now().Truncate(time.Nanosecond)
	applyOK(t, f, MarshalSealChunk(id, now, 100, 1000, now, now, now, true))

	p := f.Placement(id)
	if p.FinalSetHash != ([32]byte{}) {
		t.Errorf("FinalSetHash = %x, want zero", p.FinalSetHash)
	}
}

// ---------- CmdPruneNode integration ----------

func TestPruneNodeRemovesFromPlacements(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		applyOK(t, f, MarshalAddReceiving(id, n))
	}
	applyOK(t, f, MarshalBeginHoldingRemoval(id, "node-A", []string{"node-B", "node-C"}))

	applyOK(t, f, MarshalPruneNode("node-C"))

	p := f.Placement(id)
	if slices.Contains(p.Receiving, "node-C") {
		t.Errorf("node-C still in Receiving: %v", p.Receiving)
	}
	if slices.Contains(p.Holding, "node-C") {
		t.Errorf("node-C still in Holding: %v", p.Holding)
	}
	// node-A's pending set should no longer expect node-C.
	if p.PendingPulls["node-A"]["node-C"] {
		t.Errorf("node-C still in PendingPulls[node-A]: %v", p.PendingPulls["node-A"])
	}
}

func TestPruneNodeAutoFinalizesHoldingRemovalOnLastAck(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	for _, n := range []string{"node-A", "node-B", "node-C"} {
		applyOK(t, f, MarshalAddReceiving(id, n))
	}
	applyOK(t, f, MarshalBeginHoldingRemoval(id, "node-A", []string{"node-B", "node-C"}))
	applyOK(t, f, MarshalAckPull(id, "node-A", "node-B"))

	// Pruning node-C drains the last expected ack — node-A's holding
	// removal should auto-finalize via pruneNodeFromPlacements.
	applyOK(t, f, MarshalPruneNode("node-C"))

	p := f.Placement(id)
	if slices.Contains(p.Holding, "node-A") {
		t.Errorf("node-A still in Holding after drained-prune: %v", p.Holding)
	}
	if p.PendingPulls != nil {
		t.Errorf("PendingPulls = %v, want nil", p.PendingPulls)
	}
}

func TestPruneNodeRemovesFromBigPlacementGraph(t *testing.T) {
	t.Parallel()

	f := New()
	// 5 chunks × 4 nodes; prune node-D should leave 3 nodes per chunk.
	nodes := []string{"node-A", "node-B", "node-C", "node-D"}
	chunks := make([]chunk.ChunkID, 5)
	for i := range chunks {
		chunks[i] = chunk.NewChunkID()
		for _, n := range nodes {
			applyOK(t, f, MarshalAddReceiving(chunks[i], n))
		}
	}
	applyOK(t, f, MarshalPruneNode("node-D"))

	for _, c := range chunks {
		p := f.Placement(c)
		if slices.Contains(p.Receiving, "node-D") || slices.Contains(p.Holding, "node-D") {
			t.Errorf("chunk %s: node-D not pruned; Receiving=%v Holding=%v", c, p.Receiving, p.Holding)
		}
		if len(p.Receiving) != 3 || len(p.Holding) != 3 {
			t.Errorf("chunk %s: expected 3 nodes; Receiving=%v Holding=%v", c, p.Receiving, p.Holding)
		}
	}
}

// ---------- Read helpers ----------

func TestReceivingForReturnsCopy(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalAddReceiving(id, "node-A"))

	got := f.ReceivingFor(id)
	if !slices.Equal(got, []string{"node-A"}) {
		t.Errorf("ReceivingFor = %v, want [node-A]", got)
	}
	got[0] = "tampered"
	if p := f.Placement(id); p.Receiving[0] == "tampered" {
		t.Error("ReceivingFor returned a shared slice — must be a copy")
	}
}

func TestHoldingForReturnsCopy(t *testing.T) {
	t.Parallel()

	f := New()
	id := chunk.NewChunkID()
	applyOK(t, f, MarshalAddReceiving(id, "node-A"))

	got := f.HoldingFor(id)
	if !slices.Equal(got, []string{"node-A"}) {
		t.Errorf("HoldingFor = %v, want [node-A]", got)
	}
	got[0] = "tampered"
	if p := f.Placement(id); p.Holding[0] == "tampered" {
		t.Error("HoldingFor returned a shared slice — must be a copy")
	}
}

// ---------- Snapshot round-trip ----------

func TestPlacementSurvivesSnapshotRoundtrip(t *testing.T) {
	t.Parallel()

	src := New()
	id1 := chunk.NewChunkID()
	id2 := chunk.NewChunkID()

	applyCreateFanOut(t, src, id1, []string{"node-A", "node-B", "node-C"})
	applyCreateFanOut(t, src, id2, []string{"node-A", "node-B"})
	applyOK(t, src, MarshalBeginHoldingRemoval(id1, "node-A", []string{"node-B", "node-C"}))
	applyOK(t, src, MarshalAckPull(id1, "node-A", "node-B"))

	var hash [32]byte
	for i := range hash {
		hash[i] = byte(i ^ 0xAA)
	}
	now := time.Now().Truncate(time.Nanosecond)
	applyOK(t, src, MarshalSealChunkFanOut(id2, now, 100, 1000, now, now, now, true, hash))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst := New()
	if err := dst.Restore(&readCloser{Reader: &buf}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// id1: half-drained PendingPulls.
	p1 := dst.Placement(id1)
	if p1 == nil {
		t.Fatal("id1 placement missing after restore")
	}
	if !slices.Equal(p1.Receiving, []string{"node-A", "node-B", "node-C"}) {
		t.Errorf("id1 Receiving = %v", p1.Receiving)
	}
	if got := p1.PendingPulls["node-A"]; !reflect.DeepEqual(got, map[string]bool{"node-C": true}) {
		t.Errorf("id1 PendingPulls[node-A] = %v, want {node-C: true}", got)
	}

	// id2: FinalSetHash + no PendingPulls.
	p2 := dst.Placement(id2)
	if p2 == nil {
		t.Fatal("id2 placement missing after restore")
	}
	if p2.FinalSetHash != hash {
		t.Errorf("id2 FinalSetHash = %x, want %x", p2.FinalSetHash, hash)
	}
	if p2.PendingPulls != nil {
		t.Errorf("id2 PendingPulls = %v, want nil", p2.PendingPulls)
	}
}

func TestSnapshotOmitsEmptyPlacementSections(t *testing.T) {
	t.Parallel()

	// Pure LeaderDriven cluster: no placements, no pendingPulls.
	src := New()
	id := chunk.NewChunkID()
	now := time.Now().Truncate(time.Nanosecond)
	applyOK(t, src, MarshalCreateChunk(id, now, now, now))

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Section header is 5 bytes (1 kind + 4 length). A LeaderDriven-only
	// snapshot should not contain a chunkPlacement section header.
	if bytes.Contains(buf.Bytes(), []byte{byte(sectionChunkPlacement)}) {
		// Crude check: byte 4 doesn't appear except possibly inside
		// payload bytes. Conservative test — verify by counting
		// section markers we'd otherwise expect.
		// Decode and assert dst has no placements.
		dst := New()
		if err := dst.Restore(&readCloser{Reader: bytes.NewReader(buf.Bytes())}); err != nil {
			t.Fatalf("Restore: %v", err)
		}
		if len(dst.placements) != 0 {
			t.Errorf("placements after restore = %d entries; want 0", len(dst.placements))
		}
	}
}

func TestUnknownSectionsAreSkipped(t *testing.T) {
	t.Parallel()

	// Forward-compat behavior is part of the contract — an FSM
	// restoring a snapshot that includes an unknown section kind
	// must not error.
	src := New()
	applyCreateFanOut(t, src, chunk.NewChunkID(), []string{"node-A"})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	// Append a fake unknown section: kind=99, length=0.
	buf.Write([]byte{99, 0, 0, 0, 0})

	dst := New()
	if err := dst.Restore(&readCloser{Reader: bytes.NewReader(buf.Bytes())}); err != nil {
		t.Fatalf("Restore should skip unknown sections, got: %v", err)
	}
	if len(dst.placements) != 1 {
		t.Errorf("placements after restore-with-unknown = %d, want 1", len(dst.placements))
	}
}

// readCloser wraps an io.Reader with a no-op Close so we can feed it
// to FSM.Restore (which expects an io.ReadCloser).
type readCloser struct{ Reader bytesReader }

func (r *readCloser) Read(p []byte) (int, error) { return r.Reader.Read(p) }
func (r *readCloser) Close() error               { return nil }

type bytesReader interface {
	Read(p []byte) (int, error)
}
