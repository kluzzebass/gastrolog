// Tests for the FSM-mediated rotation coordinator (gastrolog-3yre7).

package orchestrator

import (
	"errors"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// captureApplier records every payload Apply receives and forwards the
// call through to an underlying FSM via hraft.Log. Mirrors how the
// production applier wraps proposals — the FSM apply path is the
// source of truth for ErrActiveChunkExists, so plumbing the test
// through the real FSM exercises the race-loss path end-to-end.
type captureApplier struct {
	fsm   *vaultctlfsm.FSM
	calls []byte // catenated wire bytes — tests check count, not contents
	count int
}

func (a *captureApplier) Apply(data []byte) error {
	a.count++
	a.calls = append(a.calls, data...)
	r := a.fsm.Apply(&hraft.Log{Data: data})
	if err, ok := r.(error); ok && err != nil {
		return err
	}
	return nil
}

func TestRotationCoordinator_FirstProposerWins(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	applier := &captureApplier{fsm: fsm}

	// Seed an Active chunk so BeginRotation has something to seal.
	oldID := chunk.NewChunkID()
	now := time.Now()
	if err := applier.Apply(vaultctlfsm.MarshalCreateChunk(oldID, now, now, now)); err != nil {
		t.Fatalf("seed Active: %v", err)
	}

	coord := newRotationCoordinator(glid.New(), applier, fsm, func() time.Time { return now }, []string{"node-A", "node-B"})

	newID, err := coord.BeginRotation(oldID)
	if err != nil {
		t.Fatalf("BeginRotation: %v", err)
	}
	if newID == (chunk.ChunkID{}) {
		t.Fatal("BeginRotation returned zero chunk ID")
	}
	if newID == oldID {
		t.Errorf("BeginRotation returned the old ID — expected a fresh ID")
	}

	// Two applies expected: BeginSeal(old) + CreateChunk(new).
	if applier.count != 3 { // seed + begin-seal + create
		t.Errorf("applier call count = %d, want 3 (seed + begin-seal + create)", applier.count)
	}

	// FSM should now have oldID in Sealing and newID as Active.
	entries := fsm.List()
	got := map[chunk.ChunkID]chunk.ChunkState{}
	for _, e := range entries {
		got[e.ID] = e.State
	}
	if got[oldID] != chunk.ChunkStateSealing {
		t.Errorf("old chunk state = %s, want Sealing", got[oldID])
	}
	if got[newID] != chunk.ChunkStateActive {
		t.Errorf("new chunk state = %s, want Active", got[newID])
	}
}

func TestRotationCoordinator_LoserAlignsToWinnerID(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	applier := &captureApplier{fsm: fsm}

	// Seed an Active chunk.
	oldID := chunk.NewChunkID()
	now := time.Now()
	if err := applier.Apply(vaultctlfsm.MarshalCreateChunk(oldID, now, now, now)); err != nil {
		t.Fatalf("seed Active: %v", err)
	}

	// Simulate the winning replica's rotation having already applied:
	// BeginSeal(old) + Create(winnerID). The FSM now holds winnerID
	// as Active.
	winnerID := chunk.NewChunkID()
	if err := applier.Apply(vaultctlfsm.MarshalBeginSeal(oldID)); err != nil {
		t.Fatalf("winner begin-seal: %v", err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalCreateChunk(winnerID, now, now, now)); err != nil {
		t.Fatalf("winner create: %v", err)
	}

	// Now the losing replica fires BeginRotation. Its own CreateChunk
	// will be rejected by the single-Active invariant; the coordinator
	// must read the FSM and return winnerID so the loser's chunk
	// manager opens a chunk that matches every other replica.
	coord := newRotationCoordinator(glid.New(), applier, fsm, func() time.Time { return now }, []string{"node-A", "node-B"})

	loserResult, err := coord.BeginRotation(oldID)
	if err != nil {
		t.Fatalf("BeginRotation (loser): %v", err)
	}
	if loserResult != winnerID {
		t.Errorf("loser BeginRotation returned %s, want winnerID %s — race-loss alignment broken", loserResult, winnerID)
	}

	// The losing CreateChunk should have hit the FSM and been rejected
	// — captured as an Apply call that returned ErrActiveChunkExists
	// internally. The coordinator absorbs that error rather than
	// surfacing it.
}

func TestRotationCoordinator_NoOldChunkSkipsBeginSeal(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	applier := &captureApplier{fsm: fsm}

	// Empty FSM — no chunk to BeginSeal.
	coord := newRotationCoordinator(glid.New(), applier, fsm, func() time.Time { return time.Now() }, nil)

	newID, err := coord.BeginRotation(chunk.ChunkID{})
	if err != nil {
		t.Fatalf("BeginRotation (no old): %v", err)
	}
	if newID == (chunk.ChunkID{}) {
		t.Fatal("BeginRotation returned zero ID")
	}

	// Exactly one apply expected: CreateChunk for the fresh ID.
	if applier.count != 1 {
		t.Errorf("applier call count = %d, want 1 (CreateChunk only — no BeginSeal when oldID is zero)", applier.count)
	}
}

func TestRotationCoordinator_PropagatesCreateError(t *testing.T) {
	t.Parallel()

	// Applier that fails on Apply with an error that isn't
	// ErrActiveChunkExists — the coordinator must surface it rather
	// than fall back to FSM.List().
	fsm := vaultctlfsm.New()
	failing := &alwaysFailApplier{err: errors.New("raft commit timeout")}

	coord := newRotationCoordinator(glid.New(), failing, fsm, func() time.Time { return time.Now() }, nil)

	_, err := coord.BeginRotation(chunk.ChunkID{})
	if err == nil {
		t.Fatal("expected error from failing applier")
	}
	if !errors.Is(err, failing.err) {
		t.Errorf("error chain = %v, want raft commit timeout", err)
	}
}

type alwaysFailApplier struct{ err error }

func (a *alwaysFailApplier) Apply(_ []byte) error { return a.err }

// TestRotationCoordinator_SetReceivingRefreshesNextCreatePayload verifies the
// fix for gastrolog-2oav7: a coordinator built with a stale (incomplete)
// receiving snapshot must pick up the refreshed list on the next
// BeginRotation. Pre-fix, c.receiving was set once at wireRotationCoordinator
// and never updated, so a node missing from the initial snapshot would be
// permanently absent from every subsequent CmdCreateChunk's placement.Holding.
func TestRotationCoordinator_SetReceivingRefreshesNextCreatePayload(t *testing.T) {
	t.Parallel()

	fsm := vaultctlfsm.New()
	applier := &captureApplier{fsm: fsm}
	now := time.Now()

	// Seed an Active chunk so BeginRotation has something to seal.
	seedID := chunk.NewChunkID()
	if err := applier.Apply(vaultctlfsm.MarshalCreateChunkWithReceiving(
		seedID, now, now, now, []string{"node-A", "node-B"})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Stale snapshot: two of the three placement nodes (node-C is
	// missing — simulating an NSC that hadn't replicated yet at
	// instance-build time).
	coord := newRotationCoordinator(glid.New(), applier, fsm,
		func() time.Time { return now }, []string{"node-A", "node-B"})

	firstID, err := coord.BeginRotation(seedID)
	if err != nil {
		t.Fatalf("first BeginRotation: %v", err)
	}
	if p := fsm.Placement(firstID); p == nil {
		t.Fatal("first chunk has no placement entry")
	} else if len(p.Receiving) != 2 {
		t.Errorf("first chunk Receiving = %v, want 2 nodes (stale snapshot reproduces bug)", p.Receiving)
	}

	// Operator/sweep refreshes the snapshot — node-C joins the placement.
	coord.SetReceiving([]string{"node-A", "node-B", "node-C"})

	secondID, err := coord.BeginRotation(firstID)
	if err != nil {
		t.Fatalf("second BeginRotation: %v", err)
	}
	p := fsm.Placement(secondID)
	if p == nil {
		t.Fatal("second chunk has no placement entry")
	}
	if len(p.Receiving) != 3 {
		t.Errorf("second chunk Receiving = %v, want 3 nodes (refresh didn't take effect)", p.Receiving)
	}
	if len(p.Holding) != 3 {
		t.Errorf("second chunk Holding = %v, want 3 nodes (Holding should mirror Receiving at create time)", p.Holding)
	}
	wantSet := map[string]bool{"node-A": true, "node-B": true, "node-C": true}
	for _, n := range p.Receiving {
		if !wantSet[n] {
			t.Errorf("unexpected node %q in Receiving", n)
		}
		delete(wantSet, n)
	}
	if len(wantSet) > 0 {
		t.Errorf("missing nodes after refresh: %v", wantSet)
	}
}
