package vaultctlfsm

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

func applyFenceCmd(t *testing.T, fsm *FSM, wire []byte) any {
	t.Helper()
	return fsm.Apply(&hraft.Log{Data: wire})
}

func applyFenceCmdExpectErr(t *testing.T, fsm *FSM, wire []byte, want error) {
	t.Helper()
	got := fsm.Apply(&hraft.Log{Data: wire})
	if !errors.Is(got.(error), want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFencePublishMonotonic(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 100).UTC()

	rec1 := applyFenceCmd(t, fsm, MarshalPublishFence(100, now)).(*FenceRecord)
	if rec1.ID != 1 || rec1.UpperBoundSeq != 100 || rec1.PrevBoundSeq != 0 {
		t.Fatalf("fence1 = %+v", rec1)
	}

	rec2 := applyFenceCmd(t, fsm, MarshalPublishFence(250, now.Add(time.Second))).(*FenceRecord)
	if rec2.ID != 2 || rec2.UpperBoundSeq != 250 || rec2.PrevBoundSeq != 100 {
		t.Fatalf("fence2 = %+v", rec2)
	}

	st := fsm.FenceState()
	if len(st.Records) != 2 || fsm.LatestFenceUpperBound() != 250 {
		t.Fatalf("state = %+v latest=%d", st, fsm.LatestFenceUpperBound())
	}
}

func TestFenceRejectRegression(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Now().UTC()
	applyFenceCmd(t, fsm, MarshalPublishFence(50, now))
	applyFenceCmdExpectErr(t, fsm, MarshalPublishFence(50, now), ErrFenceRegression)
	applyFenceCmdExpectErr(t, fsm, MarshalPublishFence(10, now), ErrFenceRegression)
}

func TestFenceRejectZeroSeq(t *testing.T) {
	t.Parallel()
	fsm := New()
	applyFenceCmdExpectErr(t, fsm, MarshalPublishFence(0, time.Now().UTC()), ErrFenceInvalidSeq)
}

func TestFenceSnapshotRoundtrip(t *testing.T) {
	t.Parallel()
	fsm := New()
	now := time.Unix(0, 42).UTC()
	applyFenceCmd(t, fsm, MarshalPublishFence(10, now))
	applyFenceCmd(t, fsm, MarshalPublishFence(20, now.Add(time.Second)))

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}

	fsm2 := New()
	if err := fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	got := fsm2.FenceState()
	want := fsm.FenceState()
	if len(got.Records) != len(want.Records) {
		t.Fatalf("records = %d, want %d", len(got.Records), len(want.Records))
	}
	for i := range want.Records {
		if got.Records[i] != want.Records[i] {
			t.Fatalf("record[%d]: got %+v want %+v", i, got.Records[i], want.Records[i])
		}
	}
}

func TestFenceReplayDeterministic(t *testing.T) {
	t.Parallel()
	replay := func() FenceSnapshot {
		fsm := New()
		now := time.Unix(0, 1).UTC()
		applyFenceCmd(t, fsm, MarshalPublishFence(5, now))
		applyFenceCmd(t, fsm, MarshalPublishFence(15, now.Add(time.Second)))
		return fsm.FenceState()
	}
	a := replay()
	b := replay()
	if len(a.Records) != len(b.Records) {
		t.Fatal("replay length mismatch")
	}
	for i := range a.Records {
		if a.Records[i] != b.Records[i] {
			t.Fatalf("record[%d] diverged: %+v vs %+v", i, a.Records[i], b.Records[i])
		}
	}
}
