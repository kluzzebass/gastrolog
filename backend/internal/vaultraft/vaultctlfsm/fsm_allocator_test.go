package vaultctlfsm

import (
	"bytes"
	"errors"
	"io"
	"testing"

	hraft "github.com/hashicorp/raft"
)

func applySeqCmd(t *testing.T, fsm *FSM, wire []byte) any {
	t.Helper()
	result := fsm.Apply(&hraft.Log{Data: wire})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply: %v", err)
	}
	return result
}

func applySeqCmdExpectErr(t *testing.T, fsm *FSM, wire []byte, want error) {
	t.Helper()
	result := fsm.Apply(&hraft.Log{Data: wire})
	if !errors.Is(result.(error), want) {
		t.Fatalf("apply: got %v want %v", result, want)
	}
}

func TestSeqAllocatorReserveMonotonic(t *testing.T) {
	fsm := New()
	const holder = "node-1"
	const epoch = initialSeqEpoch

	wire1, err := MarshalReserveSeqRange(holder, epoch, 100)
	if err != nil {
		t.Fatal(err)
	}
	grant1 := applySeqCmd(t, fsm, wire1).(SeqLeaseGrant)
	if grant1.Start != 1 || grant1.End != 100 || grant1.Epoch != epoch {
		t.Fatalf("grant1: %+v", grant1)
	}

	burnWire, err := MarshalBurnSeqLeaseTail(holder, epoch, 100)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmd(t, fsm, burnWire)

	wire2, err := MarshalReserveSeqRange(holder, epoch, 50)
	if err != nil {
		t.Fatal(err)
	}
	grant2 := applySeqCmd(t, fsm, wire2).(SeqLeaseGrant)
	if grant2.Start != 101 || grant2.End != 150 {
		t.Fatalf("grant2: %+v", grant2)
	}

	st := fsm.SeqAllocatorState()
	if st.NextSeq != 151 {
		t.Fatalf("next_seq: got %d want 151", st.NextSeq)
	}
}

func TestSeqAllocatorRejectStaleEpoch(t *testing.T) {
	fsm := New()
	const holder = "node-1"

	applySeqCmd(t, fsm, MarshalBumpSeqAllocatorEpoch())

	wire, err := MarshalReserveSeqRange(holder, initialSeqEpoch, 10)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmdExpectErr(t, fsm, wire, ErrSeqAllocatorStaleEpoch)
}

func TestSeqAllocatorRejectSecondSwathSameHolder(t *testing.T) {
	fsm := New()
	const holder = "node-1"

	wire, err := MarshalReserveSeqRange(holder, initialSeqEpoch, 10)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmd(t, fsm, wire)

	wire2, err := MarshalReserveSeqRange(holder, initialSeqEpoch, 5)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmdExpectErr(t, fsm, wire2, ErrSeqAllocatorActiveLease)
}

func TestSeqAllocatorBurnTailRecordsGap(t *testing.T) {
	fsm := New()
	const holder = "node-1"
	const epoch = initialSeqEpoch

	reserve, err := MarshalReserveSeqRange(holder, epoch, 10)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmd(t, fsm, reserve)

	burn, err := MarshalBurnSeqLeaseTail(holder, epoch, 7)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmd(t, fsm, burn)

	st := fsm.SeqAllocatorState()
	if len(st.BurnedTails) != 1 {
		t.Fatalf("burned tails: %+v", st.BurnedTails)
	}
	tail := st.BurnedTails[0]
	if tail.Start != 8 || tail.End != 10 || tail.Epoch != epoch {
		t.Fatalf("tail: %+v", tail)
	}
	if st.ActiveSwaths != nil && len(st.ActiveSwaths) != 0 {
		t.Fatal("expected active swaths cleared")
	}
}

func TestSeqAllocatorBumpEpochBurnsActiveLease(t *testing.T) {
	fsm := New()
	const holder = "node-1"
	const epoch = initialSeqEpoch

	reserve, err := MarshalReserveSeqRange(holder, epoch, 5)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmd(t, fsm, reserve)

	newEpoch := applySeqCmd(t, fsm, MarshalBumpSeqAllocatorEpoch()).(uint64)
	if newEpoch != epoch+1 {
		t.Fatalf("epoch: got %d want %d", newEpoch, epoch+1)
	}

	st := fsm.SeqAllocatorState()
	if st.Epoch != epoch+1 {
		t.Fatalf("state epoch: %d", st.Epoch)
	}
	if len(st.BurnedTails) != 1 || st.BurnedTails[0].Start != 1 || st.BurnedTails[0].End != 5 {
		t.Fatalf("burned: %+v", st.BurnedTails)
	}

	wire, err := MarshalReserveSeqRange(holder, epoch, 3)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmdExpectErr(t, fsm, wire, ErrSeqAllocatorStaleEpoch)

	wireOK, err := MarshalReserveSeqRange(holder, epoch+1, 3)
	if err != nil {
		t.Fatal(err)
	}
	grant := applySeqCmd(t, fsm, wireOK).(SeqLeaseGrant)
	if grant.Start != 6 || grant.End != 8 {
		t.Fatalf("grant after bump: %+v", grant)
	}
}

func TestSeqAllocatorConcurrentHolders(t *testing.T) {
	fsm := New()
	const epoch = initialSeqEpoch

	grantA := applySeqCmd(t, fsm, mustMarshalReserve(t, "node-a", epoch, 10)).(SeqLeaseGrant)
	grantB := applySeqCmd(t, fsm, mustMarshalReserve(t, "node-b", epoch, 10)).(SeqLeaseGrant)
	if grantA.Start != 1 || grantA.End != 10 {
		t.Fatalf("grantA: %+v", grantA)
	}
	if grantB.Start != 11 || grantB.End != 20 {
		t.Fatalf("grantB: %+v", grantB)
	}

	st := fsm.SeqAllocatorState()
	if len(st.ActiveSwaths) != 2 {
		t.Fatalf("active swaths: %+v", st.ActiveSwaths)
	}
	if st.NextSeq != 21 {
		t.Fatalf("next_seq: got %d want 21", st.NextSeq)
	}
}

func TestSeqAllocatorSnapshotRoundtrip(t *testing.T) {
	fsm := New()
	const holder = "node-1"
	const epoch = initialSeqEpoch

	reserve, err := MarshalReserveSeqRange(holder, epoch, 4)
	if err != nil {
		t.Fatal(err)
	}
	applySeqCmd(t, fsm, reserve)
	applySeqCmd(t, fsm, MarshalBumpSeqAllocatorEpoch())

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

	got := fsm2.SeqAllocatorState()
	want := fsm.SeqAllocatorState()
	if got.NextSeq != want.NextSeq || got.Epoch != want.Epoch {
		t.Fatalf("state mismatch: got %+v want %+v", got, want)
	}
	if len(got.BurnedTails) != len(want.BurnedTails) {
		t.Fatalf("tails: got %+v want %+v", got.BurnedTails, want.BurnedTails)
	}
}

func TestSeqAllocatorReplayDeterministic(t *testing.T) {
	replay := func() SeqAllocatorSnapshot {
		fsm := New()
		const holder = "node-1"
		applySeqCmd(t, fsm, mustMarshalReserve(t, holder, 1, 10))
		applySeqCmd(t, fsm, mustMarshalBurn(t, holder, 1, 10))
		applySeqCmd(t, fsm, mustMarshalReserve(t, holder, 1, 5))
		applySeqCmd(t, fsm, mustMarshalBurn(t, holder, 1, 13))
		applySeqCmd(t, fsm, MarshalBumpSeqAllocatorEpoch())
		applySeqCmd(t, fsm, mustMarshalReserve(t, holder, 2, 2))
		return fsm.SeqAllocatorState()
	}

	a := replay()
	b := replay()
	if a.NextSeq != b.NextSeq || a.Epoch != b.Epoch || len(a.BurnedTails) != len(b.BurnedTails) {
		t.Fatalf("replay diverged: a=%+v b=%+v", a, b)
	}
}

func mustMarshalReserve(t *testing.T, holder string, epoch, count uint64) []byte {
	t.Helper()
	wire, err := MarshalReserveSeqRange(holder, epoch, count)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func mustMarshalBurn(t *testing.T, holder string, epoch, consumed uint64) []byte {
	t.Helper()
	wire, err := MarshalBurnSeqLeaseTail(holder, epoch, consumed)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
