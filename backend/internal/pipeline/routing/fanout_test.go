package routing

// Internal tests for the fan-out delivery helpers. The revoked-sink window
// (UnregisterVault landing between route()'s target snapshot and the deliver
// call) cannot be hit deterministically through Run, so these tests drive
// fanOut / fanOutJoined directly with a revoked middle sink.

import (
	"context"
	"errors"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

// threeTargets returns targets A, B, C where B's vault sink is already revoked
// (vault unregistered mid-flight), plus the receiving channels for A and C.
func threeTargets() (targets []sinkTarget, ids [3]glid.GLID, chA, chC chan segmentation.Input) {
	ids = [3]glid.GLID{glid.New(), glid.New(), glid.New()}
	chA = make(chan segmentation.Input, 1)
	chB := make(chan segmentation.Input, 1)
	chC = make(chan segmentation.Input, 1)

	sinkB := newVaultSink(chB)
	sinkB.revoke()

	targets = []sinkTarget{
		{vaultID: ids[0], sink: newVaultSink(chA)},
		{vaultID: ids[1], sink: sinkB},
		{vaultID: ids[2], sink: newVaultSink(chC)},
	}
	return targets, ids, chA, chC
}

func TestFanOutContinuesPastRevokedSink(t *testing.T) {
	t.Parallel()

	targets, ids, chA, chC := threeTargets()
	m := New(Config{})

	rec := &record.Record{Raw: []byte("line")}
	m.fanOut(context.Background(), rec, targets)

	if got := <-chA; got.Record != rec {
		t.Errorf("vault A record = %p, want %p", got.Record, rec)
	}
	select {
	case got := <-chC:
		if got.Record != rec {
			t.Errorf("vault C record = %p, want %p", got.Record, rec)
		}
	default:
		t.Fatal("vault C never received the record — fan-out aborted on the revoked middle sink")
	}

	drops := m.Stats().PerVaultDropped
	if drops[ids[1]] != 1 {
		t.Errorf("PerVaultDropped[B] = %d, want 1", drops[ids[1]])
	}
	if len(drops) != 1 {
		t.Errorf("PerVaultDropped = %v, want a drop for vault B only", drops)
	}
}

func TestFanOutContextCancelCountsRemainingAsDropped(t *testing.T) {
	t.Parallel()

	ids := [3]glid.GLID{glid.New(), glid.New(), glid.New()}
	// Unbuffered channels with no readers: the send is never ready, so deliver
	// deterministically takes the ctx.Done branch.
	targets := []sinkTarget{
		{vaultID: ids[0], sink: newVaultSink(make(chan segmentation.Input))},
		{vaultID: ids[1], sink: newVaultSink(make(chan segmentation.Input))},
		{vaultID: ids[2], sink: newVaultSink(make(chan segmentation.Input))},
	}
	m := New(Config{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.fanOut(ctx, &record.Record{}, targets)

	drops := m.Stats().PerVaultDropped
	for i, id := range ids {
		if drops[id] != 1 {
			t.Errorf("PerVaultDropped[%d] = %d, want 1 (ctx cancel drops every undelivered target)", i, drops[id])
		}
	}
}

func TestFanOutJoinedContinuesPastRevokedSink(t *testing.T) {
	t.Parallel()

	targets, ids, chA, chC := threeTargets()
	m := New(Config{})

	rec := &record.Record{Raw: []byte("line")}
	ack := make(chan error, 1)
	m.fanOutJoined(context.Background(), rec, targets, ack)

	gotA := <-chA
	var gotC segmentation.Input
	select {
	case gotC = <-chC:
	default:
		t.Fatal("vault C never received the record — joined fan-out aborted on the revoked middle sink")
	}
	if gotA.Record != rec || gotC.Record != rec {
		t.Errorf("records = %p, %p, want %p on both healthy vaults", gotA.Record, gotC.Record, rec)
	}

	// Both healthy vaults commit; the revoked middle sink's child was nacked by
	// deliver, so the join must carry errVaultUnwired to the source ack.
	gotA.Ack <- nil
	gotC.Ack <- nil
	if err := <-ack; !errors.Is(err, errVaultUnwired) {
		t.Errorf("source ack = %v, want errVaultUnwired", err)
	}

	if drops := m.Stats().PerVaultDropped; drops[ids[1]] != 1 || len(drops) != 1 {
		t.Errorf("PerVaultDropped = %v, want a drop for vault B only", drops)
	}
}

func TestFanOutJoinedContextCancelResolvesJoin(t *testing.T) {
	t.Parallel()

	ids := [2]glid.GLID{glid.New(), glid.New()}
	targets := []sinkTarget{
		{vaultID: ids[0], sink: newVaultSink(make(chan segmentation.Input))},
		{vaultID: ids[1], sink: newVaultSink(make(chan segmentation.Input))},
	}
	m := New(Config{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ack := make(chan error, 1)
	m.fanOutJoined(ctx, &record.Record{}, targets, ack)

	// The join collector must resolve (no undelivered child left unsent) with
	// the context error, and every undelivered target counts as dropped.
	if err := <-ack; !errors.Is(err, context.Canceled) {
		t.Errorf("source ack = %v, want context.Canceled", err)
	}
	drops := m.Stats().PerVaultDropped
	if drops[ids[0]] != 1 || drops[ids[1]] != 1 {
		t.Errorf("PerVaultDropped = %v, want 1 for each target", drops)
	}
}
