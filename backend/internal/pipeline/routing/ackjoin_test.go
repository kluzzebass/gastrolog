package routing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

// runRouter starts a routing manager over a single buffered input and returns once
// the input is drained; the caller closes the input.
func runRouter(t *testing.T, mgr *routing.Manager, in <-chan routing.Input) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Run(ctx, in) }()
}

// A fan-out source ack resolves only after every matched vault has committed.
func TestRouteAckJoinWaitsForAllVaults(t *testing.T) {
	t.Parallel()

	vaultA := glid.New()
	vaultB := glid.New()
	chA := make(chan segmentation.Input, 1)
	chB := make(chan segmentation.Input, 1)

	mgr := routing.New(routing.Config{
		Workers: 2,
		Table:   catchAllTable(vaultA, vaultB),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultA: chA, vaultB: chB},
	})

	parent := make(chan error, 1)
	in := make(chan routing.Input, 1)
	inp := routing.IngestInput(&record.Record{Attrs: record.Attributes{"env": "prod"}})
	inp.Ack = parent
	in <- inp
	close(in)
	runRouter(t, mgr, in)

	gotA := <-chA
	gotB := <-chB
	if gotA.Ack == nil || gotB.Ack == nil {
		t.Fatal("each fan-out target must carry a child ack")
	}

	gotA.Ack <- nil
	select {
	case <-parent:
		t.Fatal("source ack fired before all vaults committed")
	case <-time.After(50 * time.Millisecond):
	}

	gotB.Ack <- nil
	select {
	case err := <-parent:
		if err != nil {
			t.Fatalf("source ack error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source ack did not fire after all vaults committed")
	}
}

// In a fan-out, the first per-vault error wins and propagates to the source ack.
func TestRouteAckJoinFirstErrorWins(t *testing.T) {
	t.Parallel()

	vaultA := glid.New()
	vaultB := glid.New()
	chA := make(chan segmentation.Input, 1)
	chB := make(chan segmentation.Input, 1)

	mgr := routing.New(routing.Config{
		Workers: 2,
		Table:   catchAllTable(vaultA, vaultB),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultA: chA, vaultB: chB},
	})

	parent := make(chan error, 1)
	in := make(chan routing.Input, 1)
	inp := routing.IngestInput(&record.Record{Attrs: record.Attributes{"env": "prod"}})
	inp.Ack = parent
	in <- inp
	close(in)
	runRouter(t, mgr, in)

	boom := errors.New("boom")
	(<-chA).Ack <- boom
	(<-chB).Ack <- nil

	select {
	case err := <-parent:
		if !errors.Is(err, boom) {
			t.Fatalf("source ack error = %v, want %v", err, boom)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source ack did not fire")
	}
}

// A single matched vault receives the source ack directly (no join goroutine).
func TestRouteSingleVaultAckPassthrough(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan segmentation.Input, 1)
	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   prodOnlyTable(vaultID),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: out},
	})

	parent := make(chan error, 1)
	in := make(chan routing.Input, 1)
	inp := routing.IngestInput(&record.Record{Attrs: record.Attributes{"env": "prod"}})
	inp.Ack = parent
	in <- inp
	close(in)
	runRouter(t, mgr, in)

	got := <-out
	if got.Ack == nil {
		t.Fatal("single target must carry the source ack")
	}
	got.Ack <- nil
	select {
	case err := <-parent:
		if err != nil {
			t.Fatalf("source ack error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source ack did not fire")
	}
}

// A record matching no route is an intentional drop, but its source ack still
// resolves (nil) so a synchronous sender is not left hanging.
func TestRouteUnmatchedResolvesAck(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan segmentation.Input, 1)
	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   prodOnlyTable(vaultID),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: out},
	})

	parent := make(chan error, 1)
	in := make(chan routing.Input, 1)
	inp := routing.IngestInput(&record.Record{Attrs: record.Attributes{"env": "staging"}})
	inp.Ack = parent
	in <- inp
	close(in)
	runRouter(t, mgr, in)

	select {
	case err := <-parent:
		if err != nil {
			t.Fatalf("unmatched ack error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unmatched record did not resolve its ack")
	}
	if len(out) != 0 {
		t.Fatalf("unmatched record should not be delivered, got %d", len(out))
	}
}
