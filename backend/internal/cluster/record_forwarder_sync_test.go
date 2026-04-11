package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// newMinimalForwarder constructs a RecordForwarder without calling
// NewRecordForwarder (which would spin up a chanwatch goroutine and,
// via subsequent Forward calls, a streamLoop goroutine that nil-derefs
// on a missing peer pool). The returned forwarder supports direct
// channel-level manipulation via manualStartNode.
func newMinimalForwarder(t *testing.T) *RecordForwarder {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stopCtx, stopCancel := context.WithCancel(context.Background())
	rf := &RecordForwarder{
		logger:     logger,
		cw:         chanwatch.New(logger, time.Second),
		nodes:      make(map[string]*nodeForwarder),
		stop:       make(chan struct{}),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
		cwCancel: func() {
			// No-op: newMinimalForwarder does not start a chanwatch
			// goroutine, so there's nothing to cancel.
		},
	}
	t.Cleanup(func() {
		rf.mu.Lock()
		if !rf.closed {
			rf.closed = true
			close(rf.stop)
		}
		rf.mu.Unlock()
		stopCancel()
	})
	return rf
}

// manualStartNode installs a nodeForwarder in the map without starting
// the real streamLoop. Tests inspect the channel directly.
func manualStartNode(rf *RecordForwarder, nodeID string) *nodeForwarder {
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nodes[nodeID] = nf
	if rf.pressureGate != nil {
		probe := func() (int, int) { return len(nf.ch), cap(nf.ch) }
		rf.pressureGate.AddProbe(probeNamePrefix+nodeID, probe)
	}
	return nf
}

func fillChannel(t *testing.T, nf *nodeForwarder, vaultID uuid.UUID) {
	t.Helper()
	for len(nf.ch) < cap(nf.ch) {
		nf.ch <- forwardEntry{vaultID: vaultID, record: chunk.Record{Raw: []byte("fill")}}
	}
}

// TestForwardSyncBlocksUntilSpace verifies that when the channel is full,
// ForwardSync blocks the caller — instead of dropping like Forward does.
func TestForwardSyncBlocksUntilSpace(t *testing.T) {
	t.Parallel()
	rf := newMinimalForwarder(t)

	nodeID := "test-node-block"
	vaultID := uuid.Must(uuid.NewV7())
	nf := manualStartNode(rf, nodeID)
	fillChannel(t, nf, vaultID)

	done := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		done <- rf.ForwardSync(context.Background(), nodeID, vaultID, []chunk.Record{
			{Raw: []byte("sync-record")},
		})
	})

	// It must still be blocked after 50ms.
	select {
	case err := <-done:
		t.Fatalf("ForwardSync returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Drain one slot from the channel to make room.
	<-nf.ch

	// ForwardSync should now succeed.
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("ForwardSync after drain: unexpected error %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ForwardSync did not unblock after drain")
	}
	wg.Wait()
}

// TestForwardSyncRespectsContextCancel verifies that ForwardSync returns
// an error (not a silent drop) when the context expires before space
// becomes available.
func TestForwardSyncRespectsContextCancel(t *testing.T) {
	t.Parallel()
	rf := newMinimalForwarder(t)

	nodeID := "test-node-ctx"
	vaultID := uuid.Must(uuid.NewV7())
	nf := manualStartNode(rf, nodeID)
	fillChannel(t, nf, vaultID)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := rf.ForwardSync(ctx, nodeID, vaultID, []chunk.Record{
		{Raw: []byte("doomed")},
	})
	if err == nil {
		t.Fatal("expected ForwardSync to return error on ctx timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

// TestForwardSyncAcceptsWhenSpaceAvailable verifies the happy path:
// when the channel has room, ForwardSync returns nil immediately.
func TestForwardSyncAcceptsWhenSpaceAvailable(t *testing.T) {
	t.Parallel()
	rf := newMinimalForwarder(t)

	nodeID := "test-node-ok"
	vaultID := uuid.Must(uuid.NewV7())
	nf := manualStartNode(rf, nodeID)

	err := rf.ForwardSync(context.Background(), nodeID, vaultID, []chunk.Record{
		{Raw: []byte("immediate")},
	})
	if err != nil {
		t.Fatalf("ForwardSync on empty channel: %v", err)
	}
	if len(nf.ch) != 1 {
		t.Errorf("expected 1 entry queued, got %d", len(nf.ch))
	}
}

// TestForwardSyncErrorsWhenClosed verifies that ForwardSync rejects new
// records once the forwarder is closed.
func TestForwardSyncErrorsWhenClosed(t *testing.T) {
	t.Parallel()
	rf := newMinimalForwarder(t)

	rf.mu.Lock()
	rf.closed = true
	close(rf.stop)
	rf.mu.Unlock()

	err := rf.ForwardSync(context.Background(), "node", uuid.New(), []chunk.Record{
		{Raw: []byte("post-close")},
	})
	if err == nil {
		t.Error("expected error after Close, got nil")
	}
}

// TestRegisterPressureGateAddsExistingNodes verifies that a pressure gate
// registered AFTER per-node forwarders already exist picks up their
// channels as probes so the gate can classify their fill ratios.
func TestRegisterPressureGateAddsExistingNodes(t *testing.T) {
	t.Parallel()
	rf := newMinimalForwarder(t)

	// Pre-populate two node forwarders.
	nfA := manualStartNode(rf, "peer-a")
	nfB := manualStartNode(rf, "peer-b")

	// Register gate after nodes exist.
	gate := chanwatch.NewPressureGate(chanwatch.DefaultThresholds())
	rf.RegisterPressureGate(gate)

	// Fill both channels to capacity.
	fillChannel(t, nfA, uuid.New())
	fillChannel(t, nfB, uuid.New())

	// One tick at 200ms interval — wait ~350ms for it to run once.
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	gate.Run(ctx, 200*time.Millisecond)

	if got := gate.Level(); got != chanwatch.PressureCritical {
		t.Errorf("expected Critical with both forward channels full, got %v", got)
	}
}

// TestRegisterPressureGateAddsFutureNodes verifies that per-node
// forwarders created AFTER the pressure gate is registered also get
// their channels added as probes.
func TestRegisterPressureGateAddsFutureNodes(t *testing.T) {
	t.Parallel()
	rf := newMinimalForwarder(t)

	gate := chanwatch.NewPressureGate(chanwatch.DefaultThresholds())
	rf.RegisterPressureGate(gate)

	// Create the node AFTER the gate is registered.
	nf := manualStartNode(rf, "late-peer")
	fillChannel(t, nf, uuid.New())

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	gate.Run(ctx, 200*time.Millisecond)

	if got := gate.Level(); got != chanwatch.PressureCritical {
		t.Errorf("expected Critical after registering gate pre-node, got %v", got)
	}
}
