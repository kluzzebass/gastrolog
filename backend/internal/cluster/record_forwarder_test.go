package cluster

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func TestBatching(t *testing.T) {
	var mu sync.Mutex
	var batches []int

	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}

	vaultID := uuid.Must(uuid.NewV7())

	// Fill with 250 records.
	for i := 0; i < 250; i++ {
		nf.ch <- forwardEntry{
			vaultID: vaultID,
			record:  chunk.Record{Raw: []byte("test")},
		}
	}

	if len(nf.ch) != 250 {
		t.Fatalf("expected 250 entries in channel, got %d", len(nf.ch))
	}

	// Drain in batches of forwardBatchSize (same logic as flushLoop).
	for len(nf.ch) > 0 {
		batch := make([]forwardEntry, 0, forwardBatchSize)
	drain:
		for len(batch) < forwardBatchSize {
			select {
			case entry := <-nf.ch:
				batch = append(batch, entry)
			default:
				break drain
			}
		}
		mu.Lock()
		batches = append(batches, len(batch))
		mu.Unlock()
	}

	// Should produce 3 batches: 100 + 100 + 50.
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d: %v", len(batches), batches)
	}
	if batches[0] != 100 || batches[1] != 100 || batches[2] != 50 {
		t.Errorf("batch sizes = %v, want [100 100 50]", batches)
	}
}

func TestBufferOverflow(t *testing.T) {
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}

	vaultID := uuid.Must(uuid.NewV7())

	// Fill to capacity.
	for i := 0; i < forwardChanCap; i++ {
		nf.ch <- forwardEntry{
			vaultID: vaultID,
			record:  chunk.Record{Raw: []byte("fill")},
		}
	}

	// Next send should not succeed (non-blocking select).
	select {
	case nf.ch <- forwardEntry{vaultID: vaultID, record: chunk.Record{Raw: []byte("overflow")}}:
		t.Error("expected channel send to block (buffer full)")
	default:
		// Expected: channel is full, send would block.
	}
}

func TestFlushTimerDrains(t *testing.T) {
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}

	vaultID := uuid.Must(uuid.NewV7())
	nf.ch <- forwardEntry{vaultID: vaultID, record: chunk.Record{Raw: []byte("single")}}

	// Simulate timer-based drain: after forwardFlushInterval the entry
	// should still be in the channel (timers are in flushLoop).
	// We verify the channel semantics here — the entry is retrievable.
	time.Sleep(10 * time.Millisecond) // brief yield
	if len(nf.ch) != 1 {
		t.Errorf("expected 1 entry in channel, got %d", len(nf.ch))
	}

	// Drain it.
	entry := <-nf.ch
	if string(entry.record.Raw) != "single" {
		t.Errorf("unexpected record raw: %s", entry.record.Raw)
	}
}

func TestForwardEnqueuesAndCloses(t *testing.T) {
	rf := &RecordForwarder{
		logger: discardLogger(),
		nodes:  make(map[string]*nodeForwarder),
		conns:  make(map[string]*grpc.ClientConn),
	}

	nodeID := "test-node"
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}
	rf.nodes[nodeID] = nf

	// Start a dummy flush goroutine that just drains.
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		defer close(nf.done)
		for range nf.ch {
		}
	}()

	vaultID := uuid.Must(uuid.NewV7())
	rec := chunk.Record{Raw: []byte("test")}

	err := rf.Forward(context.Background(), nodeID, vaultID, []chunk.Record{rec})
	if err != nil {
		t.Fatalf("Forward failed: %v", err)
	}

	if err := rf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestForwardClosedReturnsError(t *testing.T) {
	rf := &RecordForwarder{
		logger: discardLogger(),
		nodes:  make(map[string]*nodeForwarder),
		conns:  make(map[string]*grpc.ClientConn),
		closed: true,
	}

	err := rf.Forward(context.Background(), "node-X", uuid.Must(uuid.NewV7()), []chunk.Record{{Raw: []byte("x")}})
	if err == nil {
		t.Error("expected error from closed forwarder")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
