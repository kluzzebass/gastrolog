package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

func TestBufferOverflow(t *testing.T) {
	t.Parallel()
	nf := &nodeForwarder{
		ch: make(chan forwardEntry, forwardChanCap),
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

func TestChannelDrain(t *testing.T) {
	t.Parallel()
	nf := &nodeForwarder{
		ch: make(chan forwardEntry, forwardChanCap),
	}

	vaultID := uuid.Must(uuid.NewV7())
	nf.ch <- forwardEntry{vaultID: vaultID, record: chunk.Record{Raw: []byte("single")}}

	// Entry should be retrievable from the channel.
	time.Sleep(10 * time.Millisecond) // brief yield
	if len(nf.ch) != 1 {
		t.Errorf("expected 1 entry in channel, got %d", len(nf.ch))
	}

	entry := <-nf.ch
	if string(entry.record.Raw) != "single" {
		t.Errorf("unexpected record raw: %s", entry.record.Raw)
	}
}

func TestForwardEntryToProto(t *testing.T) {
	t.Parallel()
	vaultID := uuid.Must(uuid.NewV7())
	ingesterID := uuid.Must(uuid.NewV7())
	now := time.Now()

	entry := forwardEntry{
		vaultID: vaultID,
		record: chunk.Record{
			Raw:      []byte("test record"),
			SourceTS: now.Add(-time.Hour),
			IngestTS: now,
			Attrs:    chunk.Attributes{"level": "error"},
			EventID: chunk.EventID{
				IngesterID: ingesterID,
				IngestSeq:  42,
				IngestTS:   now,
			},
		},
	}

	msg := forwardEntryToProto(entry)

	if msg.GetVaultId() != vaultID.String() {
		t.Errorf("vault_id = %q, want %q", msg.GetVaultId(), vaultID.String())
	}
	if string(msg.GetRaw()) != "test record" {
		t.Errorf("raw = %q, want %q", msg.GetRaw(), "test record")
	}
	if msg.GetIngestSeq() != 42 {
		t.Errorf("ingest_seq = %d, want 42", msg.GetIngestSeq())
	}
	if msg.GetAttrs()["level"] != "error" {
		t.Errorf("attrs[level] = %q, want %q", msg.GetAttrs()["level"], "error")
	}
	if msg.GetSourceTs() == nil {
		t.Error("source_ts should be set")
	}
	if msg.GetIngestTs() == nil {
		t.Error("ingest_ts should be set")
	}
}

func TestForwardEnqueuesAndCloses(t *testing.T) {
	t.Parallel()
	stopCtx, stopCancel := context.WithCancel(context.Background())
	cwCtx, cwCancel := context.WithCancel(context.Background())
	rf := &RecordForwarder{
		logger:     discardLogger(),
		nodes:      make(map[string]*nodeForwarder),
		stop:       make(chan struct{}),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
		cwCancel:   cwCancel,
		cw:         chanwatch.New(discardLogger(), 1*time.Second),
	}
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		rf.cw.Run(cwCtx)
	}()

	nodeID := "test-node"
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}
	rf.nodes[nodeID] = nf

	// Start a dummy stream goroutine that drains until stop.
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		defer close(nf.done)
		for {
			select {
			case <-nf.ch:
			case <-rf.stop:
				return
			}
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
	t.Parallel()
	rf := &RecordForwarder{
		logger: discardLogger(),
		nodes:  make(map[string]*nodeForwarder),
		closed: true,
	}

	err := rf.Forward(context.Background(), "node-X", uuid.Must(uuid.NewV7()), []chunk.Record{{Raw: []byte("x")}})
	if err == nil {
		t.Error("expected error from closed forwarder")
	}
}

func TestBackoffProgression(t *testing.T) {
	t.Parallel()
	nf := &nodeForwarder{}
	rf := &RecordForwarder{
		logger: discardLogger(),
		stop:   make(chan struct{}),
	}

	// First failure: backoff = 1s
	rf.bumpBackoff("node-1", nf, io.ErrClosedPipe)
	if nf.backoff != backoffMin {
		t.Errorf("backoff after 1 failure = %v, want %v", nf.backoff, backoffMin)
	}
	if nf.failures != 1 {
		t.Errorf("failures = %d, want 1", nf.failures)
	}

	// Second failure: backoff = 2s
	rf.bumpBackoff("node-1", nf, io.ErrClosedPipe)
	if nf.backoff != 2*time.Second {
		t.Errorf("backoff after 2 failures = %v, want 2s", nf.backoff)
	}

	// Keep bumping until max.
	for range 10 {
		rf.bumpBackoff("node-1", nf, io.ErrClosedPipe)
	}
	if nf.backoff > backoffMax {
		t.Errorf("backoff = %v, exceeds max %v", nf.backoff, backoffMax)
	}

	// Simulate success reset (same logic as sendBurst).
	nf.failures = 0
	nf.backoff = 0
	if nf.failures != 0 {
		t.Errorf("failures after success = %d, want 0", nf.failures)
	}
	if nf.backoff != 0 {
		t.Errorf("backoff after success = %v, want 0", nf.backoff)
	}
}

func TestCloseDoesNotRaceWithDrain(t *testing.T) {
	t.Parallel()
	stopCtx, stopCancel := context.WithCancel(context.Background())
	cwCtx, cwCancel := context.WithCancel(context.Background())
	rf := &RecordForwarder{
		logger:     discardLogger(),
		nodes:      make(map[string]*nodeForwarder),
		stop:       make(chan struct{}),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
		cwCancel:   cwCancel,
		cw:         chanwatch.New(discardLogger(), 1*time.Second),
	}
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		rf.cw.Run(cwCtx)
	}()

	nodeID := "race-node"
	nf := &nodeForwarder{
		ch:   make(chan forwardEntry, forwardChanCap),
		done: make(chan struct{}),
	}
	rf.nodes[nodeID] = nf

	// Start a goroutine that simulates drainToStream by reading from the channel.
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		defer close(nf.done)
		for {
			select {
			case <-nf.ch:
			case <-rf.stop:
				// Drain remaining.
				for {
					select {
					case <-nf.ch:
					default:
						return
					}
				}
			}
		}
	}()

	// Enqueue records concurrently with close.
	vaultID := uuid.Must(uuid.NewV7())
	go func() {
		for range 500 {
			_ = rf.Forward(context.Background(), nodeID, vaultID, []chunk.Record{{Raw: []byte("x")}})
		}
	}()

	// Close while records are still being enqueued.
	time.Sleep(1 * time.Millisecond)
	if err := rf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
