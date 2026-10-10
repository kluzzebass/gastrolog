package ingestion_test

// Unit tests cover IngestionManager in isolation. These behaviors need
// downstream pipeline or cluster context, so they are covered by
// integration tests rather than here:
//   - PressureGate throttling under real digestion-queue backpressure
//   - ingestion Ack after durable segment write (nil / error semantics)
//   - singleton ingester reassignment across 4+ nodes
//   - checkpoint periodic save + Raft failover restore
//   - dispatch → assignment snapshot → Reconcile with real ingester factories

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/waittest"
)

type emitIngester struct {
	msgs    []ingestion.IngesterMessage
	emitted atomic.Int32
}

func (e *emitIngester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	for _, msg := range e.msgs {
		select {
		case out <- msg:
			e.emitted.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

type blockingIngester struct {
	started chan struct{}
	runs    atomic.Int32
}

func (b *blockingIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	b.runs.Add(1)
	close(b.started)
	<-ctx.Done()
	return ctx.Err()
}

type failOncePassiveIngester struct {
	attempts atomic.Int32
}

func (f *failOncePassiveIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	if f.attempts.Add(1) == 1 {
		return errors.New("bind failed")
	}
	<-ctx.Done()
	return ctx.Err()
}

type ackIngester struct {
	ack chan<- error
}

func (a *ackIngester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	select {
	case out <- ingestion.IngesterMessage{Raw: []byte("relp"), Ack: a.ack}:
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestManagerReconcileStartStop(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	idA := glid.New()
	idB := glid.New()

	blockA := &blockingIngester{started: make(chan struct{})}
	blockB := &blockingIngester{started: make(chan struct{})}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 4})

	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: idA, Ingester: blockA, Name: "a", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waittest.Recv(t, "ingester A started", blockA.started, nil)

	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: idA, Ingester: blockA, Name: "a", Type: "mock"},
		{ID: idB, Ingester: blockB, Name: "b", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile add B: %v", err)
	}

	waittest.Recv(t, "ingester B started", blockB.started, nil)

	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: idB, Ingester: blockB, Name: "b", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile remove A: %v", err)
	}

	// Drain any messages and stop.
	go func() {
		for range out {
		}
	}()
	if err := mgr.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestManagerMintsEventIDOnEmit(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &emitIngester{msgs: []ingestion.IngesterMessage{
		{Raw: []byte("one")},
		{Raw: []byte("two"), SourceTS: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
	}}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 2})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	msg0 := <-out
	msg1 := <-out

	if msg0.EventID.IngesterID != id || msg0.EventID.NodeID != nodeID {
		t.Fatalf("EventID identity = %+v, want ingester=%v node=%v", msg0.EventID, id, nodeID)
	}
	if msg0.EventID.IngestSeq != 0 || msg1.EventID.IngestSeq != 1 {
		t.Fatalf("IngestSeq = %d,%d, want 0,1", msg0.EventID.IngestSeq, msg1.EventID.IngestSeq)
	}
	if msg1.SourceTS != time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC) {
		t.Fatalf("SourceTS = %v", msg1.SourceTS)
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

func TestManagerPreservesAckChannel(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ack := make(chan error, 1)
	ing := &ackIngester{ack: ack}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 1})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	msg := <-out
	if msg.Ack == nil {
		t.Fatal("Ack channel not preserved on emitted message")
	}

	// The emitted message carries the ingester's own ack channel, so the send
	// lands in its buffer before this receive.
	msg.Ack <- nil
	select {
	case got := <-ack:
		if got != nil {
			t.Fatalf("ack = %v, want nil", got)
		}
	default:
		t.Fatal("downstream ack not delivered to ingester channel")
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

func TestManagerBackpressure(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &emitIngester{msgs: []ingestion.IngesterMessage{
		{Raw: []byte("1")},
		{Raw: []byte("2")},
		{Raw: []byte("3")},
	}}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 2})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	<-out
	<-out

	waittest.Recv(t, "third message once the queue has capacity", out, func() string {
		return fmt.Sprintf("handed-off=%d queued=%d", ing.emitted.Load(), len(out))
	})

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

func TestManagerPassiveRetry(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &failOncePassiveIngester{}

	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 1,
		RetryDelay:  func(int) time.Duration { return 0 },
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: ing, Passive: true, Name: "listener", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waittest.Progress(t, "passive ingester retried", func() (string, bool) {
		n := ing.attempts.Load()
		return fmt.Sprintf("attempts=%d", n), n >= 2
	})

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

func TestManagerReconcileReplace(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	first := &emitIngester{msgs: []ingestion.IngesterMessage{{Raw: []byte("first")}}}
	second := &emitIngester{msgs: []ingestion.IngesterMessage{{Raw: []byte("second")}}}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: first}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	progress := func() string {
		return fmt.Sprintf("handed-off first=%d second=%d queued=%d", first.emitted.Load(), second.emitted.Load(), len(out))
	}
	if msg := waittest.Recv(t, "first ingester message", out, progress); string(msg.Raw) != "first" {
		t.Fatalf("first message = %q, want first", msg.Raw)
	}

	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: second}}); err != nil {
		t.Fatalf("Reconcile replace: %v", err)
	}

	if msg := waittest.Recv(t, "replacement ingester message", out, progress); string(msg.Raw) != "second" {
		t.Fatalf("message after replace = %q, want second", msg.Raw)
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

func TestManagerStartBeforeReconcile(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &emitIngester{msgs: []ingestion.IngesterMessage{{Raw: []byte("late")}}}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	msg := <-out
	if string(msg.Raw) != "late" {
		t.Fatalf("raw = %q, want late", msg.Raw)
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

type checkpointIngester struct {
	saveCalls atomic.Int32
}

func (c *checkpointIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	<-ctx.Done()
	return ctx.Err()
}

func (c *checkpointIngester) SaveCheckpoint() ([]byte, error) {
	c.saveCalls.Add(1)
	return []byte("cp"), nil
}

func (c *checkpointIngester) LoadCheckpoint([]byte) error { return nil }

func TestManagerCheckpointOnStop(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &checkpointIngester{}

	var mu sync.Mutex
	var saved []byte
	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 1,
		OnCheckpoint: func(_ glid.GLID, data []byte) {
			mu.Lock()
			saved = append([]byte(nil), data...)
			mu.Unlock()
		},
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	go func() {
		for range out {
		}
	}()
	if err := mgr.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if string(saved) != "cp" {
		t.Fatalf("checkpoint = %q, want cp", saved)
	}
}

func TestManagerReconcileValidation(t *testing.T) {
	t.Parallel()

	mgr, _ := ingestion.New(ingestion.Config{NodeID: glid.New()})

	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: glid.GLID{}}}); err == nil {
		t.Fatal("expected error for zero ID")
	}
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: glid.New(), Ingester: nil},
	}); err == nil {
		t.Fatal("expected error for nil ingester")
	}
}

func TestManagerStartStopErrors(t *testing.T) {
	t.Parallel()

	mgr, out := ingestion.New(ingestion.Config{NodeID: glid.New(), OutCapacity: 1})
	if err := mgr.Stop(); !errors.Is(err, ingestion.ErrNotRunning) {
		t.Fatalf("Stop before Start = %v, want ErrNotRunning", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := mgr.Start(ctx); !errors.Is(err, ingestion.ErrAlreadyRunning) {
		t.Fatalf("second Start = %v, want ErrAlreadyRunning", err)
	}

	go func() {
		for range out {
		}
	}()
	if err := mgr.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := mgr.Stop(); !errors.Is(err, ingestion.ErrNotRunning) {
		t.Fatalf("second Stop = %v, want ErrNotRunning", err)
	}
}

func TestManagerReconcileNoOp(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	block := &blockingIngester{started: make(chan struct{})}
	spec := ingestion.IngesterSpec{ID: id, Ingester: block, Name: "same", Type: "mock"}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Reconcile([]ingestion.IngesterSpec{spec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waittest.Recv(t, "ingester started", block.started, nil)

	if err := mgr.Reconcile([]ingestion.IngesterSpec{spec}); err != nil {
		t.Fatalf("Reconcile no-op: %v", err)
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
	// Stop waits for every run the manager started, so the count is final.
	if n := block.runs.Load(); n != 1 {
		t.Fatalf("ingester runs = %d, want 1: no-op reconcile restarted ingester", n)
	}
}

// failNTimesIngester fails its first `failures` runs, then blocks until ctx is
// done — a recovered long-running source. Every run entry is signalled on
// attemptCh; entering the healthy run closes recovered.
type failNTimesIngester struct {
	failures  int32
	attempts  atomic.Int32
	attemptCh chan int32
	recovered chan struct{}
}

func (f *failNTimesIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	n := f.attempts.Add(1)
	f.attemptCh <- n
	if n <= f.failures {
		return errors.New("source unavailable")
	}
	close(f.recovered)
	<-ctx.Done()
	return ctx.Err()
}

// TestManagerActiveIngesterErrorRetry pins the error-retry contract: a
// non-passive ingester whose run returns an error is retried (with the
// configured delay) until a run holds, instead of being logged once and
// abandoned. Attempts are observed via channel sends from the fake — no
// wall-clock assertions.
func TestManagerActiveIngesterErrorRetry(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &failNTimesIngester{
		failures:  2,
		attemptCh: make(chan int32, 4),
		recovered: make(chan struct{}),
	}

	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 1,
		RetryDelay:  func(int) time.Duration { return 0 },
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: ing, Passive: false, Name: "tail", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	progress := func() string { return fmt.Sprintf("attempts=%d", ing.attempts.Load()) }
	for want := int32(1); want <= 3; want++ {
		if got := waittest.Recv(t, fmt.Sprintf("non-passive ingester attempt %d", want), ing.attemptCh, progress); got != want {
			t.Fatalf("attempt = %d, want %d", got, want)
		}
	}
	waittest.Recv(t, "ingester reaches its recovered run", ing.recovered, progress)

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

// cleanExitIngester returns nil immediately: a finite non-passive source that
// completed its input.
type cleanExitIngester struct {
	attemptCh chan struct{}
}

func (c *cleanExitIngester) Run(context.Context, chan<- ingestion.IngesterMessage) error {
	c.attemptCh <- struct{}{}
	return nil
}

// TestManagerActiveIngesterCleanExitNoRetry guards the other half of the
// non-passive contract: a clean (nil) exit is completion, not failure, and must
// NOT be re-run — retrying would mint the same input again. RetryDelay is zero,
// so a wrongly-scheduled re-run would land within microseconds of the first
// exit; the bounded negative window is conservative.
func TestManagerActiveIngesterCleanExitNoRetry(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &cleanExitIngester{attemptCh: make(chan struct{}, 2)}

	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 1,
		RetryDelay:  func(int) time.Duration { return 0 },
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: ing, Passive: false, Name: "import", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waittest.Recv(t, "ingester run", ing.attemptCh, nil)
	select {
	case <-ing.attemptCh:
		t.Fatal("clean-exit non-passive ingester must not be re-run")
	case <-time.After(100 * time.Millisecond):
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

type panicIngester struct {
	runs atomic.Int32
}

func (p *panicIngester) Run(context.Context, chan<- ingestion.IngesterMessage) error {
	p.runs.Add(1)
	panic("ingester boom")
}

func TestManagerIngesterPanicRecovery(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &panicIngester{}
	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 1,
		// Re-arm once immediately, then park in the backoff until Stop.
		RetryDelay: func(consecutiveFailures int) time.Duration {
			if consecutiveFailures <= 1 {
				return 0
			}
			return time.Hour
		},
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waittest.Progress(t, "panicking run recovered and re-armed", func() (string, bool) {
		n := ing.runs.Load()
		return fmt.Sprintf("runs=%d", n), n >= 2
	})

	go func() {
		for range out {
		}
	}()
	if err := mgr.Stop(); err != nil {
		t.Fatalf("Stop after panic: %v", err)
	}
}

type pressureIngester struct {
	started chan struct{}
	gate    atomic.Pointer[chanwatch.PressureGate]
}

func (p *pressureIngester) SetPressureGate(gate *chanwatch.PressureGate) {
	p.gate.Store(gate)
}

func (p *pressureIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	close(p.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestManagerPressureGateInjection(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &pressureIngester{started: make(chan struct{})}
	gate := chanwatch.NewPressureGate(chanwatch.DefaultThresholds())

	mgr, out := ingestion.New(ingestion.Config{
		NodeID:       nodeID,
		OutCapacity:  1,
		PressureGate: gate,
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waittest.Recv(t, "ingester started", ing.started, nil)
	if ing.gate.Load() != gate {
		t.Fatal("PressureAware ingester did not receive configured gate")
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

func TestManagerAckErrorDelivery(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ack := make(chan error, 1)
	ing := &ackIngester{ack: ack}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 1})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	msg := <-out
	writeErr := errors.New("segment write failed")
	// The emitted message carries the ingester's own ack channel, so the send
	// lands in its buffer before this receive.
	msg.Ack <- writeErr
	select {
	case got := <-ack:
		if !errors.Is(got, writeErr) {
			t.Fatalf("ack = %v, want %v", got, writeErr)
		}
	default:
		t.Fatal("error ack not delivered")
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

func TestManagerAttrsPassthrough(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &emitIngester{msgs: []ingestion.IngesterMessage{
		{Raw: []byte("x"), Attrs: map[string]string{"host": "a", "app": "b"}},
	}}

	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 1})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	msg := <-out
	if msg.Attrs["host"] != "a" || msg.Attrs["app"] != "b" {
		t.Fatalf("attrs = %v", msg.Attrs)
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

type failingCheckpointIngester struct {
	checkpointIngester
}

func (f *failingCheckpointIngester) SaveCheckpoint() ([]byte, error) {
	return nil, errors.New("disk full")
}

func TestManagerCheckpointSaveError(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &failingCheckpointIngester{}

	var saved bool
	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 1,
		OnCheckpoint: func(_ glid.GLID, _ []byte) {
			saved = true
		},
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	go func() {
		for range out {
		}
	}()
	if err := mgr.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if saved {
		t.Fatal("OnCheckpoint called despite SaveCheckpoint error")
	}
}

// scriptedPassiveIngester returns one scripted result per run attempt; the
// last script entry blocks until ctx is done. Each run entry is signalled on
// attemptCh.
type scriptedPassiveIngester struct {
	script    []error
	attempts  atomic.Int32
	attemptCh chan int32
}

func (s *scriptedPassiveIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	n := s.attempts.Add(1)
	s.attemptCh <- n
	if int(n) < len(s.script) {
		return s.script[n-1]
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestManagerRetryFailureCountResets pins the RetryDelay seam contract:
// consecutiveFailures counts error exits since the last clean run — it
// increments across failures and resets to 0 when a
// passive run exits cleanly, so a recovered listener that later fails again
// backs off from the base delay, not from its old streak. Observed entirely
// through the injected seam with zero delays; no sleeps.
func TestManagerRetryFailureCountResets(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	boom := errors.New("bind failed")
	// Attempts: fail, fail, clean exit, fail, then block (healthy).
	ing := &scriptedPassiveIngester{
		script:    []error{boom, boom, nil, boom, nil /* sentinel: block */},
		attemptCh: make(chan int32, 8),
	}

	var mu sync.Mutex
	var observed []int
	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      nodeID,
		OutCapacity: 1,
		RetryDelay: func(consecutiveFailures int) time.Duration {
			mu.Lock()
			observed = append(observed, consecutiveFailures)
			mu.Unlock()
			return 0
		},
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: ing, Passive: true, Name: "listener", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	progress := func() string {
		mu.Lock()
		defer mu.Unlock()
		return fmt.Sprintf("attempts=%d retry-delays=%v", ing.attempts.Load(), observed)
	}
	for want := int32(1); want <= 5; want++ {
		if got := waittest.Recv(t, fmt.Sprintf("passive ingester attempt %d", want), ing.attemptCh, progress); got != want {
			t.Fatalf("attempt = %d, want %d", got, want)
		}
	}

	mu.Lock()
	got := append([]int(nil), observed...)
	mu.Unlock()
	// Retries before attempts 2..5: failures 1, 2, then clean-exit reset to 0,
	// then a fresh streak at 1.
	want := []int{1, 2, 0, 1}
	if len(got) != len(want) {
		t.Fatalf("RetryDelay calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RetryDelay call %d = %d, want %d (all calls: %v)", i, got[i], want[i], got)
		}
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}

// rebuildFake is a controllable ingester for the rebuild ordering tests.
// Run announces itself on the shared ordered events channel
// ("<label>:run" on entry, "<label>:exit" via defer as the very last thing
// before returning), emits messages until the pipeline is saturated (one
// buffered token per successful send on sent), and exits only on ctx
// cancellation. With OutCapacity 1 and no consumer on the digestion queue,
// exactly two sends complete — msg1 fills the queue, msg2 is held by the
// manager's pump blocked in the queue send — and the third send can never
// finish: the run is then deterministically parked, wakeable only by cancel,
// reproducing the field state (a scatterbox mid-send on a pinned pipeline).
type rebuildFake struct {
	label  string
	events chan<- string
	sent   chan struct{}
	exited atomic.Bool
}

func (f *rebuildFake) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	f.events <- f.label + ":run"
	defer func() {
		f.exited.Store(true)
		f.events <- f.label + ":exit"
	}()
	for {
		select {
		case out <- ingestion.IngesterMessage{Raw: []byte(f.label)}:
			select {
			case f.sent <- struct{}{}:
			default:
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func newRebuildFake(label string, events chan<- string) *rebuildFake {
	return &rebuildFake{label: label, events: events, sent: make(chan struct{}, 8)}
}

func expectEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()
	if got := waittest.Recv(t, fmt.Sprintf("event %q", want), events, nil); got != want {
		t.Fatalf("event = %q, want %q", got, want)
	}
}

// drainOnStop keeps the digestion queue drained once the test is over so
// Stop's close cascade can complete.
func drainOnStop(t *testing.T, mgr *ingestion.Manager, out <-chan ingestion.IngestMessage) {
	t.Helper()
	t.Cleanup(func() {
		go func() {
			for range out {
			}
		}()
		_ = mgr.Stop()
	})
}

// TestManagerRebuildWaitsForOldRunUnderBackpressure is a regression test.
// Field incident: a config rebuild under a saturated
// pipeline cancelled the old run without waiting for it; the old run — parked
// in a send on the full digestion queue — woke AFTER the successor had
// started, and its deferred teardown (the orchestrator adapter's alive-false
// on the shared IngesterStats) clobbered the successor's alive-true, so the
// convergence sweep reporting divergence for a demonstrably running
// ingester (3 of 4 nodes on the live cluster).
//
// Synchronization is pure channels: the old run is deterministically parked
// (two sends absorbed by queue+pump, the third can never complete), the
// rebuild is driven synchronously, and the assertion is happens-before —
// Reconcile must not return until the old run has fully exited, and the
// ordered events stream must show old-exit strictly before new-run-entry.
func TestManagerRebuildWaitsForOldRunUnderBackpressure(t *testing.T) {
	t.Parallel()

	events := make(chan string, 32)
	oldRun := newRebuildFake("old", events)
	newRun := newRebuildFake("new", events)

	mgr, out := ingestion.New(ingestion.Config{NodeID: glid.New(), OutCapacity: 1})
	id := glid.New()
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: oldRun, Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drainOnStop(t, mgr, out)

	expectEvent(t, events, "old:run")
	// Saturate: queue full + pump blocked in the queue send. The old run is
	// now parked mid-send; only cancellation can wake it.
	<-oldRun.sent
	<-oldRun.sent

	// Rebuild: same ID, new Ingester instance.
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: newRun, Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile rebuild: %v", err)
	}

	if !oldRun.exited.Load() {
		t.Fatal("Reconcile returned before the old run fully exited")
	}
	expectEvent(t, events, "old:exit")
	expectEvent(t, events, "new:run")
	if newRun.exited.Load() {
		t.Fatal("successor exited unexpectedly")
	}
}

// TestManagerStopOnlyWaitsForOldRun: removing an ingester (no successor) also
// waits for the run to fully exit before Reconcile returns, and nothing
// starts afterwards.
func TestManagerStopOnlyWaitsForOldRun(t *testing.T) {
	t.Parallel()

	events := make(chan string, 32)
	oldRun := newRebuildFake("old", events)

	mgr, out := ingestion.New(ingestion.Config{NodeID: glid.New(), OutCapacity: 1})
	id := glid.New()
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: oldRun, Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drainOnStop(t, mgr, out)

	expectEvent(t, events, "old:run")
	<-oldRun.sent
	<-oldRun.sent

	if err := mgr.Reconcile(nil); err != nil {
		t.Fatalf("Reconcile remove: %v", err)
	}
	if !oldRun.exited.Load() {
		t.Fatal("Reconcile returned before the removed run fully exited")
	}
	expectEvent(t, events, "old:exit")
	select {
	case ev := <-events:
		t.Fatalf("unexpected event after removal: %q", ev)
	default:
	}
}

// cleanExitEventIngester completes immediately (finite non-passive source),
// announcing entry and exit on the ordered events channel.
type cleanExitEventIngester struct {
	label  string
	events chan<- string
}

func (c *cleanExitEventIngester) Run(context.Context, chan<- ingestion.IngesterMessage) error {
	c.events <- c.label + ":run"
	c.events <- c.label + ":exit"
	return nil
}

// TestManagerRebuildAfterCleanExit: waiting on a run that already exited on
// its own must not hang the rebuild.
func TestManagerRebuildAfterCleanExit(t *testing.T) {
	t.Parallel()

	events := make(chan string, 32)
	oldRun := &cleanExitEventIngester{label: "old", events: events}
	newRun := newRebuildFake("new", events)

	mgr, out := ingestion.New(ingestion.Config{NodeID: glid.New(), OutCapacity: 1})
	id := glid.New()
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: oldRun, Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drainOnStop(t, mgr, out)

	expectEvent(t, events, "old:run")
	expectEvent(t, events, "old:exit")

	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: newRun, Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile rebuild: %v", err)
	}
	expectEvent(t, events, "new:run")
}

// TestManagerRebuildChurnStrictAlternation: several rapid rebuilds in a row
// keep the invariant one-run-at-a-time — the ordered events stream must be a
// strict run/exit alternation, ending with exactly one live run.
func TestManagerRebuildChurnStrictAlternation(t *testing.T) {
	t.Parallel()

	const generations = 5
	events := make(chan string, 64)
	fakes := make([]*rebuildFake, generations)
	for i := range fakes {
		fakes[i] = newRebuildFake(fmt.Sprintf("g%d", i), events)
	}

	mgr, out := ingestion.New(ingestion.Config{NodeID: glid.New(), OutCapacity: 1})
	id := glid.New()
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: fakes[0], Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drainOnStop(t, mgr, out)

	expectEvent(t, events, "g0:run")
	// Saturate under generation 0 so every later generation starts against a
	// pinned pipeline.
	<-fakes[0].sent
	<-fakes[0].sent

	for i := 1; i < generations; i++ {
		if err := mgr.Reconcile([]ingestion.IngesterSpec{
			{ID: id, Ingester: fakes[i], Name: "src", Type: "mock"},
		}); err != nil {
			t.Fatalf("Reconcile churn %d: %v", i, err)
		}
		if !fakes[i-1].exited.Load() {
			t.Fatalf("churn %d: Reconcile returned before generation %d exited", i, i-1)
		}
		expectEvent(t, events, fmt.Sprintf("g%d:exit", i-1))
		expectEvent(t, events, fmt.Sprintf("g%d:run", i))
	}

	if fakes[generations-1].exited.Load() {
		t.Fatal("final generation is not running")
	}
	select {
	case ev := <-events:
		t.Fatalf("unexpected trailing event: %q", ev)
	default:
	}
}

// failThenBackoffIngester fails its single attempt; the manager then parks
// its run goroutine in the retry backoff (a long injected delay). The rebuild
// wait must cover that parked backoff, and cancellation must abort it
// promptly — the runIngester goroutine spans attempts AND the sleeps between
// them.
type failThenBackoffIngester struct {
	label  string
	events chan<- string
}

func (f *failThenBackoffIngester) Run(context.Context, chan<- ingestion.IngesterMessage) error {
	f.events <- f.label + ":run"
	f.events <- f.label + ":exit"
	return errors.New("source unavailable")
}

// TestManagerRebuildInterruptsRetryBackoff: a rebuild that lands while the
// old run goroutine is parked in a retry backoff (not in an attempt) still
// waits for the goroutine to exit — and the cancel aborts the pending
// backoff instead of sleeping it out (the injected delay is an hour).
func TestManagerRebuildInterruptsRetryBackoff(t *testing.T) {
	t.Parallel()

	events := make(chan string, 32)
	oldRun := &failThenBackoffIngester{label: "old", events: events}
	newRun := newRebuildFake("new", events)

	delayCalled := make(chan struct{}, 4)
	mgr, out := ingestion.New(ingestion.Config{
		NodeID:      glid.New(),
		OutCapacity: 1,
		RetryDelay: func(int) time.Duration {
			delayCalled <- struct{}{}
			return time.Hour
		},
	})
	id := glid.New()
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: oldRun, Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drainOnStop(t, mgr, out)

	expectEvent(t, events, "old:run")
	expectEvent(t, events, "old:exit")
	waittest.Recv(t, "retry delay consulted", delayCalled, nil)

	// The old run goroutine is now headed into (or already inside) an
	// hour-long backoff select. The rebuild must return promptly anyway.
	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: id, Ingester: newRun, Name: "src", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile rebuild: %v", err)
	}
	expectEvent(t, events, "new:run")
}

// TestManagerCheckpointPeriodicSave exercises the CheckpointInterval knob:
// a running Checkpointable ingester is saved on the
// configured cadence, not only at run exit. A tiny interval keeps the test
// event-driven — it waits for saves to arrive, never for wall-clock margins.
func TestManagerCheckpointPeriodicSave(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	ing := &checkpointIngester{}

	saves := make(chan struct{}, 16)
	mgr, out := ingestion.New(ingestion.Config{
		NodeID:             nodeID,
		OutCapacity:        1,
		CheckpointInterval: 5 * time.Millisecond,
		OnCheckpoint: func(_ glid.GLID, _ []byte) {
			select {
			case saves <- struct{}{}:
			default:
			}
		},
	})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: ing}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Two periodic saves while the run is still alive (stop not called yet).
	for i := range 2 {
		waittest.Recv(t, fmt.Sprintf("periodic checkpoint save %d", i+1), saves, func() string {
			return fmt.Sprintf("checkpoint saves=%d", ing.saveCalls.Load())
		})
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
}
