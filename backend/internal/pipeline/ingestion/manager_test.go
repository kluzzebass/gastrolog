package ingestion_test

// Unit tests cover IngestionManager in isolation. Integration coverage for
// behaviors that need downstream pipeline or cluster context is tracked on
// gastrolog-214bz (pipeline integration):
//   - PressureGate throttling under real digestion-queue backpressure
//   - ingestion Ack after durable segment write (nil / error semantics)
//   - singleton ingester reassignment across 4+ nodes
//   - checkpoint periodic save + Raft failover restore
//   - dispatch → assignment snapshot → Reconcile with real ingester factories

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
)

type emitIngester struct {
	msgs []ingestion.IngesterMessage
}

func (e *emitIngester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	for _, msg := range e.msgs {
		select {
		case out <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

type blockingIngester struct {
	started chan struct{}
}

func (b *blockingIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
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

	select {
	case <-blockA.started:
	case <-time.After(time.Second):
		t.Fatal("ingester A did not start")
	}

	if err := mgr.Reconcile([]ingestion.IngesterSpec{
		{ID: idA, Ingester: blockA, Name: "a", Type: "mock"},
		{ID: idB, Ingester: blockB, Name: "b", Type: "mock"},
	}); err != nil {
		t.Fatalf("Reconcile add B: %v", err)
	}

	select {
	case <-blockB.started:
	case <-time.After(time.Second):
		t.Fatal("ingester B did not start")
	}

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

	msg.Ack <- nil
	select {
	case got := <-ack:
		if got != nil {
			t.Fatalf("ack = %v, want nil", got)
		}
	case <-time.After(time.Second):
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

	done := make(chan struct{})
	go func() {
		<-out
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("third message arrived before queue had capacity")
	}

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
		RetryDelay:  func() time.Duration { return 0 },
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

	deadline := time.After(8 * time.Second)
	for ing.attempts.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("passive ingester did not retry, attempts=%d", ing.attempts.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}

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

	select {
	case msg := <-out:
		if string(msg.Raw) != "first" {
			t.Fatalf("first message = %q, want first", msg.Raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first ingester message")
	}

	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: second}}); err != nil {
		t.Fatalf("Reconcile replace: %v", err)
	}

	got := []string{"first"}
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case msg := <-out:
			got = append(got, string(msg.Raw))
		case <-deadline:
			t.Fatalf("got %v, want first and second", got)
		}
	}
	if got[0] != "first" || got[1] != "second" {
		t.Fatalf("messages = %v, want [first second]", got)
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
	select {
	case <-block.started:
	case <-time.After(time.Second):
		t.Fatal("ingester did not start")
	}

	block.started = make(chan struct{}) // would close again if restarted
	if err := mgr.Reconcile([]ingestion.IngesterSpec{spec}); err != nil {
		t.Fatalf("Reconcile no-op: %v", err)
	}
	select {
	case <-block.started:
		t.Fatal("no-op reconcile restarted ingester")
	case <-time.After(100 * time.Millisecond):
	}

	go func() {
		for range out {
		}
	}()
	_ = mgr.Stop()
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

// TestManagerActiveIngesterErrorRetry pins the gastrolog-fjwhbr fix: a
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
		RetryDelay:  func() time.Duration { return 0 },
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

	for want := int32(1); want <= 3; want++ {
		select {
		case got := <-ing.attemptCh:
			if got != want {
				t.Fatalf("attempt = %d, want %d", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("non-passive ingester was not retried (waiting for attempt %d)", want)
		}
	}
	select {
	case <-ing.recovered:
	case <-time.After(5 * time.Second):
		t.Fatal("ingester did not reach its recovered run")
	}

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
		RetryDelay:  func() time.Duration { return 0 },
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

	select {
	case <-ing.attemptCh:
	case <-time.After(2 * time.Second):
		t.Fatal("ingester did not run")
	}
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

type panicIngester struct{}

func (panicIngester) Run(context.Context, chan<- ingestion.IngesterMessage) error {
	panic("ingester boom")
}

func TestManagerIngesterPanicRecovery(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	id := glid.New()
	mgr, out := ingestion.New(ingestion.Config{NodeID: nodeID, OutCapacity: 1})
	if err := mgr.Reconcile([]ingestion.IngesterSpec{{ID: id, Ingester: panicIngester{}}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

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

	select {
	case <-ing.started:
	case <-time.After(time.Second):
		t.Fatal("ingester did not start")
	}
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
	msg.Ack <- writeErr

	select {
	case got := <-ack:
		if !errors.Is(got, writeErr) {
			t.Fatalf("ack = %v, want %v", got, writeErr)
		}
	case <-time.After(time.Second):
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
