package ingestion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/glid"
)

// ErrNotRunning is returned when an operation requires a running manager.
var ErrNotRunning = errors.New("ingestion manager not running")

// ErrAlreadyRunning is returned when Start is called twice.
var ErrAlreadyRunning = errors.New("ingestion manager already running")

// IngesterSpec describes one ingester that should run on this node.
type IngesterSpec struct {
	ID       glid.GLID
	Ingester Ingester
	Passive  bool
	Name     string
	Type     string
}

// Config configures a new IngestionManager.
type Config struct {
	NodeID glid.GLID
	// OutCapacity is the bounded digestion queue depth. Defaults to 1000.
	OutCapacity int
	Logger      *slog.Logger

	// OnCheckpoint persists opaque checkpoint blobs from Checkpointable ingesters.
	OnCheckpoint func(id glid.GLID, data []byte)

	// PressureGate throttles PressureAware ingesters when the digestion queue
	// (and any other registered probes) is backed up. Nil disables throttling.
	PressureGate *chanwatch.PressureGate

	// RetryDelay returns the pause before re-running an ingester whose run
	// ended and is eligible for retry (any passive listener exit, or a
	// non-passive run that returned an error). consecutiveFailures counts
	// error exits since the last clean run exit (0 when the previous run
	// ended cleanly). Nil uses the default jittered exponential backoff.
	// Injectable so tests synchronize on attempts instead of waiting out
	// wall-clock delays.
	RetryDelay func(consecutiveFailures int) time.Duration

	// CheckpointInterval is the period between checkpoint saves while a
	// Checkpointable ingester runs (see defaultCheckpointInterval for why
	// this is a per-run ticker and what the interval bounds). Zero or
	// negative selects the default.
	CheckpointInterval time.Duration
}

// Retry backoff for ingester re-runs. The first retry keeps the historical
// jittered 3–5s window (base 3s plus up to 2/3 jitter): long enough not to
// spin on a persistent failure, short enough that a recovered dependency
// (released port, reachable endpoint) is picked up promptly. Each further
// consecutive failure doubles the pre-jitter delay up to the cap, so a
// permanently failing ingester settles at one warn every ~5–8 minutes instead
// of flooding the log every few seconds. The counter resets on any clean run
// exit, so an ingester that recovers and later fails again starts back at the
// base delay. Operator visibility does not ride on the log line: the
// convergence sweep's divergence log (gastrolog-3mnjlo) stays
// raised between attempts and clears once a retry holds.
const (
	retryBackoffBase   = 3 * time.Second
	retryBackoffFactor = 2
	retryBackoffCap    = 5 * time.Minute
)

// retryBackoffDelay returns the pre-jitter delay for a retry after
// consecutiveFailures error exits (0 or 1 → base). Pure, so tests exercise
// the backoff curve without sleeping.
func retryBackoffDelay(consecutiveFailures int) time.Duration {
	d := retryBackoffBase
	for i := 1; i < consecutiveFailures && d < retryBackoffCap; i++ {
		d *= retryBackoffFactor
	}
	return min(d, retryBackoffCap)
}

// defaultRetryDelay jitters retryBackoffDelay by up to 2/3 of the pre-jitter
// value so co-failing ingesters (e.g. after a shared dependency outage) do
// not retry in lockstep. At the base delay this reproduces the historical
// 3–5s window.
func defaultRetryDelay(consecutiveFailures int) time.Duration {
	d := retryBackoffDelay(consecutiveFailures)
	return d + time.Duration(rand.Int64N(int64(d)*2/3)) //nolint:gosec // G404: jitter for retry delay, not security-sensitive
}

// defaultCheckpointInterval is the period between checkpoint saves for
// Checkpointable ingesters. This is deliberately a per-run ticker inside
// runWithCheckpoints, NOT an orchestrator scheduler job: the save loop is
// scoped to one ingester's run — created and torn down with the run, under
// the ingester's own context — whereas scheduler jobs are node-global work
// that outlives ingester reconciliation. The interval bounds the
// crash-redelivery window: after a hard crash, at most this much
// already-emitted input since the last saved checkpoint is re-read and
// re-minted on restart. (A save also happens on run exit and on shutdown, so
// clean stops carry no window.)
const defaultCheckpointInterval = 5 * time.Second

// Manager starts and stops ingesters from assignment snapshots and emits minted
// IngestMessages on a bounded channel.
type Manager struct {
	nodeID glid.GLID
	out    chan IngestMessage
	logger *slog.Logger

	onCheckpoint       func(id glid.GLID, data []byte)
	pressureGate       *chanwatch.PressureGate
	retryDelay         func(consecutiveFailures int) time.Duration
	checkpointInterval time.Duration

	// lifecycleMu serializes Start, Stop, and Reconcile against each other.
	// Reconcile must release mu while it waits for stopped runs to exit
	// (never wait on a goroutine while holding the state lock), so mu alone
	// cannot keep a concurrent lifecycle call from interleaving with that
	// window. Lock order: lifecycleMu before mu.
	lifecycleMu sync.Mutex

	mu      sync.Mutex
	running atomic.Bool
	runCtx  context.Context
	cancel  context.CancelFunc

	ingesters map[glid.GLID]Ingester
	meta      map[glid.GLID]ingesterMeta
	minters   map[glid.GLID]*Minter
	cancels   map[glid.GLID]context.CancelFunc
	// dones holds, per running ingester, a channel closed when its
	// runIngester goroutine has fully returned — after every goroutine of the
	// attempt (including the Ingester.Run goroutine, see pumpIngester) has
	// exited. Reconcile waits on these so a replaced ingester's old run can
	// never overlap its successor (gastrolog-4rdb9f: a stale run's deferred
	// alive-false clobbered the new run's alive-true on the shared stats).
	dones map[glid.GLID]chan struct{}

	ingesterWg sync.WaitGroup
	// outOnce guards close(out): both Stop and the Start-installed
	// context watcher close the queue, whichever exits the run first.
	outOnce sync.Once
}

type ingesterMeta struct {
	name    string
	typ     string
	passive bool
}

// New returns a manager and the read-only digestion queue fed by minted messages.
func New(cfg Config) (*Manager, <-chan IngestMessage) {
	if cfg.OutCapacity <= 0 {
		cfg.OutCapacity = 1000
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RetryDelay == nil {
		cfg.RetryDelay = defaultRetryDelay
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = defaultCheckpointInterval
	}
	out := make(chan IngestMessage, cfg.OutCapacity)
	m := &Manager{
		nodeID:             cfg.NodeID,
		out:                out,
		logger:             cfg.Logger,
		onCheckpoint:       cfg.OnCheckpoint,
		pressureGate:       cfg.PressureGate,
		retryDelay:         cfg.RetryDelay,
		checkpointInterval: cfg.CheckpointInterval,
		ingesters:          make(map[glid.GLID]Ingester),
		meta:               make(map[glid.GLID]ingesterMeta),
		minters:            make(map[glid.GLID]*Minter),
		cancels:            make(map[glid.GLID]context.CancelFunc),
		dones:              make(map[glid.GLID]chan struct{}),
	}
	return m, out
}

// Start launches all registered ingesters. Reconcile may be called before or
// after Start; ingesters added while stopped start when Start runs.
func (m *Manager) Start(parent context.Context) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	ctx, cancel := context.WithCancel(parent)
	m.runCtx = ctx
	m.cancel = cancel

	for id, ing := range m.ingesters {
		m.startLocked(id, ing)
	}

	// Close-on-exit contract (gastrolog-5kcq5q): the output queue closes on
	// EVERY run exit — Stop below, or the parent context dying without a
	// Stop call. Downstream stages (digestion, the pump, routing) shut down
	// purely by close cascade with plain per-record channel ops; a run exit
	// that left out open would strand them blocked on receive forever.
	go func() {
		<-ctx.Done()
		m.ingesterWg.Wait()
		m.outOnce.Do(func() { close(m.out) })
	}()
	return nil
}

// Stop cancels all ingesters, waits for them to exit, then closes the output
// channel so downstream can drain and exit.
func (m *Manager) Stop() error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	if !m.running.CompareAndSwap(true, false) {
		return ErrNotRunning
	}

	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	m.ingesterWg.Wait()
	m.outOnce.Do(func() { close(m.out) })

	m.mu.Lock()
	m.runCtx = nil
	m.cancel = nil
	m.cancels = make(map[glid.GLID]context.CancelFunc)
	m.dones = make(map[glid.GLID]chan struct{})
	m.mu.Unlock()

	return nil
}

// Reconcile starts ingesters present in the snapshot and stops those absent.
// An ingester whose spec changes (different Ingester value or metadata) is
// replaced idempotently; an unchanged spec keeps its running instance untouched.
//
// A replaced ingester's old run has FULLY exited before its successor starts:
// stop is cancel-then-wait, not fire-and-forget. Without the wait, a run
// parked in a send on the full digestion queue wakes on cancel AFTER the
// successor has started, and its deferred teardown (the orchestrator
// adapter's alive-false, against the same shared IngesterStats reused across
// rebuilds) lands last — leaving a running ingester reported not-running
// until the next rebuild, so the convergence sweep re-raised
// divergence forever on a healthy node (gastrolog-4rdb9f). The wait
// happens with mu released — never hold the state lock while blocking on a
// goroutine — and is unbounded by design: Ingester.Run is contractually
// required to exit promptly on cancellation (Stop already waits unboundedly
// on the same goroutines via ingesterWg).
func (m *Manager) Reconcile(snapshot []IngesterSpec) error {
	desired := make(map[glid.GLID]IngesterSpec, len(snapshot))
	for _, spec := range snapshot {
		if spec.ID.IsZero() {
			return errors.New("ingester spec missing ID")
		}
		if spec.Ingester == nil {
			return fmt.Errorf("ingester %s: nil Ingester", spec.ID)
		}
		desired[spec.ID] = spec
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	// Phase 1 (under mu): cancel removed and replaced runs, install the new
	// specs, and collect the done channels of every run we stopped.
	m.mu.Lock()

	var waits []chan struct{}
	stop := func(id glid.GLID) {
		if done := m.stopLocked(id); done != nil {
			waits = append(waits, done)
		}
	}

	for id := range m.ingesters {
		if _, ok := desired[id]; !ok {
			stop(id)
			delete(m.ingesters, id)
			delete(m.meta, id)
			delete(m.minters, id)
		}
	}

	var toStart []glid.GLID
	for id, spec := range desired {
		existing, had := m.ingesters[id]
		meta := ingesterMeta{name: spec.Name, typ: spec.Type, passive: spec.Passive}
		if had && existing == spec.Ingester && m.meta[id] == meta {
			continue
		}

		if had {
			stop(id)
		}

		m.ingesters[id] = spec.Ingester
		m.meta[id] = meta
		if m.minters[id] == nil {
			m.minters[id] = NewMinter(id, m.nodeID)
		}
		toStart = append(toStart, id)
	}

	m.mu.Unlock()

	// Phase 2 (no locks): wait for every stopped run to fully exit. Their
	// contexts are already cancelled; a run parked in a queue send or a retry
	// backoff wakes immediately. lifecycleMu keeps Start/Stop/Reconcile from
	// interleaving with this window.
	for _, done := range waits {
		<-done
	}

	// Phase 3 (under mu): start successors. Only started when the manager
	// runs; otherwise the installed specs start at Start, as before.
	m.mu.Lock()
	if m.running.Load() {
		for _, id := range toStart {
			m.startLocked(id, m.ingesters[id])
		}
	}
	m.mu.Unlock()

	return nil
}

func (m *Manager) startLocked(id glid.GLID, ing Ingester) {
	if pa, ok := ing.(PressureAware); ok && m.pressureGate != nil {
		pa.SetPressureGate(m.pressureGate)
	}

	recvCtx, recvCancel := context.WithCancel(m.runCtx)
	m.cancels[id] = recvCancel

	// Closed when runIngester returns — the whole retry loop, including any
	// in-flight attempt's goroutines (pumpIngester waits for Ingester.Run
	// before returning). stopLocked hands this to Reconcile so a successor
	// never starts while the old run can still write.
	done := make(chan struct{})
	m.dones[id] = done

	meta := m.meta[id]
	minter := m.minters[id]
	out := m.out

	m.ingesterWg.Go(func() {
		defer close(done)
		m.runIngester(id, ing, minter, meta, recvCtx, out)
	})

	m.logger.Info("ingester started", "id", id, "name", meta.name, "type", meta.typ)
}

// stopLocked cancels the ingester's run and returns the channel that closes
// when the run goroutine has fully exited (nil when no run was started).
// Callers must wait on it — outside mu — before starting a successor for the
// same ID.
func (m *Manager) stopLocked(id glid.GLID) chan struct{} {
	if cancel, ok := m.cancels[id]; ok {
		cancel()
		delete(m.cancels, id)
	}
	done := m.dones[id]
	delete(m.dones, id)
	meta := m.meta[id]
	m.logger.Info("ingester stopped", "id", id, "name", meta.name, "type", meta.typ)
	return done
}

// runIngester executes a single ingester with panic recovery so that a
// misbehaving ingester cannot crash the entire process.
//
// Runs are re-armed with a delay from RetryDelay (default: jittered
// exponential backoff, 3–5s first retry — see retryBackoffBase and friends):
//   - Passive (listener) ingesters retry on EVERY exit — port-bind errors are
//     recoverable when another process releases the port or a co-located node
//     dies.
//   - Non-passive ingesters retry only when the run returned an error; a clean
//     exit means a finite source completed. Before gastrolog-fjwhbr an error
//     exit was logged once and the goroutine returned: ingest for that source
//     stopped until a config change rebuilt the spec. Between failing attempts
//     the ingester's alive state stays down, so the convergence sweep's
//     divergence log (gastrolog-3mnjlo) surfaces the degraded
//     condition and clears it once a retry holds.
func (m *Manager) runIngester(
	id glid.GLID,
	ing Ingester,
	minter *Minter,
	meta ingesterMeta,
	ctx context.Context,
	out chan<- IngestMessage,
) {
	defer func() {
		if v := recover(); v != nil {
			m.logger.Error("ingester panicked", "id", id, "panic", v)
		}
	}()

	failures := 0
	for {
		err := m.runWithCheckpoints(ctx, id, ing, minter, out)
		if ctx.Err() != nil {
			return
		}
		if err == nil && !meta.passive {
			// A finite non-passive ingester completed its input; re-running
			// it would mint the same input again.
			return
		}
		if err != nil {
			failures++
		} else {
			// Clean passive exit (listener served and closed normally):
			// reset the backoff so the re-listen happens at the base delay.
			failures = 0
		}
		delay := m.retryDelay(failures)
		m.logger.Warn("ingester exited, retrying",
			"id", id, "name", meta.name, "type", meta.typ,
			"passive", meta.passive, "error", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// runWithCheckpoints dispatches one ingester run to the right pump mode:
// plain (pumpIngester inline) for ingesters without checkpoint persistence,
// or pump-on-a-goroutine with a per-run checkpoint save ticker for
// Checkpointable ingesters.
func (m *Manager) runWithCheckpoints(
	ctx context.Context,
	id glid.GLID,
	ing Ingester,
	minter *Minter,
	out chan<- IngestMessage,
) error {
	cp, isCheckpointable := ing.(Checkpointable)
	if !isCheckpointable || m.onCheckpoint == nil {
		return m.pumpIngester(ctx, id, ing, minter, out)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- m.pumpIngester(ctx, id, ing, minter, out) }()

	ticker := time.NewTicker(m.checkpointInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-errCh:
			m.saveCheckpointFrom(id, cp)
			return err
		case <-ticker.C:
			m.saveCheckpointFrom(id, cp)
		case <-ctx.Done():
			m.saveCheckpointFrom(id, cp)
			// pumpIngester is the sole sender on out; it lives in a separate
			// goroutine here (unlike the non-checkpoint path, which blocks in
			// pumpIngester directly). Wait for it to return before we do, so no
			// sender outlives runWithCheckpoints — otherwise ingesterWg.Wait()
			// in Stop unblocks while the pump still sends on out, racing
			// close(out).
			<-errCh
			return ctx.Err()
		}
	}
}

// pumpIngester runs one ingester attempt: it launches ing.Run on its own
// goroutine (with panic recovery) and pumps every IngesterMessage through the
// minter onto out until the ingester's output closes, then returns Run's
// error. Emission is interrupted by ctx, which propagates as the returned
// error.
func (m *Manager) pumpIngester(
	ctx context.Context,
	id glid.GLID,
	ing Ingester,
	minter *Minter,
	out chan<- IngestMessage,
) error {
	ingesterOut := make(chan IngesterMessage)
	errCh := make(chan error, 1)
	go func() {
		defer close(ingesterOut)
		var runErr error
		defer func() {
			if v := recover(); v != nil {
				m.logger.Error("ingester panicked", "id", id, "panic", v)
				runErr = fmt.Errorf("ingester panicked: %v", v)
			}
			errCh <- runErr
		}()
		runErr = ing.Run(ctx, ingesterOut)
	}()

	for msg := range ingesterOut {
		if err := emitMinted(ctx, minter, out, msg); err != nil {
			// ctx cancelled mid-emit. Run's own ctx select exits it promptly
			// (the Ingester contract); drain whatever it flushes on the way
			// out so its sends never block, then wait for it to return.
			// Returning while Run still executes would let a stale run's
			// writes — the orchestrator adapter's deferred alive-false and
			// its ingest counters — land after the manager considers this
			// attempt finished, clobbering the successor started on the
			// done signal (gastrolog-4rdb9f).
			for range ingesterOut { //nolint:revive // draining until close
			}
			<-errCh
			return err
		}
	}
	return <-errCh
}

// emitMinted mints one ingester message and delivers it to the digestion
// queue, honoring ctx while the queue is full.
func emitMinted(ctx context.Context, minter *Minter, out chan<- IngestMessage, msg IngesterMessage) error {
	minted := IngestMessage{
		EventID:  minter.Mint(),
		Attrs:    msg.Attrs,
		Raw:      msg.Raw,
		RawOwned: msg.RawOwned,
		SourceTS: msg.SourceTS,
		Ack:      msg.Ack,
	}
	select {
	case out <- minted:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) saveCheckpointFrom(id glid.GLID, cp Checkpointable) {
	data, err := cp.SaveCheckpoint()
	if err != nil {
		m.logger.Error("ingester checkpoint save failed", "id", id, "error", err)
		return
	}
	if len(data) > 0 {
		m.onCheckpoint(id, data)
	}
}
