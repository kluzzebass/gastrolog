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
// convergence sweep's ingester-not-running alert (gastrolog-3mnjlo) stays
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
// Messages on a bounded channel.
type Manager struct {
	nodeID glid.GLID
	out    chan Message
	logger *slog.Logger

	onCheckpoint       func(id glid.GLID, data []byte)
	pressureGate       *chanwatch.PressureGate
	retryDelay         func(consecutiveFailures int) time.Duration
	checkpointInterval time.Duration

	mu      sync.Mutex
	running atomic.Bool
	runCtx  context.Context
	cancel  context.CancelFunc

	ingesters map[glid.GLID]Ingester
	meta      map[glid.GLID]ingesterMeta
	minters   map[glid.GLID]*Minter
	cancels   map[glid.GLID]context.CancelFunc

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
func New(cfg Config) (*Manager, <-chan Message) {
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
	out := make(chan Message, cfg.OutCapacity)
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
	}
	return m, out
}

// Start launches all registered ingesters. Reconcile may be called before or
// after Start; ingesters added while stopped start when Start runs.
func (m *Manager) Start(parent context.Context) error {
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
	m.mu.Unlock()

	return nil
}

// Reconcile starts ingesters present in the snapshot and stops those absent.
// An ingester whose spec changes (different Ingester value or metadata) is
// replaced idempotently; an unchanged spec keeps its running instance untouched.
func (m *Manager) Reconcile(snapshot []IngesterSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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

	for id := range m.ingesters {
		if _, ok := desired[id]; !ok {
			m.stopLocked(id)
			delete(m.ingesters, id)
			delete(m.meta, id)
			delete(m.minters, id)
		}
	}

	for id, spec := range desired {
		existing, had := m.ingesters[id]
		meta := ingesterMeta{name: spec.Name, typ: spec.Type, passive: spec.Passive}
		if had && existing == spec.Ingester && m.meta[id] == meta {
			continue
		}

		if had {
			m.stopLocked(id)
		}

		m.ingesters[id] = spec.Ingester
		m.meta[id] = meta
		if m.minters[id] == nil {
			m.minters[id] = NewMinter(id, m.nodeID)
		}

		if m.running.Load() {
			m.startLocked(id, spec.Ingester)
		}
	}

	return nil
}

func (m *Manager) startLocked(id glid.GLID, ing Ingester) {
	if pa, ok := ing.(PressureAware); ok && m.pressureGate != nil {
		pa.SetPressureGate(m.pressureGate)
	}

	recvCtx, recvCancel := context.WithCancel(m.runCtx)
	m.cancels[id] = recvCancel

	meta := m.meta[id]
	minter := m.minters[id]
	out := m.out

	m.ingesterWg.Go(func() {
		m.runIngester(id, ing, minter, meta, recvCtx, out)
	})

	m.logger.Info("ingester started", "id", id, "name", meta.name, "type", meta.typ)
}

func (m *Manager) stopLocked(id glid.GLID) {
	if cancel, ok := m.cancels[id]; ok {
		cancel()
		delete(m.cancels, id)
	}
	meta := m.meta[id]
	m.logger.Info("ingester stopped", "id", id, "name", meta.name, "type", meta.typ)
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
//     ingester-not-running alert (gastrolog-3mnjlo) surfaces the degraded
//     condition and clears it once a retry holds.
func (m *Manager) runIngester(
	id glid.GLID,
	ing Ingester,
	minter *Minter,
	meta ingesterMeta,
	ctx context.Context,
	out chan<- Message,
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

func (m *Manager) runWithCheckpoints(
	ctx context.Context,
	id glid.GLID,
	ing Ingester,
	minter *Minter,
	out chan<- Message,
) error {
	emit := func(msg IngesterMessage) error {
		minted := Message{
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

	run := func() error {
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
			if err := emit(msg); err != nil {
				return err
			}
		}
		return <-errCh
	}

	cp, isCheckpointable := ing.(Checkpointable)
	if !isCheckpointable || m.onCheckpoint == nil {
		return run()
	}

	errCh := make(chan error, 1)
	go func() { errCh <- run() }()

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
			// run() is the sole sender on out; it lives in a separate
			// goroutine here (unlike the non-checkpoint path, which blocks in
			// run() directly). Wait for it to return before we do, so no sender
			// outlives runWithCheckpoints — otherwise ingesterWg.Wait() in Stop
			// unblocks while run() still sends on out, racing close(out).
			<-errCh
			return ctx.Err()
		}
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
