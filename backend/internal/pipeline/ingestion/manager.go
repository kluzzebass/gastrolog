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
}

// Manager starts and stops ingesters from assignment snapshots and emits minted
// Messages on a bounded channel.
type Manager struct {
	nodeID glid.GLID
	out    chan Message
	logger *slog.Logger

	onCheckpoint func(id glid.GLID, data []byte)
	pressureGate *chanwatch.PressureGate

	mu      sync.Mutex
	running atomic.Bool
	runCtx  context.Context
	cancel  context.CancelFunc

	ingesters map[glid.GLID]Ingester
	meta      map[glid.GLID]ingesterMeta
	minters   map[glid.GLID]*Minter
	cancels   map[glid.GLID]context.CancelFunc

	ingesterWg sync.WaitGroup
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
	out := make(chan Message, cfg.OutCapacity)
	m := &Manager{
		nodeID:       cfg.NodeID,
		out:          out,
		logger:       cfg.Logger,
		onCheckpoint: cfg.OnCheckpoint,
		pressureGate: cfg.PressureGate,
		ingesters:    make(map[glid.GLID]Ingester),
		meta:         make(map[glid.GLID]ingesterMeta),
		minters:      make(map[glid.GLID]*Minter),
		cancels:      make(map[glid.GLID]context.CancelFunc),
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
	close(m.out)

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

	for {
		err := m.runWithCheckpoints(ctx, id, ing, minter, out)
		if err != nil && ctx.Err() == nil && !meta.passive {
			m.logger.Warn("ingester exited with error",
				"id", id, "name", meta.name, "type", meta.typ, "error", err)
		}
		if ctx.Err() != nil || !meta.passive {
			return
		}
		delay := 3*time.Second + time.Duration(rand.Int64N(int64(2*time.Second))) //nolint:gosec // G404: jitter for retry delay, not security-sensitive
		m.logger.Warn("passive ingester failed, retrying",
			"id", id, "name", meta.name, "error", err, "retry_in", delay)
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

	ticker := time.NewTicker(5 * time.Second)
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
