package routing

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

// ErrAlreadyRunning is returned when Run is called twice.
var ErrAlreadyRunning = errors.New("routing manager already running")

// Input is one record entering the routing stage with its origin context.
//
// Two entry paths:
//   - Ingest→digest: Source from IngestSource(rec) (or zero Source, same default)
//   - Retention event (disposition = route): Source from RetentionSource(sourceVaultID, reason)
//
// Source is routing-time metadata only — it is not stored on the record.
//
// Ack, when non-nil, is the source-side durability ack: routing fans the record
// out to every matched vault and joins the per-vault commit results so Ack fires
// nil only after all targets have durably committed (first error wins). This is
// the cardinal ack-after-fsync path: the upstream ingester must not release a
// record until its Ack resolves.
type Input struct {
	Record *record.Record
	Source SourceContext
	Ack    chan<- error
}

// IngestInput wraps a digested record for the live-ingest routing path.
func IngestInput(rec *record.Record) Input {
	return Input{Record: rec, Source: IngestSource(rec)}
}

// Config configures a RoutingManager worker pool.
type Config struct {
	// Workers is the number of parallel routing goroutines. Defaults to 4.
	Workers int
	Table   *Table
	// Vaults maps vault ID to that vault's segmentation input queue.
	Vaults map[glid.GLID]chan<- segmentation.Input // wrapped in vaultSink at New
	// VaultGate, when non-nil, is consulted per matched destination vault
	// before fan-out. A non-nil return rejects the WHOLE record (nacked to
	// the source): delivering to the healthy subset while dropping the
	// gated vault's copy would be silent loss for that vault.
	VaultGate func(glid.GLID) error
}

// StatsSnapshot is a point-in-time view of routing counters.
type StatsSnapshot struct {
	Ingested  uint64               // records that entered routing (matched + unmatched)
	Matched   uint64               // records that matched a route and were fanned out
	Unmatched uint64               // records with no route match (intentional drop, counted)
	PerVault  map[glid.GLID]uint64 // matched-record count per destination vault
	PerRoute  map[glid.GLID]uint64 // matched-record count per route ID
	// PerVaultDropped counts delivery drops per destination vault: records
	// already counted as matched (in PerVault) whose fan-out delivery to that
	// vault's segmentation queue failed — the vault sink was revoked mid-flight
	// (unregister) or the context was cancelled at shutdown. Distinct from
	// Unmatched, which counts intentional no-route drops.
	PerVaultDropped map[glid.GLID]uint64
}

// Manager matches records to vaults and fans out record pointers.
type Manager struct {
	workers int
	// table is swapped atomically: the orchestrator recompiles the routing
	// table on config changes (vault add/remove, route edits) and publishes
	// the new one via SetTable while workers match lock-free against it.
	table atomic.Pointer[Table]

	vmu    sync.RWMutex
	vaults map[glid.GLID]*vaultSink

	// vaultGate is the per-destination admission check (see Config.VaultGate).
	vaultGate func(glid.GLID) error

	matched   atomic.Uint64
	unmatched atomic.Uint64
	// perVault / perRoute hold matched counters keyed by destination vault ID
	// and route ID respectively. Lazily populated on the match path so the hot
	// route() avoids a map write lock. perVaultDropped follows the same
	// pattern for delivery drops (see StatsSnapshot.PerVaultDropped).
	perVault        counterMap
	perRoute        counterMap
	perVaultDropped counterMap
	running         atomic.Bool
}

// New returns a routing manager.
func New(cfg Config) *Manager {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	vaults := make(map[glid.GLID]*vaultSink, len(cfg.Vaults))
	for id, ch := range cfg.Vaults {
		vaults[id] = newVaultSink(ch)
	}
	m := &Manager{
		workers:   cfg.Workers,
		vaults:    vaults,
		vaultGate: cfg.VaultGate,
	}
	m.table.Store(cfg.Table)
	return m
}

// SetTable atomically replaces the routing table. Safe to call before or during
// Run; in-flight and subsequent matches observe the new table. A nil table
// matches nothing (every record is an intentional unmatched drop).
func (m *Manager) SetTable(t *Table) {
	m.table.Store(t)
}

// RegisterVault sets (or replaces) the segmentation input queue a vault's
// matched records fan out to. Safe to call before or during Run; the orchestrator
// reconcile registers vaults as placement changes bring vault homes onto this node.
func (m *Manager) RegisterVault(vaultID glid.GLID, in chan<- segmentation.Input) {
	m.vmu.Lock()
	old := m.vaults[vaultID]
	m.vaults[vaultID] = newVaultSink(in)
	m.vmu.Unlock()
	if old != nil {
		old.revoke()
	}
}

// UnregisterVault drops a vault's fan-out target and waits for in-flight routing
// deliveries to finish. Call segmentation.UnregisterVault only after this returns
// so workers never send on a closed input channel.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.vmu.Lock()
	sink, ok := m.vaults[vaultID]
	if ok {
		delete(m.vaults, vaultID)
	}
	m.vmu.Unlock()
	if ok {
		sink.revoke()
	}
}

// Stats returns a snapshot of routing counters (global totals plus per-vault and
// per-route matched counts).
func (m *Manager) Stats() StatsSnapshot {
	matched := m.matched.Load()
	unmatched := m.unmatched.Load()
	return StatsSnapshot{
		Ingested:        matched + unmatched,
		Matched:         matched,
		Unmatched:       unmatched,
		PerVault:        m.perVault.snapshot(),
		PerRoute:        m.perRoute.snapshot(),
		PerVaultDropped: m.perVaultDropped.snapshot(),
	}
}

// TableActive reports whether a routing table is currently published. It is the
// pipeline analogue of the legacy "filter set active" flag.
func (m *Manager) TableActive() bool {
	return m.table.Load() != nil
}

// Table returns the currently published routing table (nil when none has been
// set). The returned table is immutable; callers must treat it as read-only.
func (m *Manager) Table() *Table {
	return m.table.Load()
}

// counterMap is a lazily-populated set of per-GLID monotonic counters
// (sync.Map of glid.GLID → *atomic.Uint64 underneath, so the hot match path
// increments without a map write lock). The any-type assertions live here
// instead of at every call site.
type counterMap struct {
	m sync.Map
}

func (c *counterMap) incr(id glid.GLID) {
	if v, ok := c.m.Load(id); ok {
		v.(*atomic.Uint64).Add(1)
		return
	}
	v, _ := c.m.LoadOrStore(id, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

func (c *counterMap) snapshot() map[glid.GLID]uint64 {
	out := make(map[glid.GLID]uint64)
	c.m.Range(func(k, v any) bool {
		out[k.(glid.GLID)] = v.(*atomic.Uint64).Load()
		return true
	})
	return out
}

// Run consumes inputs until in is closed.
//
// Shutdown is close-driven, not ctx-driven (gastrolog-5kcq5q): the supervisor
// pump closes in after the digest stage's output drains, so workers receive
// with a plain range instead of per-record 2-case selects (the sellock hot
// spot). ctx still bounds the per-delivery sink sends inside route() — that
// is the one boundary where a stalled consumer (segmentation stopping at
// shutdown) must be able to abort a blocking send.
func (m *Manager) Run(ctx context.Context, in <-chan Input) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	pipeline.RunWorkerPool(m.workers, in, func(item Input) {
		m.route(ctx, item)
	})
	return ctx.Err()
}

func resolveSource(in Input) SourceContext {
	if in.Source.Kind != SourceUnknown {
		return in.Source
	}
	return IngestSource(in.Record)
}

func (m *Manager) route(ctx context.Context, in Input) {
	rec := in.Record
	src := resolveSource(in)
	matchedRoute, vaults := m.table.Load().MatchRoute(rec.Attrs, src)
	if matchedRoute == nil {
		m.unmatched.Add(1)
		// No route matched: nothing to persist locally. This is an intentional,
		// counted drop, not a failure — resolve the source ack so a synchronous
		// sender is not left hanging.
		sendAck(in.Ack, nil)
		return
	}
	// Per-destination admission: any gated matched vault rejects the whole
	// record before it is counted or delivered anywhere. The nack reaches
	// the source, which retries like any other backpressure signal.
	if m.vaultGate != nil {
		for _, vaultID := range vaults {
			if err := m.vaultGate(vaultID); err != nil {
				sendAck(in.Ack, err)
				return
			}
		}
	}
	m.matched.Add(1)
	m.perRoute.incr(matchedRoute.ID)
	for _, vaultID := range vaults {
		m.perVault.incr(vaultID)
	}

	// Snapshot fan-out sinks under the read lock. Matched vaults without a local
	// segmentation queue (e.g. a home that lives on another node) are skipped;
	// cross-node fan-out lands in a later slice. Workers hold *vaultSink pointers
	// so UnregisterVault can drain in-flight deliveries before segmentation closes
	// the input channel.
	m.vmu.RLock()
	targets := make([]sinkTarget, 0, len(vaults))
	for _, vaultID := range vaults {
		if sink, ok := m.vaults[vaultID]; ok {
			targets = append(targets, sinkTarget{vaultID: vaultID, sink: sink})
		}
	}
	m.vmu.RUnlock()

	if len(targets) == 0 {
		sendAck(in.Ack, nil)
		return
	}

	if in.Ack == nil {
		m.fanOut(ctx, rec, targets)
		return
	}

	if len(targets) == 1 {
		t := targets[0]
		// deliver nacks in.Ack itself on failure; the source retries.
		if t.sink.deliver(ctx, segmentation.Input{Record: rec, Ack: in.Ack}) != nil {
			m.perVaultDropped.incr(t.vaultID)
		}
		return
	}

	m.fanOutJoined(ctx, rec, targets, in.Ack)
}

// sinkTarget pairs a matched destination vault with its local vault sink, so
// the fan-out loops can attribute delivery drops to the right vault.
type sinkTarget struct {
	vaultID glid.GLID
	sink    *vaultSink
}

// fanOut delivers rec to every target without a source ack (fire-and-forget).
// A revoked sink (vault unregistered mid-flight) drops only that vault's copy —
// counted per vault, never silent — and delivery continues to the remaining
// sinks. Context cancellation stops the fan-out: every subsequent send would
// fail the same way, so the remaining targets are counted as dropped too.
func (m *Manager) fanOut(ctx context.Context, rec *record.Record, targets []sinkTarget) {
	for i, t := range targets {
		if t.sink.deliver(ctx, segmentation.Input{Record: rec}) == nil {
			continue
		}
		m.perVaultDropped.incr(t.vaultID)
		if ctx.Err() != nil {
			for _, rest := range targets[i+1:] {
				m.perVaultDropped.incr(rest.vaultID)
			}
			return
		}
	}
}

// fanOutJoined delivers rec to every target, joining the per-vault commit acks
// into the single source ack. A revoked sink fails only its own child ack
// (deliver nacks it; the join carries the first error to the source, which
// retries) — delivery continues to the remaining sinks. Context cancellation
// stops the fan-out and nacks the still-undelivered children so the join
// resolves instead of leaking its collector.
func (m *Manager) fanOutJoined(ctx context.Context, rec *record.Record, targets []sinkTarget, ack chan<- error) {
	children := newAckJoin(len(targets), ack)
	for i, t := range targets {
		if t.sink.deliver(ctx, segmentation.Input{Record: rec, Ack: children[i]}) == nil {
			continue
		}
		m.perVaultDropped.incr(t.vaultID)
		if ctxErr := ctx.Err(); ctxErr != nil {
			for j := i + 1; j < len(targets); j++ {
				m.perVaultDropped.incr(targets[j].vaultID)
				children[j] <- ctxErr
			}
			return
		}
	}
}

func sendAck(ack chan<- error, err error) {
	if ack != nil {
		ack <- err
	}
}
