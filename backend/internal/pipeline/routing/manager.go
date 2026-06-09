package routing

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("routing manager not running")

// Input is one record entering the routing stage with its origin context.
//
// Two entry paths:
//   - Ingest→digest: Source from IngestSource(rec) (or zero Source, same default)
//   - Vault retention eject: Source from RetentionSource(sourceVaultID, reason)
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
	Vaults map[glid.GLID]chan<- segmentation.Input
}

// StatsSnapshot is a point-in-time view of routing counters.
type StatsSnapshot struct {
	Ingested  uint64                 // records that entered routing (matched + unmatched)
	Matched   uint64                 // records that matched a route and were fanned out
	Unmatched uint64                 // records with no route match (intentional drop, counted)
	PerVault  map[glid.GLID]uint64   // matched-record count per destination vault
	PerRoute  map[glid.GLID]uint64   // matched-record count per route ID
}

// Manager matches records to vaults and fans out record pointers.
type Manager struct {
	workers int
	// table is swapped atomically: the orchestrator recompiles the routing
	// table on config changes (vault add/remove, route edits) and publishes
	// the new one via SetTable while workers match lock-free against it.
	table atomic.Pointer[Table]

	vmu    sync.RWMutex
	vaults map[glid.GLID]chan<- segmentation.Input

	matched   atomic.Uint64
	unmatched atomic.Uint64
	// perVault / perRoute hold *atomic.Uint64 matched counters keyed by
	// destination vault ID and route ID respectively. Lazily populated on the
	// match path so the hot route() avoids a map write lock.
	perVault sync.Map
	perRoute sync.Map
	running  atomic.Bool
	wg       sync.WaitGroup
}

// New returns a routing manager.
func New(cfg Config) *Manager {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	vaults := cfg.Vaults
	if vaults == nil {
		vaults = make(map[glid.GLID]chan<- segmentation.Input)
	}
	m := &Manager{
		workers: cfg.Workers,
		vaults:  vaults,
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
	m.vaults[vaultID] = in
	m.vmu.Unlock()
}

// UnregisterVault drops a vault's fan-out target. Callers must stop upstream
// segmentation input for the vault before closing its channel; until re-registered,
// records matching the vault are counted as matched but not delivered.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.vmu.Lock()
	delete(m.vaults, vaultID)
	m.vmu.Unlock()
}

// Stats returns a snapshot of routing counters (global totals plus per-vault and
// per-route matched counts).
func (m *Manager) Stats() StatsSnapshot {
	matched := m.matched.Load()
	unmatched := m.unmatched.Load()
	return StatsSnapshot{
		Ingested:  matched + unmatched,
		Matched:   matched,
		Unmatched: unmatched,
		PerVault:  drainCounterMap(&m.perVault),
		PerRoute:  drainCounterMap(&m.perRoute),
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

func incrCounter(store *sync.Map, id glid.GLID) {
	if c, ok := store.Load(id); ok {
		c.(*atomic.Uint64).Add(1)
		return
	}
	c, _ := store.LoadOrStore(id, new(atomic.Uint64))
	c.(*atomic.Uint64).Add(1)
}

func drainCounterMap(store *sync.Map) map[glid.GLID]uint64 {
	out := make(map[glid.GLID]uint64)
	store.Range(func(k, v any) bool {
		out[k.(glid.GLID)] = v.(*atomic.Uint64).Load()
		return true
	})
	return out
}

// Run consumes inputs until in is closed or ctx is cancelled.
func (m *Manager) Run(ctx context.Context, in <-chan Input) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}

	work := make(chan Input)

	for range m.workers {
		m.wg.Go(func() {
			m.worker(ctx, work)
		})
	}

	m.wg.Go(func() {
		defer close(work)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				select {
				case work <- item:
				case <-ctx.Done():
					return
				}
			}
		}
	})

	m.wg.Wait()
	return ctx.Err()
}

func (m *Manager) worker(ctx context.Context, in <-chan Input) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-in:
			if !ok {
				return
			}
			m.route(ctx, item)
		}
	}
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
	m.matched.Add(1)
	incrCounter(&m.perRoute, matchedRoute.ID)
	for _, vaultID := range vaults {
		incrCounter(&m.perVault, vaultID)
	}

	// Snapshot the locally-registered fan-out targets under the read lock. Matched
	// vaults without a local segmentation queue (e.g. a home that lives on another
	// node) are skipped here; cross-node fan-out lands in a later slice.
	m.vmu.RLock()
	targets := make([]chan<- segmentation.Input, 0, len(vaults))
	for _, vaultID := range vaults {
		if out, ok := m.vaults[vaultID]; ok {
			targets = append(targets, out)
		}
	}
	m.vmu.RUnlock()

	n := len(targets)
	if n == 0 {
		sendAck(in.Ack, nil)
		return
	}

	if in.Ack == nil {
		for _, out := range targets {
			if !deliver(ctx, out, segmentation.Input{Record: rec}) {
				return
			}
		}
		return
	}

	if n == 1 {
		deliver(ctx, targets[0], segmentation.Input{Record: rec, Ack: in.Ack})
		return
	}

	// Multi-vault fan-out: join the per-vault commit acks into the single source ack.
	children := newAckJoin(n, in.Ack)
	for i, out := range targets {
		if !deliver(ctx, out, segmentation.Input{Record: rec, Ack: children[i]}) {
			// deliver already nacked children[i]; release the still-undelivered
			// children so the join resolves instead of leaking its collector.
			for j := i + 1; j < n; j++ {
				children[j] <- ctx.Err()
			}
			return
		}
	}
}

// deliver enqueues item to a vault's segmentation queue, nacking item.Ack if the
// context is cancelled before the send completes. Returns false on cancellation.
func deliver(ctx context.Context, out chan<- segmentation.Input, item segmentation.Input) bool {
	select {
	case out <- item:
		return true
	case <-ctx.Done():
		if item.Ack != nil {
			item.Ack <- ctx.Err()
		}
		return false
	}
}

func sendAck(ack chan<- error, err error) {
	if ack != nil {
		ack <- err
	}
}
