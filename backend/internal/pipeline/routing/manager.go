package routing

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
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
type Input struct {
	Record *record.Record
	Source SourceContext
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
	Vaults map[glid.GLID]chan<- *record.Record
}

// StatsSnapshot is a point-in-time view of routing counters.
type StatsSnapshot struct {
	Matched   uint64 // records that matched a route and were fanned out
	Unmatched uint64 // records with no route match (intentional drop, counted)
}

// Manager matches records to vaults and fans out record pointers.
type Manager struct {
	workers int
	table   *Table
	vaults  map[glid.GLID]chan<- *record.Record

	matched   atomic.Uint64
	unmatched atomic.Uint64
	running   atomic.Bool
	wg        sync.WaitGroup
}

// New returns a routing manager.
func New(cfg Config) *Manager {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	vaults := cfg.Vaults
	if vaults == nil {
		vaults = make(map[glid.GLID]chan<- *record.Record)
	}
	return &Manager{
		workers: cfg.Workers,
		table:   cfg.Table,
		vaults:  vaults,
	}
}

// Stats returns current matched/unmatched counts.
func (m *Manager) Stats() StatsSnapshot {
	return StatsSnapshot{
		Matched:   m.matched.Load(),
		Unmatched: m.unmatched.Load(),
	}
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
	vaults := m.table.Match(rec.Attrs, src)
	if len(vaults) == 0 {
		m.unmatched.Add(1)
		return
	}
	m.matched.Add(1)
	for _, vaultID := range vaults {
		out, ok := m.vaults[vaultID]
		if !ok {
			continue
		}
		select {
		case out <- rec:
		case <-ctx.Done():
			return
		}
	}
}
