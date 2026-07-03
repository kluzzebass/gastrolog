package digestion

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"

	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/record"
)

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("digestion manager not running")

// Config configures a DigestionManager worker pool.
type Config struct {
	// Workers is the number of parallel digest goroutines. Defaults to 4.
	Workers int
	// OutCapacity is the bounded routing queue depth. Defaults to 1000.
	OutCapacity int
	// Digesters run in registration order on each message before record build.
	Digesters []Digester
}

// Manager runs a worker pool that converts ingestion messages into immutable
// record pointers for the routing stage.
type Manager struct {
	workers   int
	out       chan Output
	digesters []Digester

	running atomic.Bool
	wg      sync.WaitGroup
}

// New returns a manager and the read-only routing queue of digested outputs.
func New(cfg Config) (*Manager, <-chan Output) {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.OutCapacity <= 0 {
		cfg.OutCapacity = 1000
	}
	out := make(chan Output, cfg.OutCapacity)
	m := &Manager{
		workers:   cfg.Workers,
		out:       out,
		digesters: cfg.Digesters,
	}
	return m, out
}

// Run consumes messages until in is closed or ctx is cancelled. It closes the
// output channel after all workers exit. Run blocks until completion.
func (m *Manager) Run(ctx context.Context, in <-chan ingestion.Message) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}
	defer close(m.out)

	work := make(chan ingestion.Message)

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
			case msg, ok := <-in:
				if !ok {
					return
				}
				select {
				case work <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	})

	m.wg.Wait()
	return ctx.Err()
}

func (m *Manager) worker(ctx context.Context, in <-chan ingestion.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-in:
			if !ok {
				return
			}
			out := m.digest(msg)
			select {
			case m.out <- out:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *Manager) digest(msg ingestion.Message) Output {
	work := msg
	for _, d := range m.digesters {
		if err := d.Digest(&work); err != nil {
			return Output{Err: err, Ack: msg.Ack}
		}
	}
	rec := buildRecord(work)
	return Output{Record: rec, Ack: msg.Ack}
}

func buildRecord(msg ingestion.Message) *record.Record {
	raw := msg.Raw
	if msg.Ack != nil || !msg.RawOwned {
		raw = append([]byte(nil), msg.Raw...)
	}

	var attrs record.Attributes
	if len(msg.Attrs) > 0 {
		attrs = make(record.Attributes, len(msg.Attrs))
		maps.Copy(attrs, msg.Attrs)
	}

	return &record.Record{
		SourceTS:       msg.SourceTS,
		IngestTS:       msg.EventID.IngestTS,
		EventID:        msg.EventID,
		Attrs:          attrs,
		Raw:            raw,
		WaitForReplica: msg.Ack != nil,
	}
}
