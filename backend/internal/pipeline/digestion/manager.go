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

// ErrAlreadyRunning is returned when Run is called twice.
var ErrAlreadyRunning = errors.New("digestion manager already running")

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

// Run consumes messages until in is closed, then closes the output channel
// after all workers exit. Run blocks until completion.
//
// Shutdown is close-driven, not ctx-driven (gastrolog-5kcq5q): the producer
// MUST close in on every exit path — ingestion.Manager guarantees this on
// both Stop and context cancellation — and downstream MUST drain m.out until
// it closes. Workers receive and send with plain channel ops: the previous
// per-record 2-case selects (recv+ctx, send+ctx, twice more in a feeder
// goroutine bridging an unbuffered hand-off channel) put four select
// rendezvous on every record and showed up as runtime sellock spin at ~31%
// of calm-profile CPU. Blocking sends on the bounded out queue are the
// backpressure mechanism — never bypass them.
func (m *Manager) Run(ctx context.Context, in <-chan ingestion.Message) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer close(m.out)

	for range m.workers {
		m.wg.Go(func() {
			m.worker(in)
		})
	}

	m.wg.Wait()
	return ctx.Err()
}

func (m *Manager) worker(in <-chan ingestion.Message) {
	for msg := range in {
		m.out <- m.digest(msg)
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
