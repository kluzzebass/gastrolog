// Package scatterbox provides a deterministic test signal generator.
//
// Unlike chatterbox (random data, random formats), scatterbox produces
// predictable, traceable records with monotonic sequence numbers, precise
// generation timestamps, and a configurable emission rate. Every record
// can be verified end-to-end: gaps, reordering, duplicates, and latency
// are all detectable from the record body alone.
//
// Modes:
//   - Continuous (interval > 0): emits burst records every interval
//   - One-shot (interval = 0): waits for Trigger() calls, emits burst records each time
package scatterbox

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/orchestrator"
)

// Ingester emits deterministic, traceable log records.
// Implements orchestrator.Ingester and orchestrator.Triggerable.
type Ingester struct {
	id       string
	node     string // cluster node ID — embedded in every record body
	interval time.Duration
	burst    int
	trigger  chan struct{} // signaled by Trigger() for one-shot mode

	seq atomic.Uint64 // monotonic sequence counter

	// pressureGate throttles burst emission when the ingest pipeline is
	// backed up. Injected by the orchestrator via SetPressureGate before
	// Run. Nil means no throttling. See gastrolog-4fguu.
	pressureGate *chanwatch.PressureGate
}

// SetPressureGate wires the orchestrator's pressure gate into the ingester.
// Implements orchestrator.PressureAware.
func (s *Ingester) SetPressureGate(gate *chanwatch.PressureGate) {
	s.pressureGate = gate
}

// Run emits records until ctx is cancelled.
// In continuous mode (interval > 0), emits on a timer.
// In one-shot mode (interval = 0), waits for Trigger() calls.
func (s *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	if s.interval == 0 {
		return s.runOneShot(ctx, out)
	}
	return s.runContinuous(ctx, out)
}

func (s *Ingester) runContinuous(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	// Ticker, not Timer+Reset: keeps cadence on wall-clock phase regardless
	// of emitBurst duration or pressure-gate waits. A Timer+Reset pattern
	// drifts by `emitBurst_duration + gate_wait` every cycle, which makes
	// the "deterministic test signal" not actually deterministic. Missed
	// ticks coalesce (Ticker.C is a 1-slot channel), so long stalls don't
	// produce a thundering-herd catch-up.
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-s.trigger:
		}

		s.emitBurst(ctx, out)
	}
}

func (s *Ingester) runOneShot(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.trigger:
			s.emitBurst(ctx, out)
		}
	}
}

func (s *Ingester) emitBurst(ctx context.Context, out chan<- orchestrator.IngestMessage) {
	// Backpressure: pause before emitting if the pipeline is elevated/critical.
	// Returns silently on ctx cancel so the caller's loop can exit.
	if s.pressureGate != nil {
		if err := s.pressureGate.Wait(ctx); err != nil {
			return
		}
	}
	for range s.burst {
		msg := s.generate()
		select {
		case out <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// Trigger causes the ingester to emit one burst of records.
// In one-shot mode, this is the only way to emit.
// In continuous mode, this emits an extra burst immediately.
// Implements orchestrator.Triggerable.
func (s *Ingester) Trigger() {
	select {
	case s.trigger <- struct{}{}:
	default:
		// Non-blocking: if a trigger is already pending, skip.
	}
}

// generate creates a single traceable record. The body embeds seq,
// generation time, ingester id, and cluster node — in a multi-node
// deployment every node's scatterbox runs the same config, so the node
// field is the only thing that tells records from different nodes apart
// (seq is per-instance-monotonic but per-node, so two nodes will produce
// overlapping seq ranges).
func (s *Ingester) generate() orchestrator.IngestMessage {
	seq := s.seq.Add(1)
	now := time.Now()

	body := fmt.Sprintf(
		`{"seq":%d,"generated_at":"%s","ingester":"%s","node":"%s"}`,
		seq,
		now.Format(time.RFC3339Nano),
		s.id,
		s.node,
	)

	return orchestrator.IngestMessage{
		Attrs: map[string]string{
			"ingester_type": "scatterbox",
			"seq":           strconv.FormatUint(seq, 10),
			"node":          s.node,
		},
		Raw: []byte(body),
		// Scatterbox synthesizes its own logs, so SourceTS and IngestTS
		// coincide — there's no upstream timestamp to preserve. Setting
		// SourceTS matches chatterbox's behavior and keeps pipeline
		// invariants uniform across synthetic ingesters.
		SourceTS:   now,
		IngestTS:   now,
		IngesterID: s.id,
	}
}

// Seq returns the current sequence number (for testing/inspection).
func (s *Ingester) Seq() uint64 {
	return s.seq.Load()
}
