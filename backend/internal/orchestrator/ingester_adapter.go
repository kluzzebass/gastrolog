package orchestrator

import (
	"context"
	"errors"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
)

// ingesterAdapter bridges an orchestrator.Ingester (emitting IngestMessage) to
// the ingestion.Ingester interface (emitting ingestion.IngesterMessage).
//
// It also rehomes the liveness and per-ingester stats the V0 live loop used to
// produce: it toggles setIngesterAlive around each run and counts messages and
// bytes as they pass through, feeding the orchestrator's existing observability
// surface (IngesterStats + onIngesterAlive → Raft) so the cluster/UI behavior is
// unchanged once the V0 loop is gone. Per-record write errors now surface
// downstream via the durable-commit ack (ack-after-fsync), so Errors counts
// run-level ingester failures rather than per-record write failures.
//
// EventID minting is owned by ingestion.Manager's Minter, so the V0 fields the
// adapter drops (IngesterID/IngestTS) are intentionally not forwarded.
type ingesterAdapter struct {
	o     *Orchestrator
	id    glid.GLID
	inner Ingester
	stats *IngesterStats
}

var (
	_ ingestion.Ingester      = (*ingesterAdapter)(nil)
	_ ingestion.PressureAware = (*ingesterAdapter)(nil)
)

// newIngesterAdapter wraps inner for the ingestion manager. The returned
// value additionally implements ingestion.Checkpointable when inner is
// Checkpointable, so the manager's checkpoint ticker engages only for ingesters
// that actually persist state.
func (o *Orchestrator) newIngesterAdapter(id glid.GLID, inner Ingester, stats *IngesterStats) ingestion.Ingester {
	base := &ingesterAdapter{o: o, id: id, inner: inner, stats: stats}
	if cp, ok := inner.(Checkpointable); ok {
		return &checkpointingIngesterAdapter{ingesterAdapter: base, cp: cp}
	}
	return base
}

// Run translates the inner ingester's messages onto the digestion queue.
func (a *ingesterAdapter) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	a.o.setIngesterAlive(a.id, a.stats, true)
	defer a.o.setIngesterAlive(a.id, a.stats, false)

	innerOut := make(chan IngestMessage)
	errCh := make(chan error, 1)
	go func() {
		err := a.inner.Run(ctx, innerOut)
		close(innerOut)
		errCh <- err
	}()

	for msg := range innerOut {
		if a.stats != nil {
			a.stats.MessagesIngested.Add(1)
			a.stats.BytesIngested.Add(int64(len(msg.Raw)))
		}
		minted := ingestion.IngesterMessage{
			Attrs:    msg.Attrs,
			Raw:      msg.Raw,
			SourceTS: msg.SourceTS,
			Ack:      msg.Ack,
		}
		select {
		case out <- minted:
		case <-ctx.Done():
			// Inner respects ctx and will return; don't block draining it (a
			// misbehaving ingester must not stall shutdown). Matches the
			// ingestion manager's own run-loop contract.
			return ctx.Err()
		}
	}

	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) && a.stats != nil {
		a.stats.Errors.Add(1)
	}
	return err
}

// SetPressureGate forwards backpressure to inner when it is PressureAware.
func (a *ingesterAdapter) SetPressureGate(gate *chanwatch.PressureGate) {
	if pa, ok := a.inner.(PressureAware); ok {
		pa.SetPressureGate(gate)
	}
}

// checkpointingIngesterAdapter adds checkpoint forwarding for a Checkpointable
// inner ingester.
type checkpointingIngesterAdapter struct {
	*ingesterAdapter
	cp Checkpointable
}

var (
	_ ingestion.Ingester       = (*checkpointingIngesterAdapter)(nil)
	_ ingestion.Checkpointable = (*checkpointingIngesterAdapter)(nil)
)

func (a *checkpointingIngesterAdapter) SaveCheckpoint() ([]byte, error) {
	return a.cp.SaveCheckpoint()
}

func (a *checkpointingIngesterAdapter) LoadCheckpoint(data []byte) error {
	return a.cp.LoadCheckpoint(data)
}
