package orchestrator

import (
	"context"
	"errors"

	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
	"gastrolog/internal/pipeline/routing"
)

// SubmitIngest pushes a single record through the pipeline's routing path the
// same way live ingest does after digestion: an IngestSource SourceContext and
// an optional ack-after-fsync channel. Test-only seam for cluster-acceptance
// tests that need deterministic records flowing through the real route →
// segmentation → distribution → collection → chunking path without running a
// synthetic ingester.
//
// When ack is non-nil it resolves only after every matched vault has durably
// committed the record to its local segment (first error wins). The ack
// channel must be buffered.
func (o *Orchestrator) SubmitIngest(ctx context.Context, rec chunk.Record, ack chan<- error) error {
	o.mu.RLock()
	pl := o.pipeline
	o.mu.RUnlock()
	if pl == nil {
		return errors.New("pipeline not initialized")
	}
	prec := convert.ChunkToRecord(rec)
	in := routing.IngestInput(&prec)
	in.Ack = ack
	return pl.Submit(ctx, in)
}
