package orchestrator

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
)

// submitIngestIngesterID and submitIngestSeq stand in for the minter of the
// ingester a record would have come through: every record SubmitIngest
// pushes carries a cluster-unique EventID, as a live ingester's do.
var (
	submitIngestIngesterID = glid.New()
	submitIngestSeq        atomic.Uint32
)

// SubmitIngest pushes a single record through the pipeline's routing path the
// same way live ingest does after digestion: an IngestSource SourceContext and
// an optional ack-after-fsync channel. Test-only seam for cluster-acceptance
// tests that need deterministic records flowing through the real route →
// segmentation → distribution → collection → chunking path without running a
// synthetic ingester.
//
// A record without an EventID gets one, minted as an ingester would.
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
	if rec.EventID == (chunk.EventID{}) {
		ts := rec.IngestTS
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		nodeID, _ := glid.ParseUUID(o.localNodeID)
		rec.EventID = chunk.EventID{
			IngesterID: submitIngestIngesterID,
			NodeID:     nodeID,
			IngestTS:   ts,
			IngestSeq:  submitIngestSeq.Add(1),
		}
	}
	prec := convert.ChunkToRecord(rec)
	in := routing.IngestInput(&prec)
	in.Ack = ack
	return pl.Submit(ctx, in)
}
