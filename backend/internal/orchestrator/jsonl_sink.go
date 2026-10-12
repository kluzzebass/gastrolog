package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// jsonlChunkSink is the chunk-manager seam of a JSONL vault: it appends a
// sealed chunk's records to the sink file once.
type jsonlChunkSink interface {
	Written(chunk.ChunkID) bool
	WriteChunk(chunk.ChunkID, chunk.RecordIterator) (bool, error)
}

// jsonlSinkRun coalesces a JSONL vault's sink triggers into one run at a time:
// a trigger during a run makes the run pass again, so no landed chunk waits
// for a later trigger.
type jsonlSinkRun struct {
	mu      sync.Mutex
	running bool
	again   bool
}

func jsonlSinkJobName(vaultID glid.GLID) string {
	return "jsonl-sink:" + vaultID.String()
}

// triggerJSONLSink writes, off the caller's goroutine, every sealed chunk of
// a JSONL vault that has landed on this home and is not yet in its sink. It is
// triggered when a GLCB lands and when the vault's instance arrives.
func (o *Orchestrator) triggerJSONLSink(vaultID glid.GLID, ti *VaultInstance) {
	if ti == nil {
		return
	}
	sink, ok := ti.Chunks.(jsonlChunkSink)
	if !ok {
		return
	}
	v, _ := o.jsonlSinks.LoadOrStore(vaultID, &jsonlSinkRun{})
	run := v.(*jsonlSinkRun)
	run.mu.Lock()
	if run.running {
		run.again = true
		run.mu.Unlock()
		return
	}
	run.running = true
	run.mu.Unlock()

	name := jsonlSinkJobName(vaultID)
	o.scheduler.Describe(name, "Write vault "+vaultID.String()+"'s sealed chunks to its JSONL sink")
	o.scheduler.Submit(name, func(context.Context, *JobProgress) {
		for {
			if err := o.writeJSONLSink(vaultID, sink); err != nil {
				o.logger.Warn("jsonl sink: chunks not written; the next landing or restart retries them",
					"vault", vaultID, "error", err)
			}
			run.mu.Lock()
			if !run.again {
				run.running = false
				run.mu.Unlock()
				return
			}
			run.again = false
			run.mu.Unlock()
		}
	})
}

// writeJSONLSink appends, in seal order, every sealed chunk whose agreed
// GLCB is on this home and which the sink has not written.
func (o *Orchestrator) writeJSONLSink(vaultID glid.GLID, sink jsonlChunkSink) error {
	fsm, chunkRoot, ok := o.pipelineGLCBSource(vaultID)
	if !ok {
		return nil
	}
	var sealed []vaultctlfsm.ManifestEntry
	for _, e := range fsm.List() {
		if e.IsSealed() && !sink.Written(e.ID) {
			sealed = append(sealed, e)
		}
	}
	slices.SortFunc(sealed, func(a, b vaultctlfsm.ManifestEntry) int {
		return cmp.Or(a.SealedAt.Compare(b.SealedAt), cmp.Compare(a.ID.String(), b.ID.String()))
	})
	var errs []error
	for _, e := range sealed {
		path := chunking.ChunkGLCBPath(chunkRoot, e.ID)
		if _, agrees, err := chunking.GLCBAgreesWithSeal(path, e); err != nil || !agrees {
			// Not landed here, or not the agreed copy yet: its landing
			// triggers the sink again.
			continue
		}
		if err := writeChunkToSink(sink, path, e.ID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func writeChunkToSink(sink jsonlChunkSink, path string, id chunk.ChunkID) error {
	cursor, err := chunking.OpenGLCBCursor(path, id)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()
	_, err = sink.WriteChunk(id, func() (chunk.Record, error) {
		rec, _, err := cursor.Next()
		return rec, err
	})
	return err
}
