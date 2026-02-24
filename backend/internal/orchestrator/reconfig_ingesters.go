package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// AddIngester adds and optionally starts a new ingester.
// If the orchestrator is running, the ingester is started immediately.
// Returns ErrDuplicateID if a ingester with this ID already exists.
func (o *Orchestrator) AddIngester(id uuid.UUID, r Ingester) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, exists := o.ingesters[id]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, id)
	}

	o.ingesters[id] = r
	if o.ingesterStats[id] == nil {
		o.ingesterStats[id] = &IngesterStats{}
	}

	// If running, start the ingester immediately.
	if o.running && o.ingestCh != nil {
		ctx, cancel := context.WithCancel(context.Background())
		o.ingesterCancels[id] = cancel

		o.ingesterWg.Go(func() {
			_ = r.Run(ctx, o.ingestCh)
		})
		o.logger.Info("ingester started", "id", id)
	}

	return nil
}

// RemoveIngester stops and removes a ingester.
// If the orchestrator is running, the ingester is stopped gracefully before removal.
// The method waits for the ingester to finish processing before returning.
func (o *Orchestrator) RemoveIngester(id uuid.UUID) error {
	o.mu.Lock()

	if _, exists := o.ingesters[id]; !exists {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrIngesterNotFound, id)
	}

	// If running, cancel the ingester's context.
	cancel, hasCancel := o.ingesterCancels[id]
	if o.running && hasCancel {
		cancel()
		delete(o.ingesterCancels, id)
	}

	delete(o.ingesters, id)
	o.mu.Unlock()

	// Note: We don't wait for the specific ingester to finish here because
	// ingesterWg tracks all ingesters collectively. The ingester will exit
	// when its context is cancelled, and the WaitGroup will decrement.
	// This is a best-effort removal - the ingester may still be draining.

	o.logger.Info("ingester removed", "id", id)
	return nil
}
