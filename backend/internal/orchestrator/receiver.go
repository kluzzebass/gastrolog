package orchestrator

import "context"

// IngestMessage is the data emitted by receivers for ingestion.
// Receivers provide attributes for identity resolution; the orchestrator
// resolves these to a SourceID via the SourceRegistry.
type IngestMessage struct {
	Attrs map[string]string
	Raw   []byte
}

// Receiver is a source of log messages.
// Implementations must respect context cancellation and exit promptly.
// Receivers do not know about SourceRegistry, ChunkManager, or indexing.
type Receiver interface {
	// Run starts the receiver and emits messages to the output channel.
	// Run blocks until ctx is cancelled or an unrecoverable error occurs.
	// Receivers must select on ctx.Done() to ensure prompt shutdown.
	Run(ctx context.Context, out chan<- IngestMessage) error
}
