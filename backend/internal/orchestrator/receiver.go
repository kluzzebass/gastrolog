package orchestrator

import (
	"context"
	"log/slog"
	"time"
)

// IngestMessage is the data emitted by receivers for ingestion.
// Receivers provide attributes for identity resolution; the orchestrator
// resolves these to a SourceID via the SourceRegistry.
type IngestMessage struct {
	Attrs    map[string]string
	Raw      []byte
	IngestTS time.Time // when the receiver received this message
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

// ReceiverFactory creates a Receiver from configuration parameters.
// Factories validate required params, apply defaults, and return a fully
// constructed receiver or a descriptive error.
// Factories must not start goroutines or perform I/O beyond validation.
//
// The logger parameter is optional. If nil, the receiver disables logging.
// Factories should scope the logger with component-specific attributes.
//
// This type is defined in the orchestrator package because Receiver is
// defined here. Concrete factory implementations live in their respective
// receiver packages (e.g., syslog.NewFactory()). The orchestrator never
// contains receiver construction logic - it only calls factories.
type ReceiverFactory func(params map[string]string, logger *slog.Logger) (Receiver, error)
