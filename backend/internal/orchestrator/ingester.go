package orchestrator

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// IngestMessage is the data emitted by ingesters for ingestion.
// Ingesters provide attributes that are stored alongside the raw log data.
// Attributes are passed through directly to chunk storage without transformation.
type IngestMessage struct {
	Attrs    map[string]string
	Raw      []byte
	SourceTS time.Time    // when the log was generated at the source (zero if unknown)
	IngestTS time.Time    // when the ingester received this message
	Ack      chan<- error // optional: if non-nil, receives nil on success or error on failure
}

// Ingester is a source of log messages.
// Implementations must respect context cancellation and exit promptly.
// Ingesters do not know about SourceRegistry, ChunkManager, or indexing.
type Ingester interface {
	// Run starts the ingester and emits messages to the output channel.
	// Run blocks until ctx is cancelled or an unrecoverable error occurs.
	// Ingesters must select on ctx.Done() to ensure prompt shutdown.
	Run(ctx context.Context, out chan<- IngestMessage) error
}

// IngesterFactory creates a Ingester from configuration parameters.
// Factories validate required params, apply defaults, and return a fully
// constructed ingester or a descriptive error.
// Factories must not start goroutines or perform I/O beyond validation.
//
// The logger parameter is optional. If nil, the ingester disables logging.
// Factories should scope the logger with component-specific attributes.
//
// This type is defined in the orchestrator package because Ingester is
// defined here. Concrete factory implementations live in their respective
// ingester packages (e.g., syslog.NewFactory()). The orchestrator never
// contains ingester construction logic - it only calls factories.
type IngesterFactory func(id uuid.UUID, params map[string]string, logger *slog.Logger) (Ingester, error)
