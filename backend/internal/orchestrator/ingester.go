package orchestrator

import (
	"gastrolog/internal/glid"
	"context"
	"log/slog"
	"time"

	"gastrolog/internal/chanwatch"

)

// IngestMessage is the data emitted by ingesters for ingestion.
// Ingesters provide attributes that are stored alongside the raw log data.
// Attributes are passed through directly to chunk storage without transformation.
type IngestMessage struct {
	Attrs      map[string]string
	Raw        []byte
	SourceTS   time.Time    // when the log was generated at the source (zero if unknown)
	IngestTS   time.Time    // when the ingester received this message
	IngesterID string       // identity of the ingester that produced this message
	Ack        chan<- error // optional: if non-nil, receives nil on success or error on failure
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

// Triggerable is an optional interface for ingesters that support
// on-demand record emission. The operator can trigger a one-shot
// burst via the UI without restarting the ingester.
type Triggerable interface {
	Trigger()
}

// Checkpointable is an optional interface for ingesters that persist
// resumable state. The orchestrator periodically calls SaveCheckpoint and
// replicates the opaque blob via Raft. On failover, LoadCheckpoint restores
// state before Run() so the new instance resumes where the old one stopped.
type Checkpointable interface {
	SaveCheckpoint() ([]byte, error)
	LoadCheckpoint(data []byte) error
}

// PressureAware is an optional interface for ingesters that can throttle
// themselves when the ingest pipeline is backed up. The orchestrator calls
// SetPressureGate before starting the ingester; the ingester then consults
// gate.Wait(ctx) before emitting each record (or each batch) to block while
// pressure is elevated or critical. Ingesters that don't implement this
// interface run at full rate — they inherit the previous (unthrottled)
// behavior. See gastrolog-4fguu.
type PressureAware interface {
	SetPressureGate(gate *chanwatch.PressureGate)
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
type IngesterFactory func(id glid.GLID, params map[string]string, logger *slog.Logger) (Ingester, error)
