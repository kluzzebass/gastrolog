package ingestion

import (
	"context"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/record"
)

// Message is emitted by IngestionManager after EventID minting. It is the
// digestion queue element.
type Message struct {
	EventID  record.EventID
	Attrs    map[string]string
	Raw      []byte
	RawOwned bool         // when true, Raw is exclusively owned and need not be copied
	SourceTS time.Time    // when the log was generated at the source (zero if unknown)
	Ack      chan<- error // optional ingestion ack; non-nil for RELP-style sources
}

// IngesterMessage is what ingesters emit before minting. IngestionManager
// stamps EventID and forwards a Message to the digestion queue.
type IngesterMessage struct {
	Attrs    map[string]string
	Raw      []byte
	RawOwned bool
	SourceTS time.Time
	Ack      chan<- error
}

// Ingester is a source of log messages. Implementations must respect context
// cancellation and exit promptly.
type Ingester interface {
	Run(ctx context.Context, out chan<- IngesterMessage) error
}

// Checkpointable is an optional interface for ingesters that persist resumable
// state. IngestionManager periodically calls SaveCheckpoint when wired.
type Checkpointable interface {
	SaveCheckpoint() ([]byte, error)
	LoadCheckpoint(data []byte) error
}

// PressureAware is an optional interface for ingesters that throttle on pipeline
// backpressure via the shared pressure gate.
type PressureAware interface {
	SetPressureGate(gate *chanwatch.PressureGate)
}
