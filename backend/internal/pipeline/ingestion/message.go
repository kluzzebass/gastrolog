package ingestion

import (
	"context"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/record"
)

// IngestMessage is emitted by IngestionManager after EventID minting. It is the
// digestion queue element.
type IngestMessage struct {
	EventID  record.EventID
	Attrs    map[string]string
	Raw      []byte
	RawOwned bool         // when true, Raw is exclusively owned and need not be copied
	SourceTS time.Time    // when the log was generated at the source (zero if unknown)
	Ack      chan<- error // optional ingestion ack; non-nil for RELP-style sources
}

// IngesterMessage is what ingesters emit before minting. IngestionManager
// stamps EventID and forwards an IngestMessage to the digestion queue.
type IngesterMessage struct {
	Attrs    map[string]string
	Raw      []byte
	RawOwned bool
	SourceTS time.Time // when the log was generated at the source (zero if unknown)
	Ack      chan<- error

	// IngestTS is when the ingester received this message, and IngesterID is
	// the identity of the ingester that produced it. Ingesters stamp both on
	// every message (the identity contract; see ingester/identitytest).
	// Minting owns the EventID, so IngestionManager does not forward these
	// onto the digestion queue — they are the ingester-side identity
	// assertion, not digestion-queue payload.
	IngestTS   time.Time
	IngesterID string
}

// Ingester is a source of log messages.
//
// Run starts the ingester and emits messages to the output channel. It blocks
// until ctx is cancelled or an unrecoverable error occurs; implementations must
// select on ctx.Done() to ensure prompt shutdown.
//
// Ingesters are dumb: they know nothing about routing, fan-out, topology,
// SourceRegistry, ChunkManager, or indexing. They only produce messages; the
// receiving node's ingestion pipeline owns everything downstream.
type Ingester interface {
	Run(ctx context.Context, out chan<- IngesterMessage) error
}

// Checkpointable is an optional interface for ingesters that persist resumable
// state. IngestionManager periodically calls SaveCheckpoint when wired, and the
// orchestrator replicates the opaque blob via Raft. On failover, LoadCheckpoint
// restores state before Run() so the new instance resumes where the old one
// stopped.
type Checkpointable interface {
	SaveCheckpoint() ([]byte, error)
	LoadCheckpoint(data []byte) error
}

// PressureAware is an optional interface for ingesters that throttle themselves
// when the ingest pipeline is backed up. SetPressureGate is called before the
// ingester starts; the ingester then consults gate.Wait(ctx) before emitting
// (each record or batch) to block while pressure is elevated or critical.
// Ingesters that don't implement this interface run at full rate.
type PressureAware interface {
	SetPressureGate(gate *chanwatch.PressureGate)
}
