package digestion

import "gastrolog/internal/pipeline/ingestion"

// Digester enriches an ingestion message before it becomes a record.
// Implementations may add or modify attributes and may set SourceTS when zero.
// They must not modify Raw. Errors are surfaced on the output stream without
// blocking other workers.
type Digester interface {
	Digest(msg *ingestion.IngestMessage) error
}
