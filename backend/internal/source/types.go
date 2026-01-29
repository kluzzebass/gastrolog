// Package source provides source identity management and metadata storage.
package source

import (
	"time"

	"gastrolog/internal/chunk"
)

// Source represents a log source with its metadata.
type Source struct {
	ID         chunk.SourceID
	Attributes map[string]string
	CreatedAt  time.Time
}

// sourceKey is the internal canonical key derived from attributes.
// Used only for identity lookup within SourceRegistry.
type sourceKey string

// Store defines the persistence interface for source metadata.
// Implementations must be safe for concurrent use.
type Store interface {
	// Save persists a source. Called asynchronously by SourceRegistry.
	Save(src *Source) error

	// LoadAll retrieves all persisted sources.
	// Called at startup to populate the in-memory registry.
	LoadAll() ([]*Source, error)
}
