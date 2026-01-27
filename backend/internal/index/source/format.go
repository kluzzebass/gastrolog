package source

import "github.com/kluzzebass/gastrolog/internal/chunk"

// IndexEntry holds all record positions for a single source within a chunk.
type IndexEntry struct {
	SourceID  chunk.SourceID
	Positions []uint64
}
