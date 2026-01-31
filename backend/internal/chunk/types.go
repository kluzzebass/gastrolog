package chunk

import (
	"time"

	"github.com/google/uuid"
)

// ChunkID uniquely identifies a chunk.
type ChunkID uuid.UUID

func NewChunkID() ChunkID {
	return ChunkID(uuid.Must(uuid.NewV7()))
}

func ParseChunkID(value string) (ChunkID, error) {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return ChunkID{}, err
	}
	return ChunkID(parsed), nil
}

func (id ChunkID) String() string {
	return uuid.UUID(id).String()
}

// SourceID identifies the source of a record.
// A zero-valued SourceID indicates an unknown or unresolved source.
type SourceID uuid.UUID

func NewSourceID() SourceID {
	return SourceID(uuid.Must(uuid.NewV7()))
}

func ParseSourceID(value string) (SourceID, error) {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return SourceID{}, err
	}
	return SourceID(parsed), nil
}

func (id SourceID) String() string {
	return uuid.UUID(id).String()
}

// RecordRef is a reference to a record within a chunk.
type RecordRef struct {
	ChunkID ChunkID
	Pos     uint64
}

// ChunkMeta contains metadata about a chunk.
type ChunkMeta struct {
	ID          ChunkID
	StartTS     time.Time
	EndTS       time.Time
	RecordCount int64
	Sealed      bool
}

// Record is a single log entry.
//
// Note: When reading from file-backed chunks, Raw may be a slice into mmap'd
// memory that becomes invalid when the cursor is closed. Callers that need
// the record to outlive the cursor should call Copy().
type Record struct {
	IngestTS time.Time
	WriteTS  time.Time
	SourceID SourceID
	Raw      []byte
}

// Copy returns a deep copy of the record with its own Raw slice.
// Use this when the record needs to outlive the cursor that created it.
func (r Record) Copy() Record {
	raw := make([]byte, len(r.Raw))
	copy(raw, r.Raw)
	return Record{
		IngestTS: r.IngestTS,
		WriteTS:  r.WriteTS,
		SourceID: r.SourceID,
		Raw:      raw,
	}
}
