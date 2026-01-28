package chunk

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrNoMoreRecords = errors.New("no more records")
var ErrChunkNotSealed = errors.New("chunk is not sealed")
var ErrChunkNotFound = errors.New("chunk not found")

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

type RecordRef struct {
	ChunkID ChunkID
	Pos     uint64
}

type ChunkMeta struct {
	ID      ChunkID
	StartTS time.Time
	EndTS   time.Time
	Size    int64
	Sealed  bool
}

type ChunkManager interface {
	Append(record Record) (ChunkID, uint64, error)
	Seal() error
	Active() *ChunkMeta
	Meta(id ChunkID) (ChunkMeta, error)
	List() ([]ChunkMeta, error)
	OpenCursor(id ChunkID) (RecordCursor, error)
}

type RecordCursor interface {
	Next() (Record, RecordRef, error)
	Prev() (Record, RecordRef, error)
	Seek(ref RecordRef) error
	Close() error
}

type MetaStore interface {
	Save(meta ChunkMeta) error
	Load(id ChunkID) (ChunkMeta, error)
	List() ([]ChunkMeta, error)
}

type Record struct {
	IngestTS time.Time
	WriteTS  time.Time
	SourceID SourceID
	Raw      []byte
}
