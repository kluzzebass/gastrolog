package chunk

import (
	"encoding/binary"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
)

var (
	ErrAttrsTooLarge    = errors.New("attributes too large to encode")
	ErrInvalidAttrsData = errors.New("invalid attributes data")
)

// Attributes represents record metadata as key-value pairs.
// Attributes are embedded directly in each chunk's attr.log file,
// making chunks fully self-contained.
type Attributes map[string]string

// Encode serializes attributes to binary format.
// Format: [count:u16][keyLen:u16][key bytes][valLen:u16][val bytes]... repeated count times
// Keys are sorted lexicographically for deterministic output.
// Returns error if the encoded size would exceed uint16 (65535 bytes).
func (a Attributes) Encode() ([]byte, error) {
	if len(a) == 0 {
		// Empty attributes: just count=0
		return []byte{0, 0}, nil
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Calculate total size: 2 (count) + sum of (2 + keyLen + 2 + valLen)
	size := 2 // count
	for _, k := range keys {
		v := a[k]
		size += 2 + len(k) + 2 + len(v)
	}

	if size > 65535 {
		return nil, ErrAttrsTooLarge
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(a)))

	offset := 2
	for _, k := range keys {
		v := a[k]

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(k)))
		offset += 2
		copy(buf[offset:], k)
		offset += len(k)

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(v)))
		offset += 2
		copy(buf[offset:], v)
		offset += len(v)
	}

	return buf, nil
}

// DecodeAttributes deserializes attributes from binary format.
// Returns error if the data is malformed.
func DecodeAttributes(data []byte) (Attributes, error) {
	if len(data) < 2 {
		return nil, ErrInvalidAttrsData
	}

	count := int(binary.LittleEndian.Uint16(data[0:2]))
	if count == 0 {
		return Attributes{}, nil
	}

	attrs := make(Attributes, count)
	offset := 2

	for i := 0; i < count; i++ {
		// Read key length
		if offset+2 > len(data) {
			return nil, ErrInvalidAttrsData
		}
		keyLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		// Read key
		if offset+keyLen > len(data) {
			return nil, ErrInvalidAttrsData
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		// Read value length
		if offset+2 > len(data) {
			return nil, ErrInvalidAttrsData
		}
		valLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		// Read value
		if offset+valLen > len(data) {
			return nil, ErrInvalidAttrsData
		}
		val := string(data[offset : offset+valLen])
		offset += valLen

		attrs[key] = val
	}

	return attrs, nil
}

// Copy returns a deep copy of the attributes.
func (a Attributes) Copy() Attributes {
	if a == nil {
		return nil
	}
	cp := make(Attributes, len(a))
	for k, v := range a {
		cp[k] = v
	}
	return cp
}

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
// Note: When reading from file-backed chunks, Raw and Attrs may reference
// mmap'd memory that becomes invalid when the cursor is closed. Callers that
// need the record to outlive the cursor should call Copy().
type Record struct {
	IngestTS time.Time
	WriteTS  time.Time
	Attrs    Attributes
	Raw      []byte
}

// Copy returns a deep copy of the record with its own Raw slice and Attrs map.
// Use this when the record needs to outlive the cursor that created it.
func (r Record) Copy() Record {
	raw := make([]byte, len(r.Raw))
	copy(raw, r.Raw)
	return Record{
		IngestTS: r.IngestTS,
		WriteTS:  r.WriteTS,
		Attrs:    r.Attrs.Copy(),
		Raw:      raw,
	}
}
