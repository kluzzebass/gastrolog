package chunk

import (
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"
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

// chunkIDEncoding is base32hex (RFC 4648) lowercase without padding.
// Alphabet 0-9a-v preserves lexicographic sort order.
var chunkIDEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// ChunkID uniquely identifies a chunk.
// It encodes a creation timestamp as big-endian uint64 unix microseconds.
// The string representation is 13-char lowercase base32hex, lexicographically
// sortable by creation time.
type ChunkID [8]byte

// lastChunkMicro ensures NewChunkID returns monotonically increasing IDs
// even when called multiple times within the same microsecond.
var lastChunkMicro atomic.Int64

// NewChunkID creates a ChunkID from the current time.
// It guarantees monotonically increasing IDs even under rapid creation.
func NewChunkID() ChunkID {
	now := time.Now().UnixMicro()
	for {
		old := lastChunkMicro.Load()
		next := max(now, old+1)
		if lastChunkMicro.CompareAndSwap(old, next) {
			var id ChunkID
			binary.BigEndian.PutUint64(id[:], uint64(next))
			return id
		}
	}
}

// ChunkIDFromTime creates a ChunkID from the given time.
func ChunkIDFromTime(t time.Time) ChunkID {
	var id ChunkID
	binary.BigEndian.PutUint64(id[:], uint64(t.UnixMicro()))
	return id
}

// ParseChunkID parses a 13-character base32hex string into a ChunkID.
func ParseChunkID(value string) (ChunkID, error) {
	if len(value) != 13 {
		return ChunkID{}, fmt.Errorf("invalid chunk ID length: %d (want 13)", len(value))
	}
	// base32hex decode expects uppercase
	decoded, err := chunkIDEncoding.DecodeString(strings.ToUpper(value))
	if err != nil {
		return ChunkID{}, fmt.Errorf("invalid chunk ID: %w", err)
	}
	var id ChunkID
	copy(id[:], decoded)
	return id, nil
}

// String returns the 13-character lowercase base32hex representation.
func (id ChunkID) String() string {
	return strings.ToLower(chunkIDEncoding.EncodeToString(id[:]))
}

// Time returns the creation time encoded in the ChunkID.
func (id ChunkID) Time() time.Time {
	us := int64(binary.BigEndian.Uint64(id[:]))
	return time.UnixMicro(us)
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
// Timestamps:
//   - SourceTS: when the log was generated at the source (e.g., parsed from syslog timestamp)
//   - IngestTS: when our ingester received the message
//   - WriteTS:  when the chunk manager wrote the record (monotonic within a chunk)
//
// Ref and StoreID are populated by the query engine when returning search
// results. They are zero-valued when appending records or reading via cursor.
//
// Note: When reading from file-backed chunks, Raw and Attrs may reference
// mmap'd memory that becomes invalid when the cursor is closed. Callers that
// need the record to outlive the cursor should call Copy().
type Record struct {
	SourceTS time.Time
	IngestTS time.Time
	WriteTS  time.Time
	Attrs    Attributes
	Raw      []byte
	Ref      RecordRef
	StoreID  string
}

// Copy returns a deep copy of the record with its own Raw slice and Attrs map.
// Use this when the record needs to outlive the cursor that created it.
func (r Record) Copy() Record {
	raw := make([]byte, len(r.Raw))
	copy(raw, r.Raw)
	return Record{
		SourceTS: r.SourceTS,
		IngestTS: r.IngestTS,
		WriteTS:  r.WriteTS,
		Attrs:    r.Attrs.Copy(),
		Raw:      raw,
	}
}
