package chunk

import (
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
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
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(a))) //nolint:gosec // G115: attribute count bounded by size check above

	offset := 2
	for _, k := range keys {
		v := a[k]

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(k))) //nolint:gosec // G115: key length bounded by size check above
		offset += 2
		copy(buf[offset:], k)
		offset += len(k)

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(v))) //nolint:gosec // G115: value length bounded by size check above
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

	for range count {
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
	maps.Copy(cp, a)
	return cp
}

// chunkIDEncoding is base32hex (RFC 4648) lowercase without padding.
// Alphabet 0-9a-v preserves lexicographic sort order.
var chunkIDEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// ChunkID uniquely identifies a chunk.
// It is a UUIDv7 (16 bytes) whose string representation is 26-char lowercase
// base32hex, lexicographically sortable by creation time.
type ChunkID [16]byte

// NewChunkID creates a ChunkID from a new UUIDv7.
// UUIDv7 embeds a millisecond timestamp and guarantees monotonically increasing IDs.
func NewChunkID() ChunkID {
	return ChunkID(uuid.Must(uuid.NewV7()))
}

// ParseChunkID parses a 26-character base32hex string into a ChunkID.
func ParseChunkID(value string) (ChunkID, error) {
	if len(value) != 26 {
		return ChunkID{}, fmt.Errorf("invalid chunk ID length: %d (want 26)", len(value))
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

// String returns the 26-character lowercase base32hex representation.
func (id ChunkID) String() string {
	return strings.ToLower(chunkIDEncoding.EncodeToString(id[:]))
}

// Time returns the creation time encoded in the UUIDv7 ChunkID.
// UUIDv7 stores millisecond Unix timestamp in bytes 0-5 (48 bits, big-endian).
func (id ChunkID) Time() time.Time {
	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 |
		int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	return time.UnixMilli(ms)
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
	Bytes       int64 // Total logical bytes (raw + attr + idx)
	Sealed      bool
	Compressed  bool  // true if raw.log/attr.log are compressed
	DiskBytes   int64 // actual on-disk size (may differ from Bytes if compressed)

	// IngestTS and SourceTS bounds (zero = unknown).
	// Used to filter chunks by ingest_start/ingest_end and source_start/source_end
	// without scanning records.
	IngestStart time.Time // min IngestTS in chunk
	IngestEnd   time.Time // max IngestTS in chunk
	SourceStart time.Time // min SourceTS (excluding zero)
	SourceEnd   time.Time // max SourceTS in chunk
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
	StoreID  uuid.UUID
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
		Ref:      r.Ref,
		StoreID:  r.StoreID,
	}
}
