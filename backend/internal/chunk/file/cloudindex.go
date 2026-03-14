package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/btree"
	"gastrolog/internal/chunk"
)

const cloudIndexFile = "cloud.idx"

// cloudMetaValue is the fixed-size encoded form of cloud chunk metadata.
// Layout (74 bytes):
//   - 9 × int64 (72 bytes): WriteStart, WriteEnd, RecordCount, Bytes, DiskBytes,
//     IngestStart, IngestEnd, SourceStart, SourceEnd — all unix nanos or raw int64
//   - 1 × uint16 (2 bytes): flags (bit 0 = sealed, bit 1 = compressed)
type cloudMetaValue [74]byte

const (
	cloudMetaValSize = 74
	flagSealed       = 1 << 0
	flagCompressed   = 1 << 1
)

func encodeCloudMeta(m *chunkMeta) cloudMetaValue {
	var v cloudMetaValue
	binary.LittleEndian.PutUint64(v[0:8], uint64(m.writeStart.UnixNano()))
	binary.LittleEndian.PutUint64(v[8:16], uint64(m.writeEnd.UnixNano()))
	binary.LittleEndian.PutUint64(v[16:24], uint64(m.recordCount)) //nolint:gosec // recordCount is always non-negative
	binary.LittleEndian.PutUint64(v[24:32], uint64(m.bytes))     //nolint:gosec // bytes is always non-negative
	binary.LittleEndian.PutUint64(v[32:40], uint64(m.diskBytes)) //nolint:gosec // diskBytes is always non-negative
	binary.LittleEndian.PutUint64(v[40:48], uint64(m.ingestStart.UnixNano()))
	binary.LittleEndian.PutUint64(v[48:56], uint64(m.ingestEnd.UnixNano()))
	binary.LittleEndian.PutUint64(v[56:64], uint64(m.sourceStart.UnixNano()))
	binary.LittleEndian.PutUint64(v[64:72], uint64(m.sourceEnd.UnixNano()))
	var flags uint16
	if m.sealed {
		flags |= flagSealed
	}
	if m.compressed {
		flags |= flagCompressed
	}
	binary.LittleEndian.PutUint16(v[72:74], flags)
	return v
}

func decodeCloudMeta(id chunk.ChunkID, v cloudMetaValue) *chunkMeta {
	flags := binary.LittleEndian.Uint16(v[72:74])
	return &chunkMeta{
		id:          id,
		writeStart:  time.Unix(0, int64(binary.LittleEndian.Uint64(v[0:8]))),   //nolint:gosec // nano timestamp round-trip
		writeEnd:    time.Unix(0, int64(binary.LittleEndian.Uint64(v[8:16]))),  //nolint:gosec // nano timestamp round-trip
		recordCount: int64(binary.LittleEndian.Uint64(v[16:24])),               //nolint:gosec // count round-trip
		bytes:       int64(binary.LittleEndian.Uint64(v[24:32])),               //nolint:gosec // round-trip
		diskBytes:   int64(binary.LittleEndian.Uint64(v[32:40])),               //nolint:gosec // round-trip
		ingestStart: time.Unix(0, int64(binary.LittleEndian.Uint64(v[40:48]))), //nolint:gosec // round-trip
		ingestEnd:   time.Unix(0, int64(binary.LittleEndian.Uint64(v[48:56]))), //nolint:gosec // round-trip
		sourceStart: time.Unix(0, int64(binary.LittleEndian.Uint64(v[56:64]))), //nolint:gosec // round-trip
		sourceEnd:   time.Unix(0, int64(binary.LittleEndian.Uint64(v[64:72]))), //nolint:gosec // round-trip
		sealed:      flags&flagSealed != 0,
		compressed:  flags&flagCompressed != 0,
		cloudBacked: true,
	}
}

// cloudIndexCodec is the btree codec for ChunkID → cloudMetaValue.
var cloudIndexCodec = btree.Codec[chunk.ChunkID, cloudMetaValue]{
	KeySize: 16,
	ValSize: cloudMetaValSize,
	PutKey:  func(b []byte, k chunk.ChunkID) { copy(b, k[:]) },
	Key:     func(b []byte) chunk.ChunkID { var id chunk.ChunkID; copy(id[:], b); return id },
	PutVal:  func(b []byte, v cloudMetaValue) { copy(b, v[:]) },
	Val:     func(b []byte) cloudMetaValue { var v cloudMetaValue; copy(v[:], b); return v },
	Compare: func(a, b chunk.ChunkID) int { return bytes.Compare(a[:], b[:]) },
}

// cloudIndex wraps a B+ tree that caches cloud chunk metadata locally.
type cloudIndex struct {
	tree *btree.Tree[chunk.ChunkID, cloudMetaValue]
}

// openCloudIndex opens an existing cloud index or creates a new one.
func openCloudIndex(dir string) (*cloudIndex, error) {
	path := filepath.Join(dir, cloudIndexFile)
	if _, err := os.Stat(path); err == nil {
		tree, err := btree.Open(path, cloudIndexCodec)
		if err != nil {
			return nil, fmt.Errorf("open cloud index: %w", err)
		}
		return &cloudIndex{tree: tree}, nil
	}
	tree, err := btree.Create(path, cloudIndexCodec)
	if err != nil {
		return nil, fmt.Errorf("create cloud index: %w", err)
	}
	return &cloudIndex{tree: tree}, nil
}

// Insert adds or overwrites metadata for a chunk.
func (ci *cloudIndex) Insert(id chunk.ChunkID, meta *chunkMeta) error {
	return ci.tree.Insert(id, encodeCloudMeta(meta))
}

// Delete removes metadata for a chunk. Returns false if not found.
func (ci *cloudIndex) Delete(id chunk.ChunkID) (bool, error) {
	return ci.tree.Delete(id)
}

// Count returns the number of entries in the index.
func (ci *cloudIndex) Count() uint64 {
	return ci.tree.Count()
}

// Sync flushes dirty pages and fsyncs.
func (ci *cloudIndex) Sync() error {
	return ci.tree.Sync()
}

// Close flushes and closes the index file.
func (ci *cloudIndex) Close() error {
	return ci.tree.Close()
}

// LoadAll scans the entire index and returns a map of all cloud chunk metadata.
func (ci *cloudIndex) LoadAll() (map[chunk.ChunkID]*chunkMeta, error) {
	it, err := ci.tree.Scan()
	if err != nil {
		return nil, fmt.Errorf("cloud index scan: %w", err)
	}
	result := make(map[chunk.ChunkID]*chunkMeta, ci.tree.Count())
	for it.Valid() {
		id := it.Key()
		result[id] = decodeCloudMeta(id, it.Value())
		it.Next()
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("cloud index iterate: %w", err)
	}
	return result, nil
}
