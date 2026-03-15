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
// Layout (106 bytes):
//   - 9 × int64 (72 bytes): WriteStart, WriteEnd, RecordCount, Bytes, DiskBytes,
//     IngestStart, IngestEnd, SourceStart, SourceEnd — all unix nanos or raw int64
//   - 1 × uint16 (2 bytes): flags (bit 0 = sealed, bit 1 = compressed)
//   - 4 × int64 (32 bytes): IngestIdxOffset, IngestIdxSize, SourceIdxOffset, SourceIdxSize
//     — GLCB section offsets for embedded TS indexes (0 = none)
type cloudMetaValue [106]byte

const (
	cloudMetaValSize = 106
	flagSealed       = 1 << 0
	flagCompressed   = 1 << 1
)

func encodeCloudMeta(m *chunkMeta) cloudMetaValue {
	var v cloudMetaValue
	binary.LittleEndian.PutUint64(v[0:8], uint64(m.writeStart.UnixNano()))
	binary.LittleEndian.PutUint64(v[8:16], uint64(m.writeEnd.UnixNano()))
	binary.LittleEndian.PutUint64(v[16:24], uint64(m.recordCount)) //nolint:gosec // recordCount is always non-negative
	binary.LittleEndian.PutUint64(v[24:32], uint64(m.bytes))       //nolint:gosec // bytes is always non-negative
	binary.LittleEndian.PutUint64(v[32:40], uint64(m.diskBytes))   //nolint:gosec // diskBytes is always non-negative
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
	binary.LittleEndian.PutUint64(v[74:82], uint64(m.ingestIdxOffset))  //nolint:gosec // offset is always non-negative
	binary.LittleEndian.PutUint64(v[82:90], uint64(m.ingestIdxSize))    //nolint:gosec // size is always non-negative
	binary.LittleEndian.PutUint64(v[90:98], uint64(m.sourceIdxOffset))  //nolint:gosec // offset is always non-negative
	binary.LittleEndian.PutUint64(v[98:106], uint64(m.sourceIdxSize))   //nolint:gosec // size is always non-negative
	return v
}

func decodeCloudMeta(id chunk.ChunkID, v cloudMetaValue) *chunkMeta {
	flags := binary.LittleEndian.Uint16(v[72:74])
	return &chunkMeta{
		id:              id,
		writeStart:      time.Unix(0, int64(binary.LittleEndian.Uint64(v[0:8]))),   //nolint:gosec // nano timestamp round-trip
		writeEnd:        time.Unix(0, int64(binary.LittleEndian.Uint64(v[8:16]))),   //nolint:gosec // nano timestamp round-trip
		recordCount:     int64(binary.LittleEndian.Uint64(v[16:24])),                //nolint:gosec // count round-trip
		bytes:           int64(binary.LittleEndian.Uint64(v[24:32])),                //nolint:gosec // round-trip
		diskBytes:       int64(binary.LittleEndian.Uint64(v[32:40])),                //nolint:gosec // round-trip
		ingestStart:     time.Unix(0, int64(binary.LittleEndian.Uint64(v[40:48]))),  //nolint:gosec // round-trip
		ingestEnd:       time.Unix(0, int64(binary.LittleEndian.Uint64(v[48:56]))),  //nolint:gosec // round-trip
		sourceStart:     time.Unix(0, int64(binary.LittleEndian.Uint64(v[56:64]))),  //nolint:gosec // round-trip
		sourceEnd:       time.Unix(0, int64(binary.LittleEndian.Uint64(v[64:72]))),  //nolint:gosec // round-trip
		sealed:          flags&flagSealed != 0,
		compressed:      flags&flagCompressed != 0,
		cloudBacked:     true,
		ingestIdxOffset: int64(binary.LittleEndian.Uint64(v[74:82])),  //nolint:gosec // round-trip
		ingestIdxSize:   int64(binary.LittleEndian.Uint64(v[82:90])),  //nolint:gosec // round-trip
		sourceIdxOffset: int64(binary.LittleEndian.Uint64(v[90:98])),  //nolint:gosec // round-trip
		sourceIdxSize:   int64(binary.LittleEndian.Uint64(v[98:106])), //nolint:gosec // round-trip
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
// If the existing index has an incompatible codec (e.g., old value size),
// it is deleted and recreated — loadCloudChunksFromStore will repopulate it.
func openCloudIndex(dir string) (*cloudIndex, error) {
	path := filepath.Join(dir, cloudIndexFile)

	tree, err := tryOpenCloudIndex(path)
	if err == nil {
		return &cloudIndex{tree: tree}, nil
	}

	// File doesn't exist or codec mismatch — create fresh.
	tree, err = btree.Create(path, cloudIndexCodec)
	if err != nil {
		return nil, fmt.Errorf("create cloud index: %w", err)
	}
	return &cloudIndex{tree: tree}, nil
}

// tryOpenCloudIndex attempts to open an existing cloud index file.
// Returns the tree on success, or an error if the file doesn't exist
// or has an incompatible codec (in which case the file is removed).
func tryOpenCloudIndex(path string) (*btree.Tree[chunk.ChunkID, cloudMetaValue], error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	tree, err := btree.Open(path, cloudIndexCodec)
	if err != nil {
		if isCodecMismatch(err) {
			_ = os.Remove(path)
		}
		return nil, err
	}
	return tree, nil
}

// isCodecMismatch checks if the error message indicates a codec mismatch.
// Fallback for when ErrCodecMismatch is not a sentinel.
func isCodecMismatch(err error) bool {
	return err != nil && bytes.Contains([]byte(err.Error()), []byte("codec mismatch"))
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

// Lookup returns metadata for a single cloud chunk by ID.
// Returns (nil, false) if the chunk is not in the index.
// Does NOT evict pages — B+ tree path pages (root + internals) are shared
// across lookups and should stay cached for the query's lifetime.
func (ci *cloudIndex) Lookup(id chunk.ChunkID) (*chunkMeta, bool) {
	it, err := ci.tree.FindGE(id)
	if err != nil || !it.Valid() || it.Key() != id {
		return nil, false
	}
	return decodeCloudMeta(id, it.Value()), true
}

// ForEach iterates all entries in the index, calling fn for each.
// Return false from fn to stop iteration early.
// Evicts B+ tree pages after iteration.
func (ci *cloudIndex) ForEach(fn func(chunk.ChunkID, *chunkMeta) bool) error {
	it, err := ci.tree.Scan()
	if err != nil {
		return fmt.Errorf("cloud index scan: %w", err)
	}
	for it.Valid() {
		id := it.Key()
		if !fn(id, decodeCloudMeta(id, it.Value())) {
			break
		}
		it.Next()
	}
	ci.tree.EvictClean()
	if err := it.Err(); err != nil {
		return fmt.Errorf("cloud index iterate: %w", err)
	}
	return nil
}

// EvictClean drops cached B+ tree pages from Go heap.
func (ci *cloudIndex) EvictClean() {
	ci.tree.EvictClean()
}
