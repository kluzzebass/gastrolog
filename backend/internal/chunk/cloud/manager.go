package cloud

import (
	"strconv"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// BlobMetaToChunkMeta converts blob object metadata to ChunkMeta.
// Used by the file vault's sealed backing integration.
func BlobMetaToChunkMeta(id chunk.ChunkID, bm blobstore.BlobInfo) chunk.ChunkMeta {
	meta := chunk.ChunkMeta{
		ID:         id,
		Sealed:     true,
		Compressed: true,
		DiskBytes:  bm.Size,
		Bytes:      bm.Size, // overwritten below if raw_bytes is known
	}
	if v, ok := bm.Metadata["raw_bytes"]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		if n > 0 {
			meta.Bytes = n
		}
	}
	if v, ok := bm.Metadata["record_count"]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		meta.RecordCount = n
	}
	if v, ok := bm.Metadata["write_start"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.WriteStart = t
	}
	if v, ok := bm.Metadata["write_end"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.WriteEnd = t
	}
	if v, ok := bm.Metadata["ingest_start"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.IngestStart = t
	}
	if v, ok := bm.Metadata["ingest_end"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.IngestEnd = t
	}
	if v, ok := bm.Metadata["source_start"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.SourceStart = t
	}
	if v, ok := bm.Metadata["source_end"]; ok {
		t, _ := time.Parse(time.RFC3339Nano, v)
		meta.SourceEnd = t
	}
	return meta
}

// ObjectMetadata builds blob object metadata from BlobMeta for upload.
// Used by the file vault's sealed backing integration.
func ObjectMetadata(bm BlobMeta) map[string]string {
	md := map[string]string{
		"chunk_id":     bm.ChunkID.String(),
		"vault_id":     bm.VaultID.String(),
		"record_count": strconv.FormatUint(uint64(bm.RecordCount), 10),
		"raw_bytes":    strconv.FormatInt(bm.RawBytes, 10),
	}
	if !bm.WriteStart.IsZero() {
		md["write_start"] = bm.WriteStart.Format(time.RFC3339Nano)
	}
	if !bm.WriteEnd.IsZero() {
		md["write_end"] = bm.WriteEnd.Format(time.RFC3339Nano)
	}
	if !bm.IngestStart.IsZero() {
		md["ingest_start"] = bm.IngestStart.Format(time.RFC3339Nano)
	}
	if !bm.IngestEnd.IsZero() {
		md["ingest_end"] = bm.IngestEnd.Format(time.RFC3339Nano)
	}
	if !bm.SourceStart.IsZero() {
		md["source_start"] = bm.SourceStart.Format(time.RFC3339Nano)
	}
	if !bm.SourceEnd.IsZero() {
		md["source_end"] = bm.SourceEnd.Format(time.RFC3339Nano)
	}
	return md
}

// --- seekableCursor: random-access cursor backed by seekable zstd ---

type seekableCursor struct {
	reader      *Reader
	id          chunk.ChunkID
	recordCount uint64
	fwdIndex    uint64
	revIndex    uint64
	fwdDone     bool
	revDone     bool
}

// NewSeekableCursor creates a seekable cursor from a Reader.
// Used by the file vault's sealed backing integration.
func NewSeekableCursor(rd *Reader, id chunk.ChunkID) chunk.RecordCursor {
	return &seekableCursor{
		reader:      rd,
		id:          id,
		recordCount: uint64(rd.Meta().RecordCount),
		fwdIndex:    0,
		revIndex:    uint64(rd.Meta().RecordCount),
	}
}

func (c *seekableCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.fwdDone || c.fwdIndex >= c.recordCount {
		c.fwdDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	rec, err := c.reader.ReadRecord(uint32(c.fwdIndex)) //nolint:gosec // G115: bounded by recordCount
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	ref := chunk.RecordRef{ChunkID: c.id, Pos: c.fwdIndex}
	c.fwdIndex++
	return rec, ref, nil
}

func (c *seekableCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	if c.revDone || c.revIndex == 0 {
		c.revDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	c.revIndex--
	rec, err := c.reader.ReadRecord(uint32(c.revIndex)) //nolint:gosec // G115: bounded by recordCount
	if err != nil {
		c.revIndex++
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	return rec, chunk.RecordRef{ChunkID: c.id, Pos: c.revIndex}, nil
}

func (c *seekableCursor) Seek(ref chunk.RecordRef) error {
	c.fwdIndex = ref.Pos
	c.revIndex = ref.Pos
	c.fwdDone = false
	c.revDone = false
	return nil
}

func (c *seekableCursor) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
