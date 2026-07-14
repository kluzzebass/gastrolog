package cloud

import (
	"errors"
	"strconv"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// BlobMetaToChunkMeta converts blob object metadata to ChunkMeta.
// Used by the file vault's sealed backing integration.
func BlobMetaToChunkMeta(id chunk.ChunkID, bm blobstore.BlobInfo) chunk.ChunkMeta {
	meta := chunk.ChunkMeta{
		ID:        id,
		Sealed:    true,
		DiskBytes: bm.Size,
		Bytes:     bm.Size, // overwritten below if raw_bytes is known
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

// --- glcbCursor: random-access cursor over a mmap-backed GLCB Reader ---
//
// Used for both local-only and cloud-backed paths. Cloud-backed callers
// download + unwrap the GLCB into a local file (see DownloadAndUnwrap),
// promote it into the chunk dir, and open it via OpenMappedBlob like any
// local blob. There is no "remote" mode — every read slices the mapping.

type glcbCursor struct {
	reader      *Reader
	id          chunk.ChunkID
	recordCount uint64
	fwdIndex    uint64
	revIndex    uint64
	fwdDone     bool
	revDone     bool

	// onClose, when non-nil, is invoked exactly once at the end of Close to
	// release the per-chunk read lock and MappedBlob retain pin that
	// openLocalGLCBCursor acquired. See gastrolog-26zu1.
	onClose func()
}

// NewSeekableCursorWithClose creates a cursor over a local GLCB Reader and
// runs onClose (when non-nil) exactly once when the cursor is closed
// (typically chunkLock.RUnlock + blob.release).
func NewSeekableCursorWithClose(rd *Reader, id chunk.ChunkID, onClose func()) chunk.RecordCursor {
	return &glcbCursor{
		reader:      rd,
		id:          id,
		recordCount: uint64(rd.Meta().RecordCount),
		fwdIndex:    0,
		revIndex:    uint64(rd.Meta().RecordCount),
		onClose:     onClose,
	}
}

func (c *glcbCursor) RecordCount() uint64 { return c.recordCount }

// ReadFanOutRecord reads one record by position for parallel retention fan-out.
// The record is detached from the GLCB mmap and safe across goroutines.
// PrewarmSequential implements chunk.SequentialPrewarmer for full-scan
// consumers (retention fan-out) — see Reader.PrewarmSequential.
func (c *glcbCursor) PrewarmSequential() {
	c.reader.PrewarmSequential()
}

func (c *glcbCursor) ReadFanOutRecord(pos uint32) (chunk.Record, error) {
	return c.reader.ReadFanOutRecord(pos)
}

func (c *glcbCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.fwdDone || c.fwdIndex >= c.recordCount {
		c.fwdDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	rec, err := c.reader.ReadFanOutRecord(uint32(c.fwdIndex)) //nolint:gosec // G115: bounded by recordCount
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	ref := chunk.RecordRef{ChunkID: c.id, Pos: c.fwdIndex}
	c.fwdIndex++
	return rec, ref, nil
}

func (c *glcbCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	if c.revDone || c.revIndex == 0 {
		c.revDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	c.revIndex--
	rec, err := c.reader.ReadFanOutRecord(uint32(c.revIndex)) //nolint:gosec // G115: bounded by recordCount
	if err != nil {
		c.revIndex++
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	return rec, chunk.RecordRef{ChunkID: c.id, Pos: c.revIndex}, nil
}

func (c *glcbCursor) Seek(ref chunk.RecordRef) error {
	c.fwdIndex = ref.Pos
	c.revIndex = ref.Pos
	c.fwdDone = false
	c.revDone = false
	return nil
}

// NextBatch reads up to max records forward without per-call interface churn.
func (c *glcbCursor) NextBatch(limit int) ([]chunk.Record, error) {
	if limit <= 0 {
		limit = 1
	}
	batch := make([]chunk.Record, 0, limit)
	for len(batch) < limit {
		rec, _, err := c.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			if len(batch) == 0 {
				return nil, chunk.ErrNoMoreRecords
			}
			return batch, nil
		}
		if err != nil {
			return batch, err
		}
		batch = append(batch, rec)
	}
	return batch, nil
}

func (c *glcbCursor) Close() error {
	var err error
	if c.reader != nil {
		err = c.reader.Close()
		c.reader = nil
	}
	if c.onClose != nil {
		c.onClose()
		c.onClose = nil
	}
	return err
}

var (
	_ chunk.RecordFanOutSource = (*glcbCursor)(nil)
	_ chunk.RecordBatchReader  = (*glcbCursor)(nil)
)
