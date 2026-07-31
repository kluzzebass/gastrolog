package glcb

import (
	"errors"
	"time"

	"gastrolog/internal/chunk"
)

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
	// openLocalGLCBCursor acquired.
	onClose func()
}

// NewGLCBCursor creates a cursor over a local GLCB Reader and
// runs onClose (when non-nil) exactly once when the cursor is closed
// (typically chunkLock.RUnlock + blob.release).
func NewGLCBCursor(rd *Reader, id chunk.ChunkID, onClose func()) chunk.RecordCursor {
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

// PrewarmSequential implements chunk.SequentialPrewarmer for full-scan
// consumers (retention fan-out): it warms the OS page cache for the whole GLCB
// mapping via madvise before the drain scan — see Reader.PrewarmSequential.
func (c *glcbCursor) PrewarmSequential() {
	c.reader.PrewarmSequential()
}

func (c *glcbCursor) ReadFanOutRecord(pos uint32) (chunk.Record, error) {
	return c.reader.ReadFanOutRecord(pos)
}

// ProjectAttrs implements chunk.AttrsProjectionSource: it decodes a record to
// just (writeTS, attrs), skipping the raw payload. See Reader.ProjectAttrs.
func (c *glcbCursor) ProjectAttrs(pos uint32) (time.Time, chunk.Attributes, error) {
	return c.reader.ProjectAttrs(pos)
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
	_ chunk.RecordFanOutSource    = (*glcbCursor)(nil)
	_ chunk.RecordBatchReader     = (*glcbCursor)(nil)
	_ chunk.AttrsProjectionSource = (*glcbCursor)(nil)
)
