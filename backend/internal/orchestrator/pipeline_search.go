package orchestrator

import (
	"iter"
	"os"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/record"
)

// SearchChunkMetasForVault returns every chunk that should participate in
// search for the vault: vault-ctl manifest entries including pipeline active
// and sealing chunks, with timestamp bounds overlaid for time-range selection.
// Falls back to sealed Reader entries plus the legacy chunk-manager active
// head when no vault-ctl FSM is wired on this node.
func (o *Orchestrator) SearchChunkMetasForVault(vaultID glid.GLID) []chunk.ChunkMeta {
	if entries := o.VaultManifestEntriesFromCtlFSM(vaultID); len(entries) > 0 {
		out := make([]chunk.ChunkMeta, 0, len(entries))
		for _, e := range entries {
			m := e.ToChunkMeta()
			o.overlayPipelineChunkMetaBounds(vaultID, &m)
			out = append(out, m)
		}
		return out
	}
	o.mu.RLock()
	v := o.vaults[vaultID]
	o.mu.RUnlock()
	if v == nil || v.Instance == nil || v.Instance.Chunks == nil {
		return nil
	}
	sealed := collectSealedEntries(v.Instance)
	out := make([]chunk.ChunkMeta, 0, len(sealed)+1)
	for _, e := range sealed {
		out = append(out, e.ToChunkMeta())
	}
	if active := v.Instance.Chunks.Active(); active != nil {
		out = append(out, *active)
	}
	return out
}

// OpenPipelineChunkCursor streams records from a pipeline active or sealing
// chunk. When a local GLCB exists it is opened for indexed seek/reverse reads;
// otherwise records come from manifest-listed segment spans.
func (o *Orchestrator) OpenPipelineChunkCursor(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	if !o.isPipelineIngestVault(vaultID) {
		return nil, chunk.ErrChunkNotFound
	}
	manifest := o.pipelineChunkManifest(vaultID, chunkID)
	if manifest == nil || len(manifest.Refs) == 0 {
		return nil, chunk.ErrChunkNotFound
	}
	if chunkRoot, ok := o.pipelineVaultChunkRoot(vaultID); ok {
		glcbPath := chunking.ChunkGLCBPath(chunkRoot, chunkID)
		if _, err := os.Stat(glcbPath); err == nil {
			if cursor, err := chunking.OpenGLCBCursor(glcbPath, chunkID); err == nil {
				return cursor, nil
			}
		}
	}
	root, err := o.originRoot(vaultID)
	if err != nil {
		return nil, err
	}
	locate := chunking.VaultSegmentLocator{Root: root}
	seq, _, err := chunking.QueryOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate:   locate,
	})
	if err != nil {
		return nil, err
	}
	openReader := func() (*chunking.OpenChunkReader, error) {
		reader, _, err := chunking.NewOpenChunkReader(chunking.OpenChunkQueryInput{
			Manifest: manifest,
			Locate:   locate,
		})
		return reader, err
	}
	return newManifestRecordCursor(chunkID, seq, manifest.TotalRecords, openReader), nil
}

// manifestRecordCursor adapts QueryOpenChunk's forward iterator to RecordCursor.
// When openReader is set, reverse seeks and Prev use a positional reader that
// shares QueryOpenChunk's span resolution and merge order — opened lazily on
// the first positional read, with its segment mappings cached for the cursor
// lifetime and released in Close (gastrolog-54mjat).
type manifestRecordCursor struct {
	chunkID      chunk.ChunkID
	pull         func() (chunk.Record, error, bool)
	stop         func()
	pos          uint64
	totalRecords uint64
	openReader   func() (*chunking.OpenChunkReader, error)
	reader       *chunking.OpenChunkReader
	readerErr    error
	revPos       uint64
	fwdExhausted bool
	buf          []chunk.Record
	bufPos       int
	useBuf       bool
}

func newManifestRecordCursor(
	chunkID chunk.ChunkID,
	seq iter.Seq2[record.Record, error],
	totalRecords uint64,
	openReader func() (*chunking.OpenChunkReader, error),
) *manifestRecordCursor {
	pull, stop := iter.Pull2(seq)
	c := &manifestRecordCursor{
		chunkID:      chunkID,
		stop:         stop,
		totalRecords: totalRecords,
		openReader:   openReader,
	}
	c.pull = func() (chunk.Record, error, bool) {
		rec, err, ok := pull()
		if !ok {
			return chunk.Record{}, nil, false
		}
		if err != nil {
			return chunk.Record{}, err, true
		}
		c.pos++
		cr := chunking.RecordToChunk(rec)
		ref := chunk.RecordRef{ChunkID: chunkID, Pos: c.pos}
		cr.Ref = ref
		return cr, nil, true
	}
	return c
}

func (c *manifestRecordCursor) Close() error {
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
	if c.reader != nil {
		_ = c.reader.Close()
		c.reader = nil
	}
	return nil
}

// ensureReader opens the positional reader on first use and caches it (or the
// open error) for the cursor lifetime. The manifest's TotalRecords counts ref
// records; the served merge order can be shorter (missing local segments,
// EventID dedup), so clamp end-relative positioning to what the reader
// actually serves.
func (c *manifestRecordCursor) ensureReader() (*chunking.OpenChunkReader, error) {
	if c.reader != nil {
		return c.reader, nil
	}
	if c.readerErr != nil {
		return nil, c.readerErr
	}
	reader, err := c.openReader()
	if err != nil {
		c.readerErr = err
		return nil, err
	}
	c.reader = reader
	if reader.Len() < c.totalRecords {
		c.totalRecords = reader.Len()
	}
	if c.revPos > c.totalRecords {
		c.revPos = c.totalRecords
	}
	return reader, nil
}

// readAtPos reads the record at 1-based merged-order position pos through the
// cached positional reader.
func (c *manifestRecordCursor) readAtPos(reader *chunking.OpenChunkReader, pos uint64) (chunk.Record, error) {
	rec, err := reader.ReadAt(pos)
	if err != nil {
		return chunk.Record{}, err
	}
	cr := chunking.RecordToChunk(rec)
	cr.Ref = chunk.RecordRef{ChunkID: c.chunkID, Pos: pos}
	return cr, nil
}

func (c *manifestRecordCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.fwdExhausted && c.openReader != nil {
		reader, err := c.ensureReader()
		if err != nil {
			return chunk.Record{}, chunk.RecordRef{}, err
		}
		nextPos := c.revPos + 1
		if nextPos == 0 || nextPos > c.totalRecords {
			return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
		}
		rec, err := c.readAtPos(reader, nextPos)
		if err != nil {
			return chunk.Record{}, chunk.RecordRef{}, err
		}
		c.revPos = nextPos
		return rec, rec.Ref, nil
	}
	if c.useBuf {
		if c.bufPos >= len(c.buf) {
			return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
		}
		rec := c.buf[c.bufPos]
		c.bufPos++
		return rec, rec.Ref, nil
	}
	rec, err, ok := c.pull()
	if !ok {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}
	return rec, rec.Ref, nil
}

func (c *manifestRecordCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	if c.openReader != nil {
		// Match the canonical cursor contract (mmapCursor/stdioCursor): Prev
		// at position 0 is exhaustion — never fall through to bufferAll,
		// which would re-pull and re-map the whole chunk.
		if c.revPos == 0 {
			return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
		}
		reader, err := c.ensureReader()
		if err != nil {
			return chunk.Record{}, chunk.RecordRef{}, err
		}
		if c.revPos == 0 { // ensureReader clamped an end seek to an empty view
			return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
		}
		rec, err := c.readAtPos(reader, c.revPos)
		if err != nil {
			return chunk.Record{}, chunk.RecordRef{}, err
		}
		c.revPos--
		return rec, rec.Ref, nil
	}
	if err := c.bufferAll(); err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}
	if c.bufPos == 0 {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	c.bufPos--
	rec := c.buf[c.bufPos]
	return rec, rec.Ref, nil
}

func (c *manifestRecordCursor) Seek(ref chunk.RecordRef) error {
	if ref.Pos == 0 {
		c.revPos = 0
		c.pos = 0
		c.fwdExhausted = false
		if c.useBuf {
			c.bufPos = 0
		}
		return nil
	}
	if c.openReader != nil && c.totalRecords > 0 {
		if ref.Pos >= c.totalRecords {
			c.revPos = c.totalRecords
			c.fwdExhausted = true
			return nil
		}
		c.revPos = ref.Pos
		c.fwdExhausted = true
		return nil
	}
	if err := c.bufferAll(); err != nil {
		return err
	}
	c.useBuf = true
	if ref.Pos > uint64(len(c.buf)) {
		c.bufPos = len(c.buf)
		return nil
	}
	c.bufPos = int(ref.Pos) //nolint:gosec // G115: Pos bounded by len(c.buf) check above
	if c.bufPos > 0 {
		c.bufPos--
	}
	return nil
}

func (c *manifestRecordCursor) bufferAll() error {
	if c.useBuf {
		return nil
	}
	for {
		rec, err, ok := c.pull()
		if !ok {
			break
		}
		if err != nil {
			return err
		}
		c.buf = append(c.buf, rec)
	}
	c.useBuf = true
	c.bufPos = len(c.buf)
	return nil
}

// Ensure manifestRecordCursor satisfies chunk.RecordCursor.
var _ chunk.RecordCursor = (*manifestRecordCursor)(nil)
