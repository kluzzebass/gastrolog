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
	if entries := o.VaultManifestEntriesIncludingOpen(vaultID); len(entries) > 0 {
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

// manifestRecordCursor adapts QueryOpenChunk's forward iterator to
// RecordCursor. Positions are 0-based merged-order indexes, the same numbering
// every other cursor uses, so a position taken while the chunk is open names
// the same record once it is sealed. A fresh cursor reads forward through the
// iterator; the first Seek switches it to a positional reader that shares
// QueryOpenChunk's span resolution and merge order — opened lazily, with its
// segment mappings cached for the cursor lifetime and released in Close. When
// no positional reader is available the cursor buffers the whole chunk on the
// first Seek or Prev.
type manifestRecordCursor struct {
	chunkID      chunk.ChunkID
	pull         func() (chunk.Record, error, bool)
	stop         func()
	pos          uint64 // records pulled so far; the next pulled record's position
	totalRecords uint64
	openReader   func() (*chunking.OpenChunkReader, error)
	reader       *chunking.OpenChunkReader
	readerErr    error
	positional   bool   // reads go through the positional reader
	idx          uint64 // positional mode: the index the next Next reads; Prev reads idx-1
	buf          []chunk.Record
	bufPos       int // buffered mode: the index the next Next reads; Prev reads bufPos-1
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
		cr := chunking.RecordToChunk(rec)
		cr.Ref = chunk.RecordRef{ChunkID: chunkID, Pos: c.pos}
		c.pos++
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
// EventID dedup), so clamp positioning to what the reader actually serves.
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
	if c.idx > c.totalRecords {
		c.idx = c.totalRecords
	}
	return reader, nil
}

// readAt reads the record at 0-based merged-order index idx through the
// positional reader, whose own positions are 1-based.
func (c *manifestRecordCursor) readAt(reader *chunking.OpenChunkReader, idx uint64) (chunk.Record, error) {
	rec, err := reader.ReadAt(idx + 1)
	if err != nil {
		return chunk.Record{}, err
	}
	cr := chunking.RecordToChunk(rec)
	cr.Ref = chunk.RecordRef{ChunkID: c.chunkID, Pos: idx}
	return cr, nil
}

func (c *manifestRecordCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.positional {
		reader, err := c.ensureReader()
		if err != nil {
			return chunk.Record{}, chunk.RecordRef{}, err
		}
		if c.idx >= c.totalRecords {
			return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
		}
		rec, err := c.readAt(reader, c.idx)
		if err != nil {
			return chunk.Record{}, chunk.RecordRef{}, err
		}
		c.idx++
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
	if c.openReader == nil {
		return c.prevBuffered()
	}
	if !c.positional {
		// A cursor that has only read forward reverses from where it is.
		c.positional = true
		c.idx = c.pos
	}
	if c.idx == 0 {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	reader, err := c.ensureReader()
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}
	if c.idx == 0 { // ensureReader clamped to an empty view
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	c.idx--
	rec, err := c.readAt(reader, c.idx)
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}
	return rec, rec.Ref, nil
}

func (c *manifestRecordCursor) prevBuffered() (chunk.Record, chunk.RecordRef, error) {
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

// Seek positions the cursor so that the next Next returns the record at
// ref.Pos and the next Prev the record before it; ref.Pos == totalRecords is
// the end, where only Prev has anything to return.
func (c *manifestRecordCursor) Seek(ref chunk.RecordRef) error {
	if c.openReader != nil && c.totalRecords > 0 {
		c.positional = true
		c.idx = min(ref.Pos, c.totalRecords)
		return nil
	}
	if err := c.bufferAll(); err != nil {
		return err
	}
	c.bufPos = int(min(ref.Pos, uint64(len(c.buf)))) //nolint:gosec // G115: bounded by len(c.buf)
	return nil
}

// bufferAll drains the forward iterator into memory, for cursors that have no
// positional reader. Records already pulled keep their positions; the buffer
// holds the ones not yet read, indexed from the position the pull reached.
func (c *manifestRecordCursor) bufferAll() error {
	if c.useBuf {
		return nil
	}
	pulled := c.pos
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
	if pulled > 0 {
		// Positions are absolute; pad so buf[pos] is the record at pos.
		padded := make([]chunk.Record, int(pulled), int(pulled)+len(c.buf)) //nolint:gosec // G115: records pulled so far fit an int
		c.buf = append(padded, c.buf...)
	}
	c.useBuf = true
	c.bufPos = len(c.buf)
	return nil
}

// Ensure manifestRecordCursor satisfies chunk.RecordCursor.
var _ chunk.RecordCursor = (*manifestRecordCursor)(nil)
