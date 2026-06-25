package orchestrator

import (
	"iter"

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
// chunk via manifest-listed segment spans (direction D). Returns
// ErrChunkNotFound when the chunk is not an open/sealed manifest on this
// vault or no local segment refs resolve.
func (o *Orchestrator) OpenPipelineChunkCursor(vaultID glid.GLID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	if !o.isPipelineIngestVault(vaultID) {
		return nil, chunk.ErrChunkNotFound
	}
	manifest := o.pipelineChunkManifest(vaultID, chunkID)
	if manifest == nil || len(manifest.Refs) == 0 {
		return nil, chunk.ErrChunkNotFound
	}
	root, err := o.originRoot(vaultID)
	if err != nil {
		return nil, err
	}
	seq, _, err := chunking.QueryOpenChunk(chunking.OpenChunkQueryInput{
		Manifest: manifest,
		Locate:   chunking.VaultSegmentLocator{Root: root},
	})
	if err != nil {
		return nil, err
	}
	return newManifestRecordCursor(chunkID, seq), nil
}

// manifestRecordCursor adapts QueryOpenChunk's forward iterator to RecordCursor.
type manifestRecordCursor struct {
	chunkID chunk.ChunkID
	pull    func() (chunk.Record, error, bool)
	stop    func()
	pos     uint64
	buf     []chunk.Record
	bufPos  int
	useBuf  bool
}

func newManifestRecordCursor(chunkID chunk.ChunkID, seq iter.Seq2[record.Record, error]) *manifestRecordCursor {
	pull, stop := iter.Pull2(seq)
	c := &manifestRecordCursor{chunkID: chunkID, stop: stop}
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
	return nil
}

func (c *manifestRecordCursor) Next() (chunk.Record, chunk.RecordRef, error) {
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
		if c.useBuf {
			c.bufPos = 0
		}
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
