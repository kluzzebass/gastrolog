package memory

import "gastrolog/internal/chunk"

type recordReader struct {
	records  []chunk.Record
	chunkID  chunk.ChunkID
	fwdIndex int
	revIndex int
}

func newRecordReader(records []chunk.Record, chunkID chunk.ChunkID) *recordReader {
	copied := make([]chunk.Record, len(records))
	copy(copied, records)
	return &recordReader{records: copied, chunkID: chunkID, revIndex: len(copied)}
}

func (r *recordReader) Next() (chunk.Record, chunk.RecordRef, error) {
	if r.fwdIndex >= len(r.records) {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	pos := r.fwdIndex
	record := r.records[pos]
	r.fwdIndex++
	return record, chunk.RecordRef{ChunkID: r.chunkID, Pos: uint64(pos)}, nil //nolint:gosec // G115: pos is a slice index, always non-negative
}

func (r *recordReader) Prev() (chunk.Record, chunk.RecordRef, error) {
	if r.revIndex <= 0 {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	r.revIndex--
	pos := r.revIndex
	record := r.records[pos]
	return record, chunk.RecordRef{ChunkID: r.chunkID, Pos: uint64(pos)}, nil
}

func (r *recordReader) Seek(ref chunk.RecordRef) error {
	// Clamp to the slice length — see file/record_reader.go mmapCursor.Seek
	// for the fan-out-divergence reasoning (gastrolog-hshgl).
	pos := min(int(ref.Pos), len(r.records)) //nolint:gosec // G115: Pos clamped by len
	r.fwdIndex = pos
	r.revIndex = pos
	return nil
}

func (r *recordReader) Close() error {
	return nil
}

var _ chunk.RecordCursor = (*recordReader)(nil)
