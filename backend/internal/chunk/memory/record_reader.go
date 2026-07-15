package memory

import (
	"errors"

	"gastrolog/internal/chunk"
)

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
	r.fwdIndex = int(ref.Pos) //nolint:gosec // G115: Pos is bounded by slice length
	r.revIndex = int(ref.Pos) //nolint:gosec // G115: Pos is bounded by slice length
	return nil
}

func (r *recordReader) RecordCount() uint64 {
	return uint64(len(r.records))
}

func (r *recordReader) ReadFanOutRecord(pos uint32) (chunk.Record, error) {
	if int(pos) >= len(r.records) {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}
	return r.records[pos], nil
}

func (r *recordReader) NextBatch(limit int) ([]chunk.Record, error) {
	if limit <= 0 {
		limit = 1
	}
	batch := make([]chunk.Record, 0, limit)
	for len(batch) < limit {
		rec, _, err := r.Next()
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

func (r *recordReader) Close() error {
	return nil
}

var (
	_ chunk.RecordCursor       = (*recordReader)(nil)
	_ chunk.RecordFanOutSource = (*recordReader)(nil)
	_ chunk.RecordBatchReader  = (*recordReader)(nil)
)
