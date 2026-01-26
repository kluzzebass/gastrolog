package memory

import "github.com/kluzzebass/gastrolog/internal/chunk"

type recordReader struct {
	records []chunk.Record
	index   int
}

func newRecordReader(records []chunk.Record) *recordReader {
	copied := make([]chunk.Record, len(records))
	copy(copied, records)
	return &recordReader{records: copied}
}

func (r *recordReader) Next() (chunk.Record, error) {
	if r.index >= len(r.records) {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

func (r *recordReader) Close() error {
	return nil
}

var _ chunk.RecordReader = (*recordReader)(nil)
