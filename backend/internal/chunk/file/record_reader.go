package file

import (
	"errors"
	"io"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

var ErrUnknownSourceLocalID = errors.New("unknown source local id")

type recordReader struct {
	reader  recordReadAt
	resolve func(uint32) (chunk.SourceID, bool)
	offset  int64
	done    bool
}

type recordReadAt interface {
	ReadRecordAt(offset int64) (chunk.Record, uint32, int64, error)
	Close() error
}

func newRecordReader(reader recordReadAt, resolve func(uint32) (chunk.SourceID, bool)) *recordReader {
	return &recordReader{reader: reader, resolve: resolve}
}

func (r *recordReader) Next() (chunk.Record, error) {
	if r.done {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}
	record, localID, next, err := r.reader.ReadRecordAt(r.offset)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			r.done = true
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		return chunk.Record{}, err
	}
	sourceID, ok := r.resolve(localID)
	if !ok {
		return chunk.Record{}, ErrUnknownSourceLocalID
	}
	record.SourceID = sourceID
	r.offset = next
	return record, nil
}

func (r *recordReader) Close() error {
	return r.reader.Close()
}

var _ chunk.RecordReader = (*recordReader)(nil)

type recordReadBefore interface {
	recordReadAt
	ReadRecordBefore(offset int64) (chunk.Record, uint32, int64, error)
}

type reverseRecordReader struct {
	reader  recordReadBefore
	resolve func(uint32) (chunk.SourceID, bool)
	offset  int64
	done    bool
}

func newReverseRecordReader(reader recordReadBefore, resolve func(uint32) (chunk.SourceID, bool), endOffset int64) *reverseRecordReader {
	return &reverseRecordReader{reader: reader, resolve: resolve, offset: endOffset}
}

func (r *reverseRecordReader) Next() (chunk.Record, error) {
	if r.done {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}
	record, localID, prevOffset, err := r.reader.ReadRecordBefore(r.offset)
	if err != nil {
		if err == ErrNoPreviousRecord {
			r.done = true
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		return chunk.Record{}, err
	}
	sourceID, ok := r.resolve(localID)
	if !ok {
		return chunk.Record{}, ErrUnknownSourceLocalID
	}
	record.SourceID = sourceID
	r.offset = prevOffset
	return record, nil
}

func (r *reverseRecordReader) Close() error {
	return r.reader.Close()
}

var _ chunk.RecordReader = (*reverseRecordReader)(nil)
