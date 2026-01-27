package file

import (
	"errors"
	"io"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

var ErrUnknownSourceLocalID = errors.New("unknown source local id")

type recordReadAt interface {
	ReadRecordAt(offset int64) (chunk.Record, uint32, int64, error)
	Close() error
}

type recordReadBefore interface {
	recordReadAt
	ReadRecordBefore(offset int64) (chunk.Record, uint32, int64, error)
}

type recordReader struct {
	reader    recordReadBefore
	resolve   func(uint32) (chunk.SourceID, bool)
	chunkID   chunk.ChunkID
	offset    int64
	endOffset int64
	fwdDone   bool
	revDone   bool
}

func newRecordReader(reader recordReadBefore, resolve func(uint32) (chunk.SourceID, bool), chunkID chunk.ChunkID, endOffset int64) *recordReader {
	return &recordReader{
		reader:    reader,
		resolve:   resolve,
		chunkID:   chunkID,
		offset:    0,
		endOffset: endOffset,
	}
}

func (r *recordReader) Next() (chunk.Record, chunk.RecordRef, error) {
	if r.fwdDone {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	pos := r.offset
	record, localID, next, err := r.reader.ReadRecordAt(pos)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			r.fwdDone = true
			return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
		}
		return chunk.Record{}, chunk.RecordRef{}, err
	}
	sourceID, ok := r.resolve(localID)
	if !ok {
		return chunk.Record{}, chunk.RecordRef{}, ErrUnknownSourceLocalID
	}
	record.SourceID = sourceID
	r.offset = next
	r.fwdDone = false
	r.revDone = false
	return record, chunk.RecordRef{ChunkID: r.chunkID, Pos: uint64(pos)}, nil
}

func (r *recordReader) Prev() (chunk.Record, chunk.RecordRef, error) {
	if r.revDone {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}
	record, localID, prevOffset, err := r.reader.ReadRecordBefore(r.endOffset)
	if err != nil {
		if err == ErrNoPreviousRecord {
			r.revDone = true
			return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
		}
		return chunk.Record{}, chunk.RecordRef{}, err
	}
	sourceID, ok := r.resolve(localID)
	if !ok {
		return chunk.Record{}, chunk.RecordRef{}, ErrUnknownSourceLocalID
	}
	record.SourceID = sourceID
	r.endOffset = prevOffset
	r.fwdDone = false
	r.revDone = false
	return record, chunk.RecordRef{ChunkID: r.chunkID, Pos: uint64(prevOffset)}, nil
}

func (r *recordReader) Seek(ref chunk.RecordRef) error {
	r.offset = int64(ref.Pos)
	r.endOffset = int64(ref.Pos)
	r.fwdDone = false
	r.revDone = false
	return nil
}

func (r *recordReader) Close() error {
	return r.reader.Close()
}

var _ chunk.RecordCursor = (*recordReader)(nil)
