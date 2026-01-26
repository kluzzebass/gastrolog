package file

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/kluzzebass/gastrolog/internal/chunk"
)

type Reader struct {
	reader io.ReaderAt
	closer io.Closer
}

func NewReader(reader io.ReaderAt, closer io.Closer) *Reader {
	return &Reader{reader: reader, closer: closer}
}

func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{reader: file, closer: file}, nil
}

func (r *Reader) ReadRecordAt(offset int64) (chunk.Record, uint32, int64, error) {
	var sizeBuf [SizeFieldBytes]byte
	if err := readFullAt(r.reader, sizeBuf[:], offset); err != nil {
		return chunk.Record{}, 0, offset, err
	}
	size := binary.LittleEndian.Uint32(sizeBuf[:])
	if size < MinRecordSize {
		return chunk.Record{}, 0, offset, ErrRecordTooSmall
	}

	buf := make([]byte, size)
	copy(buf[:SizeFieldBytes], sizeBuf[:])
	if err := readFullAt(r.reader, buf[SizeFieldBytes:], offset+SizeFieldBytes); err != nil {
		return chunk.Record{}, 0, offset, err
	}

	record, localID, err := DecodeRecord(buf)
	if err != nil {
		return chunk.Record{}, 0, offset, err
	}
	return record, localID, offset + int64(size), nil
}

func (r *Reader) ReadRecordBefore(offset int64) (chunk.Record, uint32, int64, error) {
	if offset < MinRecordSize {
		return chunk.Record{}, 0, 0, ErrNoPreviousRecord
	}
	var sizeBuf [SizeFieldBytes]byte
	if err := readFullAt(r.reader, sizeBuf[:], offset-SizeFieldBytes); err != nil {
		return chunk.Record{}, 0, 0, err
	}
	size := int64(binary.LittleEndian.Uint32(sizeBuf[:]))
	start := offset - size
	if start < 0 {
		return chunk.Record{}, 0, 0, ErrSizeMismatch
	}
	record, localID, _, err := r.ReadRecordAt(start)
	if err != nil {
		return chunk.Record{}, 0, 0, err
	}
	return record, localID, start, nil
}

func (r *Reader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

func readFullAt(reader io.ReaderAt, buf []byte, offset int64) error {
	for len(buf) > 0 {
		n, err := reader.ReadAt(buf, offset)
		if n > 0 {
			buf = buf[n:]
			offset += int64(n)
		}
		if err != nil {
			if err == io.EOF && len(buf) == 0 {
				return nil
			}
			return err
		}
	}
	return nil
}
