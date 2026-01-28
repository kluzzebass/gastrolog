package file

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"syscall"

	"gastrolog/internal/chunk"
)

var ErrMmapEmpty = errors.New("mmap file is empty")

type MmapReader struct {
	file *os.File
	data []byte
}

func OpenMmapReader(path string) (*MmapReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() == 0 {
		file.Close()
		return nil, ErrMmapEmpty
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}
	return &MmapReader{file: file, data: data}, nil
}

func (r *MmapReader) ReadRecordAt(offset int64) (chunk.Record, uint32, int64, error) {
	if offset < 0 || offset > int64(len(r.data)) {
		return chunk.Record{}, 0, offset, io.EOF
	}
	if offset+int64(SizeFieldBytes) > int64(len(r.data)) {
		return chunk.Record{}, 0, offset, io.ErrUnexpectedEOF
	}

	size := binary.LittleEndian.Uint32(r.data[offset : offset+int64(SizeFieldBytes)])
	if size < MinRecordSize {
		return chunk.Record{}, 0, offset, ErrRecordTooSmall
	}
	end := offset + int64(size)
	if end > int64(len(r.data)) {
		return chunk.Record{}, 0, offset, io.ErrUnexpectedEOF
	}

	record, localID, err := DecodeRecord(r.data[offset:end])
	if err != nil {
		return chunk.Record{}, 0, offset, err
	}
	return record, localID, end, nil
}

func (r *MmapReader) ReadRecordBefore(offset int64) (chunk.Record, uint32, int64, error) {
	if offset < MinRecordSize {
		return chunk.Record{}, 0, 0, ErrNoPreviousRecord
	}
	if offset > int64(len(r.data)) {
		return chunk.Record{}, 0, 0, io.EOF
	}
	size := int64(binary.LittleEndian.Uint32(r.data[offset-SizeFieldBytes : offset]))
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

func (r *MmapReader) Close() error {
	var err error
	if r.data != nil {
		if unmapErr := syscall.Munmap(r.data); unmapErr != nil {
			err = unmapErr
		}
		r.data = nil
	}
	if r.file != nil {
		if closeErr := r.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		r.file = nil
	}
	return err
}
