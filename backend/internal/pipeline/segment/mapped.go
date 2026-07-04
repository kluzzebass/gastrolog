package segment

import (
	"bytes"
	"errors"
	"fmt"
	"gastrolog/internal/record"
	"os"
	"path/filepath"
	"syscall"
)

// MappedSegment is a read-only mmap view of a finalized, immutable segment,
// built for bulk merge reads (gastrolog-1rca2d). The GLCB build previously
// issued ~3 preads per merged record (index entry + length prefix + body)
// plus a full-file CRC re-verification per segment open — the third full
// read of data already verified at collection and finalize time. Mapping
// the file makes index-entry and frame reads plain memory access; record
// bodies are still copied out of the mapping by the shared frame decoder,
// so no Record ever aliases the mapping's lifetime.
//
// Trust model: OpenMapped validates the header and the on-disk layout
// arithmetic but does NOT recompute file checksums. Use it only for
// segments that passed full verification when they entered this node
// (PromoteVerified at collection; Finalize at the origin). The built GLCB
// carries its own digest, so corruption still cannot propagate silently
// past the build.
type MappedSegment struct {
	f    *os.File
	data []byte
	rd   *bytes.Reader
	hdr  Header
}

// ErrNotFinalized is returned when a mapped open sees no index tail.
var ErrNotFinalized = errors.New("segment is not finalized")

// OpenMapped maps a finalized segment read-only.
func OpenMapped(path string) (*MappedSegment, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := info.Size()
	if size < int64(HeaderSize) {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %d bytes", ErrHeaderTooSmall, size)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: fd is a small non-negative int; size bounded by segment close policy
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("mmap segment: %w", err)
	}

	hdr, err := decodeHeader(data[:HeaderSize])
	if err != nil {
		_ = syscall.Munmap(data)
		_ = f.Close()
		return nil, err
	}
	if hdr.IndexOffset == 0 {
		_ = syscall.Munmap(data)
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrNotFinalized, path)
	}
	// Layout arithmetic: both index tails must fit inside the mapping.
	indexEnd := int64(hdr.IndexOffset) + int64(hdr.RecordCount)*IndexEntrySize
	sourceEnd := indexEnd + int64(hdr.SourceIndexCount)*SourceIndexEntrySize
	if int64(hdr.IndexOffset) < int64(HeaderSize) || sourceEnd > size {
		_ = syscall.Munmap(data)
		_ = f.Close()
		return nil, errors.New("segment index layout exceeds file size")
	}

	return &MappedSegment{f: f, data: data, rd: bytes.NewReader(data), hdr: hdr}, nil
}

// Header returns the decoded segment header.
func (m *MappedSegment) Header() Header {
	return m.hdr
}

// Len returns the number of records in EventID order.
func (m *MappedSegment) Len() uint32 {
	return m.hdr.RecordCount
}

// IndexEntryAt returns the index entry at position pos in EventID order —
// a 16-byte decode from the mapping, no I/O.
func (m *MappedSegment) IndexEntryAt(pos uint32) (IndexEntry, error) {
	if pos >= m.hdr.RecordCount {
		return IndexEntry{}, ErrIndexBounds
	}
	off := int(m.hdr.IndexOffset) + int(pos)*IndexEntrySize
	return decodeIndexEntry(m.data[off : off+IndexEntrySize])
}

// RecordAtFilePos decodes the record frame starting at filePos. The frame
// body is copied out of the mapping by the shared decoder.
func (m *MappedSegment) RecordAtFilePos(filePos uint32) (record.Record, error) {
	if filePos < HeaderSize || filePos >= m.hdr.IndexOffset {
		return record.Record{}, ErrFrameLength
	}
	rec, _, err := readFrameAt(m.rd, int64(filePos), m.hdr.IndexOffset-filePos)
	return rec, err
}

// Close unmaps and closes the file.
func (m *MappedSegment) Close() error {
	if m.data != nil {
		if err := syscall.Munmap(m.data); err != nil {
			_ = m.f.Close()
			return err
		}
		m.data = nil
	}
	return m.f.Close()
}
