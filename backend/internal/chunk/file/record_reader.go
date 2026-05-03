package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

var (
	ErrMmapEmpty = errors.New("cannot mmap empty file")
)

// loadDict reads attr_dict.log via mmap, validates its header, and returns a StringDict.
// DecodeDictData copies strings so the mmap region is released immediately after decoding.
func loadDict(dictPath string) (*chunk.StringDict, error) {
	f, err := os.Open(filepath.Clean(dictPath))
	if err != nil {
		return nil, fmt.Errorf("open attr_dict %s: %w", dictPath, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat attr_dict %s: %w", dictPath, err)
	}
	fileSize := info.Size()
	if fileSize < int64(format.HeaderSize) {
		return nil, fmt.Errorf("attr_dict %s too small (%d bytes)", dictPath, fileSize)
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
	if err != nil {
		return nil, fmt.Errorf("mmap attr_dict %s: %w", dictPath, err)
	}
	defer func() { _ = syscall.Munmap(data) }()

	if _, err := format.DecodeAndValidate(data[:format.HeaderSize], format.TypeAttrDict, AttrDictVersion); err != nil {
		return nil, fmt.Errorf("invalid attr_dict header in %s: %w", dictPath, err)
	}
	return chunk.DecodeDictData(data[format.HeaderSize:])
}

// mmapCursor is a RecordCursor backed by mmap'd raw.log, idx.log, and attr.log files.
// Used for sealed chunks.
//
// For uncompressed files, rawData/attrData point to the mmap'd data section
// (after the header), with rawMmap/attrMmap holding the full mmap region for Munmap.
//
// For compressed files, rawSeek/attrSeek provide random-access ReadAt through
// seekable zstd — only the frame(s) covering the requested byte range are
// decompressed. No mmap is needed; memory usage per read is ~one frame (256KB).
type mmapCursor struct {
	chunkID  chunk.ChunkID
	rawData  []byte // data section (no header); nil if compressed
	idxData  []byte // full mmap including header
	attrData []byte // data section (no header); nil if compressed
	dictPath string
	rawMmap  []byte   // full mmap region for Munmap; nil if compressed
	attrMmap []byte   // full mmap region for Munmap; nil if compressed
	rawFile  *os.File // underlying file for mmap or seekable source
	idxFile  *os.File
	attrFile *os.File        // underlying file for mmap or seekable source
	rawSeek  seekable.Reader // seekable reader for compressed raw; nil if mmap'd
	attrSeek seekable.Reader // seekable reader for compressed attr; nil if mmap'd
	dict     *chunk.StringDict

	// onClose, when non-nil, is invoked exactly once at the end of
	// Close() (after munmap + file-close). Used by Manager.OpenCursor
	// to release the per-chunk RWMutex's read lock at the cursor's
	// actual end-of-life — the indexer Build path holds the cursor
	// across many Next() calls and per-record work, and the lock must
	// stay held for that whole duration to prevent gastrolog-26zu1's
	// SIGBUS-on-rename mid-read.
	onClose func()

	recordCount uint64 // Total records in chunk
	fwdIndex    uint64 // Current forward iteration index
	revIndex    uint64 // Current reverse iteration index (points to next record to return)
	fwdDone     bool
	revDone     bool
}

func newMmapCursor(chunkID chunk.ChunkID, rawPath, idxPath, attrPath, dictPath string) (*mmapCursor, error) {
	// Load dictionary from attr_dict.log.
	dict, err := loadDict(dictPath)
	if err != nil {
		return nil, fmt.Errorf("chunk %s: %w", chunkID, err)
	}

	// Open and mmap idx.log.
	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return nil, fmt.Errorf("open idx.log for chunk %s: %w", chunkID, err)
	}
	idxInfo, err := idxFile.Stat()
	if err != nil {
		_ = idxFile.Close()
		return nil, fmt.Errorf("stat idx.log for chunk %s: %w", chunkID, err)
	}

	recordCount := RecordCount(idxInfo.Size())
	// Handle empty chunk case.
	if recordCount == 0 {
		_ = idxFile.Close()
		return &mmapCursor{
			chunkID:     chunkID,
			dict:        dict,
			recordCount: 0,
			fwdDone:     true,
			revDone:     true,
		}, nil
	}

	idxData, err := syscall.Mmap(int(idxFile.Fd()), 0, int(idxInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: uintptr->int and int64->int are safe on 64-bit
	if err != nil {
		_ = idxFile.Close()
		return nil, fmt.Errorf("mmap idx.log for chunk %s: %w", chunkID, err)
	}

	rawData, rawMmap, rawFile, rawSeek, err := openDataFile(rawPath)
	if err != nil {
		_ = syscall.Munmap(idxData)
		_ = idxFile.Close()
		return nil, fmt.Errorf("open raw.log for chunk %s: %w", chunkID, err)
	}

	cleanupRaw := func() {
		if rawSeek != nil {
			_ = rawSeek.Close()
		}
		if rawMmap != nil {
			_ = syscall.Munmap(rawMmap)
		}
		if rawFile != nil {
			_ = rawFile.Close()
		}
		_ = syscall.Munmap(idxData)
		_ = idxFile.Close()
	}

	attrData, attrMmap, attrFile, attrSeek, err := openDataFile(attrPath)
	if err != nil {
		cleanupRaw()
		return nil, fmt.Errorf("open attr.log for chunk %s: %w", chunkID, err)
	}
	return &mmapCursor{
		chunkID:     chunkID,
		rawData:     rawData,
		idxData:     idxData,
		attrData:    attrData,
		dictPath:    dictPath,
		rawMmap:     rawMmap,
		attrMmap:    attrMmap,
		rawFile:     rawFile,
		idxFile:     idxFile,
		attrFile:    attrFile,
		rawSeek:     rawSeek,
		attrSeek:    attrSeek,
		dict:        dict,
		recordCount: recordCount,
		fwdIndex:    0,
		revIndex:    recordCount, // Start past end for reverse iteration
	}, nil
}

func (c *mmapCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.fwdDone || c.fwdIndex >= c.recordCount {
		c.fwdDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	index := c.fwdIndex
	record, err := c.readRecord(index)
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	c.fwdIndex++
	return record, chunk.RecordRef{ChunkID: c.chunkID, Pos: index}, nil
}

func (c *mmapCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	if c.revDone || c.revIndex == 0 {
		c.revDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	c.revIndex--
	index := c.revIndex

	record, err := c.readRecord(index)
	if err != nil {
		c.revIndex++ // Restore on error
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	return record, chunk.RecordRef{ChunkID: c.chunkID, Pos: index}, nil
}

func (c *mmapCursor) Seek(ref chunk.RecordRef) error {
	c.fwdIndex = ref.Pos
	c.revIndex = ref.Pos
	c.fwdDone = false
	c.revDone = false
	return nil
}

func (c *mmapCursor) readRecord(index uint64) (chunk.Record, error) {
	// Read idx.log entry.
	idxOffset := int(IdxHeaderSize) + int(index)*IdxEntrySize //nolint:gosec // G115: index is bounded by record count (< 2^32)
	if idxOffset+IdxEntrySize > len(c.idxData) {
		return chunk.Record{}, ErrInvalidRecordIdx
	}
	entry := DecodeIdxEntry(c.idxData[idxOffset : idxOffset+IdxEntrySize])

	// Read raw data. Offsets are relative to the data section (after header).
	var raw []byte
	if c.rawSeek != nil {
		raw = make([]byte, entry.RawSize)
		if _, err := c.rawSeek.ReadAt(raw, int64(entry.RawOffset)); err != nil {
			return chunk.Record{}, err
		}
	} else {
		rawStart := int(entry.RawOffset)
		rawEnd := rawStart + int(entry.RawSize)
		if rawEnd > len(c.rawData) {
			return chunk.Record{}, fmt.Errorf("%w: chunk %s record %d: raw range [%d:%d] exceeds mmap size %d",
				ErrInvalidEntry, c.chunkID, c.fwdIndex, rawStart, rawEnd, len(c.rawData))
		}
		raw = c.rawData[rawStart:rawEnd]
	}

	// Read and decode attributes using dictionary.
	var attrBuf []byte
	if c.attrSeek != nil {
		attrBuf = make([]byte, entry.AttrSize)
		if _, err := c.attrSeek.ReadAt(attrBuf, int64(entry.AttrOffset)); err != nil {
			return chunk.Record{}, err
		}
	} else {
		attrStart := int(entry.AttrOffset)
		attrEnd := attrStart + int(entry.AttrSize)
		if attrEnd > len(c.attrData) {
			return chunk.Record{}, fmt.Errorf("%w: chunk %s record %d: attr range [%d:%d] exceeds mmap size %d",
				ErrInvalidEntry, c.chunkID, c.fwdIndex, attrStart, attrEnd, len(c.attrData))
		}
		attrBuf = c.attrData[attrStart:attrEnd]
	}

	attrs, err := chunk.DecodeWithDict(attrBuf, c.dict)
	if err != nil {
		return chunk.Record{}, err
	}

	// For compressed chunks, raw and attrs are freshly allocated — use them
	// directly. For mmap'd chunks, raw and attrBuf reference mmap'd memory
	// so we must copy to ensure the record outlives the cursor.
	if c.rawSeek != nil {
		return BuildRecord(entry, raw, attrs), nil
	}
	return BuildRecordCopy(entry, raw, attrs), nil
}

func (c *mmapCursor) Close() error {
	var errs []error

	if c.rawSeek != nil {
		if err := c.rawSeek.Close(); err != nil {
			errs = append(errs, err)
		}
		c.rawSeek = nil
	}
	if c.rawMmap != nil {
		if err := syscall.Munmap(c.rawMmap); err != nil {
			errs = append(errs, err)
		}
		c.rawMmap = nil
	}
	c.rawData = nil

	if c.idxData != nil {
		if err := syscall.Munmap(c.idxData); err != nil {
			errs = append(errs, err)
		}
		c.idxData = nil
	}

	if c.attrSeek != nil {
		if err := c.attrSeek.Close(); err != nil {
			errs = append(errs, err)
		}
		c.attrSeek = nil
	}
	if c.attrMmap != nil {
		if err := syscall.Munmap(c.attrMmap); err != nil {
			errs = append(errs, err)
		}
		c.attrMmap = nil
	}
	c.attrData = nil

	if c.rawFile != nil {
		if err := c.rawFile.Close(); err != nil {
			errs = append(errs, err)
		}
		c.rawFile = nil
	}
	if c.idxFile != nil {
		if err := c.idxFile.Close(); err != nil {
			errs = append(errs, err)
		}
		c.idxFile = nil
	}
	if c.attrFile != nil {
		if err := c.attrFile.Close(); err != nil {
			errs = append(errs, err)
		}
		c.attrFile = nil
	}

	if c.onClose != nil {
		c.onClose()
		c.onClose = nil
	}

	return errors.Join(errs...)
}

var _ chunk.RecordCursor = (*mmapCursor)(nil)

// stdioCursor is a RecordCursor backed by standard file I/O.
// Used for active (unsealed) chunks where files may still be growing.
type stdioCursor struct {
	chunkID  chunk.ChunkID
	rawFile  *os.File
	idxFile  *os.File
	attrFile *os.File
	dict     *chunk.StringDict
	dictPath string // path to attr_dict.log for reloading

	// onClose mirrors mmapCursor.onClose — invoked exactly once at the
	// end of Close to release the per-chunk RWMutex's read lock that
	// Manager.OpenCursor acquired. Active (unsealed) cursors take the
	// lock too because compress/delete on an active chunk shouldn't
	// race with cursor reads either, even though the active rotation
	// boundary normally serializes them at a higher layer.
	onClose func()

	fwdIndex uint64 // Current forward iteration index
	revIndex uint64 // Current reverse iteration index
	fwdDone  bool
	revDone  bool
}

func newStdioCursor(chunkID chunk.ChunkID, rawPath, idxPath, attrPath, dictPath string) (*stdioCursor, error) {
	// Load dictionary from attr_dict.log.
	dict, err := loadDict(dictPath)
	if err != nil {
		return nil, fmt.Errorf("chunk %s: %w", chunkID, err)
	}

	rawFile, err := os.Open(filepath.Clean(rawPath))
	if err != nil {
		return nil, fmt.Errorf("open raw.log for chunk %s: %w", chunkID, err)
	}

	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		_ = rawFile.Close()
		return nil, fmt.Errorf("open idx.log for chunk %s: %w", chunkID, err)
	}

	attrFile, err := os.Open(filepath.Clean(attrPath))
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		return nil, fmt.Errorf("open attr.log for chunk %s: %w", chunkID, err)
	}

	// Get current record count.
	idxInfo, err := idxFile.Stat()
	if err != nil {
		_ = rawFile.Close()
		_ = idxFile.Close()
		_ = attrFile.Close()
		return nil, fmt.Errorf("stat idx.log for chunk %s: %w", chunkID, err)
	}
	recordCount := RecordCount(idxInfo.Size())

	return &stdioCursor{
		chunkID:  chunkID,
		rawFile:  rawFile,
		idxFile:  idxFile,
		attrFile: attrFile,
		dict:     dict,
		dictPath: dictPath,
		fwdIndex: 0,
		revIndex: recordCount,
	}, nil
}

func (c *stdioCursor) Next() (chunk.Record, chunk.RecordRef, error) {
	if c.fwdDone {
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	// Re-check file size in case more records were appended.
	idxInfo, err := c.idxFile.Stat()
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}
	recordCount := RecordCount(idxInfo.Size())

	if c.fwdIndex >= recordCount {
		c.fwdDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	index := c.fwdIndex
	record, err := c.readRecord(index)
	if err != nil {
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	c.fwdIndex++
	return record, chunk.RecordRef{ChunkID: c.chunkID, Pos: index}, nil
}

func (c *stdioCursor) Prev() (chunk.Record, chunk.RecordRef, error) {
	if c.revDone || c.revIndex == 0 {
		c.revDone = true
		return chunk.Record{}, chunk.RecordRef{}, chunk.ErrNoMoreRecords
	}

	c.revIndex--
	index := c.revIndex

	record, err := c.readRecord(index)
	if err != nil {
		c.revIndex++ // Restore on error
		return chunk.Record{}, chunk.RecordRef{}, err
	}

	return record, chunk.RecordRef{ChunkID: c.chunkID, Pos: index}, nil
}

func (c *stdioCursor) Seek(ref chunk.RecordRef) error {
	c.fwdIndex = ref.Pos
	c.revIndex = ref.Pos
	c.fwdDone = false
	c.revDone = false
	return nil
}

func (c *stdioCursor) readRecord(index uint64) (chunk.Record, error) {
	// Read idx.log entry.
	idxOffset := IdxFileOffset(index)
	var entryBuf [IdxEntrySize]byte
	if _, err := c.idxFile.ReadAt(entryBuf[:], idxOffset); err != nil {
		return chunk.Record{}, err
	}
	entry := DecodeIdxEntry(entryBuf[:])

	// Read raw data.
	rawOffset := int64(format.HeaderSize) + int64(entry.RawOffset)
	raw := make([]byte, entry.RawSize)
	if _, err := c.rawFile.ReadAt(raw, rawOffset); err != nil {
		return chunk.Record{}, err
	}

	// Read and decode attributes using dictionary.
	attrOffset := int64(format.HeaderSize) + int64(entry.AttrOffset)
	attrBuf := make([]byte, entry.AttrSize)
	if _, err := c.attrFile.ReadAt(attrBuf, attrOffset); err != nil {
		return chunk.Record{}, err
	}
	attrs, err := chunk.DecodeWithDict(attrBuf, c.dict)
	if errors.Is(err, chunk.ErrInvalidAttrsData) {
		// Dict may be stale — reload from disk and retry once.
		if fresh, loadErr := loadDict(c.dictPath); loadErr == nil {
			c.dict = fresh
			attrs, err = chunk.DecodeWithDict(attrBuf, c.dict)
		}
	}
	if err != nil {
		return chunk.Record{}, err
	}

	// stdio reads are from file (not mmap), so raw and attrs are already
	// freshly allocated — no copy needed.
	return BuildRecord(entry, raw, attrs), nil
}

func (c *stdioCursor) Close() error {
	var errs []error

	if c.rawFile != nil {
		if err := c.rawFile.Close(); err != nil {
			errs = append(errs, err)
		}
		c.rawFile = nil
	}
	if c.idxFile != nil {
		if err := c.idxFile.Close(); err != nil {
			errs = append(errs, err)
		}
		c.idxFile = nil
	}
	if c.attrFile != nil {
		if err := c.attrFile.Close(); err != nil {
			errs = append(errs, err)
		}
		c.attrFile = nil
	}

	if c.onClose != nil {
		c.onClose()
		c.onClose = nil
	}

	return errors.Join(errs...)
}

var _ chunk.RecordCursor = (*stdioCursor)(nil)

// scanAttrsSealed iterates all records in a sealed chunk, reading only idx.log
// and attr.log (skipping raw.log entirely). For uncompressed chunks, idx and attr
// are mmap'd; for compressed chunks, attr uses seekable zstd.
func scanAttrsSealed(idxPath, attrPath, dictPath string, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	dict, err := loadDict(dictPath)
	if err != nil {
		return err
	}

	// Mmap idx.log (always uncompressed).
	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return fmt.Errorf("open idx.log %s: %w", idxPath, err)
	}
	defer func() { _ = idxFile.Close() }()

	idxInfo, err := idxFile.Stat()
	if err != nil {
		return fmt.Errorf("stat idx.log %s: %w", idxPath, err)
	}
	recordCount := RecordCount(idxInfo.Size())
	if recordCount == 0 {
		return nil
	}

	idxData, err := syscall.Mmap(int(idxFile.Fd()), 0, int(idxInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: safe on 64-bit
	if err != nil {
		return fmt.Errorf("mmap idx.log %s: %w", idxPath, err)
	}
	defer func() { _ = syscall.Munmap(idxData) }()

	// Open attr.log (may be compressed or mmap'd).
	attrData, attrMmap, attrFile, attrSeek, err := openDataFile(attrPath)
	if err != nil {
		return err
	}
	defer func() {
		if attrSeek != nil {
			_ = attrSeek.Close()
		}
		if attrMmap != nil {
			_ = syscall.Munmap(attrMmap)
		}
		if attrFile != nil {
			_ = attrFile.Close()
		}
	}()

	for i := startPos; i < recordCount; i++ {
		idxOffset := int(IdxHeaderSize) + int(i)*IdxEntrySize //nolint:gosec // G115: bounded by record count
		if idxOffset+IdxEntrySize > len(idxData) {
			return ErrInvalidRecordIdx
		}
		entry := DecodeIdxEntry(idxData[idxOffset : idxOffset+IdxEntrySize])

		var attrBuf []byte
		if attrSeek != nil {
			attrBuf = make([]byte, entry.AttrSize)
			if _, err := attrSeek.ReadAt(attrBuf, int64(entry.AttrOffset)); err != nil {
				return fmt.Errorf("read compressed attr at record %d: %w", i, err)
			}
		} else {
			attrStart := int(entry.AttrOffset)
			attrEnd := attrStart + int(entry.AttrSize)
			if attrEnd > len(attrData) {
				return ErrInvalidEntry
			}
			attrBuf = attrData[attrStart:attrEnd]
		}

		attrs, err := chunk.DecodeWithDict(attrBuf, dict)
		if err != nil {
			return fmt.Errorf("decode attrs at record %d: %w", i, err)
		}

		if !fn(entry.WriteTS, attrs) {
			return nil
		}
	}
	return nil
}

// scanAttrsActive iterates all records in the active (unsealed) chunk using
// stdio reads of idx.log and attr.log, skipping raw.log. Loads the dict from
// disk to avoid racing with concurrent Append calls on the live dict.
// scanIngestAttrsActive iterates an active chunk's idx + attr files,
// invoking fn with each record's IngestTS and Attributes. Reads the entire
// idx.log in one syscall (small: 12 bytes/record), then a contiguous attr
// region per record range. Used by the histogram path on non-monotonic
// active chunks. See gastrolog-66b7x.
func scanIngestAttrsActive(idxPath, attrPath, dictPath string, fn func(ingestTS time.Time, attrs chunk.Attributes) bool) error {
	dict, err := loadDict(dictPath)
	if err != nil {
		return err
	}
	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return fmt.Errorf("open idx.log %s: %w", idxPath, err)
	}
	defer func() { _ = idxFile.Close() }()
	idxInfo, err := idxFile.Stat()
	if err != nil {
		return fmt.Errorf("stat idx.log %s: %w", idxPath, err)
	}
	recordCount := RecordCount(idxInfo.Size())
	if recordCount == 0 {
		return nil
	}
	idxBuf := make([]byte, recordCount*IdxEntrySize)
	if _, err := idxFile.ReadAt(idxBuf, IdxHeaderSize); err != nil {
		return fmt.Errorf("read idx.log: %w", err)
	}
	attrFile, err := os.Open(filepath.Clean(attrPath))
	if err != nil {
		return fmt.Errorf("open attr.log %s: %w", attrPath, err)
	}
	defer func() { _ = attrFile.Close() }()
	attrInfo, err := attrFile.Stat()
	if err != nil {
		return fmt.Errorf("stat attr.log %s: %w", attrPath, err)
	}
	attrSize := attrInfo.Size() - int64(format.HeaderSize)
	attrAll := make([]byte, attrSize)
	if _, err := attrFile.ReadAt(attrAll, int64(format.HeaderSize)); err != nil {
		return fmt.Errorf("read attr.log: %w", err)
	}
	for i := range recordCount {
		entry := DecodeIdxEntry(idxBuf[i*IdxEntrySize : (i+1)*IdxEntrySize])
		attrEnd := int64(entry.AttrOffset) + int64(entry.AttrSize)
		attrs, err := chunk.DecodeWithDict(attrAll[entry.AttrOffset:attrEnd], dict)
		if err != nil {
			return fmt.Errorf("decode attrs at record %d: %w", i, err)
		}
		if !fn(entry.IngestTS, attrs) {
			return nil
		}
	}
	return nil
}

func scanAttrsActive(idxPath, attrPath, dictPath string, startPos uint64, fn func(writeTS time.Time, attrs chunk.Attributes) bool) error {
	dict, err := loadDict(dictPath)
	if err != nil {
		return err
	}

	idxFile, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return fmt.Errorf("open idx.log %s: %w", idxPath, err)
	}
	defer func() { _ = idxFile.Close() }()

	idxInfo, err := idxFile.Stat()
	if err != nil {
		return fmt.Errorf("stat idx.log %s: %w", idxPath, err)
	}
	recordCount := RecordCount(idxInfo.Size())
	if recordCount == 0 {
		return nil
	}

	attrFile, err := os.Open(filepath.Clean(attrPath))
	if err != nil {
		return fmt.Errorf("open attr.log %s: %w", attrPath, err)
	}
	defer func() { _ = attrFile.Close() }()

	var entryBuf [IdxEntrySize]byte
	for i := startPos; i < recordCount; i++ {
		if _, err := idxFile.ReadAt(entryBuf[:], IdxFileOffset(i)); err != nil {
			return fmt.Errorf("read idx entry at record %d: %w", i, err)
		}
		entry := DecodeIdxEntry(entryBuf[:])

		attrOffset := int64(format.HeaderSize) + int64(entry.AttrOffset)
		attrBuf := make([]byte, entry.AttrSize)
		if _, err := attrFile.ReadAt(attrBuf, attrOffset); err != nil {
			return fmt.Errorf("read attr at record %d: %w", i, err)
		}

		attrs, err := chunk.DecodeWithDict(attrBuf, dict)
		if err != nil {
			return fmt.Errorf("decode attrs at record %d: %w", i, err)
		}

		if !fn(entry.WriteTS, attrs) {
			return nil
		}
	}
	return nil
}

func openDataFile(path string) (data []byte, mmapRegion []byte, file *os.File, seek seekable.Reader, err error) {
	// Multi-file path is now exclusively the unsealed-active fallback
	// (sealed chunks live as data.glcb — gastrolog-24m1t). Active raw.log
	// is always uncompressed, so there's no FlagCompressed branch
	// anymore: open, header-read for validation, then mmap.
	file, err = os.Open(filepath.Clean(path))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open %s: %w", path, err)
	}

	var hdr [format.HeaderSize]byte
	if _, err := io.ReadFull(file, hdr[:]); err != nil {
		_ = file.Close()
		return nil, nil, nil, nil, fmt.Errorf("read header for %s: %w", path, err)
	}
	if _, err := format.Decode(hdr[:]); err != nil {
		_ = file.Close()
		return nil, nil, nil, nil, fmt.Errorf("decode header for %s: %w", path, err)
	}

	// stat and mmap the file we already have open. Our fd pins us to
	// this inode regardless of any subsequent path swap.
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, nil, nil, fmt.Errorf("stat %s: %w", path, err)
	}
	mmapRegion, err = syscall.Mmap(int(file.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: uintptr->int and int64->int are safe on 64-bit
	if err != nil {
		_ = file.Close()
		return nil, nil, nil, nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	return mmapRegion[format.HeaderSize:], mmapRegion, file, nil, nil
}
