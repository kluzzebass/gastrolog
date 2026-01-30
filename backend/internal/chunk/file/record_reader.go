package file

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

var (
	ErrUnknownSourceLocalID = errors.New("unknown source local id")
	ErrMmapEmpty            = errors.New("cannot mmap empty file")
)

// mmapCursor is a RecordCursor backed by mmap'd raw.log and idx.log files.
// Used for sealed chunks.
type mmapCursor struct {
	chunkID   chunk.ChunkID
	rawData   []byte
	idxData   []byte
	rawFile   *os.File
	idxFile   *os.File
	sourceMap *SourceMap

	recordCount uint64 // Total records in chunk
	fwdIndex    uint64 // Current forward iteration index
	revIndex    uint64 // Current reverse iteration index (points to next record to return)
	fwdDone     bool
	revDone     bool
}

func newMmapCursor(chunkID chunk.ChunkID, rawPath, idxPath string, sourceMap *SourceMap) (*mmapCursor, error) {
	// Open and mmap idx.log.
	idxFile, err := os.Open(idxPath)
	if err != nil {
		return nil, err
	}
	idxInfo, err := idxFile.Stat()
	if err != nil {
		idxFile.Close()
		return nil, err
	}

	recordCount := RecordCount(idxInfo.Size())

	// Handle empty chunk case.
	if recordCount == 0 {
		idxFile.Close()
		return &mmapCursor{
			chunkID:     chunkID,
			sourceMap:   sourceMap,
			recordCount: 0,
			fwdDone:     true,
			revDone:     true,
		}, nil
	}

	idxData, err := syscall.Mmap(int(idxFile.Fd()), 0, int(idxInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		idxFile.Close()
		return nil, err
	}

	// Open and mmap raw.log.
	rawFile, err := os.Open(rawPath)
	if err != nil {
		syscall.Munmap(idxData)
		idxFile.Close()
		return nil, err
	}
	rawInfo, err := rawFile.Stat()
	if err != nil {
		rawFile.Close()
		syscall.Munmap(idxData)
		idxFile.Close()
		return nil, err
	}

	rawData, err := syscall.Mmap(int(rawFile.Fd()), 0, int(rawInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		rawFile.Close()
		syscall.Munmap(idxData)
		idxFile.Close()
		return nil, err
	}

	return &mmapCursor{
		chunkID:     chunkID,
		rawData:     rawData,
		idxData:     idxData,
		rawFile:     rawFile,
		idxFile:     idxFile,
		sourceMap:   sourceMap,
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
	idxOffset := int(format.HeaderSize) + int(index)*IdxEntrySize
	if idxOffset+IdxEntrySize > len(c.idxData) {
		return chunk.Record{}, ErrInvalidRecordIdx
	}
	entry := DecodeIdxEntry(c.idxData[idxOffset : idxOffset+IdxEntrySize])

	// Read raw data. Note: this returns a slice into mmap'd memory.
	// The slice is only valid until Close() is called. Callers that need
	// the data to outlive the cursor must copy it.
	rawStart := int(format.HeaderSize) + int(entry.RawOffset)
	rawEnd := rawStart + int(entry.RawSize)
	if rawEnd > len(c.rawData) {
		return chunk.Record{}, ErrInvalidEntry
	}
	raw := c.rawData[rawStart:rawEnd]

	// Resolve source ID.
	sourceID, ok := c.sourceMap.Resolve(entry.SourceLocalID)
	if !ok {
		return chunk.Record{}, fmt.Errorf("%w: localID=%d at index=%d chunk=%s", ErrUnknownSourceLocalID, entry.SourceLocalID, index, c.chunkID)
	}

	return BuildRecord(entry, raw, sourceID), nil
}

func (c *mmapCursor) Close() error {
	var errs []error

	if c.rawData != nil {
		if err := syscall.Munmap(c.rawData); err != nil {
			errs = append(errs, err)
		}
		c.rawData = nil
	}
	if c.idxData != nil {
		if err := syscall.Munmap(c.idxData); err != nil {
			errs = append(errs, err)
		}
		c.idxData = nil
	}
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

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

var _ chunk.RecordCursor = (*mmapCursor)(nil)

// stdioCursor is a RecordCursor backed by standard file I/O.
// Used for active (unsealed) chunks where files may still be growing.
type stdioCursor struct {
	chunkID chunk.ChunkID
	rawFile *os.File
	idxFile *os.File
	resolve func(uint32) (chunk.SourceID, bool)

	fwdIndex uint64 // Current forward iteration index
	revIndex uint64 // Current reverse iteration index
	fwdDone  bool
	revDone  bool
}

func newStdioCursor(chunkID chunk.ChunkID, rawPath, idxPath string, resolve func(uint32) (chunk.SourceID, bool)) (*stdioCursor, error) {
	rawFile, err := os.Open(rawPath)
	if err != nil {
		return nil, err
	}

	idxFile, err := os.Open(idxPath)
	if err != nil {
		rawFile.Close()
		return nil, err
	}

	// Get current record count for reverse iteration starting point.
	idxInfo, err := idxFile.Stat()
	if err != nil {
		rawFile.Close()
		idxFile.Close()
		return nil, err
	}
	recordCount := RecordCount(idxInfo.Size())

	return &stdioCursor{
		chunkID:  chunkID,
		rawFile:  rawFile,
		idxFile:  idxFile,
		resolve:  resolve,
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

	// Resolve source ID.
	sourceID, ok := c.resolve(entry.SourceLocalID)
	if !ok {
		return chunk.Record{}, fmt.Errorf("%w: localID=%d at index=%d", ErrUnknownSourceLocalID, entry.SourceLocalID, index)
	}

	return BuildRecordCopy(entry, raw, sourceID), nil
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

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

var _ chunk.RecordCursor = (*stdioCursor)(nil)
