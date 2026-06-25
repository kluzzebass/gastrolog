package cloud

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/glid"
)

const (
	stagingBufferSize = 256 << 10
	copyBufferSize    = 1024 << 10
)

// tsEntry is a (timestamp, position) pair for the embedded TS index.
type tsEntry struct {
	ts  int64
	pos uint32
}

// Writer encodes GLCB. When bound to an output file in workDir, record frames
// stream directly into the blob after the fixed header (no staging copy). Otherwise
// frames spill to a buffered work file during Add; finalize copies once on WriteTo.
// workDir must be the directory where the finished blob will live so temp files
// and atomic rename stay on one filesystem.
type Writer struct {
	chunkID chunk.ChunkID
	vaultID glid.GLID
	dict    *chunk.StringDict

	workDir string

	output      *os.File // optional target for Finish / WriteTo / BindOutput
	staging     *os.File
	stagingPath string
	stagingBuf  *bufio.Writer

	direct     bool
	directFile *os.File
	directBuf  *bufio.Writer
	blobHash   hash.Hash

	recordIndex []recordIndex
	sectionOff  uint64
	count       uint32
	bounds      blobBounds

	ingestEntries []tsEntry
	sourceEntries []tsEntry
	toc           BlobTOC

	ingestMonotonic bool
	hasIngestTS     bool
	lastIngestTS    time.Time

	frameScratch []byte
}

// NewWriter creates a writer. workDir is the directory where the finished
// GLCB will reside (chunk dir or pipeline chunk root); partial files are
// created there so rename to the final name never crosses filesystems.
func NewWriter(chunkID chunk.ChunkID, vaultID glid.GLID, workDir string) (*Writer, error) {
	if workDir == "" {
		return nil, errors.New("work directory required: pass the directory where the GLCB will live")
	}
	return &Writer{
		chunkID:         chunkID,
		vaultID:         vaultID,
		workDir:         workDir,
		dict:            chunk.NewStringDict(),
		ingestMonotonic: true,
	}, nil
}

// ReserveRecords pre-sizes per-record metadata slices when the merge record
// count is known ahead of time (pipeline GLCB builds).
func (w *Writer) ReserveRecords(n uint32) {
	if n == 0 {
		return
	}
	w.recordIndex = make([]recordIndex, 0, n)
	w.ingestEntries = make([]tsEntry, 0, n)
	w.sourceEntries = make([]tsEntry, 0, n)
}

// IngestTSMonotonic reports whether IngestTS was non-decreasing in GLCB record
// order (the same order records were passed to Add).
func (w *Writer) IngestTSMonotonic() bool {
	return w.ingestMonotonic
}

// WorkDirForFile returns the directory where GLCB partial files must be
// created alongside f so atomic rename to f stays on one filesystem.
func WorkDirForFile(f *os.File) (string, error) {
	if f == nil {
		return "", errors.New("nil output file")
	}
	name := f.Name()
	if name == "" {
		return "", errors.New("output file has no path: open a file in its final directory")
	}
	return filepath.Dir(name), nil
}

// OpenWriter prepares to write a GLCB into f on Finish. Records stream
// directly into f after the fixed header.
func OpenWriter(f *os.File, chunkID chunk.ChunkID, vaultID glid.GLID) (*Writer, error) {
	workDir, err := WorkDirForFile(f)
	if err != nil {
		return nil, err
	}
	w, err := NewWriter(chunkID, vaultID, workDir)
	if err != nil {
		return nil, err
	}
	if err := w.BindOutput(f); err != nil {
		return nil, err
	}
	return w, nil
}

// BindOutput streams record frames into f after the fixed GLCB header. f must
// live in workDir. Call before the first Add.
func (w *Writer) BindOutput(f *os.File) error {
	if err := w.checkOutputWorkDir(f); err != nil {
		return err
	}
	w.output = f
	return w.beginDirect(f)
}

func (w *Writer) beginDirect(f *os.File) error {
	if w.direct {
		return nil
	}
	pre, err := encodePreamble()
	if err != nil {
		return err
	}
	var layoutPad [layoutMetaSize]byte
	w.blobHash = sha256.New()
	if _, err := w.blobHash.Write(pre); err != nil {
		return err
	}
	if _, err := f.Write(pre); err != nil {
		return err
	}
	if _, err := f.Write(layoutPad[:]); err != nil {
		return err
	}
	w.direct = true
	w.directFile = f
	w.directBuf = bufio.NewWriterSize(f, stagingBufferSize)
	return nil
}

func (w *Writer) ensureStaging() error {
	if w.staging != nil {
		return nil
	}
	f, err := os.CreateTemp(w.workDir, "glcb-records-*.tmp")
	if err != nil {
		return err
	}
	w.staging = f
	w.stagingPath = f.Name()
	w.stagingBuf = bufio.NewWriterSize(f, stagingBufferSize)
	return nil
}

// Close removes the record staging file when present.
func (w *Writer) Close() error {
	return w.closeStaging()
}

func (w *Writer) closeStaging() error {
	if w.stagingBuf != nil {
		if err := w.stagingBuf.Flush(); err != nil {
			return err
		}
		w.stagingBuf = nil
	}
	if w.staging == nil {
		return nil
	}
	path := w.stagingPath
	err := w.staging.Close()
	w.staging = nil
	w.stagingPath = ""
	if path != "" {
		_ = os.Remove(path)
	}
	return err
}

func (w *Writer) noteIngestTS(ts time.Time) {
	if w.hasIngestTS && ts.Before(w.lastIngestTS) {
		w.ingestMonotonic = false
	}
	w.lastIngestTS = ts
	w.hasIngestTS = true
}

// Add encodes one record and appends [frameLen][frame] to the output stream.
func (w *Writer) Add(rec chunk.Record) error {
	if w.output != nil && !w.direct {
		if err := w.beginDirect(w.output); err != nil {
			return err
		}
	}
	if !w.direct {
		if err := w.ensureStaging(); err != nil {
			return err
		}
	}

	frame, err := encodeRecordFrame(rec, w.dict)
	if err != nil {
		return err
	}

	pos := w.count
	w.bounds.update(rec)
	w.noteIngestTS(rec.IngestTS)

	bodySize := uint32(len(frame)) //nolint:gosec // G115: frame size bounded by record limits
	w.recordIndex = append(w.recordIndex, recordIndex{
		Offset: w.sectionOff + 4,
		Size:   bodySize,
	})
	w.sectionOff += 4 + uint64(bodySize)

	var frameLenBuf [4]byte
	binary.LittleEndian.PutUint32(frameLenBuf[:], bodySize)
	w.frameScratch = append(w.frameScratch[:0], frameLenBuf[:]...)
	w.frameScratch = append(w.frameScratch, frame...)

	var sink *bufio.Writer
	if w.direct {
		sink = w.directBuf
	} else {
		sink = w.stagingBuf
	}
	if _, err := sink.Write(w.frameScratch); err != nil {
		return fmt.Errorf("write record frame: %w", err)
	}

	w.ingestEntries = append(w.ingestEntries, tsEntry{ts: rec.IngestTS.UnixNano(), pos: pos})
	if !rec.SourceTS.IsZero() {
		w.sourceEntries = append(w.sourceEntries, tsEntry{ts: rec.SourceTS.UnixNano(), pos: pos})
	}
	w.count++
	return nil
}

// Finish writes the complete GLCB to the file passed to OpenWriter.
func (w *Writer) Finish() (BlobTOC, error) {
	if w.output == nil {
		return BlobTOC{}, errors.New("Finish requires OpenWriter")
	}
	if _, err := w.emitBlob(w.output); err != nil {
		_ = w.closeStaging()
		return BlobTOC{}, err
	}
	if err := w.closeStaging(); err != nil {
		return BlobTOC{}, err
	}
	return w.toc, nil
}

// WriteTo writes a complete GLCB to dst. When dst is a named *os.File it must
// live in workDir so the build never spans filesystems.
func (w *Writer) WriteTo(dst io.Writer) (int64, error) {
	if err := w.checkOutputWorkDir(dst); err != nil {
		return 0, err
	}
	n, err := w.emitBlob(dst)
	if closeErr := w.closeStaging(); err == nil {
		err = closeErr
	}
	return n, err
}

func (w *Writer) checkOutputWorkDir(dst io.Writer) error {
	f, ok := dst.(*os.File)
	if !ok || f.Name() == "" {
		return nil
	}
	outDir := filepath.Dir(f.Name())
	if outDir != w.workDir {
		return fmt.Errorf("output directory %q != work directory %q: build GLCB in its final directory", outDir, w.workDir)
	}
	return nil
}

func (w *Writer) emitBlob(dst io.Writer) (int64, error) {
	if w.direct {
		if w.directFile == nil {
			return 0, errors.New("direct GLCB writer not initialized")
		}
		if f, ok := dst.(*os.File); ok && f != w.directFile {
			return 0, errors.New("direct GLCB output mismatch")
		}
		return w.emitBlobDirect()
	}
	return w.emitBlobStaging(dst)
}

func (w *Writer) emitBlobDirect() (int64, error) {
	if err := w.directBuf.Flush(); err != nil {
		return 0, fmt.Errorf("flush records: %w", err)
	}

	dictBuf := encodeDictionary(w.dict)
	indexBuf := encodeRecordIndex(w.recordIndex)

	recordsOff := uint32(headerSize)
	dictOff := recordsOff + uint32(w.sectionOff) //nolint:gosec // G115: section bounded by chunk policy
	indexOff := dictOff + uint32(len(dictBuf))   //nolint:gosec // G115: dict bytes bounded

	layout := encodeBlobLayoutMeta(blobLayoutMeta{
		ChunkID:     w.chunkID,
		VaultID:     w.vaultID,
		RecordCount: w.count,
		WriteStart:  tsNanos(w.bounds.writeStart),
		WriteEnd:    tsNanos(w.bounds.writeEnd),
		IngestStart: tsNanos(w.bounds.ingestStart),
		IngestEnd:   tsNanos(w.bounds.ingestEnd),
		SourceStart: tsNanos(w.bounds.sourceStart),
		SourceEnd:   tsNanos(w.bounds.sourceEnd),
		DictEntries: uint32(w.dict.Len()), //nolint:gosec // G115: dict bounded
		DictSize:    uint32(len(dictBuf)), //nolint:gosec // G115: dict bytes bounded
		RecordsOff:  recordsOff,
		RecordsSize: uint32(w.sectionOff), //nolint:gosec // G115: section bounded by chunk policy
		DictOff:     dictOff,
		IndexOff:    indexOff,
		IndexSize:   uint32(len(indexBuf)), //nolint:gosec // G115: index bounded by record count
	})
	if _, err := w.blobHash.Write(layout); err != nil {
		return 0, err
	}
	if _, err := w.directFile.WriteAt(layout, preambleSize); err != nil {
		return 0, fmt.Errorf("patch layout: %w", err)
	}
	if _, err := w.directFile.Seek(headerSize, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek records: %w", err)
	}
	if _, err := io.CopyN(w.blobHash, w.directFile, int64(w.sectionOff)); err != nil { //nolint:gosec // G115: section bounded by chunk policy
		return 0, fmt.Errorf("hash records: %w", err)
	}
	if _, err := w.directFile.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("seek end: %w", err)
	}

	tailBase := int64(headerSize) + int64(w.sectionOff) //nolint:gosec // G115: section bounded by chunk policy
	cw := &countWriter{w: w.directFile, hash: w.blobHash}

	if _, err := cw.Write(dictBuf); err != nil {
		return 0, err
	}
	if _, err := cw.Write(indexBuf); err != nil {
		return 0, err
	}

	sortEntries := w.tsSortFunc()
	encodeEntries := w.tsEncodeFunc()

	sortEntries(w.ingestEntries)
	ingestEntry, err := w.writeSectionAt(cw, tailBase, SectionIngestTSIndex, 1, encodeEntries(w.ingestEntries))
	if err != nil {
		return 0, err
	}
	sortEntries(w.sourceEntries)
	sourceEntry, err := w.writeSectionAt(cw, tailBase, SectionSourceTSIndex, 1, encodeEntries(w.sourceEntries))
	if err != nil {
		return 0, err
	}

	layoutEntry := makeTOCEntry(SectionBlobLayout, 1, preambleSize, int64(len(layout)), sha256.Sum256(layout))
	if err := w.finalizeTOC(cw, []TOCEntry{layoutEntry, ingestEntry, sourceEntry}); err != nil {
		return 0, err
	}
	info, err := w.directFile.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (w *Writer) emitBlobStaging(dst io.Writer) (int64, error) {
	if w.stagingBuf != nil {
		if err := w.stagingBuf.Flush(); err != nil {
			return 0, fmt.Errorf("flush staging: %w", err)
		}
	}
	if w.staging != nil {
		if _, err := w.staging.Seek(0, io.SeekStart); err != nil {
			return 0, fmt.Errorf("rewind staging: %w", err)
		}
	}

	cw := &countWriter{w: dst, hash: sha256.New()}

	pre, err := encodePreamble()
	if err != nil {
		return 0, err
	}
	if _, err := cw.Write(pre); err != nil {
		return cw.n, err
	}

	dictBuf := encodeDictionary(w.dict)
	indexBuf := encodeRecordIndex(w.recordIndex)

	recordsOff := uint32(headerSize)
	dictOff := recordsOff + uint32(w.sectionOff) //nolint:gosec // G115: section bounded by chunk policy
	indexOff := dictOff + uint32(len(dictBuf))   //nolint:gosec // G115: dict bytes bounded

	layout := encodeBlobLayoutMeta(blobLayoutMeta{
		ChunkID:     w.chunkID,
		VaultID:     w.vaultID,
		RecordCount: w.count,
		WriteStart:  tsNanos(w.bounds.writeStart),
		WriteEnd:    tsNanos(w.bounds.writeEnd),
		IngestStart: tsNanos(w.bounds.ingestStart),
		IngestEnd:   tsNanos(w.bounds.ingestEnd),
		SourceStart: tsNanos(w.bounds.sourceStart),
		SourceEnd:   tsNanos(w.bounds.sourceEnd),
		DictEntries: uint32(w.dict.Len()), //nolint:gosec // G115: dict bounded
		DictSize:    uint32(len(dictBuf)), //nolint:gosec // G115: dict bytes bounded
		RecordsOff:  recordsOff,
		RecordsSize: uint32(w.sectionOff), //nolint:gosec // G115: section bounded by chunk policy
		DictOff:     dictOff,
		IndexOff:    indexOff,
		IndexSize:   uint32(len(indexBuf)), //nolint:gosec // G115: index bounded by record count
	})
	layoutOff := cw.n
	if _, err := cw.Write(layout); err != nil {
		return cw.n, err
	}

	if w.staging != nil {
		buf := make([]byte, copyBufferSize)
		if _, err := io.CopyBuffer(cw, w.staging, buf); err != nil {
			return cw.n, fmt.Errorf("copy records: %w", err)
		}
	}
	if _, err := cw.Write(dictBuf); err != nil {
		return cw.n, err
	}
	if _, err := cw.Write(indexBuf); err != nil {
		return cw.n, err
	}

	sortEntries := w.tsSortFunc()
	encodeEntries := w.tsEncodeFunc()

	sortEntries(w.ingestEntries)
	ingestEntry, err := w.writeSectionAt(cw, 0, SectionIngestTSIndex, 1, encodeEntries(w.ingestEntries))
	if err != nil {
		return cw.n, err
	}
	sortEntries(w.sourceEntries)
	sourceEntry, err := w.writeSectionAt(cw, 0, SectionSourceTSIndex, 1, encodeEntries(w.sourceEntries))
	if err != nil {
		return cw.n, err
	}

	layoutEntry := makeTOCEntry(SectionBlobLayout, 1, layoutOff, int64(len(layout)), sha256.Sum256(layout))
	if err := w.finalizeTOC(cw, []TOCEntry{layoutEntry, ingestEntry, sourceEntry}); err != nil {
		return cw.n, err
	}
	return cw.n, nil
}

func (w *Writer) tsSortFunc() func([]tsEntry) {
	return func(entries []tsEntry) {
		slices.SortStableFunc(entries, func(a, b tsEntry) int {
			if a.ts != b.ts {
				if a.ts < b.ts {
					return -1
				}
				return 1
			}
			return int(a.pos) - int(b.pos)
		})
	}
}

func (w *Writer) tsEncodeFunc() func([]tsEntry) []byte {
	return func(entries []tsEntry) []byte {
		buf := make([]byte, len(entries)*tsIndexEntrySize)
		for i, e := range entries {
			off := i * tsIndexEntrySize
			binary.LittleEndian.PutUint64(buf[off:], uint64(e.ts)) //nolint:gosec // G115: nanosecond timestamps stored as uint64
			binary.LittleEndian.PutUint32(buf[off+8:], e.pos)
		}
		return buf
	}
}

func encodePreamble() ([]byte, error) {
	var pre [preambleSize]byte
	format.Header{Type: format.TypeCloudBlob, Version: formatVersion, Flags: 0}.EncodeInto(pre[:])
	return pre[:], nil
}

type countWriter struct {
	w    io.Writer
	hash hash.Hash
	n    int64
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if n > 0 {
		_, _ = cw.hash.Write(p[:n])
	}
	cw.n += int64(n)
	return n, err
}

func (w *Writer) writeSectionAt(cw *countWriter, base int64, sectionType, version uint8, body []byte) (TOCEntry, error) {
	offset := base + cw.n
	if _, err := cw.Write(body); err != nil {
		return TOCEntry{}, err
	}
	return makeTOCEntry(sectionType, version, offset, int64(len(body)), sha256.Sum256(body)), nil
}

func (w *Writer) finalizeTOC(cw *countWriter, entries []TOCEntry) error {
	for _, e := range entries {
		if _, err := cw.Write(encodeTOCEntry(e)); err != nil {
			return err
		}
	}
	var blobDigest [32]byte
	copy(blobDigest[:], cw.hash.Sum(nil))
	footer := encodeTOCFooter(uint32(len(entries)), blobDigest) //nolint:gosec // G115: entry count fits in u32
	if _, err := cw.Write(footer); err != nil {
		return err
	}
	w.toc = BlobTOC{
		Entries:    entries,
		BlobDigest: blobDigest,
		Version:    tocFooterVersion,
	}
	if e, ok := w.toc.Find(SectionIngestTSIndex); ok {
		w.toc.IngestIdxOffset = e.Offset
		w.toc.IngestIdxSize = e.Size
		w.toc.IngestIdxHash = e.Hash
	}
	if e, ok := w.toc.Find(SectionSourceTSIndex); ok {
		w.toc.SourceIdxOffset = e.Offset
		w.toc.SourceIdxSize = e.Size
		w.toc.SourceIdxHash = e.Hash
	}
	return nil
}

func makeTOCEntry(sectionType, version uint8, offset, size int64, hash [32]byte) TOCEntry {
	return TOCEntry{Type: sectionType, Version: version, Offset: offset, Size: size, Hash: hash}
}

func encodeTOCEntry(e TOCEntry) []byte {
	if e.Offset < 0 || e.Offset > math.MaxUint32 {
		panic(fmt.Sprintf("TOC offset %d outside u32 range", e.Offset))
	}
	if e.Size < 0 || e.Size > math.MaxUint32 {
		panic(fmt.Sprintf("TOC size %d outside u32 range", e.Size))
	}
	buf := make([]byte, tocEntrySize)
	buf[0] = e.Type
	buf[1] = e.Version
	binary.LittleEndian.PutUint32(buf[2:6], uint32(e.Offset))
	binary.LittleEndian.PutUint32(buf[6:10], uint32(e.Size))
	copy(buf[10:42], e.Hash[:])
	return buf
}

func encodeTOCFooter(entryCount uint32, blobDigest [32]byte) []byte {
	buf := make([]byte, tocFooterSize)
	binary.LittleEndian.PutUint32(buf[0:4], entryCount)
	copy(buf[4:36], blobDigest[:])
	binary.LittleEndian.PutUint32(buf[36:40], tocFooterVersion)
	copy(buf[40:44], tocFooterMagic)
	return buf
}

func (w *Writer) TOC() BlobTOC { return w.toc }

func (w *Writer) Meta() BlobMeta {
	return BlobMeta{
		ChunkID:         w.chunkID,
		VaultID:         w.vaultID,
		RecordCount:     w.count,
		RawBytes:        int64(w.sectionOff), //nolint:gosec // G115: section bounded by chunk policy
		WriteStart:      w.bounds.writeStart,
		WriteEnd:        w.bounds.writeEnd,
		IngestStart:     w.bounds.ingestStart,
		IngestEnd:       w.bounds.ingestEnd,
		SourceStart:     w.bounds.sourceStart,
		SourceEnd:       w.bounds.sourceEnd,
		IngestIdxOffset: w.toc.IngestIdxOffset,
		IngestIdxSize:   w.toc.IngestIdxSize,
		SourceIdxOffset: w.toc.SourceIdxOffset,
		SourceIdxSize:   w.toc.SourceIdxSize,
	}
}
