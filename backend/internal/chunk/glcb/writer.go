package glcb

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
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/glid"
	"gastrolog/internal/record"
	"gastrolog/internal/tsindex"
)

const (
	stagingBufferSize = 256 << 10
	copyBufferSize    = 1024 << 10

	// RecordsStagingPrefix is the os.CreateTemp pattern prefix used for
	// the record-staging file created by ensureStaging when a Writer is
	// used without BindOutput (chunk/file.Manager.sealToGLCB is the only
	// such caller — pipeline/chunking always binds an output file and
	// stays in direct mode). Exported so orphan-sweep code that shares a
	// directory with a Writer's staging file can match its exact naming
	// contract instead of guessing a pattern.
	RecordsStagingPrefix = "glcb-records-"
)

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

	recordIndex []recordIndexEntry
	sectionOff  uint64
	count       uint32
	bounds      blobBounds

	ingestEntries []tsindex.Entry
	sourceEntries []tsindex.Entry
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
	w.recordIndex = make([]recordIndexEntry, 0, n)
	w.ingestEntries = make([]tsindex.Entry, 0, n)
	w.sourceEntries = make([]tsindex.Entry, 0, n)
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
	f, err := os.CreateTemp(w.workDir, RecordsStagingPrefix+"*.tmp")
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

	// Encode directly into the reused scratch after a frameLenSize length
	// placeholder, so no per-record frame is allocated and copied.
	if cap(w.frameScratch) < frameLenSize {
		w.frameScratch = make([]byte, frameLenSize, 512)
	}
	scratch, err := appendRecordFrame(w.frameScratch[:frameLenSize], rec, w.dict)
	if err != nil {
		return err
	}
	w.frameScratch = scratch

	return w.commitFrame(rec.SourceTS, rec.IngestTS, rec.WriteTS)
}

// AddView is Add for a record.View: attributes transcode straight from
// segment wire form to dict form with no intermediate map, and Raw copies
// exactly once (view -> frame scratch). The bulk GLCB merge feeds this;
// the map-per-record path cost ~24GB of garbage per soak run.
func (w *Writer) AddView(v record.View) error {
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

	if cap(w.frameScratch) < frameLenSize {
		w.frameScratch = make([]byte, frameLenSize, 512)
	}
	scratch, err := appendRecordFrameView(w.frameScratch[:frameLenSize], v, w.dict)
	if err != nil {
		return err
	}
	w.frameScratch = scratch

	return w.commitFrame(v.SourceTS, v.IngestTS, v.WriteTS)
}

// commitFrame finishes an Add/AddView after w.frameScratch holds
// [frameLenSize placeholder][frame]: stamps the length, updates
// bounds/indexes, and writes the frame to the active sink.
func (w *Writer) commitFrame(sourceTS, ingestTS, writeTS time.Time) error {
	pos := w.count
	w.bounds.update(chunk.Record{SourceTS: sourceTS, IngestTS: ingestTS, WriteTS: writeTS})
	w.noteIngestTS(ingestTS)

	bodySize := uint32(len(w.frameScratch) - frameLenSize) //nolint:gosec // G115: frame size bounded by record limits
	w.recordIndex = append(w.recordIndex, recordIndexEntry{
		Offset: w.sectionOff + frameLenSize,
		Size:   bodySize,
	})
	w.sectionOff += frameLenSize + uint64(bodySize)
	binary.LittleEndian.PutUint32(w.frameScratch[:frameLenSize], bodySize)

	var sink *bufio.Writer
	if w.direct {
		sink = w.directBuf
	} else {
		sink = w.stagingBuf
	}
	if _, err := sink.Write(w.frameScratch); err != nil {
		return fmt.Errorf("write record frame: %w", err)
	}

	w.ingestEntries = append(w.ingestEntries, tsindex.Entry{TS: ingestTS.UnixNano(), Pos: pos})
	if !sourceTS.IsZero() {
		w.sourceEntries = append(w.sourceEntries, tsindex.Entry{TS: sourceTS.UnixNano(), Pos: pos})
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

	dictBuf, indexBuf, layout := w.tailSections()
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
	if err := w.emitTail(cw, tailBase, dictBuf, indexBuf, layout); err != nil {
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

	dictBuf, indexBuf, layout := w.tailSections()
	if _, err := cw.Write(layout); err != nil {
		return cw.n, err
	}

	if w.staging != nil {
		buf := make([]byte, copyBufferSize)
		if _, err := io.CopyBuffer(cw, w.staging, buf); err != nil {
			return cw.n, fmt.Errorf("copy records: %w", err)
		}
	}
	if err := w.emitTail(cw, 0, dictBuf, indexBuf, layout); err != nil {
		return cw.n, err
	}
	return cw.n, nil
}

// tailSections assembles the encoded dictionary, record index, and the
// layout-metadata block that describes them. Together with emitTail this
// is the single source of the GLCB tail format: the direct and staging
// builds both go through here, so a new layout field lands identically
// in blobs from either path.
func (w *Writer) tailSections() (dictBuf, indexBuf, layout []byte) {
	dictBuf = encodeDictionary(w.dict)
	indexBuf = encodeRecordIndex(w.recordIndex)

	recordsOff := uint32(headerSize)
	dictOff := recordsOff + uint32(w.sectionOff) //nolint:gosec // G115: section bounded by chunk policy
	indexOff := dictOff + uint32(len(dictBuf))   //nolint:gosec // G115: dict bytes bounded

	layout = encodeBlobLayoutMeta(blobLayoutMeta{
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

		IngestTSMonotonic: w.ingestMonotonic,

		RecordsOff:  recordsOff,
		RecordsSize: uint32(w.sectionOff), //nolint:gosec // G115: section bounded by chunk policy
		DictOff:     dictOff,
		IndexOff:    indexOff,
		IndexSize:   uint32(len(indexBuf)), //nolint:gosec // G115: index bounded by record count
	})
	return dictBuf, indexBuf, layout
}

// emitTail writes the GLCB tail — dictionary, record index, sorted TS
// index sections, TOC entries, and footer — through cw and records the
// resulting TOC on the writer. tailBase is the absolute blob offset
// where cw started counting: headerSize+records for the direct build
// (records are already on disk), 0 for the staging build (cw counts
// from byte 0 of the blob).
func (w *Writer) emitTail(cw *countWriter, tailBase int64, dictBuf, indexBuf, layout []byte) error {
	if _, err := cw.Write(dictBuf); err != nil {
		return err
	}
	if _, err := cw.Write(indexBuf); err != nil {
		return err
	}

	tsindex.Sort(w.ingestEntries)
	ingestEntry, err := writeSectionAt(cw, tailBase, SectionIngestTSIndex, tsIndexSectionVersion, tsindex.EncodeAll(w.ingestEntries))
	if err != nil {
		return err
	}
	tsindex.Sort(w.sourceEntries)
	sourceEntry, err := writeSectionAt(cw, tailBase, SectionSourceTSIndex, tsIndexSectionVersion, tsindex.EncodeAll(w.sourceEntries))
	if err != nil {
		return err
	}

	layoutEntry := makeTOCEntry(SectionBlobLayout, 1, preambleSize, int64(len(layout)), sha256.Sum256(layout))
	return w.finalizeTOC(cw, []TOCEntry{layoutEntry, ingestEntry, sourceEntry})
}

func encodePreamble() ([]byte, error) {
	var pre [preambleSize]byte
	format.Header{Type: format.TypeGLCB, Version: formatVersion, Flags: 0}.EncodeInto(pre[:])
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

// writeSectionAt writes one section body through cw and returns its TOC
// entry; base is the absolute blob offset of cw.n == 0.
func writeSectionAt(cw *countWriter, base int64, sectionType, version uint8, body []byte) (TOCEntry, error) {
	offset := base + cw.n
	if _, err := cw.Write(body); err != nil {
		return TOCEntry{}, err
	}
	return makeTOCEntry(sectionType, version, offset, int64(len(body)), sha256.Sum256(body)), nil
}

func (w *Writer) finalizeTOC(cw *countWriter, entries []TOCEntry) error {
	for _, e := range entries {
		buf, err := encodeTOCEntry(e)
		if err != nil {
			return fmt.Errorf("finalize TOC: %w", err)
		}
		if _, err := cw.Write(buf); err != nil {
			return err
		}
	}
	var blobDigest [32]byte
	copy(blobDigest[:], cw.hash.Sum(nil))
	footer := encodeTOCFooter(uint32(len(entries)), blobDigest) //nolint:gosec // G115: entry count fits in u32
	if _, err := cw.Write(footer); err != nil {
		return err
	}
	w.toc = newBlobTOC(entries, blobDigest)
	return nil
}

func makeTOCEntry(sectionType, version uint8, offset, size int64, hash [32]byte) TOCEntry {
	return TOCEntry{Type: sectionType, Version: version, Offset: offset, Size: size, Hash: hash}
}

// encodeTOCEntry serializes a TOCEntry to its tocEntrySize-byte on-disk
// form. Layout: [type:u8][version:u8][offset:u32][size:u32][hash:32].
//
// Offset and Size are stored on disk as u32; chunk policy bounds blobs
// well below 4 GiB so the narrowing is safe. The MaxUint32 guards return
// an error — not a panic — so a pathological oversized blob fails the
// seal with a propagated error instead of crashing the node.
func encodeTOCEntry(e TOCEntry) ([]byte, error) {
	if e.Offset < 0 || e.Offset > math.MaxUint32 {
		return nil, fmt.Errorf("TOC entry type 0x%02x: offset %d outside u32 range", e.Type, e.Offset)
	}
	if e.Size < 0 || e.Size > math.MaxUint32 {
		return nil, fmt.Errorf("TOC entry type 0x%02x: size %d outside u32 range", e.Type, e.Size)
	}
	buf := make([]byte, tocEntrySize)
	buf[0] = e.Type
	buf[1] = e.Version
	binary.LittleEndian.PutUint32(buf[2:6], uint32(e.Offset))
	binary.LittleEndian.PutUint32(buf[6:10], uint32(e.Size))
	copy(buf[10:42], e.Hash[:])
	return buf, nil
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
		ChunkID:           w.chunkID,
		VaultID:           w.vaultID,
		RecordCount:       w.count,
		IngestTSMonotonic: w.ingestMonotonic,
		RawBytes:          int64(w.sectionOff), //nolint:gosec // G115: section bounded by chunk policy
		WriteStart:        w.bounds.writeStart,
		WriteEnd:          w.bounds.writeEnd,
		IngestStart:       w.bounds.ingestStart,
		IngestEnd:         w.bounds.ingestEnd,
		SourceStart:       w.bounds.sourceStart,
		SourceEnd:         w.bounds.sourceEnd,
		IngestIdxOffset:   w.toc.IngestIdxOffset,
		IngestIdxSize:     w.toc.IngestIdxSize,
		SourceIdxOffset:   w.toc.SourceIdxOffset,
		SourceIdxSize:     w.toc.SourceIdxSize,
	}
}
