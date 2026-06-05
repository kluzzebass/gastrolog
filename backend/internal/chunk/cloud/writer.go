package cloud

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/glid"
)

// tsEntry is a (timestamp, position) pair for the embedded TS index.
type tsEntry struct {
	ts  int64
	pos uint32
}

// Writer encodes GLCB. Record frames spill to a staging file during Add;
// finalize writes sections in on-disk order (fixed header blocks first).
type Writer struct {
	chunkID chunk.ChunkID
	vaultID glid.GLID
	dict    *chunk.StringDict

	output      *os.File // optional target for Finish / WriteTo
	staging     *os.File
	stagingPath string

	recordIndex []recordIndex
	sectionOff  uint64
	count       uint32
	bounds      blobBounds

	ingestEntries []tsEntry
	sourceEntries []tsEntry
	toc           BlobTOC
}

// NewWriter creates a writer. Call WriteTo to emit the blob.
func NewWriter(chunkID chunk.ChunkID, vaultID glid.GLID) *Writer {
	return &Writer{
		chunkID: chunkID,
		vaultID: vaultID,
		dict:    chunk.NewStringDict(),
	}
}

// OpenWriter prepares to write a GLCB into f on Finish.
func OpenWriter(f *os.File, chunkID chunk.ChunkID, vaultID glid.GLID) (*Writer, error) {
	w := NewWriter(chunkID, vaultID)
	w.output = f
	return w, nil
}

func (w *Writer) ensureStaging() error {
	if w.staging != nil {
		return nil
	}
	f, err := os.CreateTemp("", "glcb-records-*.tmp")
	if err != nil {
		return err
	}
	w.staging = f
	w.stagingPath = f.Name()
	return nil
}

// Close removes the record staging file.
func (w *Writer) Close() error {
	if w.staging == nil {
		return nil
	}
	path := w.stagingPath
	err := w.staging.Close()
	w.staging = nil
	if path != "" {
		_ = os.Remove(path)
	}
	return err
}

// Add encodes one record and appends [frameLen][frame] to the staging file.
func (w *Writer) Add(rec chunk.Record) error {
	if err := w.ensureStaging(); err != nil {
		return err
	}

	frame, err := encodeRecordFrame(rec, w.dict)
	if err != nil {
		return err
	}

	pos := w.count
	w.bounds.update(rec)

	bodySize := uint32(len(frame)) //nolint:gosec // G115: frame size bounded by record limits
	w.recordIndex = append(w.recordIndex, recordIndex{
		Offset: w.sectionOff + 4,
		Size:   bodySize,
	})
	w.sectionOff += 4 + uint64(bodySize)

	var frameLenBuf [4]byte
	binary.LittleEndian.PutUint32(frameLenBuf[:], bodySize)
	if _, err := w.staging.Write(frameLenBuf[:]); err != nil {
		return fmt.Errorf("write frame length: %w", err)
	}
	if _, err := w.staging.Write(frame); err != nil {
		return fmt.Errorf("write frame body: %w", err)
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
		return BlobTOC{}, err
	}
	return w.toc, nil
}

// WriteTo writes a complete GLCB to dst.
func (w *Writer) WriteTo(dst io.Writer) (int64, error) {
	return w.emitBlob(dst)
}

func (w *Writer) emitBlob(dst io.Writer) (int64, error) {
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

	recordsOff := uint32(headerSize) // preamble + layout
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
		if _, err := io.Copy(cw, w.staging); err != nil {
			return cw.n, fmt.Errorf("copy records: %w", err)
		}
	}
	if _, err := cw.Write(dictBuf); err != nil {
		return cw.n, err
	}
	if _, err := cw.Write(indexBuf); err != nil {
		return cw.n, err
	}

	sortEntries := func(entries []tsEntry) {
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
	encodeEntries := func(entries []tsEntry) []byte {
		buf := make([]byte, len(entries)*tsIndexEntrySize)
		for i, e := range entries {
			off := i * tsIndexEntrySize
			binary.LittleEndian.PutUint64(buf[off:], uint64(e.ts)) //nolint:gosec // G115: nanosecond timestamps stored as uint64
			binary.LittleEndian.PutUint32(buf[off+8:], e.pos)
		}
		return buf
	}

	sortEntries(w.ingestEntries)
	ingestEntry, err := w.writeSection(cw, SectionIngestTSIndex, 1, encodeEntries(w.ingestEntries))
	if err != nil {
		return cw.n, err
	}
	sortEntries(w.sourceEntries)
	sourceEntry, err := w.writeSection(cw, SectionSourceTSIndex, 1, encodeEntries(w.sourceEntries))
	if err != nil {
		return cw.n, err
	}

	layoutEntry := makeTOCEntry(SectionBlobLayout, 1, layoutOff, int64(len(layout)), sha256.Sum256(layout))
	if err := w.finalizeTOC(cw, []TOCEntry{layoutEntry, ingestEntry, sourceEntry}); err != nil {
		return cw.n, err
	}
	return cw.n, nil
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

func (w *Writer) writeSection(cw *countWriter, sectionType, version uint8, body []byte) (TOCEntry, error) {
	offset := cw.n
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
