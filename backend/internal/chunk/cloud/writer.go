package cloud

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"gastrolog/internal/glid"
	"hash"
	"io"
	"math"
	"slices"
	"time"

	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// tsEntry is a (timestamp, position) pair for the embedded TS index.
type tsEntry struct {
	ts  int64  // unix nanos
	pos uint32 // 0-based record position
}

// Writer encodes records into the cloud blob format.
// Records are buffered in memory, then flushed with an uncompressed
// header/dict/index prefix followed by seekable zstd record data,
// and finally the embedded TS indexes + TOC footer (v2).
type Writer struct {
	chunkID chunk.ChunkID
	vaultID glid.GLID
	dict    *chunk.StringDict
	enc     *zstd.Encoder // caller-provided, reused across uploads

	frames [][]byte // pre-encoded record frames
	count  uint32

	writeStart  time.Time
	writeEnd    time.Time
	ingestStart time.Time
	ingestEnd   time.Time
	sourceStart time.Time
	sourceEnd   time.Time

	// TS index entries built during Add(), sorted and written in WriteTo().
	ingestEntries []tsEntry
	sourceEntries []tsEntry

	toc       BlobTOC // populated by WriteTo
	numFrames int32   // seekable zstd frame count, populated by WriteTo
}

// NewWriter creates a writer for the given chunk and vault.
func NewWriter(chunkID chunk.ChunkID, vaultID glid.GLID, enc *zstd.Encoder) *Writer {
	return &Writer{
		chunkID: chunkID,
		vaultID: vaultID,
		dict:    chunk.NewStringDict(),
		enc:     enc,
	}
}

// Add buffers a record. Records must be added in WriteTS order.
func (w *Writer) Add(rec chunk.Record) error {
	w.updateBounds(rec)

	// Encode attributes using the shared dictionary.
	attrData, _, err := chunk.EncodeWithDict(rec.Attrs, w.dict)
	if err != nil {
		return fmt.Errorf("encode attrs: %w", err)
	}

	// Build the record frame (without the 4-byte frameLen prefix).
	//   3×i64 + 16 (IngesterID) + 16 (NodeID) + u32 + attrData + u32 + raw
	frameSize := 3*8 + 16 + 16 + 4 + len(attrData) + 4 + len(rec.Raw)
	frame := make([]byte, frameSize)
	off := 0

	binary.LittleEndian.PutUint64(frame[off:], tsNanos(rec.SourceTS))
	off += 8
	binary.LittleEndian.PutUint64(frame[off:], tsNanos(rec.IngestTS))
	off += 8
	binary.LittleEndian.PutUint64(frame[off:], tsNanos(rec.WriteTS))
	off += 8
	copy(frame[off:], rec.EventID.IngesterID[:])
	off += 16
	copy(frame[off:], rec.EventID.NodeID[:])
	off += 16
	binary.LittleEndian.PutUint32(frame[off:], rec.EventID.IngestSeq)
	off += 4
	copy(frame[off:], attrData)
	off += len(attrData)
	binary.LittleEndian.PutUint32(frame[off:], uint32(len(rec.Raw))) //nolint:gosec // G115: raw size bounded by chunk limits
	off += 4
	copy(frame[off:], rec.Raw)

	// Track TS entries for the embedded indexes.
	w.ingestEntries = append(w.ingestEntries, tsEntry{
		ts:  rec.IngestTS.UnixNano(),
		pos: w.count,
	})
	if !rec.SourceTS.IsZero() {
		w.sourceEntries = append(w.sourceEntries, tsEntry{
			ts:  rec.SourceTS.UnixNano(),
			pos: w.count,
		})
	}

	w.frames = append(w.frames, frame)
	w.count++
	return nil
}

// countWriter wraps an io.Writer, tracks total bytes written, and tees
// every byte through a SHA-256 hash so the whole-blob digest in the TOC
// footer covers everything before the footer itself. Bytes written by
// nested writers (seekable zstd's Close, etc.) flow through here too.
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

// WriteTo writes the cloud blob to dst:
//   - Uncompressed header (96 bytes)
//   - Uncompressed dictionary
//   - Uncompressed record index (12 bytes per record)
//   - Seekable zstd compressed record data (256KB frames)
//   - IngestTS index (sorted entries)
//   - SourceTS index (sorted entries)
//   - TOC footer (48 bytes)
func (w *Writer) WriteTo(dst io.Writer) (int64, error) {
	// countWriter tracks total bytes written to dst, including bytes
	// written by the seekable zstd writer during Close() (seek table).
	// The teed SHA-256 builds up the whole-blob digest written into the
	// TOC footer.
	cw := &countWriter{w: dst, hash: sha256.New()}

	// --- Encode dictionary to compute dictSize ---
	var dictBuf []byte
	for i := range w.dict.Len() {
		s, _ := w.dict.Get(uint32(i))
		dictBuf = append(dictBuf, chunk.EncodeDictEntry(s)...)
	}

	// --- Build record index and compute decompressed offsets ---
	index := make([]recordIndex, w.count)
	var decompressedOff uint64
	for i, frame := range w.frames {
		index[i] = recordIndex{
			Offset: decompressedOff + 4, // past the u32 frameLen prefix
			Size:   uint32(len(frame)),  //nolint:gosec // G115: frame size bounded by record limits
		}
		decompressedOff += 4 + uint64(len(frame))
	}

	// --- Header (96 bytes) ---
	var hdr [headerSize]byte
	format.Header{Type: format.TypeCloudBlob, Version: formatVersion}.EncodeInto(hdr[:])
	copy(hdr[4:20], w.chunkID[:])
	copy(hdr[20:36], w.vaultID[:])
	binary.LittleEndian.PutUint32(hdr[36:40], w.count)
	binary.LittleEndian.PutUint64(hdr[40:48], tsNanos(w.writeStart))
	binary.LittleEndian.PutUint64(hdr[48:56], tsNanos(w.writeEnd))
	binary.LittleEndian.PutUint64(hdr[56:64], tsNanos(w.ingestStart))
	binary.LittleEndian.PutUint64(hdr[64:72], tsNanos(w.ingestEnd))
	binary.LittleEndian.PutUint64(hdr[72:80], tsNanos(w.sourceStart))
	binary.LittleEndian.PutUint64(hdr[80:88], tsNanos(w.sourceEnd))
	binary.LittleEndian.PutUint32(hdr[88:92], uint32(w.dict.Len())) //nolint:gosec // G115: dict size bounded by StringDict capacity
	binary.LittleEndian.PutUint32(hdr[92:96], uint32(len(dictBuf))) //nolint:gosec // G115: dict bytes bounded

	if _, err := cw.Write(hdr[:]); err != nil {
		return cw.n, err
	}

	// --- Dictionary ---
	if _, err := cw.Write(dictBuf); err != nil {
		return cw.n, err
	}

	// --- Record Index ---
	idxBuf := make([]byte, int(w.count)*indexEntrySize)
	for i, idx := range index {
		off := i * indexEntrySize
		binary.LittleEndian.PutUint64(idxBuf[off:], idx.Offset)
		binary.LittleEndian.PutUint32(idxBuf[off+8:], idx.Size)
	}
	if _, err := cw.Write(idxBuf); err != nil {
		return cw.n, err
	}

	// --- Seekable Zstd Record Data ---
	// The seekable writer wraps cw so that sw.Close() (which writes the
	// seek table) is also tracked in cw.n.
	// The encoder is caller-provided to avoid allocating ~144 MB per call.
	sw, err := seekable.NewWriter(cw, w.enc)
	if err != nil {
		return cw.n, fmt.Errorf("create seekable writer: %w", err)
	}

	// Write record frames in ~256KB batches. Each batch becomes one
	// independently-decompressible zstd frame in the seekable stream.
	var batch []byte
	var frameLenBuf [4]byte
	var frameCount int32
	for _, frame := range w.frames {
		binary.LittleEndian.PutUint32(frameLenBuf[:], uint32(len(frame))) //nolint:gosec // G115: frame size bounded
		batch = append(batch, frameLenBuf[:]...)
		batch = append(batch, frame...)

		if len(batch) >= seekableFrameSize {
			if _, werr := sw.Write(batch); werr != nil {
				_ = sw.Close()
				return cw.n, werr
			}
			batch = batch[:0]
			frameCount++
		}
	}
	if len(batch) > 0 {
		if _, werr := sw.Write(batch); werr != nil {
			_ = sw.Close()
			return cw.n, werr
		}
		frameCount++
	}
	w.numFrames = frameCount

	if err := sw.Close(); err != nil {
		return cw.n, fmt.Errorf("close seekable writer: %w", err)
	}

	// --- Embedded TS Indexes + TOC ---
	// cw.n now includes all bytes from the seekable writer (data + seek table).
	if err := w.writeTSIndexes(cw); err != nil {
		return cw.n, err
	}
	return cw.n, nil
}

// writeSection appends a section to cw: writes the body bytes, hashes
// them, and returns a TOCEntry pinned to the body's offset within the
// blob. Callers append the returned entry to their entry list and pass
// the list to finalizeTOC when all sections are written.
//
// This is the single primitive every section type goes through — TS
// indexes, built-in chunk indexes, anything future. Centralising it
// means hash + offset + entry construction can never drift between
// callers.
func (w *Writer) writeSection(cw *countWriter, sectionType, version uint8, body []byte) (TOCEntry, error) {
	offset := cw.n
	if _, err := cw.Write(body); err != nil {
		return TOCEntry{}, err
	}
	return makeTOCEntry(sectionType, version, offset, int64(len(body)), sha256.Sum256(body)), nil
}

// finalizeTOC writes the encoded TOC entries followed by the 44-byte
// footer (whole-blob digest snapshot + entry count + magic). Populates
// w.toc with the entries, digest, and convenience fields for the
// well-known section types.
func (w *Writer) finalizeTOC(cw *countWriter, entries []TOCEntry) error {
	for _, e := range entries {
		if _, err := cw.Write(encodeTOCEntry(e)); err != nil {
			return err
		}
	}

	// Snapshot the running blob digest BEFORE writing the footer so it
	// covers exactly the bytes preceding it.
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

// writeTSIndexes sorts the ingest + source TS entries, writes them as
// sections via writeSection, and finalises the TOC. ITSI and STSI are
// the only sections this writer emits today; built-in chunk indexes
// land in the same TOC via additional writeSection calls in step 6
// (PostSealProcess restructure).
func (w *Writer) writeTSIndexes(cw *countWriter) error {
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
		return err
	}

	sortEntries(w.sourceEntries)
	sourceEntry, err := w.writeSection(cw, SectionSourceTSIndex, 1, encodeEntries(w.sourceEntries))
	if err != nil {
		return err
	}

	return w.finalizeTOC(cw, []TOCEntry{ingestEntry, sourceEntry})
}

// makeTOCEntry builds a TOCEntry from a section type byte and metadata.
func makeTOCEntry(sectionType, version uint8, offset, size int64, hash [32]byte) TOCEntry {
	return TOCEntry{
		Type:    sectionType,
		Version: version,
		Offset:  offset,
		Size:    size,
		Hash:    hash,
	}
}

// encodeTOCEntry serializes a TOCEntry to its 42-byte on-disk form.
// Layout: [type:u8][version:u8][offset:u32][size:u32][hash:32].
//
// Offset and Size are stored on disk as u32; chunk policy bounds blobs
// well below 4 GB so the narrowing is safe. The MaxUint32 guards exist
// to fail loudly if a future change blows past that.
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

// encodeTOCFooter serializes the 44-byte footer that lives at the end of
// the blob. Layout: entryCount (u32) + blobDigest (32) + footerVersion
// (u32) + magic (4).
func encodeTOCFooter(entryCount uint32, blobDigest [32]byte) []byte {
	buf := make([]byte, tocFooterSize)
	binary.LittleEndian.PutUint32(buf[0:4], entryCount)
	copy(buf[4:36], blobDigest[:])
	binary.LittleEndian.PutUint32(buf[36:40], tocFooterVersion)
	copy(buf[40:44], tocFooterMagic)
	return buf
}

// TOC returns the section offsets for the embedded TS indexes.
// Only valid after WriteTo has been called.
func (w *Writer) TOC() BlobTOC {
	return w.toc
}

// NumFrames returns the number of seekable zstd frames written.
// Only valid after WriteTo has been called.
func (w *Writer) NumFrames() int32 {
	return w.numFrames
}

func (w *Writer) updateBounds(rec chunk.Record) {
	if w.count == 0 {
		w.writeStart = rec.WriteTS
		w.writeEnd = rec.WriteTS
		w.ingestStart = rec.IngestTS
		w.ingestEnd = rec.IngestTS
		if !rec.SourceTS.IsZero() {
			w.sourceStart = rec.SourceTS
			w.sourceEnd = rec.SourceTS
		}
		return
	}
	if rec.WriteTS.Before(w.writeStart) {
		w.writeStart = rec.WriteTS
	}
	if rec.WriteTS.After(w.writeEnd) {
		w.writeEnd = rec.WriteTS
	}
	if rec.IngestTS.Before(w.ingestStart) {
		w.ingestStart = rec.IngestTS
	}
	if rec.IngestTS.After(w.ingestEnd) {
		w.ingestEnd = rec.IngestTS
	}
	if !rec.SourceTS.IsZero() {
		if w.sourceStart.IsZero() || rec.SourceTS.Before(w.sourceStart) {
			w.sourceStart = rec.SourceTS
		}
		if rec.SourceTS.After(w.sourceEnd) {
			w.sourceEnd = rec.SourceTS
		}
	}
}

// Meta returns the blob metadata computed from added records.
// TOC fields are populated only after WriteTo has been called.
func (w *Writer) Meta() BlobMeta {
	var rawBytes int64
	for _, frame := range w.frames {
		rawBytes += 4 + int64(len(frame)) // u32 frameLen prefix + frame
	}
	return BlobMeta{
		ChunkID:         w.chunkID,
		VaultID:         w.vaultID,
		RecordCount:     w.count,
		RawBytes:        rawBytes,
		WriteStart:      w.writeStart,
		WriteEnd:        w.writeEnd,
		IngestStart:     w.ingestStart,
		IngestEnd:       w.ingestEnd,
		SourceStart:     w.sourceStart,
		SourceEnd:       w.sourceEnd,
		IngestIdxOffset: w.toc.IngestIdxOffset,
		IngestIdxSize:   w.toc.IngestIdxSize,
		SourceIdxOffset: w.toc.SourceIdxOffset,
		SourceIdxSize:   w.toc.SourceIdxSize,
	}
}
