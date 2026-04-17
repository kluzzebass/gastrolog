package cloud

import (
	"gastrolog/internal/glid"
	"encoding/binary"
	"fmt"
	"io"
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

// countWriter wraps an io.Writer and tracks total bytes written,
// including bytes written by wrapped writers (e.g., seekable zstd's Close).
type countWriter struct {
	w io.Writer
	n int64
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
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
	cw := &countWriter{w: dst}

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

// writeTSIndexes sorts and writes the ingest + source TS index sections and TOC.
func (w *Writer) writeTSIndexes(cw *countWriter) error {
	// Sort entries by timestamp (stable to preserve position order for equal timestamps).
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

	// --- Ingest TS Index ---
	sortEntries(w.ingestEntries)
	ingestBuf := encodeEntries(w.ingestEntries)
	ingestOffset := cw.n

	if _, err := cw.Write(ingestBuf); err != nil {
		return err
	}

	// --- Source TS Index ---
	sortEntries(w.sourceEntries)
	sourceBuf := encodeEntries(w.sourceEntries)
	sourceOffset := cw.n

	if _, err := cw.Write(sourceBuf); err != nil {
		return err
	}

	// --- TOC (48 bytes) ---
	w.toc = BlobTOC{
		IngestIdxOffset: ingestOffset,
		IngestIdxSize:   int64(len(ingestBuf)),
		SourceIdxOffset: sourceOffset,
		SourceIdxSize:   int64(len(sourceBuf)),
	}

	var tocBuf [tocSize]byte
	copy(tocBuf[0:4], tocMagic)
	binary.LittleEndian.PutUint32(tocBuf[4:8], 1) // tocVersion
	binary.LittleEndian.PutUint64(tocBuf[8:16], uint64(w.toc.IngestIdxOffset))  //nolint:gosec // G115: offset is always positive
	binary.LittleEndian.PutUint64(tocBuf[16:24], uint64(w.toc.IngestIdxSize))   //nolint:gosec // G115: size is always positive
	binary.LittleEndian.PutUint64(tocBuf[24:32], uint64(w.toc.SourceIdxOffset)) //nolint:gosec // G115: offset is always positive
	binary.LittleEndian.PutUint64(tocBuf[32:40], uint64(w.toc.SourceIdxSize))   //nolint:gosec // G115: size is always positive
	// bytes 40-47: reserved (zero)

	_, err := cw.Write(tocBuf[:])
	return err
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
