package cloud

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// Writer encodes records into the cloud blob format.
// Records are buffered in memory, then flushed with an uncompressed
// header/dict/index prefix followed by seekable zstd record data.
type Writer struct {
	chunkID chunk.ChunkID
	vaultID uuid.UUID
	dict    *chunk.StringDict

	frames [][]byte // pre-encoded record frames
	count  uint32

	writeStart     time.Time
	writeEnd       time.Time
	ingestStart time.Time
	ingestEnd   time.Time
	sourceStart time.Time
	sourceEnd   time.Time
}

// NewWriter creates a writer for the given chunk and vault.
func NewWriter(chunkID chunk.ChunkID, vaultID uuid.UUID) *Writer {
	return &Writer{
		chunkID: chunkID,
		vaultID: vaultID,
		dict:    chunk.NewStringDict(),
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
	//   3×i64 + 16 + u32 + attrData + u32 + raw
	frameSize := 3*8 + 16 + 4 + len(attrData) + 4 + len(rec.Raw)
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
	binary.LittleEndian.PutUint32(frame[off:], rec.EventID.IngestSeq)
	off += 4
	copy(frame[off:], attrData)
	off += len(attrData)
	binary.LittleEndian.PutUint32(frame[off:], uint32(len(rec.Raw))) //nolint:gosec // G115: raw size bounded by chunk limits
	off += 4
	copy(frame[off:], rec.Raw)

	w.frames = append(w.frames, frame)
	w.count++
	return nil
}

// WriteTo writes the cloud blob to dst:
//   - Uncompressed header (96 bytes)
//   - Uncompressed dictionary
//   - Uncompressed record index (12 bytes per record)
//   - Seekable zstd compressed record data (256KB frames)
func (w *Writer) WriteTo(dst io.Writer) (int64, error) {
	var written int64

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

	n, err := dst.Write(hdr[:])
	written += int64(n)
	if err != nil {
		return written, err
	}

	// --- Dictionary ---
	n, err = dst.Write(dictBuf)
	written += int64(n)
	if err != nil {
		return written, err
	}

	// --- Record Index ---
	idxBuf := make([]byte, int(w.count)*indexEntrySize)
	for i, idx := range index {
		off := i * indexEntrySize
		binary.LittleEndian.PutUint64(idxBuf[off:], idx.Offset)
		binary.LittleEndian.PutUint32(idxBuf[off+8:], idx.Size)
	}
	n, err = dst.Write(idxBuf)
	written += int64(n)
	if err != nil {
		return written, err
	}

	// --- Seekable Zstd Record Data ---
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return written, fmt.Errorf("create zstd encoder: %w", err)
	}
	defer func() { _ = enc.Close() }()

	sw, err := seekable.NewWriter(dst, enc)
	if err != nil {
		return written, fmt.Errorf("create seekable writer: %w", err)
	}

	// Write record frames in ~256KB batches. Each batch becomes one
	// independently-decompressible zstd frame in the seekable stream.
	var batch []byte
	var frameLenBuf [4]byte
	for _, frame := range w.frames {
		binary.LittleEndian.PutUint32(frameLenBuf[:], uint32(len(frame))) //nolint:gosec // G115: frame size bounded
		batch = append(batch, frameLenBuf[:]...)
		batch = append(batch, frame...)

		if len(batch) >= seekableFrameSize {
			nn, werr := sw.Write(batch)
			written += int64(nn)
			if werr != nil {
				_ = sw.Close()
				return written, werr
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		nn, werr := sw.Write(batch)
		written += int64(nn)
		if werr != nil {
			_ = sw.Close()
			return written, werr
		}
	}

	if err := sw.Close(); err != nil {
		return written, fmt.Errorf("close seekable writer: %w", err)
	}
	return written, nil
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
func (w *Writer) Meta() BlobMeta {
	var rawBytes int64
	for _, frame := range w.frames {
		rawBytes += 4 + int64(len(frame)) // u32 frameLen prefix + frame
	}
	return BlobMeta{
		ChunkID:     w.chunkID,
		VaultID:     w.vaultID,
		RecordCount: w.count,
		RawBytes:    rawBytes,
		WriteStart:     w.writeStart,
		WriteEnd:       w.writeEnd,
		IngestStart: w.ingestStart,
		IngestEnd:   w.ingestEnd,
		SourceStart: w.sourceStart,
		SourceEnd:   w.sourceEnd,
	}
}
