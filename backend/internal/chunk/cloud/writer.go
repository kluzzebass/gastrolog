package cloud

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"

	"gastrolog/internal/chunk"
)

// Writer encodes records into the cloud blob format.
// Records are buffered in memory, then flushed as a single
// zstd-compressed stream on WriteTo.
type Writer struct {
	chunkID chunk.ChunkID
	vaultID uuid.UUID
	dict    *chunk.StringDict

	frames [][]byte // pre-encoded record frames
	count  uint32

	startTS     time.Time
	endTS       time.Time
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

// WriteTo compresses and writes the complete blob to dst.
// After WriteTo, the Writer should not be reused.
func (w *Writer) WriteTo(dst io.Writer) (int64, error) {
	enc, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return 0, fmt.Errorf("create zstd writer: %w", err)
	}

	var written int64

	// --- Header (94 bytes) ---
	var hdr [headerSize]byte
	copy(hdr[0:4], magic[:])
	hdr[4] = formatVersion
	// hdr[5] = flags (reserved, 0)
	copy(hdr[6:22], w.chunkID[:])
	copy(hdr[22:38], w.vaultID[:])
	binary.LittleEndian.PutUint32(hdr[38:42], w.count)
	binary.LittleEndian.PutUint64(hdr[42:50], tsNanos(w.startTS))
	binary.LittleEndian.PutUint64(hdr[50:58], tsNanos(w.endTS))
	binary.LittleEndian.PutUint64(hdr[58:66], tsNanos(w.ingestStart))
	binary.LittleEndian.PutUint64(hdr[66:74], tsNanos(w.ingestEnd))
	binary.LittleEndian.PutUint64(hdr[74:82], tsNanos(w.sourceStart))
	binary.LittleEndian.PutUint64(hdr[82:90], tsNanos(w.sourceEnd))
	binary.LittleEndian.PutUint32(hdr[90:94], uint32(w.dict.Len())) //nolint:gosec // G115: dict size bounded by StringDict capacity

	n, err := enc.Write(hdr[:])
	written += int64(n)
	if err != nil {
		_ = enc.Close()
		return written, err
	}

	// --- Dictionary ---
	for i := range w.dict.Len() {
		s, _ := w.dict.Get(uint32(i))
		entry := chunk.EncodeDictEntry(s)
		n, err := enc.Write(entry)
		written += int64(n)
		if err != nil {
			_ = enc.Close()
			return written, err
		}
	}

	// --- Record frames ---
	var frameLenBuf [4]byte
	for _, frame := range w.frames {
		binary.LittleEndian.PutUint32(frameLenBuf[:], uint32(len(frame))) //nolint:gosec // G115: frame size bounded by record limits
		n, err := enc.Write(frameLenBuf[:])
		written += int64(n)
		if err != nil {
			_ = enc.Close()
			return written, err
		}
		n, err = enc.Write(frame)
		written += int64(n)
		if err != nil {
			_ = enc.Close()
			return written, err
		}
	}

	if err := enc.Close(); err != nil {
		return written, fmt.Errorf("close zstd: %w", err)
	}
	return written, nil
}

func (w *Writer) updateBounds(rec chunk.Record) {
	if w.count == 0 {
		w.startTS = rec.WriteTS
		w.endTS = rec.WriteTS
		w.ingestStart = rec.IngestTS
		w.ingestEnd = rec.IngestTS
		if !rec.SourceTS.IsZero() {
			w.sourceStart = rec.SourceTS
			w.sourceEnd = rec.SourceTS
		}
		return
	}
	if rec.WriteTS.Before(w.startTS) {
		w.startTS = rec.WriteTS
	}
	if rec.WriteTS.After(w.endTS) {
		w.endTS = rec.WriteTS
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
	return BlobMeta{
		ChunkID:     w.chunkID,
		VaultID:     w.vaultID,
		RecordCount: w.count,
		StartTS:     w.startTS,
		EndTS:       w.endTS,
		IngestStart: w.ingestStart,
		IngestEnd:   w.ingestEnd,
		SourceStart: w.sourceStart,
		SourceEnd:   w.sourceEnd,
	}
}
