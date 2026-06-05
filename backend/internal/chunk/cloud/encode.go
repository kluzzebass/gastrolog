package cloud

import (
	"encoding/binary"
	"fmt"
	"time"

	"gastrolog/internal/chunk"
)

func encodeRecordFrame(rec chunk.Record, dict *chunk.StringDict) ([]byte, error) {
	attrData, _, err := chunk.EncodeWithDict(rec.Attrs, dict)
	if err != nil {
		return nil, fmt.Errorf("encode attrs: %w", err)
	}

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
	binary.LittleEndian.PutUint32(frame[off:], uint32(len(rec.Raw))) //nolint:gosec // G115: raw bounded by chunk limits
	off += 4
	copy(frame[off:], rec.Raw)
	return frame, nil
}

func encodeDictionary(dict *chunk.StringDict) []byte {
	var buf []byte
	for i := range dict.Len() {
		s, _ := dict.Get(uint32(i))
		buf = append(buf, chunk.EncodeDictEntry(s)...)
	}
	return buf
}

func encodeRecordIndex(index []recordIndex) []byte {
	buf := make([]byte, len(index)*indexEntrySize)
	for i, idx := range index {
		off := i * indexEntrySize
		binary.LittleEndian.PutUint64(buf[off:], idx.Offset)
		binary.LittleEndian.PutUint32(buf[off+8:], idx.Size)
	}
	return buf
}

type blobBounds struct {
	seen        bool
	writeStart  time.Time
	writeEnd    time.Time
	ingestStart time.Time
	ingestEnd   time.Time
	sourceStart time.Time
	sourceEnd   time.Time
}

func (b *blobBounds) update(rec chunk.Record) {
	if !b.seen {
		b.seen = true
		b.writeStart = rec.WriteTS
		b.writeEnd = rec.WriteTS
		b.ingestStart = rec.IngestTS
		b.ingestEnd = rec.IngestTS
		if !rec.SourceTS.IsZero() {
			b.sourceStart = rec.SourceTS
			b.sourceEnd = rec.SourceTS
		}
		return
	}
	if rec.WriteTS.Before(b.writeStart) {
		b.writeStart = rec.WriteTS
	}
	if rec.WriteTS.After(b.writeEnd) {
		b.writeEnd = rec.WriteTS
	}
	if rec.IngestTS.Before(b.ingestStart) {
		b.ingestStart = rec.IngestTS
	}
	if rec.IngestTS.After(b.ingestEnd) {
		b.ingestEnd = rec.IngestTS
	}
	if !rec.SourceTS.IsZero() {
		if b.sourceStart.IsZero() || rec.SourceTS.Before(b.sourceStart) {
			b.sourceStart = rec.SourceTS
		}
		if rec.SourceTS.After(b.sourceEnd) {
			b.sourceEnd = rec.SourceTS
		}
	}
}
