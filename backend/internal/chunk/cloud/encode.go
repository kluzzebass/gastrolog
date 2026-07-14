package cloud

import (
	"encoding/binary"
	"fmt"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/record"
)

// encodeRecordFrame allocates a fresh frame. Hot writers use
// appendRecordFrame with a reused scratch buffer instead — per-record
// frame slices were 14GB of garbage per soak run (gastrolog-11y2iv).
func encodeRecordFrame(rec chunk.Record, dict *chunk.StringDict) ([]byte, error) {
	return appendRecordFrame(nil, rec, dict)
}

// appendRecordFrame appends the encoded frame to dst and returns the
// (possibly grown) slice. dst may carry a prefix the caller wrote (e.g.
// a length placeholder); only bytes after len(dst) are produced here.
func appendRecordFrame(dst []byte, rec chunk.Record, dict *chunk.StringDict) ([]byte, error) {
	attrData, _, err := chunk.EncodeWithDict(rec.Attrs, dict)
	if err != nil {
		return dst, fmt.Errorf("encode attrs: %w", err)
	}

	frameSize := frameFixedHeaderSize + len(attrData) + frameRawLenSize + len(rec.Raw)
	base := len(dst)
	if cap(dst)-base < frameSize {
		grown := make([]byte, base, base+frameSize)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:base+frameSize]
	frame := dst[base:]
	off := 0

	binary.LittleEndian.PutUint64(frame[off:], tsNanos(rec.SourceTS))
	off += frameTSSize
	binary.LittleEndian.PutUint64(frame[off:], tsNanos(rec.IngestTS))
	off += frameTSSize
	binary.LittleEndian.PutUint64(frame[off:], tsNanos(rec.WriteTS))
	off += frameTSSize
	copy(frame[off:], rec.EventID.IngesterID[:])
	off += frameGLIDSize
	copy(frame[off:], rec.EventID.NodeID[:])
	off += frameGLIDSize
	binary.LittleEndian.PutUint32(frame[off:], rec.EventID.IngestSeq)
	off += frameIngestSeqSize
	copy(frame[off:], attrData)
	off += len(attrData)
	binary.LittleEndian.PutUint32(frame[off:], uint32(len(rec.Raw))) //nolint:gosec // G115: raw bounded by chunk limits
	off += frameRawLenSize
	copy(frame[off:], rec.Raw)
	return dst, nil
}

// appendRecordFrameView is appendRecordFrame for a record.View: the attrs
// blob transcodes from segment wire form via chunk.AppendWithDictWire
// without materializing a map; header fields and Raw come from the view.
func appendRecordFrameView(dst []byte, v record.View, dict *chunk.StringDict) ([]byte, error) {
	base := len(dst)
	fixed := frameFixedHeaderSize
	if cap(dst)-base < fixed {
		grown := make([]byte, base, base+fixed+len(v.AttrsWire)*2+frameRawLenSize+len(v.Raw))
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:base+fixed]
	frame := dst[base:]
	off := 0

	binary.LittleEndian.PutUint64(frame[off:], tsNanos(v.SourceTS))
	off += frameTSSize
	binary.LittleEndian.PutUint64(frame[off:], tsNanos(v.IngestTS))
	off += frameTSSize
	binary.LittleEndian.PutUint64(frame[off:], tsNanos(v.WriteTS))
	off += frameTSSize
	copy(frame[off:], v.EventID.IngesterID[:])
	off += frameGLIDSize
	copy(frame[off:], v.EventID.NodeID[:])
	off += frameGLIDSize
	binary.LittleEndian.PutUint32(frame[off:], v.EventID.IngestSeq)

	out, _, err := chunk.AppendWithDictWire(dst, v.AttrsWire, dict)
	if err != nil {
		return dst[:base], fmt.Errorf("encode attrs: %w", err)
	}
	dst = out

	rawPrefix := len(dst)
	need := frameRawLenSize + len(v.Raw)
	if cap(dst)-rawPrefix < need {
		grown := make([]byte, rawPrefix, rawPrefix+need)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:rawPrefix+need]
	binary.LittleEndian.PutUint32(dst[rawPrefix:], uint32(len(v.Raw))) //nolint:gosec // G115: raw bounded by chunk limits
	copy(dst[rawPrefix+frameRawLenSize:], v.Raw)
	return dst, nil
}

func encodeDictionary(dict *chunk.StringDict) []byte {
	var buf []byte
	for i := range dict.Len() {
		s, _ := dict.Get(uint32(i))
		buf = append(buf, chunk.EncodeDictEntry(s)...)
	}
	return buf
}

func encodeRecordIndex(index []recordIndexEntry) []byte {
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
