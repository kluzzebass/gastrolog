package record

import (
	"encoding/binary"
	"time"
)

// View is a decoded record whose variable-length fields alias the buffer
// they were parsed from (an mmap'd segment, a read scratch). It exists so
// bulk transcode paths — GLCB builds merging segment records into chunk
// frames — can move a record from one wire form to another without
// materializing the attrs map and Raw copy that a full Record carries:
// the per-record map alone was ~24GB of garbage per soak run.
//
// Lifetime: a View is valid only until its source buffer is reused or
// unmapped, and only on the goroutine that produced it. Consumers that
// need to retain anything must copy (Materialize).
type View struct {
	EventID  EventID
	SourceTS time.Time
	IngestTS time.Time
	WriteTS  time.Time
	// AttrsWire is the record's attributes in segment wire form:
	// [count:u16] then per pair [klen:u16 key][vlen:u16 value],
	// key-sorted at encode time.
	AttrsWire []byte
	Raw       []byte
}

// Materialize copies a View into a self-contained Record.
func (v View) Materialize() (Record, error) {
	attrs, _, err := DecodeAttributes(v.AttrsWire)
	if err != nil {
		return Record{}, err
	}
	raw := make([]byte, len(v.Raw))
	copy(raw, v.Raw)
	return Record{
		EventID:  v.EventID,
		SourceTS: v.SourceTS,
		IngestTS: v.IngestTS,
		WriteTS:  v.WriteTS,
		Attrs:    attrs,
		Raw:      raw,
	}, nil
}

// IterEncodedAttributes walks wire-form attributes, yielding each key and
// value as sub-slices of data — no allocation. Returns the encoded length
// consumed; the scan always completes so the caller can advance past the
// blob, but fn stops being invoked once it returns false.
func IterEncodedAttributes(data []byte, fn func(k, v []byte) bool) (int, error) {
	if len(data) < 2 {
		return 0, ErrInvalidAttrsData
	}
	count := int(binary.LittleEndian.Uint16(data[0:2]))
	off := 2
	stopped := false
	for range count {
		if off+2 > len(data) {
			return 0, ErrInvalidAttrsData
		}
		klen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+klen+2 > len(data) {
			return 0, ErrInvalidAttrsData
		}
		k := data[off : off+klen]
		off += klen
		vlen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+vlen > len(data) {
			return 0, ErrInvalidAttrsData
		}
		v := data[off : off+vlen]
		off += vlen
		if !stopped && !fn(k, v) {
			stopped = true
		}
	}
	return off, nil
}

// EncodedAttributesCount returns the pair count of wire-form attributes.
func EncodedAttributesCount(data []byte) (int, error) {
	if len(data) < 2 {
		return 0, ErrInvalidAttrsData
	}
	return int(binary.LittleEndian.Uint16(data[0:2])), nil
}
