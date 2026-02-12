// Package format provides shared binary format utilities.
package format

import "errors"

// Header layout (4 bytes):
//
//	signature (1 byte, 'i' = 0x69)
//	type (1 byte, identifies format)
//	version (1 byte)
//	flags (1 byte, reserved)
//
// Type codes:
//
//	't' = time index
//	's' = source index (SourceTS)
//	'I' = ingest index (IngestTS)
//	'k' = token index
//	'm' = chunk metadata (deprecated)
//	'z' = source registry
//	'c' = chunk source map
//	'r' = raw log data
//	'i' = index log (record metadata)
const (
	Signature  = 'i'
	HeaderSize = 4

	TypeTimeIndex      = 't'
	TypeSourceIndex    = 's' // SourceTS timestamp index
	TypeIngestIndex    = 'I' // IngestTS timestamp index
	TypeTokenIndex     = 'k'
	TypeAttrKeyIndex   = 'K' // Attribute key index
	TypeAttrValueIndex = 'V' // Attribute value index
	TypeAttrKVIndex    = 'P' // Attribute key-value pair index
	TypeKVIndex        = 'M' // Key-value pair index (heuristic)
	TypeKVKeyIndex     = 'N' // Key index (heuristic)
	TypeKVValueIndex   = 'O' // Value index (heuristic)
	TypeChunkMeta      = 'm' // Deprecated: meta.bin no longer used
	TypeSourceRegistry = 'z' // Deprecated: source registry no longer used
	TypeChunkSourceMap = 'c' // Deprecated: sources.bin no longer used
	TypeRawLog         = 'r'
	TypeIdxLog         = 'i'
	TypeAttrLog        = 'a'

	// Flag bits for raw.log, idx.log, and attr.log headers.
	FlagSealed    = 0x01
	FlagSmallTime = 0x02 // Timestamps in idx.log entries use Unix nanoseconds; unset = Unix microseconds

	// Flag bits for index file headers.
	// FlagComplete indicates the index was fully written (not a partial/crashed write).
	FlagComplete = 0x01
)

var (
	ErrHeaderTooSmall    = errors.New("header too small")
	ErrSignatureMismatch = errors.New("signature mismatch")
	ErrTypeMismatch      = errors.New("type mismatch")
	ErrVersionMismatch   = errors.New("version mismatch")
)

// Header represents the common 4-byte header.
type Header struct {
	Type    byte
	Version byte
	Flags   byte
}

// Encode writes the header to a 4-byte slice.
func (h Header) Encode() [HeaderSize]byte {
	return [HeaderSize]byte{Signature, h.Type, h.Version, h.Flags}
}

// EncodeInto writes the header into the given buffer at offset 0.
// Returns the number of bytes written (always HeaderSize).
func (h Header) EncodeInto(buf []byte) int {
	buf[0] = Signature
	buf[1] = h.Type
	buf[2] = h.Version
	buf[3] = h.Flags
	return HeaderSize
}

// Decode reads a header from the given buffer.
// Returns ErrHeaderTooSmall if buf is less than HeaderSize bytes.
// Returns ErrSignatureMismatch if the signature byte is not 'i'.
func Decode(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, ErrHeaderTooSmall
	}
	if buf[0] != Signature {
		return Header{}, ErrSignatureMismatch
	}
	return Header{
		Type:    buf[1],
		Version: buf[2],
		Flags:   buf[3],
	}, nil
}

// DecodeAndValidate reads a header and validates the type and version.
// Returns ErrTypeMismatch if the type doesn't match expectedType.
// Returns ErrVersionMismatch if the version doesn't match expectedVersion.
func DecodeAndValidate(buf []byte, expectedType, expectedVersion byte) (Header, error) {
	h, err := Decode(buf)
	if err != nil {
		return Header{}, err
	}
	if h.Type != expectedType {
		return Header{}, ErrTypeMismatch
	}
	if h.Version != expectedVersion {
		return Header{}, ErrVersionMismatch
	}
	return h, nil
}
