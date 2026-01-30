package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"

	"github.com/google/uuid"
)

const (
	sourcesFileName      = "sources.bin"
	currentSourceVersion = 0x01

	sourceSizeFieldBytes = 4
	sourceUUIDBytes      = 16
	sourceLocalIDBytes   = 4

	sourcePayloadBytes = format.HeaderSize + sourceUUIDBytes + sourceLocalIDBytes
	sourceTotalBytes   = sourceSizeFieldBytes + sourcePayloadBytes + sourceSizeFieldBytes
)

var ErrSourceSizeMismatch = errors.New("source map size mismatch")
var ErrSourceTooSmall = errors.New("source map size too small")

type SourceMap struct {
	mu       sync.Mutex
	path     string
	fileMode os.FileMode
	forward  map[chunk.SourceID]uint32
	reverse  map[uint32]chunk.SourceID
	next     uint32
}

func NewSourceMap(dir string, fileMode os.FileMode) *SourceMap {
	if fileMode == 0 {
		fileMode = 0o644
	}
	return &SourceMap{
		path:     filepath.Join(dir, sourcesFileName),
		fileMode: fileMode,
		forward:  make(map[chunk.SourceID]uint32),
		reverse:  make(map[uint32]chunk.SourceID),
		next:     1,
	}
}

func (m *SourceMap) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := os.Open(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	for {
		var sizeBuf [sourceSizeFieldBytes]byte
		if _, err := io.ReadFull(file, sizeBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		size := binary.LittleEndian.Uint32(sizeBuf[:])
		if int(size) < sourceTotalBytes {
			return ErrSourceTooSmall
		}
		payload := make([]byte, int(size)-sourceSizeFieldBytes)
		if _, err := io.ReadFull(file, payload); err != nil {
			return err
		}
		record, err := decodeSourceRecord(payload)
		if err != nil {
			return err
		}
		m.forward[record.sourceID] = record.localID
		m.reverse[record.localID] = record.sourceID
		if record.localID >= m.next {
			m.next = record.localID + 1
		}
	}
}

func (m *SourceMap) GetOrAssign(sourceID chunk.SourceID) (uint32, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if id, ok := m.forward[sourceID]; ok {
		return id, false, nil
	}
	localID := m.next
	m.next++
	if err := m.appendLocked(sourceID, localID); err != nil {
		return 0, false, err
	}
	m.forward[sourceID] = localID
	m.reverse[localID] = sourceID
	return localID, true, nil
}

func (m *SourceMap) Resolve(localID uint32) (chunk.SourceID, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.reverse[localID]
	return id, ok
}

// ResolveWithLen is like Resolve but also returns the map length for debugging.
func (m *SourceMap) ResolveWithLen(localID uint32) (chunk.SourceID, bool, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.reverse[localID]
	return id, ok, len(m.reverse)
}

func (m *SourceMap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.reverse)
}

func (m *SourceMap) appendLocked(sourceID chunk.SourceID, localID uint32) error {
	record := encodeSourceRecord(sourceID, localID)
	file, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, m.fileMode)
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.Write(record)
	if err != nil {
		return err
	}
	if n != len(record) {
		return io.ErrShortWrite
	}
	return nil
}

type sourceRecord struct {
	sourceID chunk.SourceID
	localID  uint32
}

// encodeSourceRecord encodes a source mapping into binary format.
// The source map file is append-only, with each record mapping a global
// SourceID (UUID) to a chunk-local uint32 ID for compact storage in records.
//
// Layout (32 bytes per record):
//
//	size (4 bytes, little-endian uint32, always 32)
//	signature (1 byte, 'i')
//	type (1 byte, 'c')
//	version (1 byte, 0x01)
//	flags (1 byte, reserved)
//	sourceID (16 bytes, UUID)
//	localID (4 bytes, little-endian uint32)
//	size (4 bytes, little-endian uint32, repeated for validation)
func encodeSourceRecord(sourceID chunk.SourceID, localID uint32) []byte {
	buf := make([]byte, sourceTotalBytes)
	cursor := 0

	binary.LittleEndian.PutUint32(buf[cursor:cursor+sourceSizeFieldBytes], uint32(sourceTotalBytes))
	cursor += sourceSizeFieldBytes

	h := format.Header{Type: format.TypeChunkSourceMap, Version: currentSourceVersion, Flags: 0}
	cursor += h.EncodeInto(buf[cursor:])

	copy(buf[cursor:cursor+sourceUUIDBytes], sourceIDBytes(sourceID))
	cursor += sourceUUIDBytes

	binary.LittleEndian.PutUint32(buf[cursor:cursor+sourceLocalIDBytes], localID)
	cursor += sourceLocalIDBytes

	binary.LittleEndian.PutUint32(buf[cursor:cursor+sourceSizeFieldBytes], uint32(sourceTotalBytes))
	return buf
}

func decodeSourceRecord(payload []byte) (sourceRecord, error) {
	if len(payload) < sourcePayloadBytes+sourceSizeFieldBytes {
		return sourceRecord{}, ErrSourceTooSmall
	}

	_, err := format.DecodeAndValidate(payload, format.TypeChunkSourceMap, currentSourceVersion)
	if err != nil {
		return sourceRecord{}, fmt.Errorf("chunk source map: %w", err)
	}
	cursor := format.HeaderSize

	idBytes := payload[cursor : cursor+sourceUUIDBytes]
	cursor += sourceUUIDBytes

	localID := binary.LittleEndian.Uint32(payload[cursor : cursor+sourceLocalIDBytes])
	cursor += sourceLocalIDBytes

	trailing := binary.LittleEndian.Uint32(payload[cursor : cursor+sourceSizeFieldBytes])
	if trailing != uint32(sourceTotalBytes) {
		return sourceRecord{}, ErrSourceSizeMismatch
	}

	return sourceRecord{
		sourceID: sourceIDFromBytes(idBytes),
		localID:  localID,
	}, nil
}

func sourceIDFromBytes(input []byte) chunk.SourceID {
	var raw [sourceUUIDBytes]byte
	copy(raw[:], input)
	return chunk.SourceID(uuid.UUID(raw))
}

func sourceIDBytes(id chunk.SourceID) []byte {
	raw := uuid.UUID(id)
	bytes := raw[:]
	out := make([]byte, len(bytes))
	copy(out, bytes)
	return out
}
