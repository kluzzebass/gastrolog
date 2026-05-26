package file

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
)

// ChunkContainsVaultSeq reports whether a sealed or active chunk contains seq.
func (m *Manager) ChunkContainsVaultSeq(id chunk.ChunkID, seq uint64) (bool, error) {
	if seq == 0 {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.metas[id]
	if !ok {
		if m.active != nil && m.active.meta.id == id {
			return chunkStateContainsVaultSeq(m.active, seq), nil
		}
		return false, nil
	}
	return scanChunkFileVaultSeq(m.idxLogPath(id), meta.idxLogVersion, seq)
}

func chunkStateContainsVaultSeq(state *chunkState, seq uint64) bool {
	if state == nil || state.idxFile == nil {
		return false
	}
	version := state.meta.idxLogVersion
	if version == 0 {
		version = IdxLogVersion
	}
	count := state.recordCount
	stride := IdxEntryStride(version)
	buf := make([]byte, stride)
	for i := range count {
		offset := int64(IdxHeaderSize) + int64(i)*int64(stride)
		if _, err := state.idxFile.ReadAt(buf, offset); err != nil {
			return false
		}
		entry := DecodeIdxEntry(buf)
		if entry.VaultSeq == seq {
			return true
		}
	}
	return false
}

func scanChunkFileVaultSeq(idxPath string, version byte, seq uint64) (bool, error) {
	if version == 0 {
		version = IdxLogVersion
	}
	f, err := os.Open(filepath.Clean(idxPath))
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	var headerBuf [IdxHeaderSize]byte
	if _, err := f.ReadAt(headerBuf[:], 0); err != nil {
		return false, err
	}
	h, err := format.Decode(headerBuf[:format.HeaderSize])
	if err != nil {
		return false, err
	}
	if h.Type != format.TypeIdxLog {
		return false, fmt.Errorf("chunk: unexpected idx type 0x%02x", h.Type)
	}
	if h.Version >= IdxLogVersionV2 {
		version = h.Version
	}
	count := RecordCountForVersion(info.Size(), version)
	stride := IdxEntryStride(version)
	buf := make([]byte, stride)
	for i := range count {
		offset := int64(IdxHeaderSize) + int64(i)*int64(stride)
		if _, err := f.ReadAt(buf, offset); err != nil {
			return false, err
		}
		if DecodeIdxEntry(buf).VaultSeq == seq {
			return true, nil
		}
	}
	return false, nil
}

func (m *Manager) ensureActiveIdxV2Locked() error {
	if m.active == nil {
		return nil
	}
	if m.active.meta.idxLogVersion >= IdxLogVersionV2 {
		return nil
	}
	if m.active.recordCount > 0 {
		return nil
	}
	var headerBuf [IdxHeaderSize]byte
	header := format.Header{
		Type:    format.TypeIdxLog,
		Version: IdxLogVersionV2,
		Flags:   0,
	}
	header.EncodeInto(headerBuf[:])
	binary.LittleEndian.PutUint64(headerBuf[format.HeaderSize:], uint64(m.active.createdAt.UnixNano()))
	if _, err := m.active.idxFile.WriteAt(headerBuf[:], 0); err != nil {
		return fmt.Errorf("upgrade idx.log to v2: %w", err)
	}
	m.active.meta.idxLogVersion = IdxLogVersionV2
	return nil
}
