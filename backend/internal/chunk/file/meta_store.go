package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"

	"github.com/google/uuid"
)

const (
	currentMetaVersion = 0x01
	metaFileName       = "meta.bin"
	recordsFileName    = "records.log"

	metaChunkIDBytes = 16
	metaTSBytes      = 8
	metaSizeBytes    = 8

	metaTotalBytes = format.HeaderSize + metaChunkIDBytes + metaTSBytes + metaTSBytes + metaSizeBytes
)

const (
	metaFlagSealed = 0x01
)

var ErrMetaTooSmall = errors.New("meta size too small")

type MetaStore struct {
	dir      string
	fileMode os.FileMode
}

func NewMetaStore(dir string, fileMode os.FileMode) *MetaStore {
	if fileMode == 0 {
		fileMode = 0o644
	}
	return &MetaStore{dir: dir, fileMode: fileMode}
}

func (s *MetaStore) Save(meta chunk.ChunkMeta) error {
	chunkDir := filepath.Join(s.dir, meta.ID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return err
	}
	data := encodeMeta(meta)
	tmpFile, err := os.CreateTemp(chunkDir, "meta-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	if err := tmpFile.Chmod(s.fileMode); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	metaPath := filepath.Join(chunkDir, metaFileName)
	return os.Rename(tmpPath, metaPath)
}

func (s *MetaStore) Load(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	metaPath := filepath.Join(s.dir, id.String(), metaFileName)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return chunk.ChunkMeta{}, err
	}
	return decodeMeta(data)
}

func (s *MetaStore) List() ([]chunk.ChunkMeta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	metas := make([]chunk.ChunkMeta, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id, err := chunk.ParseChunkID(entry.Name())
		if err != nil {
			continue
		}
		meta, err := s.Load(id)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

// encodeMeta encodes chunk metadata into binary format.
//
// Layout (44 bytes total):
//
//	signature (1 byte, 'i')
//	type (1 byte, 'm')
//	version (1 byte, 0x01)
//	flags (1 byte, bit 0 = sealed)
//	chunkID (16 bytes, UUID)
//	startTS (8 bytes, Unix microseconds, little-endian int64)
//	endTS (8 bytes, Unix microseconds, little-endian int64)
//	size (8 bytes, little-endian int64, chunk size in bytes)
func encodeMeta(meta chunk.ChunkMeta) []byte {
	buf := make([]byte, metaTotalBytes)
	cursor := 0

	h := format.Header{Type: format.TypeChunkMeta, Version: currentMetaVersion, Flags: metaFlags(meta)}
	cursor += h.EncodeInto(buf[cursor:])

	copy(buf[cursor:cursor+metaChunkIDBytes], uuidBytes(meta.ID))
	cursor += metaChunkIDBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+metaTSBytes], uint64(meta.StartTS.UnixMicro()))
	cursor += metaTSBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+metaTSBytes], uint64(meta.EndTS.UnixMicro()))
	cursor += metaTSBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+metaSizeBytes], uint64(meta.Size))
	return buf
}

func decodeMeta(buf []byte) (chunk.ChunkMeta, error) {
	if len(buf) != metaTotalBytes {
		return chunk.ChunkMeta{}, ErrMetaTooSmall
	}

	h, err := format.DecodeAndValidate(buf, format.TypeChunkMeta, currentMetaVersion)
	if err != nil {
		return chunk.ChunkMeta{}, fmt.Errorf("chunk meta: %w", err)
	}
	cursor := format.HeaderSize

	idBytes := buf[cursor : cursor+metaChunkIDBytes]
	cursor += metaChunkIDBytes
	startMicros := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+metaTSBytes]))
	cursor += metaTSBytes
	endMicros := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+metaTSBytes]))
	cursor += metaTSBytes
	sizeBytes := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+metaSizeBytes]))
	id := chunkIDFromBytes(idBytes)
	return chunk.ChunkMeta{
		ID:      id,
		StartTS: time.UnixMicro(startMicros),
		EndTS:   time.UnixMicro(endMicros),
		Size:    sizeBytes,
		Sealed:  h.Flags&metaFlagSealed != 0,
	}, nil
}

func metaFlags(meta chunk.ChunkMeta) byte {
	if meta.Sealed {
		return metaFlagSealed
	}
	return 0
}

func uuidBytes(id chunk.ChunkID) []byte {
	raw := uuid.UUID(id)
	bytes := raw[:]
	out := make([]byte, len(bytes))
	copy(out, bytes)
	return out
}

func chunkIDFromBytes(input []byte) chunk.ChunkID {
	var raw [metaChunkIDBytes]byte
	copy(raw[:], input)
	return chunk.ChunkID(uuid.UUID(raw))
}
