package file

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/kluzzebass/gastrolog/internal/chunk"
)

const (
	metaVersion     = 0x01
	metaFileName    = "meta.bin"
	recordsFileName = "records.log"

	metaSizeFieldBytes = 4
	metaVersionBytes   = 1
	metaChunkIDBytes   = 16
	metaTSBytes        = 8
	metaSizeBytes      = 8
	metaFlagsBytes     = 1

	metaPayloadBytes = metaVersionBytes + metaChunkIDBytes + metaTSBytes + metaTSBytes + metaSizeBytes + metaFlagsBytes
	metaTotalBytes   = metaSizeFieldBytes + metaPayloadBytes + metaSizeFieldBytes
)

const (
	metaFlagSealed = 0x01
)

var ErrMetaSizeMismatch = errors.New("meta size mismatch")
var ErrMetaTooSmall = errors.New("meta size too small")
var ErrMetaVersionMismatch = errors.New("meta version mismatch")

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

func encodeMeta(meta chunk.ChunkMeta) []byte {
	buf := make([]byte, metaTotalBytes)
	binary.LittleEndian.PutUint32(buf[:metaSizeFieldBytes], uint32(metaTotalBytes))
	cursor := metaSizeFieldBytes
	buf[cursor] = metaVersion
	cursor += metaVersionBytes
	copy(buf[cursor:cursor+metaChunkIDBytes], uuidBytes(meta.ID))
	cursor += metaChunkIDBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+metaTSBytes], uint64(meta.StartTS))
	cursor += metaTSBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+metaTSBytes], uint64(meta.EndTS))
	cursor += metaTSBytes
	binary.LittleEndian.PutUint64(buf[cursor:cursor+metaSizeBytes], uint64(meta.Size))
	cursor += metaSizeBytes
	buf[cursor] = metaFlags(meta)
	cursor += metaFlagsBytes
	binary.LittleEndian.PutUint32(buf[cursor:cursor+metaSizeFieldBytes], uint32(metaTotalBytes))
	return buf
}

func decodeMeta(buf []byte) (chunk.ChunkMeta, error) {
	if len(buf) < metaTotalBytes {
		return chunk.ChunkMeta{}, ErrMetaTooSmall
	}
	size := binary.LittleEndian.Uint32(buf[:metaSizeFieldBytes])
	if int(size) != len(buf) || int(size) != metaTotalBytes {
		return chunk.ChunkMeta{}, ErrMetaSizeMismatch
	}
	cursor := metaSizeFieldBytes
	if buf[cursor] != metaVersion {
		return chunk.ChunkMeta{}, ErrMetaVersionMismatch
	}
	cursor += metaVersionBytes
	idBytes := buf[cursor : cursor+metaChunkIDBytes]
	cursor += metaChunkIDBytes
	startTS := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+metaTSBytes]))
	cursor += metaTSBytes
	endTS := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+metaTSBytes]))
	cursor += metaTSBytes
	sizeBytes := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+metaSizeBytes]))
	cursor += metaSizeBytes
	flags := buf[cursor]
	cursor += metaFlagsBytes
	trailing := binary.LittleEndian.Uint32(buf[cursor : cursor+metaSizeFieldBytes])
	if trailing != size {
		return chunk.ChunkMeta{}, ErrMetaSizeMismatch
	}
	id := chunkIDFromBytes(idBytes)
	return chunk.ChunkMeta{
		ID:      id,
		StartTS: startTS,
		EndTS:   endTS,
		Size:    sizeBytes,
		Sealed:  flags&metaFlagSealed != 0,
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
