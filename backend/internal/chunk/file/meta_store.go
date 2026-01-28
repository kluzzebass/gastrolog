package file

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"gastrolog/internal/chunk"
)

const (
	metaSignatureByte = 'i'
	metaTypeByte      = 'm'
	metaVersion       = 0x01
	metaFileName      = "meta.bin"
	recordsFileName   = "records.log"

	metaSignatureBytes = 1
	metaTypeBytes      = 1
	metaVersionBytes   = 1
	metaChunkIDBytes   = 16
	metaTSBytes        = 8
	metaSizeBytes      = 8
	metaFlagsBytes     = 1

	metaTotalBytes = metaSignatureBytes + metaTypeBytes + metaVersionBytes + metaFlagsBytes + metaChunkIDBytes + metaTSBytes + metaTSBytes + metaSizeBytes
)

const (
	metaFlagSealed = 0x01
)

var ErrMetaTooSmall = errors.New("meta size too small")
var ErrMetaSignatureMismatch = errors.New("meta signature mismatch")
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
	cursor := 0
	buf[cursor] = metaSignatureByte
	cursor += metaSignatureBytes
	buf[cursor] = metaTypeByte
	cursor += metaTypeBytes
	buf[cursor] = metaVersion
	cursor += metaVersionBytes
	buf[cursor] = metaFlags(meta)
	cursor += metaFlagsBytes
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
	cursor := 0
	if buf[cursor] != metaSignatureByte || buf[cursor+metaSignatureBytes] != metaTypeByte {
		return chunk.ChunkMeta{}, ErrMetaSignatureMismatch
	}
	cursor += metaSignatureBytes + metaTypeBytes
	if buf[cursor] != metaVersion {
		return chunk.ChunkMeta{}, ErrMetaVersionMismatch
	}
	cursor += metaVersionBytes
	flags := buf[cursor]
	cursor += metaFlagsBytes
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
