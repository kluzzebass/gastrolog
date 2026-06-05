package cloud

import (
	"encoding/binary"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

const (
	preambleSize   = 4 // format.HeaderSize
	layoutMetaSize = 128
	headerSize     = preambleSize + layoutMetaSize // fixed prefix before records

	// SectionBlobLayout is the layout-metadata block at offset 4.
	SectionBlobLayout = 'B'
)

// blobLayoutMeta is the layout-metadata section written at finalize.
type blobLayoutMeta struct {
	ChunkID     chunk.ChunkID
	VaultID     glid.GLID
	RecordCount uint32
	WriteStart  uint64
	WriteEnd    uint64
	IngestStart uint64
	IngestEnd   uint64
	SourceStart uint64
	SourceEnd   uint64
	DictEntries uint32
	DictSize    uint32
	RecordsOff  uint32
	RecordsSize uint32
	DictOff     uint32
	IndexOff    uint32
	IndexSize   uint32
}

func encodeBlobLayoutMeta(m blobLayoutMeta) []byte {
	buf := make([]byte, layoutMetaSize)
	copy(buf[0:16], m.ChunkID[:])
	copy(buf[16:32], m.VaultID[:])
	binary.LittleEndian.PutUint32(buf[32:36], m.RecordCount)
	binary.LittleEndian.PutUint64(buf[36:44], m.WriteStart)
	binary.LittleEndian.PutUint64(buf[44:52], m.WriteEnd)
	binary.LittleEndian.PutUint64(buf[52:60], m.IngestStart)
	binary.LittleEndian.PutUint64(buf[60:68], m.IngestEnd)
	binary.LittleEndian.PutUint64(buf[68:76], m.SourceStart)
	binary.LittleEndian.PutUint64(buf[76:84], m.SourceEnd)
	binary.LittleEndian.PutUint32(buf[84:88], m.DictEntries)
	binary.LittleEndian.PutUint32(buf[88:92], m.DictSize)
	binary.LittleEndian.PutUint32(buf[92:96], m.RecordsOff)
	binary.LittleEndian.PutUint32(buf[96:100], m.RecordsSize)
	binary.LittleEndian.PutUint32(buf[100:104], m.DictOff)
	binary.LittleEndian.PutUint32(buf[104:108], m.IndexOff)
	binary.LittleEndian.PutUint32(buf[108:112], m.IndexSize)
	return buf
}

func decodeBlobLayoutMeta(buf []byte) (blobLayoutMeta, error) {
	if len(buf) < layoutMetaSize {
		return blobLayoutMeta{}, fmt.Errorf("layout meta too small: %d", len(buf))
	}
	var m blobLayoutMeta
	copy(m.ChunkID[:], buf[0:16])
	copy(m.VaultID[:], buf[16:32])
	m.RecordCount = binary.LittleEndian.Uint32(buf[32:36])
	m.WriteStart = binary.LittleEndian.Uint64(buf[36:44])
	m.WriteEnd = binary.LittleEndian.Uint64(buf[44:52])
	m.IngestStart = binary.LittleEndian.Uint64(buf[52:60])
	m.IngestEnd = binary.LittleEndian.Uint64(buf[60:68])
	m.SourceStart = binary.LittleEndian.Uint64(buf[68:76])
	m.SourceEnd = binary.LittleEndian.Uint64(buf[76:84])
	m.DictEntries = binary.LittleEndian.Uint32(buf[84:88])
	m.DictSize = binary.LittleEndian.Uint32(buf[88:92])
	m.RecordsOff = binary.LittleEndian.Uint32(buf[92:96])
	m.RecordsSize = binary.LittleEndian.Uint32(buf[96:100])
	m.DictOff = binary.LittleEndian.Uint32(buf[100:104])
	m.IndexOff = binary.LittleEndian.Uint32(buf[104:108])
	m.IndexSize = binary.LittleEndian.Uint32(buf[108:112])
	return m, nil
}

func layoutMetaToBlobMeta(layout blobLayoutMeta, toc BlobTOC) BlobMeta {
	return BlobMeta{
		ChunkID:         layout.ChunkID,
		VaultID:         layout.VaultID,
		RecordCount:     layout.RecordCount,
		RawBytes:        int64(layout.RecordsSize),
		WriteStart:      tsFromNanos(layout.WriteStart),
		WriteEnd:        tsFromNanos(layout.WriteEnd),
		IngestStart:     tsFromNanos(layout.IngestStart),
		IngestEnd:       tsFromNanos(layout.IngestEnd),
		SourceStart:     tsFromNanos(layout.SourceStart),
		SourceEnd:       tsFromNanos(layout.SourceEnd),
		IngestIdxOffset: toc.IngestIdxOffset,
		IngestIdxSize:   toc.IngestIdxSize,
		SourceIdxOffset: toc.SourceIdxOffset,
		SourceIdxSize:   toc.SourceIdxSize,
	}
}
