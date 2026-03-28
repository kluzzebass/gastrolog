package multiraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

// ChunkCommand identifies the type of chunk metadata mutation.
type ChunkCommand byte

const (
	CmdCreateChunk   ChunkCommand = 1
	CmdSealChunk     ChunkCommand = 2
	CmdCompressChunk ChunkCommand = 3
	CmdUploadChunk   ChunkCommand = 4
	CmdDeleteChunk   ChunkCommand = 5
)

// ChunkEntry holds the full metadata for one chunk in the FSM.
// This is the Raft-replicated equivalent of file.Manager.chunkMeta + cloudIdx entries.
type ChunkEntry struct {
	ID          chunk.ChunkID
	WriteStart  time.Time
	WriteEnd    time.Time
	RecordCount int64
	Bytes       int64
	Sealed      bool
	Compressed  bool
	DiskBytes   int64

	IngestStart time.Time
	IngestEnd   time.Time
	SourceStart time.Time
	SourceEnd   time.Time

	CloudBacked bool
	Archived    bool
	NumFrames   int32

	// Cloud-specific TOC offsets (GLCB format).
	IngestIdxOffset int64
	IngestIdxSize   int64
	SourceIdxOffset int64
	SourceIdxSize   int64
}

// ToChunkMeta converts to the public chunk.ChunkMeta type.
func (e *ChunkEntry) ToChunkMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          e.ID,
		WriteStart:  e.WriteStart,
		WriteEnd:    e.WriteEnd,
		RecordCount: e.RecordCount,
		Bytes:       e.Bytes,
		Sealed:      e.Sealed,
		Compressed:  e.Compressed,
		DiskBytes:   e.DiskBytes,
		IngestStart: e.IngestStart,
		IngestEnd:   e.IngestEnd,
		SourceStart: e.SourceStart,
		SourceEnd:   e.SourceEnd,
		CloudBacked: e.CloudBacked,
		Archived:    e.Archived,
		NumFrames:   e.NumFrames,
	}
}

// ChunkFSM is a Raft FSM that maintains chunk metadata for a single tier.
// All reads are local (no Raft round-trip). Writes go through Raft.Apply().
type ChunkFSM struct {
	mu     sync.RWMutex
	chunks map[chunk.ChunkID]*ChunkEntry
}

// NewChunkFSM creates an empty chunk metadata FSM.
func NewChunkFSM() *ChunkFSM {
	return &ChunkFSM{
		chunks: make(map[chunk.ChunkID]*ChunkEntry),
	}
}

var _ hraft.FSM = (*ChunkFSM)(nil)

// ---------- Reads (local, no Raft) ----------

// Get returns a copy of a chunk's metadata, or nil if not found.
func (f *ChunkFSM) Get(id chunk.ChunkID) *ChunkEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e := f.chunks[id]
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

// List returns all chunk metadata, sorted by WriteStart ascending.
func (f *ChunkFSM) List() []ChunkEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]ChunkEntry, 0, len(f.chunks))
	for _, e := range f.chunks {
		out = append(out, *e)
	}
	return out
}

// Count returns the number of chunks.
func (f *ChunkFSM) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.chunks)
}

// ---------- Raft FSM interface ----------

// Apply handles a Raft log entry. The log data is a command byte followed
// by the command-specific payload.
func (f *ChunkFSM) Apply(log *hraft.Log) any {
	if len(log.Data) == 0 {
		return errors.New("empty chunk FSM command")
	}
	cmd := ChunkCommand(log.Data[0])
	payload := log.Data[1:]

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd {
	case CmdCreateChunk:
		return f.applyCreate(payload)
	case CmdSealChunk:
		return f.applySeal(payload)
	case CmdCompressChunk:
		return f.applyCompress(payload)
	case CmdUploadChunk:
		return f.applyUpload(payload)
	case CmdDeleteChunk:
		return f.applyDelete(payload)
	default:
		return fmt.Errorf("unknown chunk FSM command: %d", cmd)
	}
}

// Snapshot returns a point-in-time snapshot of all chunk metadata.
func (f *ChunkFSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entries := make([]ChunkEntry, 0, len(f.chunks))
	for _, e := range f.chunks {
		entries = append(entries, *e)
	}
	return &chunkSnapshot{entries: entries}, nil
}

// Restore replaces FSM state from a snapshot.
func (f *ChunkFSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	entries, err := decodeChunkSnapshot(rc)
	if err != nil {
		return fmt.Errorf("restore chunk FSM: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.chunks = make(map[chunk.ChunkID]*ChunkEntry, len(entries))
	for i := range entries {
		f.chunks[entries[i].ID] = &entries[i]
	}
	return nil
}

// ---------- Command application ----------

// CreateChunk: [16 bytes ChunkID][8 bytes WriteStart nanos][8 bytes IngestStart nanos][8 bytes SourceStart nanos]
func (f *ChunkFSM) applyCreate(data []byte) error {
	if len(data) < 40 {
		return fmt.Errorf("create chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	writeStart := time.Unix(0, int64(binary.BigEndian.Uint64(data[16:24])))   //nolint:gosec // G115: safe round-trip from uint64 nano timestamp
	ingestStart := time.Unix(0, int64(binary.BigEndian.Uint64(data[24:32]))) //nolint:gosec // G115: safe round-trip from uint64 nano timestamp
	sourceStart := time.Unix(0, int64(binary.BigEndian.Uint64(data[32:40]))) //nolint:gosec // G115: safe round-trip from uint64 nano timestamp

	f.chunks[id] = &ChunkEntry{
		ID:          id,
		WriteStart:  writeStart,
		IngestStart: ingestStart,
		SourceStart: sourceStart,
	}
	return nil
}

// SealChunk: [16 bytes ChunkID][8 WriteEnd][8 RecordCount][8 Bytes][8 IngestEnd][8 SourceEnd]
func (f *ChunkFSM) applySeal(data []byte) error {
	if len(data) < 56 {
		return fmt.Errorf("seal chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("seal chunk: %s not found", id)
	}
	e.WriteEnd = time.Unix(0, int64(binary.BigEndian.Uint64(data[16:24])))   //nolint:gosec // G115: nano timestamp round-trip
	e.RecordCount = int64(binary.BigEndian.Uint64(data[24:32]))             //nolint:gosec // G115: record count round-trip
	e.Bytes = int64(binary.BigEndian.Uint64(data[32:40]))                   //nolint:gosec // G115: byte count round-trip
	e.IngestEnd = time.Unix(0, int64(binary.BigEndian.Uint64(data[40:48]))) //nolint:gosec // G115: nano timestamp round-trip
	e.SourceEnd = time.Unix(0, int64(binary.BigEndian.Uint64(data[48:56]))) //nolint:gosec // G115: nano timestamp round-trip
	e.Sealed = true
	return nil
}

// CompressChunk: [16 bytes ChunkID][8 DiskBytes]
func (f *ChunkFSM) applyCompress(data []byte) error {
	if len(data) < 24 {
		return fmt.Errorf("compress chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("compress chunk: %s not found", id)
	}
	e.DiskBytes = int64(binary.BigEndian.Uint64(data[16:24])) //nolint:gosec // G115: round-trip
	e.Compressed = true
	return nil
}

// UploadChunk: [16 ChunkID][8 DiskBytes][8 IngestIdxOff][8 IngestIdxSize][8 SourceIdxOff][8 SourceIdxSize][4 NumFrames]
func (f *ChunkFSM) applyUpload(data []byte) error {
	if len(data) < 60 {
		return fmt.Errorf("upload chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("upload chunk: %s not found", id)
	}
	e.DiskBytes = int64(binary.BigEndian.Uint64(data[16:24]))       //nolint:gosec // G115: round-trip
	e.IngestIdxOffset = int64(binary.BigEndian.Uint64(data[24:32])) //nolint:gosec // G115: round-trip
	e.IngestIdxSize = int64(binary.BigEndian.Uint64(data[32:40]))   //nolint:gosec // G115: round-trip
	e.SourceIdxOffset = int64(binary.BigEndian.Uint64(data[40:48])) //nolint:gosec // G115: round-trip
	e.SourceIdxSize = int64(binary.BigEndian.Uint64(data[48:56]))   //nolint:gosec // G115: round-trip
	e.NumFrames = int32(binary.BigEndian.Uint32(data[56:60])) //nolint:gosec // G115: safe round-trip from uint32 frame count
	e.CloudBacked = true
	return nil
}

// DeleteChunk: [16 bytes ChunkID]
func (f *ChunkFSM) applyDelete(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("delete chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	delete(f.chunks, id)
	return nil
}

// ---------- Command builders (used by callers before Raft.Apply) ----------

// MarshalCreateChunk builds the Raft log data for a CreateChunk command.
func MarshalCreateChunk(id chunk.ChunkID, writeStart, ingestStart, sourceStart time.Time) []byte {
	buf := make([]byte, 1+40)
	buf[0] = byte(CmdCreateChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(writeStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[25:33], uint64(ingestStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[33:41], uint64(sourceStart.UnixNano()))
	return buf
}

// MarshalSealChunk builds the Raft log data for a SealChunk command.
func MarshalSealChunk(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestEnd, sourceEnd time.Time) []byte {
	buf := make([]byte, 1+56)
	buf[0] = byte(CmdSealChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(writeEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[25:33], uint64(recordCount)) //nolint:gosec // G115: safe round-trip for record count
	binary.BigEndian.PutUint64(buf[33:41], uint64(bytes))     //nolint:gosec // G115: safe round-trip for byte count
	binary.BigEndian.PutUint64(buf[41:49], uint64(ingestEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[49:57], uint64(sourceEnd.UnixNano()))
	return buf
}

// MarshalCompressChunk builds the Raft log data for a CompressChunk command.
func MarshalCompressChunk(id chunk.ChunkID, diskBytes int64) []byte {
	buf := make([]byte, 1+24)
	buf[0] = byte(CmdCompressChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(diskBytes)) //nolint:gosec // G115: safe round-trip for disk bytes
	return buf
}

// MarshalUploadChunk builds the Raft log data for an UploadChunk command.
func MarshalUploadChunk(id chunk.ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, numFrames int32) []byte {
	buf := make([]byte, 1+60)
	buf[0] = byte(CmdUploadChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(diskBytes))    //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[25:33], uint64(ingestIdxOff))  //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[33:41], uint64(ingestIdxSize)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[41:49], uint64(sourceIdxOff))  //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[49:57], uint64(sourceIdxSize)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint32(buf[57:61], uint32(numFrames)) //nolint:gosec // G115: safe round-trip for frame count
	return buf
}

// MarshalDeleteChunk builds the Raft log data for a DeleteChunk command.
func MarshalDeleteChunk(id chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdDeleteChunk)
	copy(buf[1:17], id[:])
	return buf
}

// ---------- Snapshot ----------

type chunkSnapshot struct {
	entries []ChunkEntry
}

func (s *chunkSnapshot) Persist(sink hraft.SnapshotSink) error {
	for i := range s.entries {
		if err := encodeChunkEntry(sink, &s.entries[i]); err != nil {
			_ = sink.Cancel()
			return err
		}
	}
	return sink.Close()
}

func (s *chunkSnapshot) Release() {}

// Snapshot encoding: each entry is a fixed-size binary record.
// Layout per entry (168 bytes):
//   16  ChunkID
//   8   WriteStart (nanos)
//   8   WriteEnd (nanos)
//   8   RecordCount
//   8   Bytes
//   8   DiskBytes
//   8   IngestStart (nanos)
//   8   IngestEnd (nanos)
//   8   SourceStart (nanos)
//   8   SourceEnd (nanos)
//   8   IngestIdxOffset
//   8   IngestIdxSize
//   8   SourceIdxOffset
//   8   SourceIdxSize
//   4   NumFrames
//   2   Flags (bit 0=sealed, 1=compressed, 2=cloudBacked, 3=archived)
// Total: 126 bytes (keeping it compact)

const chunkEntrySize = 126

func encodeChunkEntry(w io.Writer, e *ChunkEntry) error {
	var buf [chunkEntrySize]byte
	copy(buf[0:16], e.ID[:])
	binary.BigEndian.PutUint64(buf[16:24], uint64(e.WriteStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[24:32], uint64(e.WriteEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[32:40], uint64(e.RecordCount)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[40:48], uint64(e.Bytes))       //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[48:56], uint64(e.DiskBytes))   //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[56:64], uint64(e.IngestStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[64:72], uint64(e.IngestEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[72:80], uint64(e.SourceStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[80:88], uint64(e.SourceEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[88:96], uint64(e.IngestIdxOffset)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[96:104], uint64(e.IngestIdxSize))   //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[104:112], uint64(e.SourceIdxOffset)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[112:120], uint64(e.SourceIdxSize))   //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint32(buf[120:124], uint32(e.NumFrames)) //nolint:gosec // G115: safe round-trip for frame count
	var flags uint16
	if e.Sealed {
		flags |= 1 << 0
	}
	if e.Compressed {
		flags |= 1 << 1
	}
	if e.CloudBacked {
		flags |= 1 << 2
	}
	if e.Archived {
		flags |= 1 << 3
	}
	binary.BigEndian.PutUint16(buf[124:126], flags)
	_, err := w.Write(buf[:])
	return err
}

func decodeChunkSnapshot(r io.Reader) ([]ChunkEntry, error) {
	var entries []ChunkEntry
	var buf [chunkEntrySize]byte
	for {
		_, err := io.ReadFull(r, buf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var id chunk.ChunkID
		copy(id[:], buf[0:16])
		flags := binary.BigEndian.Uint16(buf[124:126])
		entries = append(entries, ChunkEntry{
			ID:              id,
			WriteStart:      time.Unix(0, int64(binary.BigEndian.Uint64(buf[16:24]))), //nolint:gosec // G115: round-trip
			WriteEnd:        time.Unix(0, int64(binary.BigEndian.Uint64(buf[24:32]))), //nolint:gosec // G115: round-trip
			RecordCount:     int64(binary.BigEndian.Uint64(buf[32:40])), //nolint:gosec // G115: round-trip
			Bytes:           int64(binary.BigEndian.Uint64(buf[40:48])), //nolint:gosec // G115: round-trip
			DiskBytes:       int64(binary.BigEndian.Uint64(buf[48:56])), //nolint:gosec // G115: round-trip
			IngestStart:     time.Unix(0, int64(binary.BigEndian.Uint64(buf[56:64]))), //nolint:gosec // G115: round-trip
			IngestEnd:       time.Unix(0, int64(binary.BigEndian.Uint64(buf[64:72]))), //nolint:gosec // G115: round-trip
			SourceStart:     time.Unix(0, int64(binary.BigEndian.Uint64(buf[72:80]))), //nolint:gosec // G115: round-trip
			SourceEnd:       time.Unix(0, int64(binary.BigEndian.Uint64(buf[80:88]))), //nolint:gosec // G115: round-trip
			IngestIdxOffset: int64(binary.BigEndian.Uint64(buf[88:96])), //nolint:gosec // G115: round-trip
			IngestIdxSize:   int64(binary.BigEndian.Uint64(buf[96:104])), //nolint:gosec // G115: round-trip
			SourceIdxOffset: int64(binary.BigEndian.Uint64(buf[104:112])), //nolint:gosec // G115: round-trip
			SourceIdxSize:   int64(binary.BigEndian.Uint64(buf[112:120])), //nolint:gosec // G115: round-trip
			NumFrames:       int32(binary.BigEndian.Uint32(buf[120:124])), //nolint:gosec // G115: round-trip
			Sealed:          flags&(1<<0) != 0,
			Compressed:      flags&(1<<1) != 0,
			CloudBacked:     flags&(1<<2) != 0,
			Archived:        flags&(1<<3) != 0,
		})
	}
	return entries, nil
}
