package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/format"
	"gastrolog/internal/spool"
)

const lockFileName = ".spool.lock"

var (
	ErrVaultSeqRequired   = errors.New("spool: record missing vault_seq")
	ErrSegmentSealed      = errors.New("spool: segment is sealed")
	ErrNoActiveSegment    = errors.New("spool: no active segment")
	ErrActiveSegmentEmpty = errors.New("spool: active segment empty")
)

// Config configures a file-backed spool manager root directory.
type Config struct {
	Dir      string
	FileMode os.FileMode
}

// Manager stores spool segments on disk under Config.Dir.
type Manager struct {
	cfg               Config
	mu                sync.Mutex
	lock              *os.File
	byID              map[spool.SegmentID]*Segment
	active            spool.SegmentID
	reclaimThroughSeq uint64
}

type segmentFiles struct {
	raw  *os.File
	attr *os.File
	idx  *os.File
}

// Segment is one first_seq-addressable spool segment directory.
type Segment struct {
	dir          string
	files        segmentFiles
	recordCount  uint64
	rawOffset    uint64
	attrOffset   uint64
	firstSeq     uint64
	lastSeq      uint64
	sealed       bool
	createdAt    time.Time
	fileMode     os.FileMode
}

// NewManager opens or creates a spool directory and recovers segments.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.Dir == "" {
		return nil, errors.New("spool: dir required")
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = 0o640
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("spool: mkdir %s: %w", cfg.Dir, err)
	}
	lockPath := filepath.Join(cfg.Dir, lockFileName)
	lock, err := os.OpenFile(filepath.Clean(lockPath), os.O_CREATE|os.O_RDWR, cfg.FileMode)
	if err != nil {
		return nil, fmt.Errorf("spool: open lock: %w", err)
	}
	m := &Manager{cfg: cfg, lock: lock, byID: make(map[spool.SegmentID]*Segment)}
	if err := m.loadExisting(); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return m, nil
}

// Close releases the spool directory lock.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, seg := range m.byID {
		seg.closeFiles()
	}
	if m.lock != nil {
		return m.lock.Close()
	}
	return nil
}

func (m *Manager) loadExisting() error {
	entries, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		id, err := spool.ParseSegmentID(ent.Name())
		if err != nil {
			continue
		}
		seg, err := openSegment(filepath.Join(m.cfg.Dir, ent.Name()), m.cfg.FileMode)
		if err != nil {
			return fmt.Errorf("spool: open segment %s: %w", ent.Name(), err)
		}
		if spool.SegmentID(seg.firstSeq) != id {
			seg.closeFiles()
			return fmt.Errorf("spool: segment dir %s first_seq mismatch (%d vs %d)", ent.Name(), seg.firstSeq, id)
		}
		m.byID[id] = seg
		if !seg.sealed && (m.active == 0 || id > m.active) {
			m.active = id
		}
	}
	return nil
}

// Append appends one record to the active segment (creating it from record.VaultSeq when needed).
func (m *Manager) Append(rec chunk.Record) (spool.SegmentMeta, error) {
	if rec.VaultSeq == 0 {
		return spool.SegmentMeta{}, ErrVaultSeqRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	seg, err := m.activeSegmentLocked(rec.VaultSeq)
	if err != nil {
		return spool.SegmentMeta{}, err
	}
	if err := seg.appendDurable(rec); err != nil {
		return spool.SegmentMeta{}, err
	}
	return seg.meta(), nil
}

func (m *Manager) activeSegmentLocked(firstSeq uint64) (*Segment, error) {
	if m.active != 0 {
		if seg := m.byID[m.active]; seg != nil && !seg.sealed {
			return seg, nil
		}
	}
	id := spool.SegmentID(firstSeq)
	if seg := m.byID[id]; seg != nil {
		if seg.sealed {
			return nil, ErrSegmentSealed
		}
		m.active = id
		return seg, nil
	}
	seg, err := createSegment(filepath.Join(m.cfg.Dir, id.DirName()), firstSeq, m.cfg.FileMode)
	if err != nil {
		return nil, err
	}
	m.byID[id] = seg
	m.active = id
	return seg, nil
}

// SealActive seals the writable segment.
func (m *Manager) SealActive() (spool.SegmentMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == 0 {
		return spool.SegmentMeta{}, ErrNoActiveSegment
	}
	seg := m.byID[m.active]
	if seg == nil || seg.recordCount == 0 {
		return spool.SegmentMeta{}, ErrActiveSegmentEmpty
	}
	if err := seg.seal(); err != nil {
		return spool.SegmentMeta{}, err
	}
	meta := seg.meta()
	m.active = 0
	return meta, nil
}

// ListSegments returns segment metadata sorted by first_seq.
func (m *Manager) ListSegments() []spool.SegmentMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]spool.SegmentMeta, 0, len(m.byID))
	for _, seg := range m.byID {
		out = append(out, seg.meta())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FirstSeq < out[j].FirstSeq
	})
	return out
}

// Meta returns metadata for one segment.
func (m *Manager) Meta(id spool.SegmentID) (spool.SegmentMeta, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seg, ok := m.byID[id]
	if !ok {
		return spool.SegmentMeta{}, false
	}
	return seg.meta(), true
}

// SetReclaimThroughSeq sets the materialization safety watermark. Segments with
// last_seq above this value cannot be reclaimed.
func (m *Manager) SetReclaimThroughSeq(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reclaimThroughSeq = seq
}

// ReclaimThroughSeq returns the current materialization safety watermark.
func (m *Manager) ReclaimThroughSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reclaimThroughSeq
}

// ListReclaimable returns sealed segments eligible for reclaim at the current watermark.
func (m *Manager) ListReclaimable() []spool.SegmentMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []spool.SegmentMeta
	for _, seg := range m.byID {
		meta := seg.meta()
		if spool.Reclaimable(meta, m.reclaimThroughSeq, m.active) == nil {
			out = append(out, meta)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FirstSeq < out[j].FirstSeq
	})
	return out
}

// Reclaim deletes a sealed segment directory when it is at or below the safety watermark.
func (m *Manager) Reclaim(id spool.SegmentID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	seg, ok := m.byID[id]
	if !ok {
		return fmt.Errorf("%w: %s", spool.ErrSegmentNotFound, id.DirName())
	}
	meta := seg.meta()
	if err := spool.Reclaimable(meta, m.reclaimThroughSeq, m.active); err != nil {
		return err
	}
	dir := seg.dir
	seg.closeFiles()
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("spool: remove segment %s: %w", id.DirName(), err)
	}
	delete(m.byID, id)
	if m.active == id {
		m.active = 0
	}
	return nil
}

func (s *Segment) meta() spool.SegmentMeta {
	return spool.SegmentMeta{
		ID:          spool.SegmentID(s.firstSeq),
		FirstSeq:    s.firstSeq,
		LastSeq:     s.lastSeq,
		RecordCount: s.recordCount,
		Sealed:      s.sealed,
	}
}

func createSegment(dir string, firstSeq uint64, mode os.FileMode) (*Segment, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	raw, err := createRawFile(filepath.Join(dir, "raw.log"), mode)
	if err != nil {
		return nil, err
	}
	attr, err := createAttrFile(filepath.Join(dir, "attr.log"), mode)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	idx, err := createIdxFile(filepath.Join(dir, "idx.log"), now, mode)
	if err != nil {
		_ = raw.Close()
		_ = attr.Close()
		return nil, err
	}
	return &Segment{
		dir:       dir,
		files:     segmentFiles{raw: raw, attr: attr, idx: idx},
		firstSeq:  firstSeq,
		createdAt: now,
		fileMode:  mode,
	}, nil
}

func openSegment(dir string, mode os.FileMode) (*Segment, error) {
	raw, err := os.OpenFile(filepath.Clean(filepath.Join(dir, "raw.log")), os.O_RDWR, mode)
	if err != nil {
		return nil, err
	}
	attr, err := os.OpenFile(filepath.Clean(filepath.Join(dir, "attr.log")), os.O_RDWR, mode)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	idx, err := os.OpenFile(filepath.Clean(filepath.Join(dir, "idx.log")), os.O_RDWR, mode)
	if err != nil {
		_ = raw.Close()
		_ = attr.Close()
		return nil, err
	}
	seg := &Segment{dir: dir, files: segmentFiles{raw: raw, attr: attr, idx: idx}, fileMode: mode}
	if err := seg.recover(); err != nil {
		seg.closeFiles()
		return nil, err
	}
	return seg, nil
}

func (s *Segment) recover() error {
	var headerBuf [spool.IdxHeaderSize]byte
	if _, err := s.files.idx.ReadAt(headerBuf[:], 0); err != nil {
		return fmt.Errorf("read idx header: %w", err)
	}
	h, err := format.DecodeAndValidate(headerBuf[:format.HeaderSize], format.TypeIdxLog, chunkfile.IdxLogVersion)
	if err != nil {
		return err
	}
	s.sealed = h.Flags&format.FlagSealed != 0
	createdAtNanos := binary.LittleEndian.Uint64(headerBuf[format.HeaderSize:])
	s.createdAt = time.Unix(0, int64(createdAtNanos)).UTC() //nolint:gosec // G115: nanosecond timestamp fits in int64

	idxCount, err := s.committedRecordCount()
	if err != nil {
		return err
	}
	idxInfo, err := s.files.idx.Stat()
	if err != nil {
		return err
	}
	if spool.RecordCount(idxInfo.Size()) != idxCount {
		if err := truncateFileTo(s.files.idx, idxEndOffset(idxCount)); err != nil {
			return fmt.Errorf("truncate idx.log: %w", err)
		}
	} else if end := idxEndOffset(idxCount); idxInfo.Size() > end {
		// Drop a partial trailing idx entry from a torn crash write.
		if err := truncateFileTo(s.files.idx, end); err != nil {
			return fmt.Errorf("truncate partial idx tail: %w", err)
		}
	}
	s.recordCount = idxCount

	var expectedRawSize, expectedAttrSize int64
	if s.recordCount > 0 {
		last, err := s.readIdxEntry(s.recordCount - 1)
		if err != nil {
			return err
		}
		expectedRawSize = dataEndOffset(last.RawOffset, last.RawSize)
		expectedAttrSize = dataEndOffset(last.AttrOffset, uint32(last.AttrSize))
		s.lastSeq = last.VaultSeq
		first, err := s.readIdxEntry(0)
		if err != nil {
			return err
		}
		s.firstSeq = first.VaultSeq
	} else {
		expectedRawSize = int64(format.HeaderSize)
		expectedAttrSize = int64(format.HeaderSize)
		id, err := spool.ParseSegmentID(filepath.Base(s.dir))
		if err != nil {
			return err
		}
		s.firstSeq = uint64(id)
	}

	if err := truncateFileTo(s.files.raw, expectedRawSize); err != nil {
		return err
	}
	if err := truncateFileTo(s.files.attr, expectedAttrSize); err != nil {
		return err
	}
	s.rawOffset = uint64(expectedRawSize) - uint64(format.HeaderSize)
	s.attrOffset = uint64(expectedAttrSize) - uint64(format.HeaderSize)
	return nil
}

// committedRecordCount walks idx entries from the tail until raw and attr
// files contain the bytes referenced by each entry. Trailing idx entries
// without durable data (crash between idx and data flush ordering) are excluded.
func (s *Segment) committedRecordCount() (uint64, error) {
	idxInfo, err := s.files.idx.Stat()
	if err != nil {
		return 0, err
	}
	count := spool.RecordCount(idxInfo.Size())
	if count == 0 {
		return 0, nil
	}
	rawInfo, err := s.files.raw.Stat()
	if err != nil {
		return 0, err
	}
	attrInfo, err := s.files.attr.Stat()
	if err != nil {
		return 0, err
	}
	rawSize := rawInfo.Size()
	attrSize := attrInfo.Size()

	for count > 0 {
		entry, err := s.readIdxEntry(count - 1)
		if err != nil {
			return 0, err
		}
		needRaw := dataEndOffset(entry.RawOffset, entry.RawSize)
		needAttr := dataEndOffset(entry.AttrOffset, uint32(entry.AttrSize))
		if rawSize >= needRaw && attrSize >= needAttr {
			break
		}
		count--
	}
	return count, nil
}

func (s *Segment) readIdxEntry(index uint64) (spool.IdxEntry, error) {
	var entryBuf [spool.SpoolIdxEntrySize]byte
	if _, err := s.files.idx.ReadAt(entryBuf[:], spool.IdxFileOffset(index)); err != nil {
		return spool.IdxEntry{}, err
	}
	return spool.DecodeIdxEntry(entryBuf[:]), nil
}

func dataEndOffset(offset uint32, size uint32) int64 {
	return int64(format.HeaderSize) + int64(offset) + int64(size)
}

func idxEndOffset(recordCount uint64) int64 {
	return int64(spool.IdxHeaderSize) + int64(recordCount)*int64(spool.SpoolIdxEntrySize) //nolint:gosec // G115: bounded by segment size limits
}

func truncateFileTo(f *os.File, expectedSize int64) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != expectedSize {
		return f.Truncate(expectedSize)
	}
	return nil
}

// appendDurable appends one record with idx-as-commit-marker crash safety:
// raw and attr are fsynced before idx is written, then idx is fsynced before return.
func (s *Segment) appendDurable(rec chunk.Record) error {
	if s.sealed {
		return ErrSegmentSealed
	}
	attrBytes, err := rec.Attrs.Encode()
	if err != nil {
		return err
	}
	rawSize := uint32(len(rec.Raw))     //nolint:gosec // G115: individual record size bounded by protocol
	attrSize := uint16(len(attrBytes)) //nolint:gosec // G115: attribute size bounded by protocol
	if int64(s.rawOffset)+int64(rawSize) > chunkfile.MaxRawLogSize { //nolint:gosec // G115: bounded
		return chunkfile.ErrRawTooLarge
	}
	if int64(s.attrOffset)+int64(attrSize) > chunkfile.MaxAttrLogSize { //nolint:gosec // G115: bounded
		return chunkfile.ErrAttrTooLarge
	}

	entry := spool.EntryFromRecord(rec, uint32(s.rawOffset), uint32(s.attrOffset), rawSize, attrSize) //nolint:gosec // G115: offsets bounded by segment size limits
	rawPos := int64(format.HeaderSize) + int64(s.rawOffset)                                           //nolint:gosec // G115: bounded
	attrPos := int64(format.HeaderSize) + int64(s.attrOffset)                                         //nolint:gosec // G115: bounded
	idxPos := spool.IdxFileOffset(s.recordCount)

	if _, err := s.files.raw.WriteAt(rec.Raw, rawPos); err != nil {
		return err
	}
	if _, err := s.files.attr.WriteAt(attrBytes, attrPos); err != nil {
		return err
	}
	if err := s.files.raw.Sync(); err != nil {
		return fmt.Errorf("fsync raw.log: %w", err)
	}
	if err := s.files.attr.Sync(); err != nil {
		return fmt.Errorf("fsync attr.log: %w", err)
	}

	var idxBuf [spool.SpoolIdxEntrySize]byte
	spool.EncodeIdxEntry(entry, idxBuf[:])
	if _, err := s.files.idx.WriteAt(idxBuf[:], idxPos); err != nil {
		return err
	}
	if err := s.files.idx.Sync(); err != nil {
		return fmt.Errorf("fsync idx.log: %w", err)
	}

	s.rawOffset += uint64(rawSize)
	s.attrOffset += uint64(attrSize)
	s.recordCount++
	if s.recordCount == 1 {
		s.firstSeq = rec.VaultSeq
	}
	s.lastSeq = rec.VaultSeq
	return nil
}

func (s *Segment) seal() error {
	if s.sealed {
		return nil
	}
	for _, pair := range []struct {
		f   *os.File
		typ byte
		ver byte
	}{
		{s.files.idx, format.TypeIdxLog, chunkfile.IdxLogVersion},
		{s.files.raw, format.TypeRawLog, chunkfile.RawLogVersion},
		{s.files.attr, format.TypeAttrLog, chunkfile.AttrLogVersion},
	} {
		if err := setHeaderSealed(pair.f, pair.typ, pair.ver); err != nil {
			return err
		}
		if err := pair.f.Sync(); err != nil {
			return fmt.Errorf("fsync sealed %s: %w", pair.f.Name(), err)
		}
	}
	s.sealed = true
	return nil
}

func setHeaderSealed(f *os.File, typ, ver byte) error {
	var hdr [format.HeaderSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return err
	}
	if _, err := format.DecodeAndValidate(hdr[:], typ, ver); err != nil {
		return err
	}
	hdr[3] |= format.FlagSealed
	_, err := f.WriteAt(hdr[:], 0)
	return err
}

func (s *Segment) closeFiles() {
	for _, f := range []*os.File{s.files.raw, s.files.attr, s.files.idx} {
		if f != nil {
			_ = f.Close()
		}
	}
}

func decodePlainAttributes(data []byte) (chunk.Attributes, error) {
	if len(data) < 2 {
		return nil, chunk.ErrInvalidAttrsData
	}
	count := int(binary.LittleEndian.Uint16(data[0:2]))
	attrs := make(chunk.Attributes, count)
	offset := 2
	for range count {
		if offset+2 > len(data) {
			return nil, chunk.ErrInvalidAttrsData
		}
		keyLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+keyLen+2 > len(data) {
			return nil, chunk.ErrInvalidAttrsData
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen
		valLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+valLen > len(data) {
			return nil, chunk.ErrInvalidAttrsData
		}
		val := string(data[offset : offset+valLen])
		offset += valLen
		attrs[key] = val
	}
	return attrs, nil
}

func createRawFile(path string, mode os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
	if err != nil {
		return nil, err
	}
	header := format.Header{Type: format.TypeRawLog, Version: chunkfile.RawLogVersion}
	hb := header.Encode()
	if _, err := f.Write(hb[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func createAttrFile(path string, mode os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
	if err != nil {
		return nil, err
	}
	header := format.Header{Type: format.TypeAttrLog, Version: chunkfile.AttrLogVersion}
	hb := header.Encode()
	if _, err := f.Write(hb[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func createIdxFile(path string, createdAt time.Time, mode os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
	if err != nil {
		return nil, err
	}
	var buf [spool.IdxHeaderSize]byte
	header := format.Header{Type: format.TypeIdxLog, Version: chunkfile.IdxLogVersion}
	header.EncodeInto(buf[:])
	binary.LittleEndian.PutUint64(buf[format.HeaderSize:], uint64(createdAt.UnixNano()))
	if _, err := f.Write(buf[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// ReadRecord reads one record by index within the segment (test/diagnostic helper).
func (s *Segment) ReadRecord(index uint64) (chunk.Record, error) {
	if index >= s.recordCount {
		return chunk.Record{}, spool.ErrInvalidSpoolEntry
	}
	var entryBuf [spool.SpoolIdxEntrySize]byte
	if _, err := s.files.idx.ReadAt(entryBuf[:], spool.IdxFileOffset(index)); err != nil {
		return chunk.Record{}, err
	}
	entry := spool.DecodeIdxEntry(entryBuf[:])
	raw := make([]byte, entry.RawSize)
	if _, err := s.files.raw.ReadAt(raw, int64(format.HeaderSize)+int64(entry.RawOffset)); err != nil {
		return chunk.Record{}, err
	}
	attrBytes := make([]byte, entry.AttrSize)
	if _, err := s.files.attr.ReadAt(attrBytes, int64(format.HeaderSize)+int64(entry.AttrOffset)); err != nil {
		return chunk.Record{}, err
	}
	attrs, err := decodePlainAttributes(attrBytes)
	if err != nil {
		return chunk.Record{}, err
	}
	return spool.BuildRecord(entry, raw, attrs), nil
}

// ReadAll loads all records in segment order (test helper).
func (s *Segment) ReadAll() ([]chunk.Record, error) {
	out := make([]chunk.Record, 0, s.recordCount)
	for i := range s.recordCount {
		rec, err := s.ReadRecord(i)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// OpenSegmentForTest opens one segment directory (exported for tests in this package).
func OpenSegmentForTest(dir string, mode os.FileMode) (*Segment, error) {
	return openSegment(dir, mode)
}

// WriteOrphanRaw appends bytes past the indexed tail (crash simulation helper).
func WriteOrphanRaw(s *Segment, data []byte) error {
	info, err := s.files.raw.Stat()
	if err != nil {
		return err
	}
	_, err = s.files.raw.WriteAt(data, info.Size())
	return err
}

// WriteOrphanIdxEntry appends an idx entry without writing the referenced raw/attr
// payload (simulates idx flushed ahead of data).
func WriteOrphanIdxEntry(s *Segment, entry spool.IdxEntry) error {
	idxInfo, err := s.files.idx.Stat()
	if err != nil {
		return err
	}
	count := spool.RecordCount(idxInfo.Size())
	pos := spool.IdxFileOffset(count)
	var buf [spool.SpoolIdxEntrySize]byte
	spool.EncodeIdxEntry(entry, buf[:])
	_, err = s.files.idx.WriteAt(buf[:], pos)
	if err != nil {
		return err
	}
	return s.files.idx.Sync()
}

// WritePartialIdxTail appends incomplete idx bytes (simulates torn idx write).
func WritePartialIdxTail(s *Segment, nbytes int) error {
	if nbytes <= 0 {
		return nil
	}
	info, err := s.files.idx.Stat()
	if err != nil {
		return err
	}
	buf := make([]byte, nbytes)
	_, err = s.files.idx.WriteAt(buf, info.Size())
	return err
}

// Sync flushes segment files.
func (s *Segment) Sync() error {
	for _, f := range []*os.File{s.files.raw, s.files.attr, s.files.idx} {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Dir returns the segment directory path.
func (s *Segment) Dir() string { return s.dir }

func ReopenSegment(dir string, mode os.FileMode) (*Segment, error) {
	return openSegment(dir, mode)
}

// ReadByVaultSeq returns the record with the given acceptance sequence if present.
func (m *Manager) ReadByVaultSeq(seq uint64) (chunk.Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, seg := range m.byID {
		meta := seg.meta()
		if !meta.CoversSeq(seq) {
			continue
		}
		for i := range seg.recordCount {
			rec, err := seg.ReadRecord(i)
			if err != nil {
				continue
			}
			if rec.VaultSeq == seq {
				return rec, true
			}
		}
	}
	return chunk.Record{}, false
}

// LookupEventID scans spool segments for a prior assignment of eventID.
func (m *Manager) LookupEventID(id chunk.EventID) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, seg := range m.byID {
		for i := range seg.recordCount {
			rec, err := seg.ReadRecord(i)
			if err != nil {
				continue
			}
			if rec.EventID == id {
				return rec.VaultSeq, true
			}
		}
	}
	return 0, false
}

// DurableWatermark returns the highest vault_seq durably present in spool (S_r).
func (m *Manager) DurableWatermark() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var maxSeq uint64
	for _, seg := range m.byID {
		if seg.lastSeq > maxSeq {
			maxSeq = seg.lastSeq
		}
	}
	return maxSeq
}
