package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	ErrVaultSeqRequired = errors.New("spool: record missing vault_seq")
	ErrWindowNotFound   = errors.New("spool: no window covers vault_seq")
	ErrSeqOutOfWindow   = errors.New("spool: vault_seq outside window bounds")
	ErrWindowSealed     = errors.New("spool: window is sealed")
	ErrWindowEmpty      = errors.New("spool: window empty")
)

// Config configures a file-backed spool manager root directory.
type Config struct {
	Dir      string
	FileMode os.FileMode
}

// Manager stores spool windows on disk under Config.Dir.
type Manager struct {
	cfg               Config
	mu                sync.Mutex
	lock              *os.File
	windows           map[spool.WindowID]*window
	reclaimThroughSeq uint64
}

type windowFiles struct {
	raw  *os.File
	attr *os.File
	idx  *os.File
}

// window is one allocator-aligned sequence window directory.
type window struct {
	dir          string
	id           spool.WindowID
	files        windowFiles
	present      map[uint64]struct{}
	recordCount  uint64
	rawOffset    uint64
	attrOffset   uint64
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
	m := &Manager{cfg: cfg, lock: lock, windows: make(map[spool.WindowID]*window)}
	if err := m.loadExisting(); err != nil {
		_ = lock.Close()
		return nil, err
	}
	if ckpt, err := m.loadReplicaCheckpointLocked(); err == nil {
		if ckpt.ReclaimThroughSeq > m.reclaimThroughSeq {
			m.reclaimThroughSeq = ckpt.ReclaimThroughSeq
		}
	}
	return m, nil
}

// Close releases the spool directory lock.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, win := range m.windows {
		win.closeFiles()
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
		id, err := spool.ParseWindowDirName(ent.Name())
		if err != nil {
			continue
		}
		win, err := openWindow(filepath.Join(m.cfg.Dir, ent.Name()), id, m.cfg.FileMode)
		if err != nil {
			return fmt.Errorf("spool: open window %s: %w", ent.Name(), err)
		}
		if win.id != id {
			win.closeFiles()
			return fmt.Errorf("spool: window dir %s id mismatch", ent.Name())
		}
		m.windows[id] = win
	}
	return nil
}

// EnsureWindow creates a window [start..end] when absent.
func (m *Manager) EnsureWindow(start, end uint64) error {
	if start == 0 || end == 0 || start > end {
		return fmt.Errorf("spool: invalid window bounds %d..%d", start, end)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := spool.WindowID{Start: start, End: end}
	if _, ok := m.windows[id]; ok {
		return nil
	}
	win, err := createWindow(filepath.Join(m.cfg.Dir, id.DirName()), id, m.cfg.FileMode)
	if err != nil {
		return err
	}
	m.windows[id] = win
	return nil
}

// PutSlot durably writes one window slot keyed by rec.VaultSeq.
func (m *Manager) PutSlot(rec chunk.Record) error {
	if rec.VaultSeq == 0 {
		return ErrVaultSeqRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	win := m.windowForSeqLocked(rec.VaultSeq)
	if win == nil {
		return fmt.Errorf("%w: seq %d", ErrWindowNotFound, rec.VaultSeq)
	}
	if rec.VaultSeq < win.id.Start || rec.VaultSeq > win.id.End {
		return ErrSeqOutOfWindow
	}
	return win.putSlotDurable(rec)
}

func (m *Manager) windowForSeqLocked(seq uint64) *window {
	for _, win := range m.windows {
		if seq >= win.id.Start && seq <= win.id.End {
			return win
		}
	}
	return nil
}

// SealWindow seals one writable window.
func (m *Manager) SealWindow(id spool.WindowID) (spool.SegmentMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	win, ok := m.windows[id]
	if !ok {
		return spool.SegmentMeta{}, spool.ErrSegmentNotFound
	}
	if win.recordCount == 0 {
		return spool.SegmentMeta{}, ErrWindowEmpty
	}
	if err := win.seal(); err != nil {
		return spool.SegmentMeta{}, err
	}
	return win.meta(), nil
}

// ListWindows returns window metadata sorted by first_seq.
func (m *Manager) ListWindows() []spool.SegmentMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]spool.SegmentMeta, 0, len(m.windows))
	for _, win := range m.windows {
		out = append(out, win.meta())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FirstSeq < out[j].FirstSeq
	})
	return out
}

// ListSegments aliases ListWindows for compatibility with existing tests.
func (m *Manager) ListSegments() []spool.SegmentMeta {
	return m.ListWindows()
}

// Meta returns metadata for one window.
func (m *Manager) Meta(id spool.WindowID) (spool.SegmentMeta, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	win, ok := m.windows[id]
	if !ok {
		return spool.SegmentMeta{}, false
	}
	return win.meta(), true
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

// ListReclaimable returns sealed windows eligible for reclaim at the current watermark.
func (m *Manager) ListReclaimable() []spool.SegmentMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []spool.SegmentMeta
	for _, win := range m.windows {
		meta := win.meta()
		if spool.Reclaimable(meta, m.reclaimThroughSeq, spool.WindowID{}) == nil {
			out = append(out, meta)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FirstSeq < out[j].FirstSeq
	})
	return out
}

// Reclaim deletes a sealed window directory when it is at or below the safety watermark.
func (m *Manager) Reclaim(id spool.WindowID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	win, ok := m.windows[id]
	if !ok {
		return fmt.Errorf("%w: %s", spool.ErrSegmentNotFound, id.DirName())
	}
	meta := win.meta()
	if err := spool.Reclaimable(meta, m.reclaimThroughSeq, spool.WindowID{}); err != nil {
		return err
	}
	dir := win.dir
	win.closeFiles()
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("spool: remove window %s: %w", id.DirName(), err)
	}
	delete(m.windows, id)
	return nil
}

func (w *window) meta() spool.SegmentMeta {
	return spool.SegmentMeta{
		ID:          spool.SegmentID(w.id.Start),
		Window:      w.id,
		FirstSeq:    w.id.Start,
		EndSeq:      w.id.End,
		LastSeq:     w.lastSeq,
		RecordCount: w.recordCount,
		Sealed:      w.sealed,
	}
}

func createWindow(dir string, id spool.WindowID, mode os.FileMode) (*window, error) {
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
	return &window{
		dir:       dir,
		id:        id,
		files:     windowFiles{raw: raw, attr: attr, idx: idx},
		present:   make(map[uint64]struct{}),
		createdAt: now,
		fileMode:  mode,
	}, nil
}

func openWindow(dir string, id spool.WindowID, mode os.FileMode) (*window, error) {
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
	win := &window{
		dir:      dir,
		id:       id,
		files:    windowFiles{raw: raw, attr: attr, idx: idx},
		present:  make(map[uint64]struct{}),
		fileMode: mode,
	}
	if err := win.recover(); err != nil {
		win.closeFiles()
		return nil, err
	}
	return win, nil
}

func (w *window) recover() error {
	var headerBuf [spool.IdxHeaderSize]byte
	if _, err := w.files.idx.ReadAt(headerBuf[:], 0); err != nil {
		return fmt.Errorf("read idx header: %w", err)
	}
	h, err := format.DecodeAndValidate(headerBuf[:format.HeaderSize], format.TypeIdxLog, chunkfile.IdxLogVersion)
	if err != nil {
		return err
	}
	w.sealed = h.Flags&format.FlagSealed != 0
	createdAtNanos := binary.LittleEndian.Uint64(headerBuf[format.HeaderSize:])
	w.createdAt = time.Unix(0, int64(createdAtNanos)).UTC() //nolint:gosec // G115: nanosecond timestamp fits in int64

	if err := w.truncatePartialIdxTail(); err != nil {
		return err
	}
	rawInfo, err := w.files.raw.Stat()
	if err != nil {
		return err
	}
	attrInfo, err := w.files.attr.Stat()
	if err != nil {
		return err
	}
	maxRawEnd, maxAttrEnd, err := w.scanCommittedSlots(rawInfo.Size(), attrInfo.Size())
	if err != nil {
		return err
	}
	if err := truncateFileTo(w.files.raw, maxRawEnd); err != nil {
		return err
	}
	if err := truncateFileTo(w.files.attr, maxAttrEnd); err != nil {
		return err
	}
	w.rawOffset = uint64(maxRawEnd - int64(format.HeaderSize))   //nolint:gosec // G115: offsets bounded by file size
	w.attrOffset = uint64(maxAttrEnd - int64(format.HeaderSize)) //nolint:gosec // G115: offsets bounded by file size
	return nil
}

func (w *window) truncatePartialIdxTail() error {
	idxInfo, err := w.files.idx.Stat()
	if err != nil {
		return err
	}
	if end := spool.WindowIdxFileSize(w.id.Start, w.id.End); idxInfo.Size() > end {
		if err := truncateFileTo(w.files.idx, end); err != nil {
			return fmt.Errorf("truncate partial idx tail: %w", err)
		}
	}
	return nil
}

func (w *window) scanCommittedSlots(rawSize, attrSize int64) (maxRawEnd, maxAttrEnd int64, err error) {
	maxRawEnd = int64(format.HeaderSize)
	maxAttrEnd = int64(format.HeaderSize)
	w.recordCount = 0
	w.lastSeq = 0
	w.present = make(map[uint64]struct{})

	for seq := w.id.Start; ; seq++ {
		entry, ok, slotErr := w.readSlotEntry(seq)
		if slotErr != nil {
			return 0, 0, slotErr
		}
		if ok {
			maxRawEnd, maxAttrEnd, err = w.commitRecoveredSlot(seq, entry, rawSize, attrSize, maxRawEnd, maxAttrEnd)
			if err != nil {
				return 0, 0, err
			}
		}
		if seq == w.id.End {
			break
		}
	}
	return maxRawEnd, maxAttrEnd, nil
}

func (w *window) commitRecoveredSlot(seq uint64, entry spool.IdxEntry, rawSize, attrSize, maxRawEnd, maxAttrEnd int64) (int64, int64, error) {
	needRaw := dataEndOffset(entry.RawOffset, entry.RawSize)
	needAttr := dataEndOffset(entry.AttrOffset, uint32(entry.AttrSize))
	if entry.VaultSeq != seq || rawSize < needRaw || attrSize < needAttr {
		if err := w.clearSlot(seq); err != nil {
			return 0, 0, err
		}
		return maxRawEnd, maxAttrEnd, nil
	}
	w.recordCount++
	w.present[seq] = struct{}{}
	if seq > w.lastSeq {
		w.lastSeq = seq
	}
	if needRaw > maxRawEnd {
		maxRawEnd = needRaw
	}
	if needAttr > maxAttrEnd {
		maxAttrEnd = needAttr
	}
	return maxRawEnd, maxAttrEnd, nil
}

func (w *window) readSlotEntry(seq uint64) (spool.IdxEntry, bool, error) {
	var entryBuf [spool.SpoolIdxEntrySize]byte
	offset := spool.SlotIdxFileOffset(w.id.Start, seq)
	_, err := w.files.idx.ReadAt(entryBuf[:], offset)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return spool.IdxEntry{}, false, nil
		}
		return spool.IdxEntry{}, false, err
	}
	if isZeroSlot(entryBuf[:]) {
		return spool.IdxEntry{}, false, nil
	}
	entry := spool.DecodeIdxEntry(entryBuf[:])
	if entry.VaultSeq == 0 {
		return spool.IdxEntry{}, false, nil
	}
	return entry, true, nil
}

func isZeroSlot(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func (w *window) clearSlot(seq uint64) error {
	var zero [spool.SpoolIdxEntrySize]byte
	if _, err := w.files.idx.WriteAt(zero[:], spool.SlotIdxFileOffset(w.id.Start, seq)); err != nil {
		return err
	}
	return w.files.idx.Sync()
}

func dataEndOffset(offset uint32, size uint32) int64 {
	return int64(format.HeaderSize) + int64(offset) + int64(size)
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

// putSlotDurable writes one record with idx-as-commit-marker crash safety:
// raw and attr are fsynced before idx is written, then idx is fsynced before return.
func (w *window) putSlotDurable(rec chunk.Record) error {
	if w.sealed {
		return ErrWindowSealed
	}
	if rec.VaultSeq < w.id.Start || rec.VaultSeq > w.id.End {
		return ErrSeqOutOfWindow
	}
	attrBytes, err := rec.Attrs.Encode()
	if err != nil {
		return err
	}
	rawSize := uint32(len(rec.Raw))     //nolint:gosec // G115: individual record size bounded by protocol
	attrSize := uint16(len(attrBytes)) //nolint:gosec // G115: attribute size bounded by protocol
	if int64(w.rawOffset)+int64(rawSize) > chunkfile.MaxRawLogSize { //nolint:gosec // G115: bounded
		return chunkfile.ErrRawTooLarge
	}
	if int64(w.attrOffset)+int64(attrSize) > chunkfile.MaxAttrLogSize { //nolint:gosec // G115: bounded
		return chunkfile.ErrAttrTooLarge
	}

	entry := spool.EntryFromRecord(rec, uint32(w.rawOffset), uint32(w.attrOffset), rawSize, attrSize) //nolint:gosec // G115: offsets bounded by window size limits
	rawPos := int64(format.HeaderSize) + int64(w.rawOffset)                                             //nolint:gosec // G115: bounded
	attrPos := int64(format.HeaderSize) + int64(w.attrOffset)                                           //nolint:gosec // G115: bounded
	idxPos := spool.SlotIdxFileOffset(w.id.Start, rec.VaultSeq)

	if _, err := w.files.raw.WriteAt(rec.Raw, rawPos); err != nil {
		return err
	}
	if _, err := w.files.attr.WriteAt(attrBytes, attrPos); err != nil {
		return err
	}
	if err := w.files.raw.Sync(); err != nil {
		return fmt.Errorf("fsync raw.log: %w", err)
	}
	if err := w.files.attr.Sync(); err != nil {
		return fmt.Errorf("fsync attr.log: %w", err)
	}

	var idxBuf [spool.SpoolIdxEntrySize]byte
	spool.EncodeIdxEntry(entry, idxBuf[:])
	if _, err := w.files.idx.WriteAt(idxBuf[:], idxPos); err != nil {
		return err
	}
	if err := w.files.idx.Sync(); err != nil {
		return fmt.Errorf("fsync idx.log: %w", err)
	}

	w.rawOffset += uint64(rawSize)
	w.attrOffset += uint64(attrSize)
	if _, had := w.present[rec.VaultSeq]; !had {
		w.present[rec.VaultSeq] = struct{}{}
		w.recordCount++
	}
	if rec.VaultSeq > w.lastSeq {
		w.lastSeq = rec.VaultSeq
	}
	return nil
}

func (w *window) seal() error {
	if w.sealed {
		return nil
	}
	for _, pair := range []struct {
		f   *os.File
		typ byte
		ver byte
	}{
		{w.files.idx, format.TypeIdxLog, chunkfile.IdxLogVersion},
		{w.files.raw, format.TypeRawLog, chunkfile.RawLogVersion},
		{w.files.attr, format.TypeAttrLog, chunkfile.AttrLogVersion},
	} {
		if err := setHeaderSealed(pair.f, pair.typ, pair.ver); err != nil {
			return err
		}
		if err := pair.f.Sync(); err != nil {
			return fmt.Errorf("fsync sealed %s: %w", pair.f.Name(), err)
		}
	}
	w.sealed = true
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

func (w *window) closeFiles() {
	for _, f := range []*os.File{w.files.raw, w.files.attr, w.files.idx} {
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

// ReadRecord reads one record by slot index within the window (test helper).
func (w *window) ReadRecord(index uint64) (chunk.Record, error) {
	if index >= spool.WindowSlotCount(w.id.Start, w.id.End) {
		return chunk.Record{}, spool.ErrInvalidSpoolEntry
	}
	return w.readRecordBySeq(w.id.Start + index)
}

func (w *window) readRecordBySeq(seq uint64) (chunk.Record, error) {
	entry, ok, err := w.readSlotEntry(seq)
	if err != nil {
		return chunk.Record{}, err
	}
	if !ok {
		return chunk.Record{}, spool.ErrInvalidSpoolEntry
	}
	raw := make([]byte, entry.RawSize)
	if _, err := w.files.raw.ReadAt(raw, int64(format.HeaderSize)+int64(entry.RawOffset)); err != nil {
		return chunk.Record{}, err
	}
	attrBytes := make([]byte, entry.AttrSize)
	if _, err := w.files.attr.ReadAt(attrBytes, int64(format.HeaderSize)+int64(entry.AttrOffset)); err != nil {
		return chunk.Record{}, err
	}
	attrs, err := decodePlainAttributes(attrBytes)
	if err != nil {
		return chunk.Record{}, err
	}
	return spool.BuildRecord(entry, raw, attrs), nil
}

// ReadAllSlots loads all occupied slots in slot order (test helper).
func (w *window) ReadAllSlots() ([]chunk.Record, error) {
	count := spool.WindowSlotCount(w.id.Start, w.id.End)
	out := make([]chunk.Record, 0, w.recordCount)
	for i := range count {
		rec, err := w.ReadRecord(i)
		if errors.Is(err, spool.ErrInvalidSpoolEntry) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// OpenWindowForTest opens one window directory (exported for tests).
func OpenWindowForTest(dir string, mode os.FileMode) (*window, error) {
	id, err := spool.ParseWindowDirName(filepath.Base(dir))
	if err != nil {
		return nil, err
	}
	return openWindow(dir, id, mode)
}

// WriteOrphanRaw appends bytes past the indexed tail (crash simulation helper).
func WriteOrphanRaw(w *window, data []byte) error {
	info, err := w.files.raw.Stat()
	if err != nil {
		return err
	}
	_, err = w.files.raw.WriteAt(data, info.Size())
	return err
}

// WriteOrphanIdxEntry appends an idx entry without writing the referenced raw/attr
// payload (simulates idx flushed ahead of data).
func WriteOrphanIdxEntry(w *window, entry spool.IdxEntry) error {
	pos := spool.SlotIdxFileOffset(w.id.Start, entry.VaultSeq)
	var buf [spool.SpoolIdxEntrySize]byte
	spool.EncodeIdxEntry(entry, buf[:])
	_, err := w.files.idx.WriteAt(buf[:], pos)
	if err != nil {
		return err
	}
	return w.files.idx.Sync()
}

// WritePartialIdxTail appends incomplete idx bytes (simulates torn idx write).
func WritePartialIdxTail(w *window, nbytes int) error {
	if nbytes <= 0 {
		return nil
	}
	info, err := w.files.idx.Stat()
	if err != nil {
		return err
	}
	buf := make([]byte, nbytes)
	_, err = w.files.idx.WriteAt(buf, info.Size())
	return err
}

// Sync flushes window files.
func (w *window) Sync() error {
	for _, f := range []*os.File{w.files.raw, w.files.attr, w.files.idx} {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Dir returns the window directory path.
func (w *window) Dir() string { return w.dir }

// ReopenWindow reopens an existing window (test helper).
func ReopenWindow(dir string, mode os.FileMode) (*window, error) {
	id, err := spool.ParseWindowDirName(filepath.Base(dir))
	if err != nil {
		return nil, err
	}
	return openWindow(dir, id, mode)
}

// ReadByVaultSeq returns the record with the given acceptance sequence if present.
func (m *Manager) ReadByVaultSeq(seq uint64) (chunk.Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	win := m.windowForSeqLocked(seq)
	if win == nil {
		return chunk.Record{}, false
	}
	rec, err := win.readRecordBySeq(seq)
	if err != nil {
		return chunk.Record{}, false
	}
	return rec, true
}

// LookupEventID scans spool windows for a prior assignment of eventID.
func (m *Manager) LookupEventID(id chunk.EventID) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var latest uint64
	found := false
	for _, win := range m.windows {
		recs, err := win.ReadAllSlots()
		if err != nil {
			continue
		}
		for _, rec := range recs {
			if rec.EventID == id {
				if !found || rec.VaultSeq > latest {
					latest = rec.VaultSeq
					found = true
				}
			}
		}
	}
	return latest, found
}

// DurableWatermark returns the highest vault_seq durably present in spool (S_r).
func (m *Manager) DurableWatermark() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var maxSeq uint64
	for _, win := range m.windows {
		if win.lastSeq > maxSeq {
			maxSeq = win.lastSeq
		}
	}
	return maxSeq
}
