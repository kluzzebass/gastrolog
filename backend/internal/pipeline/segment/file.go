package segment

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash/v2"

	"gastrolog/internal/format"
	"gastrolog/internal/record"
)

var errSegmentFinalized = errors.New("segment is finalized")

// File is a durable segment on disk. The fixed header is rewritten after
// each append batch; record data follows the header as
// [frameLen:u32][frame body] frames.
type File struct {
	f      *os.File
	hdr    Header
	hdrBuf [HeaderSize]byte
	// recordDigest is a rolling XXH64 over [HeaderSize:recordsEnd). A
	// non-linear digest, NOT a CRC: each frame carries its own trailing
	// CRC32, and rolling a CRC over lenPrefix ++ body ++ bodyCRC cancels the
	// content contribution by CRC linearity, leaving the segment checksum
	// blind to same-length substitution (gastrolog-1vepg0).
	recordDigest *xxhash.Digest
	dataEnd      uint32 // exclusive end of committed record bytes (hot-path append anchor)
	batchBuf     []byte // reused AppendFrames scratch (gastrolog-1ojsm6)
	// memEntries captures (EventID, filePos, sourceTS) per appended frame so
	// Finalize can build both index tails from memory instead of re-reading
	// the whole file (gastrolog-oin19g). Only writer-created segments have a
	// complete capture; Open-path (recovered) segments fall back to the disk
	// scan. Freed after BuildIndex, and HARD-CAPPED at memIndexEntryCap:
	// past the cap the capture is dropped (memCaptureOff) and Finalize uses
	// the disk scan — the capture is an optimization, never a correctness
	// requirement or a memory liability. The bound holds regardless of the
	// caller's complete policy.
	memEntries    []memIndexEntry
	memCaptureOff bool
}

// memIndexEntryCap bounds the in-memory index capture (~80B/entry → ~21 MiB
// worst case). The production complete policy (8 MiB segments) yields ~30K
// entries, ~8x under the cap; a missing or misconfigured complete policy hits
// the cap and degrades to the disk-scan finalize instead of growing RAM with
// the file (gastrolog-oin19g). Var, not const, so tests can exercise the
// overflow path without 262K appends.
var memIndexEntryCap = 1 << 18

// memIndexEntry is the in-memory per-frame index capture (gastrolog-oin19g).
type memIndexEntry struct {
	entry    IndexEntry
	sourceNS uint64 // tsNanos(rec.SourceTS); 0 = unset, excluded from source index
}

// Create initializes a new empty segment file at path.
func Create(path string, meta Meta) (*File, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	sf := &File{
		f: f,
		hdr: Header{
			ID:      meta.ID,
			VaultID: meta.VaultID,
			DataEnd: HeaderSize,
		},
		recordDigest: xxhash.New(),
		dataEnd:      HeaderSize,
	}
	if err := sf.writeHeader(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return sf, nil
}

// ReadHeader decodes just the fixed header of a segment file without opening,
// reconciling, or checksum-verifying it. For cheap metadata reads (record
// counts for stage throughput counters — gastrolog-10n6k8; distribution
// publish staging and stranded rescans — gastrolog-faj2yv); NOT a validity
// check.
func ReadHeader(path string) (Header, error) {
	headerReads.Add(1)
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return Header{}, err
	}
	defer func() { _ = f.Close() }()
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return Header{}, err
	}
	return decodeHeader(buf[:])
}

// Open opens an existing segment and reconciles the header against on-disk frames.
func Open(path string) (*File, error) {
	opens.Add(1)
	f, err := os.OpenFile(filepath.Clean(path), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	sf := &File{f: f}
	if err := sf.readHeader(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := sf.reconcileOnOpen(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := sf.verifyChecksum(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return sf, nil
}

// Header returns the current decoded header.
func (sf *File) Header() Header {
	return sf.hdr
}

// EncodeFrame builds the frame body for a record (excluding the u32 length prefix).
func EncodeFrame(rec *record.Record, writeTS time.Time) ([]byte, error) {
	return encodeFrame(rec, writeTS)
}

// Append encodes and appends a record frame, then rewrites the header.
// DataEnd is the byte offset where the record starts (recovery anchor).
func (sf *File) Append(rec *record.Record, writeTS time.Time) error {
	if writeTS.IsZero() {
		writeTS = time.Now().UTC()
	}
	body, err := encodeFrame(rec, writeTS)
	if err != nil {
		return err
	}
	return sf.AppendFrame(rec, writeTS, body)
}

// TakeScratch surrenders the file's batch buffer for reuse by a successor
// file. Segment rotation under load created a fresh multi-megabyte batch
// buffer per segment (top allocation site under pour load); the writer
// hands the buffer across rotations instead (gastrolog-11y2iv). Call only
// after appends to this file have stopped.
func (sf *File) TakeScratch() []byte {
	b := sf.batchBuf
	sf.batchBuf = nil
	return b
}

// GiveScratch seeds the batch buffer, typically with a predecessor's via
// TakeScratch. No-op if the file already grew its own.
func (sf *File) GiveScratch(b []byte) {
	if sf.batchBuf == nil {
		sf.batchBuf = b[:0]
	}
}

// AppendFrame appends a pre-encoded frame body and rewrites the header.
func (sf *File) AppendFrame(rec *record.Record, _ time.Time, body []byte) error {
	return sf.AppendFrames([]Frame{{Rec: rec, Body: body}})
}

// Frame is one pre-encoded record frame for batched append.
type Frame struct {
	Rec  *record.Record
	Body []byte
}

// AppendFrames appends a batch of pre-encoded frames with ONE data write and
// ONE header rewrite. The previous per-record shape (length-prefix pwrite +
// body pwrite + header-rewrite pwrite) put 3 serialized syscalls on the hot
// path and capped a vault writer at ~25K rec/s while the node sat mostly idle
// (gastrolog-1ojsm6). Crash safety is unchanged: recovery reconciles frames
// beyond (or torn around) the last header rewrite from the fsynced prefix
// (reconcileOnOpen/scanForward), and acks only release after the group-commit
// fsync.
func (sf *File) AppendFrames(frames []Frame) error {
	if len(frames) == 0 {
		return nil
	}
	if sf.hdr.IndexOffset > 0 {
		return errSegmentFinalized
	}
	for i := range frames {
		if frames[i].Rec == nil {
			return errors.New("nil record")
		}
	}

	writeOff, err := sf.appendOffset()
	if err != nil {
		return err
	}

	if sf.recordDigest == nil {
		sf.recordDigest = xxhash.New()
	}

	// Build the batch buffer — [lenPrefix|body]... — feeding the running
	// digest in frame order, exactly as sequential single appends would have.
	// Size it exactly up front: append-doubling growth across batches was
	// ~10GB of garbage per soak run (gastrolog-11y2iv).
	need := 0
	for i := range frames {
		need += frameLenPrefixSize + len(frames[i].Body)
	}
	if cap(sf.batchBuf) < need {
		// Grow with headroom: exact-fit reallocated on every slightly
		// larger batch (measured 6GB/run of churn); 25% amortizes growth
		// while staying bounded by the largest batch this file sees.
		sf.batchBuf = make([]byte, 0, need+need/4)
	}
	sf.batchBuf = sf.batchBuf[:0]
	var lastFrameStart uint32
	var lenPrefix [frameLenPrefixSize]byte
	for i := range frames {
		lastFrameStart = writeOff + uint32(len(sf.batchBuf))                     //nolint:gosec // G115: batch bounded by commit window
		binary.LittleEndian.PutUint32(lenPrefix[:], uint32(len(frames[i].Body))) //nolint:gosec // G115: frame bounded by encode
		sf.batchBuf = append(sf.batchBuf, lenPrefix[:]...)
		sf.batchBuf = append(sf.batchBuf, frames[i].Body...)
		if _, err := sf.recordDigest.Write(lenPrefix[:]); err != nil {
			return err
		}
		if _, err := sf.recordDigest.Write(frames[i].Body); err != nil {
			return err
		}
		if !sf.memCaptureOff {
			sf.memEntries = append(sf.memEntries, memIndexEntry{
				entry:    IndexEntry{EventID: frames[i].Rec.EventID, FilePos: lastFrameStart},
				sourceNS: tsNanos(frames[i].Rec.SourceTS),
			})
			if len(sf.memEntries) > memIndexEntryCap {
				sf.memEntries = nil
				sf.memCaptureOff = true
			}
		}
	}

	if _, err := sf.f.WriteAt(sf.batchBuf, int64(writeOff)); err != nil {
		return err
	}

	first := sf.hdr.RecordCount == 0
	sf.hdr.RecordCount += uint32(len(frames)) //nolint:gosec // G115: batch bounded by commit window
	if first {
		sf.hdr.FirstIngestTS = frames[0].Rec.EventID.IngestTS
	}
	sf.hdr.LastIngestTS = frames[len(frames)-1].Rec.EventID.IngestTS
	sf.hdr.DataEnd = lastFrameStart
	sf.hdr.SegmentChecksum = sf.recordDigest.Sum64()
	sf.dataEnd = writeOff + uint32(len(sf.batchBuf)) //nolint:gosec // G115: batch bounded by commit window
	return sf.writeHeader()
}

// DataSize returns the current end-of-data offset (header plus every appended
// frame) from the in-memory append anchor — no Stat syscall. Valid for the
// writer's rotation checks until Finalize writes the index tail
// (gastrolog-1ojsm6).
func (sf *File) DataSize() int64 {
	return int64(sf.dataEnd)
}

// Sync persists appended frames and header rewrites to stable storage.
func (sf *File) Sync() error {
	return sf.f.Sync()
}

// Size returns the current on-disk file length.
func (sf *File) Size() (int64, error) {
	info, err := sf.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// MarkComplete sets the completed flag in the header (working→completed rename
// is the caller's responsibility).
func (sf *File) MarkComplete() error {
	sf.hdr.Flags |= FlagComplete
	return sf.writeHeader()
}

// Close closes the underlying file.
func (sf *File) Close() error {
	return sf.f.Close()
}

// ReadAll decodes every valid record frame in the segment record region.
func (sf *File) ReadAll() ([]record.Record, error) {
	recEnd, err := sf.recordsEnd()
	if err != nil {
		return nil, err
	}
	var out []record.Record
	if sf.hdr.RecordCount > 0 {
		out = make([]record.Record, 0, sf.hdr.RecordCount)
	}
	off := uint32(HeaderSize)
	var scratch []byte
	for off < recEnd {
		rec, n, buf, err := readFrameAtBuf(sf.f, int64(off), recEnd-off, scratch)
		if err != nil {
			return nil, err
		}
		scratch = buf
		out = append(out, rec)
		off += n
	}
	return out, nil
}

func (sf *File) readHeader() error {
	info, err := sf.f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < int64(HeaderSize) {
		return ErrHeaderTooSmall
	}
	n, err := sf.f.ReadAt(sf.hdrBuf[:HeaderSize], 0)
	if err != nil {
		return err
	}
	if n < HeaderSize {
		return ErrHeaderTooSmall
	}
	hdr, err := decodeHeader(sf.hdrBuf[:n])
	if err != nil {
		return err
	}
	sf.hdr = hdr
	return nil
}

func (sf *File) writeHeader() error {
	sf.hdr.Type = format.TypeSegment
	sf.hdr.Version = formatVersion
	encodeHeader(sf.hdr, sf.hdrBuf[:])
	_, err := sf.f.WriteAt(sf.hdrBuf[:], 0)
	return err
}

// appendOffset is the byte offset where the next frame write starts.
func (sf *File) appendOffset() (uint32, error) {
	return sf.dataEnd, nil
}

// validDataEnd is the exclusive end of committed record bytes on disk.
func (sf *File) validDataEnd() (uint32, error) {
	if sf.hdr.RecordCount == 0 {
		return HeaderSize, nil
	}
	if sf.dataEnd > sf.hdr.DataEnd {
		return sf.dataEnd, nil
	}
	_, n, err := sf.frameAt(sf.hdr.DataEnd)
	if err != nil {
		return 0, err
	}
	return sf.hdr.DataEnd + n, nil
}

func (sf *File) frameAt(off uint32) (record.Record, uint32, error) {
	info, err := sf.f.Stat()
	if err != nil {
		return record.Record{}, 0, err
	}
	if int64(off) >= info.Size() {
		return record.Record{}, 0, ErrFrameLength
	}
	remaining := uint32(info.Size() - int64(off)) //nolint:gosec // G115: segment file size bounded
	return readFrameAt(sf.f, int64(off), remaining)
}

// reconcileOnOpen rebuilds header fields and truncates any torn tail. The
// common case uses DataEnd as an O(1) anchor: read the frame there, valid data
// ends at the end of that frame, truncate anything after. A short forward scan
// picks up header lag (frame written before header rewrite). If the anchor
// frame is bad, fall back to scanning from the front.
func (sf *File) reconcileOnOpen() error {
	if sf.hdr.IndexOffset > 0 {
		if err := sf.verifyIndexedLayout(); err != nil {
			return err
		}
		sf.dataEnd = sf.hdr.IndexOffset
		return nil
	}

	info, err := sf.f.Stat()
	if err != nil {
		return err
	}
	fileSize := uint32(info.Size()) //nolint:gosec // G115: segment file size bounded

	res, err := sf.reconcileFromAnchor(fileSize)
	if err != nil {
		res = sf.resyncFromFront(fileSize)
	}

	if int64(res.validEnd) < info.Size() {
		if err := sf.f.Truncate(int64(res.validEnd)); err != nil {
			return err
		}
	}

	sf.hdr.RecordCount = res.count
	sf.hdr.FirstIngestTS = res.firstTS
	sf.hdr.LastIngestTS = res.lastTS
	if res.count == 0 {
		sf.hdr.DataEnd = HeaderSize
	} else {
		sf.hdr.DataEnd = res.lastStart
	}

	sum, err := sf.initRecordDigest(res.validEnd)
	if err != nil {
		return err
	}
	sf.hdr.SegmentChecksum = sum
	sf.dataEnd = res.validEnd
	return sf.writeHeader()
}

// scanResult is the header state a torn-tail recovery scan rebuilds: where
// valid data ends, where the last whole frame starts, and the record count
// and IngestTS extrema confirmed so far.
type scanResult struct {
	validEnd  uint32    // byte offset just past the last whole frame
	lastStart uint32    // byte offset where the last whole frame starts
	count     uint32    // records confirmed so far
	firstTS   time.Time // IngestTS of the first record
	lastTS    time.Time // IngestTS of the last record
}

func (sf *File) reconcileFromAnchor(fileSize uint32) (scanResult, error) {
	if sf.hdr.RecordCount == 0 {
		return sf.scanForward(HeaderSize, fileSize, scanResult{}), nil
	}

	rec, n, err := sf.frameAt(sf.hdr.DataEnd)
	if err != nil {
		return scanResult{}, err
	}

	return sf.scanForward(sf.hdr.DataEnd+n, fileSize, scanResult{
		count:   sf.hdr.RecordCount,
		firstTS: sf.hdr.FirstIngestTS,
		lastTS:  rec.EventID.IngestTS,
	}), nil
}

// scanForward extends prior with whole frames read from off until the first
// torn or unreadable frame, and returns the combined result.
func (sf *File) scanForward(off, fileSize uint32, prior scanResult) scanResult {
	res := prior
	res.validEnd = off
	if prior.count == 0 {
		res.lastStart = HeaderSize
	} else {
		res.lastStart = sf.hdr.DataEnd
	}

	var scanScratch []byte
	for off < fileSize {
		rec, n, buf, readErr := readFrameAtBuf(sf.f, int64(off), fileSize-off, scanScratch)
		scanScratch = buf
		if readErr != nil {
			break
		}
		res.count++
		if res.count == 1 {
			res.firstTS = rec.EventID.IngestTS
		}
		res.lastStart = off
		res.lastTS = rec.EventID.IngestTS
		res.validEnd = off + n
		off += n
	}
	return res
}

func (sf *File) resyncFromFront(fileSize uint32) scanResult {
	return sf.scanForward(HeaderSize, fileSize, scanResult{})
}

// initRecordDigest seeds the rolling record digest from on-disk bytes
// [HeaderSize:recEnd) and returns the checksum to publish: 0 for an empty
// record region (the "no data / no expectation" sentinel across the publish
// and collection paths), the XXH64 sum otherwise.
func (sf *File) initRecordDigest(recEnd uint32) (uint64, error) {
	h := xxhash.New()
	sf.recordDigest = h
	if recEnd <= HeaderSize {
		return 0, nil
	}
	if err := hashFeed(h, sf.f, HeaderSize, recEnd); err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

func (sf *File) verifyChecksum() error {
	if sf.hdr.IndexOffset > 0 {
		return nil // verifyIndexedLayout in reconcileOnOpen already checked both regions.
	}
	if sf.recordDigest == nil {
		return errors.New("segment record digest not initialized")
	}
	sum := uint64(0)
	if sf.dataEnd > HeaderSize {
		sum = sf.recordDigest.Sum64()
	}
	if sum != sf.hdr.SegmentChecksum {
		return errors.New("segment checksum mismatch")
	}
	return nil
}

// readFrameAt decodes one frame. Allocates a fresh body buffer per call;
// loop callers use readFrameAtBuf to reuse a scratch buffer across
// records — the per-record body alloc was 18GB flat / 51GB cumulative of
// garbage per soak run in merge reads (gastrolog-11y2iv). Safe because
// decodeFrameBody copies everything out of the body (Raw via make+copy,
// attrs via string conversion); nothing aliases the buffer after return.
func readFrameAt(r io.ReaderAt, offset int64, limit uint32) (record.Record, uint32, error) {
	rec, n, _, err := readFrameAtBuf(r, offset, limit, nil)
	return rec, n, err
}

// readFrameAtBuf is readFrameAt with a caller-owned scratch buffer. The
// returned buffer replaces the caller's (it may have grown); the caller
// must not retain views into it across calls.
func readFrameAtBuf(r io.ReaderAt, offset int64, limit uint32, scratch []byte) (record.Record, uint32, []byte, error) {
	body, scratch, err := readFrameBodyAtBuf(r, offset, limit, scratch)
	if err != nil {
		return record.Record{}, 0, scratch, err
	}
	rec, err := decodeFrameBody(body)
	if err != nil {
		return record.Record{}, 0, scratch, err
	}
	return rec, frameLenPrefixSize + uint32(len(body)), scratch, nil //nolint:gosec // G115: bodyLen fits uint32 by construction
}

// readFrameViewAtBuf is readFrameAtBuf's zero-copy counterpart: the returned
// view aliases scratch (no attrs map, no Raw copy) and is valid only until
// the buffer's next reuse. CRC verification is identical.
func readFrameViewAtBuf(r io.ReaderAt, offset int64, limit uint32, scratch []byte) (record.View, []byte, error) {
	body, scratch, err := readFrameBodyAtBuf(r, offset, limit, scratch)
	if err != nil {
		return record.View{}, scratch, err
	}
	v, err := decodeFrameView(body)
	if err != nil {
		return record.View{}, scratch, err
	}
	return v, scratch, nil
}

// readFrameBodyAtBuf reads one frame's body bytes into scratch, growing it
// as needed. Shared by the Record and View decode paths. The length prefix
// reads through scratch too — a stack byte array escapes via the io.ReaderAt
// interface call, which cost one heap alloc per record in scan loops.
func readFrameBodyAtBuf(r io.ReaderAt, offset int64, limit uint32, scratch []byte) ([]byte, []byte, error) {
	if cap(scratch) < frameLenPrefixSize {
		scratch = make([]byte, frameLenPrefixSize)
	}
	lenBuf := scratch[:frameLenPrefixSize]
	if _, err := r.ReadAt(lenBuf, offset); err != nil {
		return nil, scratch, err
	}
	bodyLen := binary.LittleEndian.Uint32(lenBuf)
	if bodyLen == 0 || bodyLen > limit-frameLenPrefixSize {
		return nil, scratch, ErrFrameLength
	}
	if uint32(cap(scratch)) < bodyLen { //nolint:gosec // G115: cap bounded by segment size
		scratch = make([]byte, bodyLen)
	}
	body := scratch[:bodyLen]
	if _, err := r.ReadAt(body, offset+frameLenPrefixSize); err != nil {
		return nil, scratch, err
	}
	return body, scratch, nil
}
