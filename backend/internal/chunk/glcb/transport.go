package glcb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

// Cloud transport format. The local data.glcb is uncompressed for mmap; on
// upload it is re-framed so that a byte range of the OBJECT means something:
//
//	object := bodyFrame sectionFrame* tailFrame directory footer
//
//	bodyFrame    zstd(blob[0 : firstTailSection))      preamble+layout+records+dict+ridx
//	sectionFrame zstd(one tail TOC section)            ITSI, STSI, future index sections
//	tailFrame    zstd(blob[tocStart : EOF))            original TOC+footer, verbatim
//	directory    raw dirEntry × N                      where each frame lives
//	footer       raw, fixed size                       where the directory lives
//
// Every frame is separately zstd'd, so the bulk — and any future index
// section that rivals the records in size — stays compressed, while a reader
// that wants one section fetches the footer, the directory, and that one
// frame: KB-scale range GETs instead of a full-blob download.
//
// Full download reassembles the local blob byte-identically by decompressing
// the frames in order; the tail frame carries the original TOC and footer
// verbatim so no re-encoding is involved.
//
// dirEntry: [kind:u8][sectionType:u8][sectionVersion:u8][reserved:u8]
//
//	[objOffset:u64][objSize:u64][rawSize:u64][sha256:32]
//
// footer:   [dirOffset:u64][entryCount:u32][transportVersion:u32][magic:4]
const (
	transportDirEntrySize = 60
	transportFooterSize   = 20
	transportMagic        = "GXFR"
	transportVersion      = uint32(1)

	frameKindBody    = byte(0x01)
	frameKindSection = byte(0x02)
	frameKindTail    = byte(0x03)

	// transportTailProbe is how much of the object's tail a ranged reader
	// fetches first. Large enough that footer + directory land in one GET for
	// any realistic section count (60 B per entry → 60+ sections), small
	// enough to stay a trivial read.
	transportTailProbe = int64(4096)
)

// ErrNotTransportObject is returned when bytes presented as a cloud object do
// not carry the transport footer — a foreign or pre-format object.
var ErrNotTransportObject = errors.New("glcb: not a transport-framed cloud object")

type transportDirEntry struct {
	Kind           byte
	SectionType    byte
	SectionVersion uint8
	ObjOffset      int64
	ObjSize        int64
	RawSize        int64
	Hash           [32]byte
}

// blobSpan is one contiguous byte range of the local blob that becomes one
// frame of the object.
type blobSpan struct {
	kind           byte
	sectionType    byte
	sectionVersion uint8
	off, size      int64
}

// transportSpans partitions a blob of the given size into the body span, one
// span per tail TOC section, and the TOC+footer tail span. The tail sections
// are required to tile [firstTailOffset, tocStart) exactly — the writer emits
// them contiguously, and a gap would silently drop bytes from the object.
func transportSpans(toc BlobTOC, blobSize int64) ([]blobSpan, error) {
	tocStart := blobSize - tocFooterSize - int64(len(toc.Entries))*tocEntrySize
	if tocStart < 0 {
		return nil, fmt.Errorf("glcb: TOC larger than blob (%d entries, %d bytes)", len(toc.Entries), blobSize)
	}

	// Collect the contiguous run of sections that ends exactly at tocStart,
	// walking backwards. Sections outside the run (the layout block at the
	// blob's front) ride inside the body frame.
	var tail []blobSpan
	cursor := tocStart
	for {
		var found bool
		for _, e := range toc.Entries {
			if e.Offset+e.Size == cursor && e.Offset >= 0 && e.Size >= 0 {
				tail = append([]blobSpan{{
					kind:           frameKindSection,
					sectionType:    e.Type,
					sectionVersion: e.Version,
					off:            e.Offset,
					size:           e.Size,
				}}, tail...)
				cursor = e.Offset
				found = true
				break
			}
		}
		if !found {
			break
		}
	}

	spans := make([]blobSpan, 0, len(tail)+2)
	spans = append(spans, blobSpan{kind: frameKindBody, off: 0, size: cursor})
	spans = append(spans, tail...)
	spans = append(spans, blobSpan{kind: frameKindTail, off: tocStart, size: blobSize - tocStart})
	return spans, nil
}

// WrapForTransport reads the local blob at blobPath and streams the framed
// cloud object to dst. Returns the object's total size.
func WrapForTransport(dst io.Writer, blobPath string) (int64, error) {
	f, err := os.Open(filepath.Clean(blobPath)) //nolint:gosec // G703: chunk-dir path from the manager, not user input
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", blobPath, err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", blobPath, err)
	}
	toc, err := ReadTOC(f, info.Size())
	if err != nil {
		return 0, fmt.Errorf("read TOC of %s: %w", blobPath, err)
	}
	spans, err := transportSpans(toc, info.Size())
	if err != nil {
		return 0, err
	}

	written := int64(0)
	entries := make([]transportDirEntry, 0, len(spans))
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return 0, fmt.Errorf("zstd writer: %w", err)
	}
	defer func() { _ = enc.Close() }()

	for _, sp := range spans {
		hash := sha256.New()
		var frame bytes.Buffer
		enc.Reset(&frame)
		src := io.TeeReader(io.NewSectionReader(f, sp.off, sp.size), hash)
		if _, err := io.Copy(enc, src); err != nil {
			return written, fmt.Errorf("compress frame at %d: %w", sp.off, err)
		}
		if err := enc.Close(); err != nil {
			return written, fmt.Errorf("finish frame at %d: %w", sp.off, err)
		}
		n, err := dst.Write(frame.Bytes())
		if err != nil {
			return written + int64(n), fmt.Errorf("write frame: %w", err)
		}
		e := transportDirEntry{
			Kind:           sp.kind,
			SectionType:    sp.sectionType,
			SectionVersion: sp.sectionVersion,
			ObjOffset:      written,
			ObjSize:        int64(frame.Len()),
			RawSize:        sp.size,
		}
		copy(e.Hash[:], hash.Sum(nil))
		entries = append(entries, e)
		written += int64(frame.Len())
	}

	dirOffset := written
	dir := encodeTransportDir(entries)
	if _, err := dst.Write(dir); err != nil {
		return written, fmt.Errorf("write directory: %w", err)
	}
	written += int64(len(dir))

	var foot [transportFooterSize]byte
	binary.LittleEndian.PutUint64(foot[0:8], uint64(dirOffset))
	binary.LittleEndian.PutUint32(foot[8:12], uint32(len(entries))) //nolint:gosec // bounded section count
	binary.LittleEndian.PutUint32(foot[12:16], transportVersion)
	copy(foot[16:20], transportMagic)
	if _, err := dst.Write(foot[:]); err != nil {
		return written, fmt.Errorf("write footer: %w", err)
	}
	return written + transportFooterSize, nil
}

func encodeTransportDir(entries []transportDirEntry) []byte {
	buf := make([]byte, len(entries)*transportDirEntrySize)
	for i, e := range entries {
		b := buf[i*transportDirEntrySize:]
		b[0] = e.Kind
		b[1] = e.SectionType
		b[2] = e.SectionVersion
		b[3] = 0
		binary.LittleEndian.PutUint64(b[4:12], uint64(e.ObjOffset)) //nolint:gosec // non-negative
		binary.LittleEndian.PutUint64(b[12:20], uint64(e.ObjSize))  //nolint:gosec // non-negative
		binary.LittleEndian.PutUint64(b[20:28], uint64(e.RawSize))  //nolint:gosec // non-negative
		copy(b[28:60], e.Hash[:])
	}
	return buf
}

func decodeTransportDir(buf []byte, count int) ([]transportDirEntry, error) {
	if len(buf) != count*transportDirEntrySize {
		return nil, fmt.Errorf("glcb: transport directory is %d bytes, want %d", len(buf), count*transportDirEntrySize)
	}
	entries := make([]transportDirEntry, count)
	for i := range entries {
		b := buf[i*transportDirEntrySize:]
		e := &entries[i]
		e.Kind = b[0]
		e.SectionType = b[1]
		e.SectionVersion = b[2]
		e.ObjOffset = int64(binary.LittleEndian.Uint64(b[4:12])) //nolint:gosec // written non-negative
		e.ObjSize = int64(binary.LittleEndian.Uint64(b[12:20]))  //nolint:gosec // written non-negative
		e.RawSize = int64(binary.LittleEndian.Uint64(b[20:28]))  //nolint:gosec // written non-negative
		copy(e.Hash[:], b[28:60])
		if e.ObjOffset < 0 || e.ObjSize < 0 || e.RawSize < 0 {
			return nil, fmt.Errorf("glcb: transport directory entry %d has negative geometry", i)
		}
	}
	return entries, nil
}

// parseTransportFooter validates the 20-byte tail of an object and returns
// the directory's offset and entry count.
func parseTransportFooter(foot []byte, objSize int64) (dirOffset int64, count int, err error) {
	if len(foot) != transportFooterSize {
		return 0, 0, fmt.Errorf("glcb: transport footer is %d bytes, want %d", len(foot), transportFooterSize)
	}
	if string(foot[16:20]) != transportMagic {
		return 0, 0, ErrNotTransportObject
	}
	if v := binary.LittleEndian.Uint32(foot[12:16]); v != transportVersion {
		return 0, 0, fmt.Errorf("glcb: transport version %d not supported", v)
	}
	dirOffset = int64(binary.LittleEndian.Uint64(foot[0:8])) //nolint:gosec // written non-negative
	count = int(binary.LittleEndian.Uint32(foot[8:12]))
	dirEnd := dirOffset + int64(count)*transportDirEntrySize
	if dirOffset < 0 || dirEnd != objSize-transportFooterSize {
		return 0, 0, fmt.Errorf("glcb: transport directory geometry inconsistent (dir at %d+%d entries, object %d bytes)", dirOffset, count, objSize)
	}
	return dirOffset, count, nil
}

// RangedFetcher is the one blob-store capability the ranged section reader
// needs. blobstore.Store satisfies it.
type RangedFetcher interface {
	DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

// FetchRemoteSection resolves one TOC section of a cloud object via range
// GETs: the object's tail (footer + directory, usually one GET), then the
// section's frame. The frame is decompressed in memory and verified against
// the directory's SHA-256 before being returned, so a corrupt object cannot
// feed a search index garbage. objSize is the object's total size — the FSM
// manifest carries it as CloudBytes, so no HEAD round-trip is needed.
//
// The returned TOCEntry carries the section's type, version and raw size,
// ready for Registry.NewView. Returns ErrSectionNotFound when the object has
// no frame for sectionType.
func FetchRemoteSection(ctx context.Context, fetcher RangedFetcher, key string, objSize int64, sectionType byte) (TOCEntry, []byte, error) {
	if objSize < transportFooterSize {
		return TOCEntry{}, nil, fmt.Errorf("%w: object is %d bytes", ErrNotTransportObject, objSize)
	}
	probe := min(transportTailProbe, objSize)
	tail, err := fetchRange(ctx, fetcher, key, objSize-probe, probe)
	if err != nil {
		return TOCEntry{}, nil, fmt.Errorf("fetch object tail: %w", err)
	}
	dirOffset, count, err := parseTransportFooter(tail[len(tail)-transportFooterSize:], objSize)
	if err != nil {
		return TOCEntry{}, nil, err
	}

	dirLen := int64(count) * transportDirEntrySize
	var dirBytes []byte
	if tailStart := objSize - probe; dirOffset >= tailStart {
		dirBytes = tail[dirOffset-tailStart : dirOffset-tailStart+dirLen]
	} else {
		dirBytes, err = fetchRange(ctx, fetcher, key, dirOffset, dirLen)
		if err != nil {
			return TOCEntry{}, nil, fmt.Errorf("fetch transport directory: %w", err)
		}
	}
	entries, err := decodeTransportDir(dirBytes, count)
	if err != nil {
		return TOCEntry{}, nil, err
	}

	for _, e := range entries {
		if e.Kind != frameKindSection || e.SectionType != sectionType {
			continue
		}
		frame, err := fetchRange(ctx, fetcher, key, e.ObjOffset, e.ObjSize)
		if err != nil {
			return TOCEntry{}, nil, fmt.Errorf("fetch section frame: %w", err)
		}
		raw, err := decompressFrame(frame, e.RawSize)
		if err != nil {
			return TOCEntry{}, nil, fmt.Errorf("section 0x%02x frame: %w", sectionType, err)
		}
		if sum := sha256.Sum256(raw); sum != e.Hash {
			return TOCEntry{}, nil, fmt.Errorf("glcb: section 0x%02x frame hash mismatch — object corrupt", sectionType)
		}
		return TOCEntry{
			Type:    e.SectionType,
			Version: e.SectionVersion,
			Size:    e.RawSize,
			Hash:    e.Hash,
		}, raw, nil
	}
	return TOCEntry{}, nil, fmt.Errorf("%w: type=0x%02x in %s", ErrSectionNotFound, sectionType, key)
}

func fetchRange(ctx context.Context, fetcher RangedFetcher, key string, off, length int64) ([]byte, error) {
	rc, err := fetcher.DownloadRange(ctx, key, off, length)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, length)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return nil, fmt.Errorf("short range read at %d+%d: %w", off, length, err)
	}
	return buf, nil
}

func decompressFrame(frame []byte, rawSize int64) ([]byte, error) {
	dec, err := zstd.NewReader(bytes.NewReader(frame), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	defer dec.Close()
	raw := make([]byte, rawSize)
	if _, err := io.ReadFull(dec, raw); err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}
	// A frame that keeps producing bytes past its recorded raw size is not
	// the frame the directory described.
	var extra [1]byte
	if n, _ := dec.Read(extra[:]); n != 0 {
		return nil, errors.New("frame longer than directory's raw size")
	}
	return raw, nil
}
