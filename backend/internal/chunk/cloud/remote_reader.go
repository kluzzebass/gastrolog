package cloud

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// DownloadTOC downloads + parses the TOC tail of a blob via range requests.
// Two-step: pull the 44-byte footer first to learn the entry count, then
// pull the entries region. Used by adoptCloudBlob and any caller that
// only needs the TOC (no record reads). Closes nothing — the underlying
// store handles its own connections.
func DownloadTOC(ctx context.Context, store blobstore.Store, key string, blobSize int64) (BlobTOC, error) {
	if blobSize < int64(tocFooterSize) {
		return BlobTOC{}, fmt.Errorf("blob size %d smaller than TOC footer (%d)", blobSize, tocFooterSize)
	}
	footerBuf, err := readRangeFully(ctx, store, key, blobSize-int64(tocFooterSize), int64(tocFooterSize))
	if err != nil {
		return BlobTOC{}, fmt.Errorf("read TOC footer: %w", err)
	}
	count, _, err := parseTOCFooter(footerBuf)
	if err != nil {
		return BlobTOC{}, fmt.Errorf("parse TOC footer: %w", err)
	}
	regionSize := int64(count)*int64(tocEntrySize) + int64(tocFooterSize)
	if regionSize > blobSize {
		return BlobTOC{}, fmt.Errorf("TOC region %d exceeds blob size %d", regionSize, blobSize)
	}
	regionBuf, err := readRangeFully(ctx, store, key, blobSize-regionSize, regionSize)
	if err != nil {
		return BlobTOC{}, fmt.Errorf("read TOC region: %w", err)
	}
	return ParseTOC(regionBuf)
}

func readRangeFully(ctx context.Context, store blobstore.Store, key string, offset, size int64) ([]byte, error) {
	rc, err := store.DownloadRange(ctx, key, offset, size)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, size)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// rangeReaderAt implements io.ReaderAt using blobstore range requests.
// Each ReadAt call is one DownloadRange — the OS/CDN/S3 caches handle
// repeated reads to the same region.
type rangeReaderAt struct {
	store  blobstore.Store
	key    string
	offset int64 // base offset into the blob (start of zstd section)
	size   int64 // total size of the section
}

func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	absOff := r.offset + off
	rc, err := r.store.DownloadRange(context.Background(), r.key, absOff, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadFull(rc, p)
}

// RemoteReader provides random-access record reads from a cloud blob
// WITHOUT downloading the entire blob. Uses range requests for:
// 1. Header + dictionary + record index (one request at init)
// 2. Seekable zstd frames on demand (one request per frame read)
type RemoteReader struct {
	meta  BlobMeta
	dict  *chunk.StringDict
	index []recordIndex
	seek  seekable.Reader
}

// NewRemoteReader creates a reader backed by range requests to the blob store.
// Only downloads the header/dict/record index at creation time (small, contiguous).
// Record data is fetched on demand via seekable zstd + range requests.
func NewRemoteReader(store blobstore.Store, key string, blobSize int64) (*RemoteReader, error) {
	if blobSize < int64(headerSize+tocFooterSize) {
		return nil, fmt.Errorf("blob too small (%d bytes, need at least %d)", blobSize, headerSize+tocFooterSize)
	}

	// --- Read header (96 bytes) via range request ---
	hdrBuf, err := downloadBytes(store, key, 0, headerSize)
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	meta, dictEntries := decodeHeaderCommon(hdrBuf)
	dictSize := binary.LittleEndian.Uint32(hdrBuf[92:96])

	// --- Read dictionary + record index in one range request ---
	prefixSize := int64(dictSize) + int64(meta.RecordCount)*int64(indexEntrySize)
	prefixBuf, err := downloadBytes(store, key, headerSize, prefixSize)
	if err != nil {
		return nil, fmt.Errorf("read prefix: %w", err)
	}

	dict, err := decodeDictFromBuf(prefixBuf[:dictSize], dictEntries)
	if err != nil {
		return nil, err
	}

	idxBuf := prefixBuf[dictSize:]
	index := make([]recordIndex, meta.RecordCount)
	for i := range meta.RecordCount {
		off := int(i) * indexEntrySize
		index[i] = recordIndex{
			Offset: binary.LittleEndian.Uint64(idxBuf[off:]),
			Size:   binary.LittleEndian.Uint32(idxBuf[off+8:]),
		}
	}

	// --- Read TOC to find zstd section bounds ---
	toc, err := DownloadTOC(context.Background(), store, key, blobSize)
	if err != nil {
		return nil, err
	}
	meta.IngestIdxOffset = toc.IngestIdxOffset
	meta.IngestIdxSize = toc.IngestIdxSize
	meta.SourceIdxOffset = toc.SourceIdxOffset
	meta.SourceIdxSize = toc.SourceIdxSize

	// Seekable zstd section: from end of record index to start of ingest TS index.
	dataOffset := int64(headerSize) + int64(dictSize) + int64(meta.RecordCount)*int64(indexEntrySize)
	dataSize := toc.IngestIdxOffset - dataOffset

	// --- Create seekable zstd reader backed by range requests ---
	rra := &rangeReaderAt{store: store, key: key, offset: dataOffset, size: dataSize}
	section := io.NewSectionReader(rra, 0, dataSize)

	seek, err := seekable.NewReader(section, zstdDec)
	if err != nil {
		return nil, fmt.Errorf("open seekable reader: %w", err)
	}

	return &RemoteReader{
		meta:  meta,
		dict:  dict,
		index: index,
		seek:  seek,
	}, nil
}

// Meta returns the blob metadata.
func (rd *RemoteReader) Meta() BlobMeta { return rd.meta }

// ReadRecord reads a single record by position (0-based).
// Each call may trigger one range request for the zstd frame containing
// the record (cached by the seekable reader if the frame was already fetched).
func (rd *RemoteReader) ReadRecord(pos uint32) (chunk.Record, error) {
	if pos >= rd.meta.RecordCount {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}

	idx := rd.index[pos]
	buf := make([]byte, idx.Size)
	if _, err := rd.seek.ReadAt(buf, int64(idx.Offset)); err != nil { //nolint:gosec // G115: offset bounded by blob size
		return chunk.Record{}, fmt.Errorf("read record %d: %w", pos, err)
	}

	return decodeFrame(buf, rd.dict)
}

// Close releases the seekable reader.
func (rd *RemoteReader) Close() error {
	if rd.seek != nil {
		return rd.seek.Close()
	}
	return nil
}

// downloadBytes fetches a byte range from the blob store.
func downloadBytes(store blobstore.Store, key string, offset, size int64) ([]byte, error) {
	rc, err := store.DownloadRange(context.Background(), key, offset, size)
	if err != nil {
		if errors.Is(err, blobstore.ErrBlobArchived) {
			return nil, fmt.Errorf("%w: %s", chunk.ErrChunkArchived, key)
		}
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return nil, fmt.Errorf("%w: %s", chunk.ErrChunkSuspect, key)
		}
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, size)
	_, err = io.ReadFull(rc, buf)
	return buf, err
}
