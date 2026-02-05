package token

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/logging"
	"gastrolog/internal/tokenizer"
)

// FNV-1a constants
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// Indexer builds a token index for sealed chunks.
// For each chunk, it maps every distinct token to the list of
// record positions where that token appears, and writes the result
// to <dir>/<chunkID>/_token.idx.
//
// Memory invariant: Each distinct token string is allocated exactly once.
// All subsequent uses reuse that same interned instance.
//
// The indexer uses a two-pass algorithm:
//   - Pass 1: Count occurrences of each token, interning token strings
//   - Allocate: Create posting slices with exact capacity
//   - Pass 2: Fill posting slices using interned tokens (no new allocations)
type Indexer struct {
	dir     string
	manager chunk.ChunkManager
	logger  *slog.Logger
}

func NewIndexer(dir string, manager chunk.ChunkManager, logger *slog.Logger) *Indexer {
	return &Indexer{
		dir:     dir,
		manager: manager,
		logger:  logging.Default(logger).With("component", "indexer", "type", "token"),
	}
}

func (t *Indexer) Name() string {
	return "token"
}

func (t *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	buildStart := time.Now()

	meta, err := t.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	// PASS 1: Count token occurrences, intern all distinct tokens.
	pass1Start := time.Now()
	intern := newTokenIntern()
	counts, recordCount, err := t.countTokens(ctx, chunkID, intern)
	if err != nil {
		return fmt.Errorf("pass 1 (count): %w", err)
	}
	pass1Duration := time.Since(pass1Start)

	// Sort tokens for deterministic output and binary search.
	sortedTokens := make([]string, 0, len(counts))
	for tok := range counts {
		sortedTokens = append(sortedTokens, tok)
	}
	slices.Sort(sortedTokens)

	// Compute file layout: header + key table + posting blob.
	// Key entry: tokenLen (2) + token (variable) + postingOffset (8) + postingCount (4)
	totalTokenBytes := 0
	totalPositions := uint64(0)
	for _, tok := range sortedTokens {
		totalTokenBytes += len(tok)
		totalPositions += uint64(counts[tok])
	}
	keyTableSize := len(sortedTokens)*(tokenLenSize+postingOffsetSize+postingCountSize) + totalTokenBytes
	postingBlobStart := int64(headerSize + keyTableSize)

	// Compute each token's offset into the posting blob, and its write cursor.
	// fileOffset[tok] = absolute file position where this token's postings start
	// writeIdx[tok] = how many positions written so far for this token
	fileOffset := make(map[string]uint32, len(counts))
	writeIdx := make(map[string]uint32, len(counts))
	offset := uint32(postingBlobStart)
	for _, tok := range sortedTokens {
		fileOffset[tok] = offset
		writeIdx[tok] = 0
		offset += counts[tok] * positionSize
	}
	totalFileSize := int64(offset)

	// Create temp file and pre-allocate space.
	chunkDir := filepath.Join(t.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	target := filepath.Join(chunkDir, indexFileName)
	tmpFile, err := os.CreateTemp(chunkDir, indexFileName+".tmp.*")
	if err != nil {
		return fmt.Errorf("create temp index: %w", err)
	}
	tmpName := tmpFile.Name()

	if err := tmpFile.Chmod(0o644); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("chmod temp index: %w", err)
	}

	// Truncate to final size to pre-allocate space.
	if err := tmpFile.Truncate(totalFileSize); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("truncate index file: %w", err)
	}

	// Write header.
	if err := writeIndexHeader(tmpFile, uint32(len(sortedTokens))); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write header: %w", err)
	}

	// Write key table.
	postingOffset := uint32(0) // relative offset within posting blob
	for _, tok := range sortedTokens {
		if err := writeKeyEntry(tmpFile, tok, postingOffset, counts[tok]); err != nil {
			tmpFile.Close()
			os.Remove(tmpName)
			return fmt.Errorf("write key entry: %w", err)
		}
		postingOffset += counts[tok] * positionSize
	}

	// PASS 2: Write positions directly to file at pre-computed offsets.
	pass2Start := time.Now()
	if err := t.fillPostingsToFile(ctx, chunkID, intern, tmpFile, fileOffset, writeIdx, totalFileSize); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("pass 2 (fill): %w", err)
	}
	pass2Duration := time.Since(pass2Start)

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp index: %w", err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename index: %w", err)
	}

	totalDuration := time.Since(buildStart)

	t.logger.Debug("token index built",
		"chunk", chunkID.String(),
		"chunk_start", meta.StartTS,
		"chunk_end", meta.EndTS,
		"chunk_duration", meta.EndTS.Sub(meta.StartTS),
		"records", recordCount,
		"tokens", len(counts),
		"positions", totalPositions,
		"file_size", totalFileSize,
		"pass1", pass1Duration,
		"pass2", pass2Duration,
		"total", totalDuration,
	)

	return nil
}

// tokenIntern is a hash map that interns token strings.
// It uses []byte keys directly without allocating strings for lookups.
// Each distinct token is allocated exactly once.
type tokenIntern struct {
	buckets map[uint64][]string
}

func newTokenIntern() *tokenIntern {
	return &tokenIntern{
		buckets: make(map[uint64][]string),
	}
}

// hash computes FNV-1a hash of the byte slice without allocation.
func (ti *tokenIntern) hash(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// equalStringBytes compares a string to a byte slice without allocation.
func equalStringBytes(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := range b {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}

// intern returns the interned string for the given bytes.
// If the token has been seen before, returns the existing interned string.
// Otherwise, allocates a new string and adds it to the intern map.
//
// This is the ONLY place where string(b) allocation is allowed during indexing.
func (ti *tokenIntern) intern(b []byte) string {
	h := ti.hash(b)
	bucket := ti.buckets[h]

	// Check for existing entry in bucket (handle hash collisions).
	for _, s := range bucket {
		if equalStringBytes(s, b) {
			return s
		}
	}

	// Not found: allocate the string exactly once.
	s := string(b)
	ti.buckets[h] = append(bucket, s)
	return s
}

// lookup returns the interned string for the given bytes.
// Returns false if the token was not interned in pass 1 (indicates a logic error
// or data changed between passes).
func (ti *tokenIntern) lookup(b []byte) (string, bool) {
	h := ti.hash(b)
	bucket := ti.buckets[h]

	for _, s := range bucket {
		if equalStringBytes(s, b) {
			return s, true
		}
	}
	return "", false
}

// tokens returns all interned token strings.
func (ti *tokenIntern) tokens() []string {
	var result []string
	for _, bucket := range ti.buckets {
		result = append(result, bucket...)
	}
	return result
}

// writeIndexHeader writes the index file header.
func writeIndexHeader(w *os.File, keyCount uint32) error {
	buf := make([]byte, headerSize)
	cursor := 0
	h := format.Header{Type: format.TypeTokenIndex, Version: currentVersion, Flags: format.FlagComplete}
	cursor += h.EncodeInto(buf[cursor:])

	binary.LittleEndian.PutUint32(buf[cursor:cursor+keyCountSize], keyCount)

	_, err := w.Write(buf)
	return err
}

// writeKeyEntry writes a single key table entry.
func writeKeyEntry(w *os.File, tok string, postingOffset uint32, postingCount uint32) error {
	buf := make([]byte, tokenLenSize+len(tok)+postingOffsetSize+postingCountSize)
	cursor := 0

	binary.LittleEndian.PutUint16(buf[cursor:cursor+tokenLenSize], uint16(len(tok)))
	cursor += tokenLenSize

	copy(buf[cursor:cursor+len(tok)], tok)
	cursor += len(tok)

	binary.LittleEndian.PutUint32(buf[cursor:cursor+postingOffsetSize], postingOffset)
	cursor += postingOffsetSize

	binary.LittleEndian.PutUint32(buf[cursor:cursor+postingCountSize], postingCount)

	_, err := w.Write(buf)
	return err
}

// countTokens performs pass 1: count occurrences of each token.
// All tokens are interned via the intern pool.
// Returns map[interned_token]count and total record count.
func (t *Indexer) countTokens(ctx context.Context, chunkID chunk.ChunkID, intern *tokenIntern) (map[string]uint32, uint64, error) {
	cursor, err := t.manager.OpenCursor(chunkID)
	if err != nil {
		return nil, 0, fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	counts := make(map[string]uint32)
	var recordCount uint64

	// Reusable per-record deduplication buffer.
	// Stores interned string pointers, cleared between records.
	seenInRecord := make(map[string]struct{}, 32)

	// Reusable buffer for tokenization.
	tokBuf := make([]byte, 0, 64)

	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}

		rec, _, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			return nil, 0, fmt.Errorf("read record: %w", err)
		}
		recordCount++

		// Clear seen set for this record (reuse map to avoid allocs).
		clear(seenInRecord)

		tokenizer.IterTokens(rec.Raw, tokBuf, tokenizer.DefaultMaxTokenLen, func(tokBytes []byte) bool {
			// Intern the token (allocates only on first global occurrence).
			tok := intern.intern(tokBytes)

			// Skip if already counted in this record.
			if _, dup := seenInRecord[tok]; dup {
				return true
			}
			seenInRecord[tok] = struct{}{}

			counts[tok]++
			return true
		})
	}

	return counts, recordCount, nil
}

// fillPostingsToFile performs pass 2: write positions directly to mmap'd file.
// Uses only interned tokens from pass 1. No posting lists held in memory.
func (t *Indexer) fillPostingsToFile(ctx context.Context, chunkID chunk.ChunkID, intern *tokenIntern, f *os.File, fileOffset map[string]uint32, writeIdx map[string]uint32, fileSize int64) error {
	cursor, err := t.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	// Mmap the file for writing.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	defer syscall.Munmap(data)

	// Reusable per-record deduplication buffer.
	seenInRecord := make(map[string]struct{}, 32)

	// Reusable buffer for tokenization.
	tokBuf := make([]byte, 0, 64)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rec, ref, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			return fmt.Errorf("read record: %w", err)
		}

		// Clear seen set for this record.
		clear(seenInRecord)

		tokenizer.IterTokens(rec.Raw, tokBuf, tokenizer.DefaultMaxTokenLen, func(tokBytes []byte) bool {
			// Look up the interned token (no allocation).
			tok, found := intern.lookup(tokBytes)
			if !found {
				// Token not interned - this indicates a logic error or data changed
				// between passes. Skip silently to avoid crashing.
				return true
			}

			// Skip if already processed in this record.
			if _, dup := seenInRecord[tok]; dup {
				return true
			}
			seenInRecord[tok] = struct{}{}

			// Write position directly to mmap'd memory.
			idx := writeIdx[tok]
			offset := fileOffset[tok] + idx*positionSize
			binary.LittleEndian.PutUint32(data[offset:], uint32(ref.Pos))
			writeIdx[tok] = idx + 1
			return true
		})
	}

	return nil
}
