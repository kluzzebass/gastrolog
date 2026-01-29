// Package file provides file-based persistence for source metadata.
package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/source"
)

const (
	currentVersion = 1
)

// Store persists sources to disk using atomic writes.
//
// File format:
//
//	Header (4 bytes):
//	  signature (1 byte, 'i')
//	  type (1 byte, 'z')
//	  version (1 byte)
//	  flags (1 byte, reserved)
//
//	Source count (4 bytes, little-endian uint32)
//
//	For each source:
//	  SourceID (16 bytes, UUID)
//	  CreatedAt (8 bytes, Unix microseconds)
//	  Attribute count (2 bytes, little-endian uint16)
//	  For each attribute:
//	    Key length (2 bytes, little-endian uint16)
//	    Key (variable)
//	    Value length (2 bytes, little-endian uint16)
//	    Value (variable)
type Store struct {
	mu   sync.Mutex
	path string

	// In-memory cache of all sources for atomic writes.
	sources map[string]*source.Source
}

// NewStore creates a Store that persists to the given path.
func NewStore(path string) *Store {
	return &Store{
		path:    path,
		sources: make(map[string]*source.Source),
	}
}

// Save persists a source. The entire file is rewritten atomically.
func (s *Store) Save(src *source.Source) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update in-memory cache.
	s.sources[src.ID.String()] = copySource(src)

	// Write all sources atomically.
	return s.writeFile()
}

// LoadAll reads all sources from disk.
func (s *Store) LoadAll() ([]*source.Source, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if file exists.
	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		return nil, nil
	}

	f, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}
	defer f.Close()

	sources, err := s.readFile(f)
	if err != nil {
		return nil, err
	}

	// Populate in-memory cache.
	s.sources = make(map[string]*source.Source, len(sources))
	for _, src := range sources {
		s.sources[src.ID.String()] = src
	}

	return sources, nil
}

// writeFile writes all sources to disk atomically.
func (s *Store) writeFile() error {
	// Ensure directory exists.
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	// Write to temp file first.
	tmpPath := s.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	// Write header.
	h := format.Header{Type: format.TypeSourceRegistry, Version: currentVersion, Flags: 0}
	header := h.Encode()
	if _, err := f.Write(header[:]); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write header: %w", err)
	}

	// Write source count.
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(s.sources)))
	if _, err := f.Write(countBuf); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write count: %w", err)
	}

	// Write each source.
	for _, src := range s.sources {
		if err := s.writeSource(f, src); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("write source: %w", err)
		}
	}

	// Sync and close.
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close: %w", err)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, s.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

// writeSource writes a single source to the writer.
func (s *Store) writeSource(w io.Writer, src *source.Source) error {
	// SourceID (16 bytes).
	if _, err := w.Write(src.ID[:]); err != nil {
		return err
	}

	// CreatedAt (8 bytes, Unix microseconds).
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(src.CreatedAt.UnixMicro()))
	if _, err := w.Write(buf); err != nil {
		return err
	}

	// Attribute count.
	attrCountBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(attrCountBuf, uint16(len(src.Attributes)))
	if _, err := w.Write(attrCountBuf); err != nil {
		return err
	}

	// Attributes.
	lenBuf := make([]byte, 2)
	for k, v := range src.Attributes {
		// Key.
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(k)))
		if _, err := w.Write(lenBuf); err != nil {
			return err
		}
		if _, err := w.Write([]byte(k)); err != nil {
			return err
		}

		// Value.
		binary.LittleEndian.PutUint16(lenBuf, uint16(len(v)))
		if _, err := w.Write(lenBuf); err != nil {
			return err
		}
		if _, err := w.Write([]byte(v)); err != nil {
			return err
		}
	}

	return nil
}

// readFile reads all sources from the reader.
func (s *Store) readFile(r io.Reader) ([]*source.Source, error) {
	// Read header.
	header := make([]byte, format.HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("read header: %w", err)
	}

	_, err := format.DecodeAndValidate(header, format.TypeSourceRegistry, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("source registry: %w", err)
	}

	// Read source count.
	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, countBuf); err != nil {
		return nil, fmt.Errorf("read count: %w", err)
	}
	count := binary.LittleEndian.Uint32(countBuf)

	// Read sources.
	sources := make([]*source.Source, 0, count)
	for i := uint32(0); i < count; i++ {
		src, err := s.readSource(r)
		if err != nil {
			return nil, fmt.Errorf("read source %d: %w", i, err)
		}
		sources = append(sources, src)
	}

	return sources, nil
}

// readSource reads a single source from the reader.
func (s *Store) readSource(r io.Reader) (*source.Source, error) {
	// SourceID (16 bytes).
	var id chunk.SourceID
	if _, err := io.ReadFull(r, id[:]); err != nil {
		return nil, err
	}

	// CreatedAt.
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	createdAt := time.UnixMicro(int64(binary.LittleEndian.Uint64(buf)))

	// Attribute count.
	attrCountBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, attrCountBuf); err != nil {
		return nil, err
	}
	attrCount := binary.LittleEndian.Uint16(attrCountBuf)

	// Attributes.
	attrs := make(map[string]string, attrCount)
	lenBuf := make([]byte, 2)
	for i := uint16(0); i < attrCount; i++ {
		// Key.
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return nil, err
		}
		keyLen := binary.LittleEndian.Uint16(lenBuf)
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return nil, err
		}

		// Value.
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return nil, err
		}
		valLen := binary.LittleEndian.Uint16(lenBuf)
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBuf); err != nil {
			return nil, err
		}

		attrs[string(keyBuf)] = string(valBuf)
	}

	return &source.Source{
		ID:         id,
		Attributes: attrs,
		CreatedAt:  createdAt,
	}, nil
}

// copySource creates a copy of a Source.
func copySource(src *source.Source) *source.Source {
	attrs := make(map[string]string, len(src.Attributes))
	for k, v := range src.Attributes {
		attrs[k] = v
	}
	return &source.Source{
		ID:         src.ID,
		Attributes: attrs,
		CreatedAt:  src.CreatedAt,
	}
}
