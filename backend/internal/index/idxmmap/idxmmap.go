// Package idxmmap provides a shared helper for loading index files via mmap.
// All on-disk index files in gastrolog (tsidx, attr, kv, json, token) are
// sealed/immutable once written, which makes them ideal mmap candidates.
// Slurping them into heap-allocated []byte caused unnecessary GC pressure
// during query load — see gastrolog-3rvws.
//
// The helper assumes the decoder is "self-copying": it extracts all needed
// values out of the mmap region (via binary.LittleEndian.* and string(data[...]))
// without retaining slices that reference the underlying mmap. With that
// invariant, the mmap can be released immediately after the decode call.
package idxmmap

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrEmpty is returned when the file at path exists but has zero size.
// Callers typically map this to their own "index too small" error.
var ErrEmpty = errors.New("idxmmap: file is empty")

// Load mmaps the file at path, calls decode with the mmap'd bytes, and
// releases the mmap before returning. The decoder MUST NOT retain slices
// that reference the input bytes — strings should be created via
// `string(data[a:b])` (which copies) and primitive values should be
// extracted via `binary.LittleEndian.*` (which copies). If the decoder
// returns a slice of structs containing []byte fields that alias the input,
// they will become dangling pointers after Munmap.
func Load[T any](path string, decode func(data []byte) (T, error)) (T, error) {
	var zero T

	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return zero, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return zero, fmt.Errorf("stat %s: %w", path, err)
	}
	size := info.Size()
	if size == 0 {
		return zero, ErrEmpty
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
	if err != nil {
		return zero, fmt.Errorf("mmap %s: %w", path, err)
	}
	defer func() { _ = syscall.Munmap(data) }()

	return decode(data)
}
