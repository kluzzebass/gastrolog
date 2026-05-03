package cloud

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrSectionNotFound is returned when LoadSection is asked for a section
// type whose entry is not present in the blob's TOC.
var ErrSectionNotFound = errors.New("cloud: section not found in TOC")

// MapSection opens the GLCB blob at blobPath, locates the section with
// the given type byte via the blob's TOC, and returns a long-lived
// mmap'd window covering exactly that section. The caller MUST call
// the returned close function to release the mapping when done.
//
// Unlike LoadSection, the mapping survives the call — useful for
// cached read paths that want to perform many reads against the
// section without re-mmapping each time. The returned slice MUST
// remain in use only until close is called; pointing decoded data
// into the slice past close produces undefined behaviour.
//
// Errors are the same as LoadSection: ErrSectionNotFound for an
// absent type, plus underlying os/syscall errors.
func MapSection(blobPath string, sectionType byte) ([]byte, func() error, error) {
	f, err := os.Open(filepath.Clean(blobPath))
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", blobPath, err)
	}
	// f is closed eagerly after mmap — the mapping survives the close
	// (POSIX mmap holds its own reference to the underlying inode).
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("stat %s: %w", blobPath, err)
	}
	toc, err := ReadTOC(f, info.Size())
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("read TOC of %s: %w", blobPath, err)
	}
	entry, ok := toc.Find(sectionType)
	if !ok {
		_ = f.Close()
		return nil, nil, fmt.Errorf("%w: type=0x%02x in %s", ErrSectionNotFound, sectionType, blobPath)
	}

	pageSize := int64(syscall.Getpagesize())
	pageOffset := entry.Offset - (entry.Offset % pageSize)
	mapStart := entry.Offset - pageOffset
	mapLen := mapStart + entry.Size

	data, err := syscall.Mmap(int(f.Fd()), pageOffset, int(mapLen), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
	_ = f.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("mmap section 0x%02x in %s: %w", sectionType, blobPath, err)
	}
	closer := func() error { return syscall.Munmap(data) }
	return data[mapStart : mapStart+entry.Size], closer, nil
}

// LoadSection opens the GLCB blob at blobPath, locates the section with
// the given type byte via the blob's TOC, mmaps a page-aligned window
// covering exactly that section, calls decode with a sub-slice that
// covers `[offset, offset+size)` of the blob, and unmaps before
// returning.
//
// The decoder MUST NOT retain slices that alias the input — strings
// should be created via `string(data[a:b])` and primitives extracted
// via `binary.LittleEndian.*`. After LoadSection returns, the mmap is
// gone and any retained alias points at unmapped memory.
//
// LoadSection does not verify the section's SHA-256 against its TOC
// entry on every call. Local sealed blobs are trusted; corruption is
// the caller's problem to detect via the per-section hash if needed
// (see TOCEntry.VerifyHash).
//
// Errors:
//   - ErrSectionNotFound: TOC has no entry for sectionType.
//   - underlying os/syscall errors for open/stat/mmap failures.
func LoadSection[T any](blobPath string, sectionType byte, decode func(data []byte) (T, error)) (T, error) {
	var zero T

	f, err := os.Open(filepath.Clean(blobPath))
	if err != nil {
		return zero, fmt.Errorf("open %s: %w", blobPath, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return zero, fmt.Errorf("stat %s: %w", blobPath, err)
	}

	toc, err := ReadTOC(f, info.Size())
	if err != nil {
		return zero, fmt.Errorf("read TOC of %s: %w", blobPath, err)
	}

	entry, ok := toc.Find(sectionType)
	if !ok {
		return zero, fmt.Errorf("%w: type=0x%02x in %s", ErrSectionNotFound, sectionType, blobPath)
	}

	// mmap requires a page-aligned offset. Compute the largest page
	// boundary at or before the section start, mmap from there, and
	// return a sub-slice that begins at the section's actual offset.
	pageSize := int64(syscall.Getpagesize())
	pageOffset := entry.Offset - (entry.Offset % pageSize)
	mapStart := entry.Offset - pageOffset
	mapLen := mapStart + entry.Size

	data, err := syscall.Mmap(int(f.Fd()), pageOffset, int(mapLen), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
	if err != nil {
		return zero, fmt.Errorf("mmap section 0x%02x in %s: %w", sectionType, blobPath, err)
	}
	defer func() { _ = syscall.Munmap(data) }()

	return decode(data[mapStart : mapStart+entry.Size])
}
