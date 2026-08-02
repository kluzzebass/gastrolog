package glcb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrSectionNotFound is returned when LoadSection is asked for a section
// type whose entry is not present in the blob's TOC.
var ErrSectionNotFound = errors.New("glcb: section not found in TOC")

// MapSection opens the GLCB blob at blobPath, locates the section with
// the given type byte via the blob's TOC, and returns the section's TOC
// entry plus a long-lived mmap'd window covering exactly that section.
// The caller MUST call the returned close function to release the
// mapping when done.
//
// The TOC entry is returned so decode dispatch can honor the section's
// recorded version (Registry.NewView) — bytes without their version are
// only decodable by assuming a layout.
//
// The mapping survives the call — useful for cached read paths that
// perform many reads against the section without re-mmapping each time.
// The returned slice MUST remain in use only until close is called;
// pointing decoded data into the slice past close produces undefined
// behaviour.
//
// MapSection does not verify the section's SHA-256 against its TOC
// entry on every call. Local sealed blobs are trusted; corruption is
// the caller's problem to detect via the per-section hash if needed
// (TOCEntry.Hash records each section's SHA-256).
//
// Errors: ErrSectionNotFound for an absent type, plus underlying
// os/syscall errors.
func MapSection(blobPath string, sectionType byte) (TOCEntry, []byte, func() error, error) {
	f, err := os.Open(filepath.Clean(blobPath))
	if err != nil {
		return TOCEntry{}, nil, nil, fmt.Errorf("open %s: %w", blobPath, err)
	}
	// f is closed eagerly after mmap — the mapping survives the close
	// (POSIX mmap holds its own reference to the underlying inode).
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return TOCEntry{}, nil, nil, fmt.Errorf("stat %s: %w", blobPath, err)
	}
	toc, err := ReadTOC(f, info.Size())
	if err != nil {
		_ = f.Close()
		return TOCEntry{}, nil, nil, fmt.Errorf("read TOC of %s: %w", blobPath, err)
	}
	entry, ok := toc.Find(sectionType)
	if !ok {
		_ = f.Close()
		return TOCEntry{}, nil, nil, fmt.Errorf("%w: type=0x%02x in %s", ErrSectionNotFound, sectionType, blobPath)
	}

	pageSize := int64(syscall.Getpagesize())
	pageOffset := entry.Offset - (entry.Offset % pageSize)
	mapStart := entry.Offset - pageOffset
	mapLen := mapStart + entry.Size

	data, err := syscall.Mmap(int(f.Fd()), pageOffset, int(mapLen), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
	_ = f.Close()
	if err != nil {
		return TOCEntry{}, nil, nil, fmt.Errorf("mmap section 0x%02x in %s: %w", sectionType, blobPath, err)
	}
	closer := func() error { return syscall.Munmap(data) }
	return entry, data[mapStart : mapStart+entry.Size], closer, nil
}
