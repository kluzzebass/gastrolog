//go:build !darwin && !linux

package glcb

// madviseSequential is a no-op on platforms without madvise. The backend only
// targets darwin and linux (see raftwal preallocate_darwin/linux), but the
// stub keeps this package buildable anywhere the SequentialPrewarmer interface
// is referenced.
func madviseSequential(data []byte) {}
