// Package diskusage measures on-disk footprint for node storage reporting.
package diskusage

import (
	"io/fs"
	"os"
	"path/filepath"
)

// FileBytes returns the size of a regular file, or 0 when absent or not a file.
func FileBytes(path string) int64 {
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return 0
	}
	return st.Size()
}

// DirBytes returns the sum of regular file sizes under root. Missing roots
// return 0. Symlinks are not followed; per-file errors are skipped.
func DirBytes(root string) int64 {
	root = filepath.Clean(root)
	st, err := os.Stat(root)
	if err != nil || !st.IsDir() {
		return 0
	}

	var total int64
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil //nolint:nilerr // skip unreadable entries; walk continues
		}
		total += info.Size()
		return nil
	})
	return total
}
