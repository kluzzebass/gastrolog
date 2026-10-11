package paths

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
)

// moveTempSuffix names a cross-filesystem copy that is not yet complete. It
// never parses as a segment or chunk ID, so no reader mistakes one for data.
const moveTempSuffix = ".moving"

// MoveTree moves every file under src to the same relative path under dst
// and removes the directories it emptied, src included. A file that already
// exists at its destination is a finished move, and src's copy is dropped.
//
// Within one filesystem a file moves by rename. Across filesystems it is
// copied under a temporary name beside its destination, synced, renamed into
// place and the directory synced, and only then removed from src, so an
// interrupted move leaves every file whole in at least one of the two trees
// and a repeated call finishes it. A missing src is an empty tree.
func MoveTree(src, dst string) error {
	return moveTree(src, dst, os.Rename)
}

func moveTree(src, dst string, rename func(oldpath, newpath string) error) error {
	var dirs, files []string
	var errs []error
	walkErr := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && path == src {
				return filepath.SkipDir
			}
			errs = append(errs, err)
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}
		return nil
	})
	errs = append(errs, walkErr)
	for _, path := range files {
		if strings.HasSuffix(path, moveTempSuffix) {
			// An unfinished copy into this tree: its source was never
			// removed, so the copy carries nothing the source does not.
			if err := os.Remove(path); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := moveFile(path, filepath.Join(dst, rel), rename); err != nil {
			errs = append(errs, fmt.Errorf("move %s: %w", rel, err))
		}
	}
	// Deepest first, so a parent is empty by the time it is tried. A
	// directory still holding a file that failed to move stays.
	slices.Reverse(dirs)
	for _, dir := range dirs {
		_ = os.Remove(dir)
	}
	return errors.Join(errs...)
}

func moveFile(src, dst string, rename func(oldpath, newpath string) error) error {
	if _, err := os.Lstat(dst); err == nil {
		return os.Remove(src)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	err := rename(src, dst)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	if err := copySynced(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

// copySynced installs a durable copy of src at dst: the bytes are written
// and synced under a temporary name, renamed into place, and the directory
// synced, so dst never names a partial file.
func copySynced(src, dst string) error {
	in, err := os.Open(filepath.Clean(src))
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	tmp := dst + moveTempSuffix
	out, err := os.OpenFile(filepath.Clean(tmp), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return SyncDir(filepath.Dir(dst))
}
