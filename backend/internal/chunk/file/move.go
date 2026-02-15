package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// MoveDir moves a directory from src to dst.
// It first attempts os.Rename (atomic on same filesystem).
// If that fails with EXDEV (cross-device), it falls back to recursive copy + remove.
func MoveDir(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}

	// Check for cross-device error.
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV) {
		if err := copyDir(src, dst); err != nil {
			return fmt.Errorf("copy dir: %w", err)
		}
		return os.RemoveAll(src)
	}

	return err
}

func copyDir(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}
