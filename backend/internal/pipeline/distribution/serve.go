package distribution

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

var (
	ErrSegmentNotFound = errors.New("segment not found")
	ErrSegmentGone     = errors.New("segment file missing")
)

// StreamSegment copies the segment file bytes to w.
func StreamSegment(path string, w io.Writer) error {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return ErrSegmentGone
		}
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(w, f)
	return err
}
