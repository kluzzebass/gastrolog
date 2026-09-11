package file

// A data file's own mode is what travels with it. The directory it sits in
// is 0o750, but widen that directory, copy the file elsewhere, or restore
// it from a backup, and the mode on the file is the only thing left
// protecting the records inside.

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

func TestWrittenChunkFilesAreNotGroupOrWorldReadable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := NewManager(Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(4),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	base := time.Now()
	for i := range 10 {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		if _, _, err := m.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte("record")}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := m.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	var checked int
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return statErr
		}
		checked++
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			t.Errorf("%s has mode %04o; group and other must have nothing",
				strings.TrimPrefix(path, dir+"/"), mode)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if checked == 0 {
		t.Fatal("no files were written, so the mode assertion proves nothing")
	}
}

// The default is what an operator gets without saying anything, so it is
// the one that has to be right.
func TestDefaultFileModeIsOwnerOnly(t *testing.T) {
	t.Parallel()
	if DefaultFileMode&0o077 != 0 {
		t.Errorf("DefaultFileMode = %04o; group and other must have nothing", DefaultFileMode)
	}
}
