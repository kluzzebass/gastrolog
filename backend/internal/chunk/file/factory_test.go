package file

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFactoryMissingDir(t *testing.T) {
	factory := NewFactory()

	_, err := factory(map[string]string{})
	if err != ErrMissingDirParam {
		t.Errorf("expected ErrMissingDirParam, got %v", err)
	}

	_, err = factory(map[string]string{ParamDir: ""})
	if err != ErrMissingDirParam {
		t.Errorf("expected ErrMissingDirParam for empty dir, got %v", err)
	}
}

func TestFactoryDefaultValues(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	cm, err := factory(map[string]string{ParamDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	if mgr.cfg.MaxChunkBytes != DefaultMaxChunkBytes {
		t.Errorf("expected MaxChunkBytes=%d, got %d", DefaultMaxChunkBytes, mgr.cfg.MaxChunkBytes)
	}
	if mgr.cfg.FileMode != DefaultFileMode {
		t.Errorf("expected FileMode=%o, got %o", DefaultFileMode, mgr.cfg.FileMode)
	}
}

func TestFactoryCustomValues(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	cm, err := factory(map[string]string{
		ParamDir:           dir,
		ParamMaxChunkBytes: "1024",
		ParamFileMode:      "0600",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	if mgr.cfg.MaxChunkBytes != 1024 {
		t.Errorf("expected MaxChunkBytes=1024, got %d", mgr.cfg.MaxChunkBytes)
	}
	if mgr.cfg.FileMode != os.FileMode(0600) {
		t.Errorf("expected FileMode=0600, got %o", mgr.cfg.FileMode)
	}
}

func TestFactoryInvalidMaxChunkBytes(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	_, err := factory(map[string]string{
		ParamDir:           dir,
		ParamMaxChunkBytes: "not-a-number",
	})
	if err == nil {
		t.Error("expected error for invalid max_chunk_bytes")
	}

	_, err = factory(map[string]string{
		ParamDir:           dir,
		ParamMaxChunkBytes: "0",
	})
	if err == nil {
		t.Error("expected error for zero max_chunk_bytes")
	}

	_, err = factory(map[string]string{
		ParamDir:           dir,
		ParamMaxChunkBytes: "-1",
	})
	if err == nil {
		t.Error("expected error for negative max_chunk_bytes")
	}
}

func TestFactoryInvalidFileMode(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	_, err := factory(map[string]string{
		ParamDir:      dir,
		ParamFileMode: "not-octal",
	})
	if err == nil {
		t.Error("expected error for invalid file_mode")
	}
}

func TestFactoryCreatesDirectory(t *testing.T) {
	factory := NewFactory()
	dir := filepath.Join(t.TempDir(), "subdir", "chunks")

	_, err := factory(map[string]string{ParamDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}
}
