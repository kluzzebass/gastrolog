package file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
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

	// Verify rotation policy is set
	if mgr.cfg.RotationPolicy == nil {
		t.Fatal("expected RotationPolicy to be set")
	}

	// Verify default file mode
	if mgr.cfg.FileMode != DefaultFileMode {
		t.Errorf("expected FileMode=%o, got %o", DefaultFileMode, mgr.cfg.FileMode)
	}

	// Test that default size policy triggers rotation at default size
	// Create a state that's at the default limit
	state := chunk.ActiveChunkState{
		Bytes: DefaultMaxChunkBytes,
	}
	next := chunk.Record{Raw: []byte("x")}

	// Should rotate because we're at the limit and adding more
	if !mgr.cfg.RotationPolicy.ShouldRotate(state, next) {
		t.Error("expected rotation policy to trigger at default max chunk bytes")
	}
}

func TestFactoryCustomMaxChunkBytes(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	cm, err := factory(map[string]string{
		ParamDir:           dir,
		ParamMaxChunkBytes: "1024",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	// Test that custom size policy works
	// State under limit
	state := chunk.ActiveChunkState{Bytes: 500}
	next := chunk.Record{Raw: []byte("x")}

	if mgr.cfg.RotationPolicy.ShouldRotate(state, next) {
		t.Error("should not rotate when under limit")
	}

	// State at limit
	state.Bytes = 1024
	if !mgr.cfg.RotationPolicy.ShouldRotate(state, next) {
		t.Error("should rotate when at/over limit")
	}
}

func TestFactoryCustomFileMode(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	cm, err := factory(map[string]string{
		ParamDir:      dir,
		ParamFileMode: "0600",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	if mgr.cfg.FileMode != os.FileMode(0600) {
		t.Errorf("expected FileMode=0600, got %o", mgr.cfg.FileMode)
	}
}

func TestFactoryCustomMaxChunkAge(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	cm, err := factory(map[string]string{
		ParamDir:         dir,
		ParamMaxChunkAge: "1h",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	// Test that age policy works
	now := time.Now()
	next := chunk.Record{Raw: []byte("x")}

	// State created recently - should not rotate
	state := chunk.ActiveChunkState{
		CreatedAt: now.Add(-30 * time.Minute),
	}
	if mgr.cfg.RotationPolicy.ShouldRotate(state, next) {
		t.Error("should not rotate when chunk is younger than max age")
	}

	// State created over 1 hour ago - should rotate
	state.CreatedAt = now.Add(-2 * time.Hour)
	if !mgr.cfg.RotationPolicy.ShouldRotate(state, next) {
		t.Error("should rotate when chunk is older than max age")
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

func TestFactoryInvalidMaxChunkAge(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()

	_, err := factory(map[string]string{
		ParamDir:         dir,
		ParamMaxChunkAge: "not-a-duration",
	})
	if err == nil {
		t.Error("expected error for invalid max_chunk_age")
	}

	_, err = factory(map[string]string{
		ParamDir:         dir,
		ParamMaxChunkAge: "0s",
	})
	if err == nil {
		t.Error("expected error for zero max_chunk_age")
	}

	_, err = factory(map[string]string{
		ParamDir:         dir,
		ParamMaxChunkAge: "-1h",
	})
	if err == nil {
		t.Error("expected error for negative max_chunk_age")
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

func TestFactoryHardLimitAlwaysIncluded(t *testing.T) {
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

	// Test that hard limit is enforced even with large custom max_chunk_bytes
	// State approaching the hard raw limit
	state := chunk.ActiveChunkState{
		Bytes: MaxRawLogSize - 100,
	}
	// A record that would push us over the hard limit
	next := chunk.Record{Raw: make([]byte, 200)}

	if !mgr.cfg.RotationPolicy.ShouldRotate(state, next) {
		t.Error("hard limit should always trigger rotation")
	}
}
