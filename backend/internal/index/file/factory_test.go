package file

import (
	"testing"

	chunkmem "gastrolog/internal/chunk/memory"
)

func TestFactoryMissingDir(t *testing.T) {
	factory := NewFactory()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	_, err := factory(map[string]string{}, cm)
	if err != ErrMissingDirParam {
		t.Errorf("expected ErrMissingDirParam, got %v", err)
	}

	_, err = factory(map[string]string{ParamDir: ""}, cm)
	if err != ErrMissingDirParam {
		t.Errorf("expected ErrMissingDirParam for empty dir, got %v", err)
	}
}

func TestFactoryDefaultValues(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	im, err := factory(map[string]string{ParamDir: dir}, cm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := im.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	// Should have 3 indexers: time, source, token
	if len(mgr.indexers) != 3 {
		t.Errorf("expected 3 indexers, got %d", len(mgr.indexers))
	}
}

func TestFactoryCustomTimeSparsity(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	_, err := factory(map[string]string{
		ParamDir:          dir,
		ParamTimeSparsity: "500",
	}, cm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFactoryInvalidTimeSparsity(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	_, err := factory(map[string]string{
		ParamDir:          dir,
		ParamTimeSparsity: "not-a-number",
	}, cm)
	if err == nil {
		t.Error("expected error for invalid time_sparsity")
	}

	_, err = factory(map[string]string{
		ParamDir:          dir,
		ParamTimeSparsity: "0",
	}, cm)
	if err == nil {
		t.Error("expected error for zero time_sparsity")
	}

	_, err = factory(map[string]string{
		ParamDir:          dir,
		ParamTimeSparsity: "-1",
	}, cm)
	if err == nil {
		t.Error("expected error for negative time_sparsity")
	}
}
