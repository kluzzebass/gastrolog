package file

import (
	"testing"

	chunkmem "gastrolog/internal/chunk/memory"
)

func TestFactoryMissingDir(t *testing.T) {
	factory := NewFactory()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	_, err := factory(map[string]string{}, cm, nil)
	if err != ErrMissingDirParam {
		t.Errorf("expected ErrMissingDirParam, got %v", err)
	}

	_, err = factory(map[string]string{ParamDir: ""}, cm, nil)
	if err != ErrMissingDirParam {
		t.Errorf("expected ErrMissingDirParam for empty dir, got %v", err)
	}
}

func TestFactoryDefaultValues(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	im, err := factory(map[string]string{ParamDir: dir}, cm, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := im.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	// Should have 2 indexers: time, token
	if len(mgr.indexers) != 2 {
		t.Errorf("expected 2 indexers, got %d", len(mgr.indexers))
	}
}

func TestFactoryCustomTimeSparsity(t *testing.T) {
	factory := NewFactory()
	dir := t.TempDir()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	_, err := factory(map[string]string{
		ParamDir:          dir,
		ParamTimeSparsity: "500",
	}, cm, nil)
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
	}, cm, nil)
	if err == nil {
		t.Error("expected error for invalid time_sparsity")
	}

	_, err = factory(map[string]string{
		ParamDir:          dir,
		ParamTimeSparsity: "0",
	}, cm, nil)
	if err == nil {
		t.Error("expected error for zero time_sparsity")
	}

	_, err = factory(map[string]string{
		ParamDir:          dir,
		ParamTimeSparsity: "-1",
	}, cm, nil)
	if err == nil {
		t.Error("expected error for negative time_sparsity")
	}
}
