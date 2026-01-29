package memory

import (
	"testing"
)

func TestFactoryDefaultValues(t *testing.T) {
	factory := NewFactory()

	cm, err := factory(map[string]string{})
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
}

func TestFactoryCustomValues(t *testing.T) {
	factory := NewFactory()

	cm, err := factory(map[string]string{
		ParamMaxChunkBytes: "2048",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	if mgr.cfg.MaxChunkBytes != 2048 {
		t.Errorf("expected MaxChunkBytes=2048, got %d", mgr.cfg.MaxChunkBytes)
	}
}

func TestFactoryInvalidMaxChunkBytes(t *testing.T) {
	factory := NewFactory()

	_, err := factory(map[string]string{
		ParamMaxChunkBytes: "not-a-number",
	})
	if err == nil {
		t.Error("expected error for invalid max_chunk_bytes")
	}

	_, err = factory(map[string]string{
		ParamMaxChunkBytes: "0",
	})
	if err == nil {
		t.Error("expected error for zero max_chunk_bytes")
	}

	_, err = factory(map[string]string{
		ParamMaxChunkBytes: "-1",
	})
	if err == nil {
		t.Error("expected error for negative max_chunk_bytes")
	}
}
