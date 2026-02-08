package memory

import (
	"testing"

	"gastrolog/internal/chunk"
)

func TestFactoryDefaultValues(t *testing.T) {
	factory := NewFactory()

	cm, err := factory(map[string]string{}, nil)
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

	// Test that default policy triggers rotation at default record count
	state := chunk.ActiveChunkState{Records: DefaultMaxRecords}
	next := chunk.Record{Raw: []byte("x")}

	if mgr.cfg.RotationPolicy.ShouldRotate(state, next) == nil {
		t.Error("expected rotation policy to trigger at default max records")
	}

	// Under limit should not rotate
	state.Records = DefaultMaxRecords - 1
	if mgr.cfg.RotationPolicy.ShouldRotate(state, next) != nil {
		t.Error("should not rotate when under limit")
	}
}

func TestFactoryCustomValues(t *testing.T) {
	factory := NewFactory()

	cm, err := factory(map[string]string{
		ParamMaxRecords: "2048",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	// Test that custom policy works
	state := chunk.ActiveChunkState{Records: 2047}
	next := chunk.Record{Raw: []byte("x")}

	if mgr.cfg.RotationPolicy.ShouldRotate(state, next) != nil {
		t.Error("should not rotate when under limit")
	}

	state.Records = 2048
	if mgr.cfg.RotationPolicy.ShouldRotate(state, next) == nil {
		t.Error("should rotate when at limit")
	}
}

func TestFactoryInvalidMaxRecords(t *testing.T) {
	factory := NewFactory()

	_, err := factory(map[string]string{
		ParamMaxRecords: "not-a-number",
	}, nil)
	if err == nil {
		t.Error("expected error for invalid max_records")
	}

	_, err = factory(map[string]string{
		ParamMaxRecords: "0",
	}, nil)
	if err == nil {
		t.Error("expected error for zero max_records")
	}

	_, err = factory(map[string]string{
		ParamMaxRecords: "-1",
	}, nil)
	if err == nil {
		t.Error("expected error for negative max_records")
	}
}
