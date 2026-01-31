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

	if mgr.cfg.MaxRecords != DefaultMaxRecords {
		t.Errorf("expected MaxRecords=%d, got %d", DefaultMaxRecords, mgr.cfg.MaxRecords)
	}
}

func TestFactoryCustomValues(t *testing.T) {
	factory := NewFactory()

	cm, err := factory(map[string]string{
		ParamMaxRecords: "2048",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := cm.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	if mgr.cfg.MaxRecords != 2048 {
		t.Errorf("expected MaxRecords=2048, got %d", mgr.cfg.MaxRecords)
	}
}

func TestFactoryInvalidMaxRecords(t *testing.T) {
	factory := NewFactory()

	_, err := factory(map[string]string{
		ParamMaxRecords: "not-a-number",
	})
	if err == nil {
		t.Error("expected error for invalid max_records")
	}

	_, err = factory(map[string]string{
		ParamMaxRecords: "0",
	})
	if err == nil {
		t.Error("expected error for zero max_records")
	}

	_, err = factory(map[string]string{
		ParamMaxRecords: "-1",
	})
	if err == nil {
		t.Error("expected error for negative max_records")
	}
}
