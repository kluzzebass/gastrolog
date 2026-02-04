package memory

import (
	"testing"

	chunkmem "gastrolog/internal/chunk/memory"
)

func TestFactoryDefaultValues(t *testing.T) {
	factory := NewFactory()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	im, err := factory(map[string]string{}, cm, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr, ok := im.(*Manager)
	if !ok {
		t.Fatal("expected *Manager")
	}

	// Should have 3 indexers: token, attr, kv
	if len(mgr.indexers) != 3 {
		t.Errorf("expected 3 indexers, got %d", len(mgr.indexers))
	}
}

func TestFactoryCustomKVBudget(t *testing.T) {
	factory := NewFactory()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	_, err := factory(map[string]string{
		ParamKVBudget: "5242880", // 5 MB
	}, cm, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFactoryInvalidKVBudget(t *testing.T) {
	factory := NewFactory()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})

	_, err := factory(map[string]string{
		ParamKVBudget: "not-a-number",
	}, cm, nil)
	if err == nil {
		t.Error("expected error for invalid kv_budget")
	}

	_, err = factory(map[string]string{
		ParamKVBudget: "0",
	}, cm, nil)
	if err == nil {
		t.Error("expected error for zero kv_budget")
	}

	_, err = factory(map[string]string{
		ParamKVBudget: "-1",
	}, cm, nil)
	if err == nil {
		t.Error("expected error for negative kv_budget")
	}
}
