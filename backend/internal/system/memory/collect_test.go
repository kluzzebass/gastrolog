package memory

import (
	"cmp"
	"testing"
)

func TestCollectAndSort_Empty(t *testing.T) {
	t.Parallel()
	got := collectAndSort(map[string]int{}, func(v int) int { return v }, func(a, b int) int { return cmp.Compare(a, b) })
	if got != nil {
		t.Fatalf("expected nil for empty map, got %v", got)
	}
}

func TestCollectAndSort_SortsOutput(t *testing.T) {
	t.Parallel()
	m := map[string]int{"c": 3, "a": 1, "b": 2}
	got := collectAndSort(m, func(v int) int { return v }, func(a, b int) int { return cmp.Compare(a, b) })
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("expected [1 2 3], got %v", got)
	}
}

func TestCollectAndSort_AppliesTransform(t *testing.T) {
	t.Parallel()
	m := map[string]int{"x": 10, "y": 20}
	got := collectAndSort(m, func(v int) int { return v * 2 }, func(a, b int) int { return cmp.Compare(a, b) })
	if len(got) != 2 || got[0] != 20 || got[1] != 40 {
		t.Fatalf("expected [20 40], got %v", got)
	}
}

func TestCollectAndSort_DeepCopyIsolation(t *testing.T) {
	t.Parallel()
	type item struct {
		ID   string
		Tags []string
	}
	m := map[string]item{
		"a": {ID: "a", Tags: []string{"one", "two"}},
	}
	deepCopy := func(v item) item {
		tags := make([]string, len(v.Tags))
		copy(tags, v.Tags)
		return item{ID: v.ID, Tags: tags}
	}
	got := collectAndSort(m, deepCopy, func(a, b item) int { return cmp.Compare(a.ID, b.ID) })

	// Mutate the output — should not affect the source map.
	got[0].Tags[0] = "mutated"
	if m["a"].Tags[0] == "mutated" {
		t.Fatal("transform did not deep copy — source map was mutated")
	}
}

func TestCollectAndSort_NilMap(t *testing.T) {
	t.Parallel()
	var m map[string]int
	got := collectAndSort(m, func(v int) int { return v }, func(a, b int) int { return cmp.Compare(a, b) })
	if got != nil {
		t.Fatalf("expected nil for nil map, got %v", got)
	}
}
