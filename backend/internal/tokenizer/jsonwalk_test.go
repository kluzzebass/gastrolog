package tokenizer

import (
	"slices"
	"testing"
)

func TestWalkJSON_Simple(t *testing.T) {
	msg := []byte(`{"level":"error","message":"hello"}`)

	var paths []string
	var leaves []string

	ok := WalkJSON(msg, func(path []byte) {
		paths = append(paths, string(path))
	}, func(path, value []byte) {
		leaves = append(leaves, string(path)+"="+string(value))
	})

	if !ok {
		t.Fatal("expected ok=true for valid JSON")
	}

	slices.Sort(paths)
	slices.Sort(leaves)

	wantPaths := []string{"level", "message"}
	slices.Sort(wantPaths)
	if !slices.Equal(paths, wantPaths) {
		t.Errorf("paths = %v, want %v", paths, wantPaths)
	}

	wantLeaves := []string{"level=error", "message=hello"}
	slices.Sort(wantLeaves)
	if !slices.Equal(leaves, wantLeaves) {
		t.Errorf("leaves = %v, want %v", leaves, wantLeaves)
	}
}

func TestWalkJSON_Nested(t *testing.T) {
	msg := []byte(`{"service":{"name":"gateway","version":2}}`)

	var paths []string
	var leaves []string

	WalkJSON(msg, func(path []byte) {
		paths = append(paths, string(path))
	}, func(path, value []byte) {
		leaves = append(leaves, string(path)+"="+string(value))
	})

	slices.Sort(paths)
	slices.Sort(leaves)

	// "service" is a path (object node), plus "service\x00name" and "service\x00version"
	wantPaths := []string{"service", "service\x00name", "service\x00version"}
	slices.Sort(wantPaths)
	if !slices.Equal(paths, wantPaths) {
		t.Errorf("paths = %v, want %v", paths, wantPaths)
	}

	wantLeaves := []string{"service\x00name=gateway", "service\x00version=2"}
	slices.Sort(wantLeaves)
	if !slices.Equal(leaves, wantLeaves) {
		t.Errorf("leaves = %v, want %v", leaves, wantLeaves)
	}
}

func TestWalkJSON_ArrayOfObjects(t *testing.T) {
	msg := []byte(`{"spans":[{"name":"auth"},{"name":"db"}]}`)

	var paths []string
	var leaves []string

	WalkJSON(msg, func(path []byte) {
		paths = append(paths, string(path))
	}, func(path, value []byte) {
		leaves = append(leaves, string(path)+"="+string(value))
	})

	slices.Sort(paths)
	slices.Sort(leaves)

	// spans is a path, spans[*] is from the array, spans[*]name are the nested object paths
	wantPaths := []string{
		"spans",
		"spans\x00[*]",
		"spans\x00[*]",
		"spans\x00[*]\x00name",
		"spans\x00[*]\x00name",
	}
	slices.Sort(wantPaths)
	if !slices.Equal(paths, wantPaths) {
		t.Errorf("paths = %v, want %v", paths, wantPaths)
	}

	wantLeaves := []string{
		"spans\x00[*]\x00name=auth",
		"spans\x00[*]\x00name=db",
	}
	slices.Sort(wantLeaves)
	if !slices.Equal(leaves, wantLeaves) {
		t.Errorf("leaves = %v, want %v", leaves, wantLeaves)
	}
}

func TestWalkJSON_DottedKeyVsNested(t *testing.T) {
	// This is the key test: dotted key must not be confused with nested path.
	dotted := []byte(`{"service.name":"x"}`)
	nested := []byte(`{"service":{"name":"x"}}`)

	var dottedPaths, nestedPaths []string

	WalkJSON(dotted, func(path []byte) {
		dottedPaths = append(dottedPaths, string(path))
	}, func(_, _ []byte) {})

	WalkJSON(nested, func(path []byte) {
		nestedPaths = append(nestedPaths, string(path))
	}, func(_, _ []byte) {})

	// Dotted key: path is literally "service.name" (no null byte)
	if !slices.Contains(dottedPaths, "service.name") {
		t.Errorf("dotted paths should contain literal 'service.name', got %v", dottedPaths)
	}

	// Nested: path is "service\x00name"
	if !slices.Contains(nestedPaths, "service\x00name") {
		t.Errorf("nested paths should contain 'service\\x00name', got %v", nestedPaths)
	}

	// They must be different
	if slices.Contains(dottedPaths, "service\x00name") {
		t.Error("dotted key should NOT produce null-separated path")
	}
}

func TestWalkJSON_ScalarArray(t *testing.T) {
	msg := []byte(`{"tags":["web","api"]}`)

	var leaves []string

	WalkJSON(msg, func(_ []byte) {}, func(path, value []byte) {
		leaves = append(leaves, string(path)+"="+string(value))
	})

	slices.Sort(leaves)

	wantLeaves := []string{
		"tags\x00[*]=api",
		"tags\x00[*]=web",
	}
	slices.Sort(wantLeaves)
	if !slices.Equal(leaves, wantLeaves) {
		t.Errorf("leaves = %v, want %v", leaves, wantLeaves)
	}
}

func TestWalkJSON_NotJSON(t *testing.T) {
	tests := []struct {
		name string
		msg  []byte
	}{
		{"empty", nil},
		{"text", []byte("hello world")},
		{"array", []byte(`[1, 2, 3]`)},
		{"number", []byte(`42`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok := WalkJSON(tt.msg, func(_ []byte) {
				t.Error("unexpected path callback")
			}, func(_, _ []byte) {
				t.Error("unexpected leaf callback")
			})
			if ok {
				t.Error("expected ok=false for non-JSON-object input")
			}
		})
	}
}

func TestWalkJSON_ValuesLowercased(t *testing.T) {
	msg := []byte(`{"level":"ERROR","service":"MyApp"}`)

	var leaves []string
	WalkJSON(msg, func(_ []byte) {}, func(path, value []byte) {
		leaves = append(leaves, string(path)+"="+string(value))
	})

	slices.Sort(leaves)
	wantLeaves := []string{"level=error", "service=myapp"}
	slices.Sort(wantLeaves)
	if !slices.Equal(leaves, wantLeaves) {
		t.Errorf("leaves = %v, want %v", leaves, wantLeaves)
	}
}

func TestWalkJSON_BoolAndNull(t *testing.T) {
	msg := []byte(`{"enabled":true,"disabled":false,"empty":null}`)

	var paths []string
	var leaves []string

	WalkJSON(msg, func(path []byte) {
		paths = append(paths, string(path))
	}, func(path, value []byte) {
		leaves = append(leaves, string(path)+"="+string(value))
	})

	slices.Sort(paths)
	slices.Sort(leaves)

	// null emits a path but no leaf
	wantPaths := []string{"disabled", "empty", "enabled"}
	if !slices.Equal(paths, wantPaths) {
		t.Errorf("paths = %v, want %v", paths, wantPaths)
	}

	wantLeaves := []string{"disabled=false", "enabled=true"}
	if !slices.Equal(leaves, wantLeaves) {
		t.Errorf("leaves = %v, want %v", leaves, wantLeaves)
	}
}

func TestWalkJSON_DeepNesting(t *testing.T) {
	// No depth limit - should walk deeply nested structures
	msg := []byte(`{"a":{"b":{"c":{"d":{"e":{"f":"deep"}}}}}}`)

	var leaves []string
	WalkJSON(msg, func(_ []byte) {}, func(path, value []byte) {
		leaves = append(leaves, string(path)+"="+string(value))
	})

	want := "a\x00b\x00c\x00d\x00e\x00f=deep"
	if len(leaves) != 1 || leaves[0] != want {
		t.Errorf("leaves = %v, want [%q]", leaves, want)
	}
}
