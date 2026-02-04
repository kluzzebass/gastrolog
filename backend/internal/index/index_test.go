package index

import (
	"testing"

	"gastrolog/internal/chunk"
)

func TestNewIndexWithIntType(t *testing.T) {
	// Verify generics work with arbitrary types.
	idx := NewIndex([]int{1, 2, 3})
	got := idx.Entries()
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("expected [1 2 3], got %v", got)
	}
}

// TokenIndexReader tests

func TestTokenLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "apple", Positions: []uint64{0, 128}},
		{Token: "banana", Positions: []uint64{64}},
		{Token: "cherry", Positions: []uint64{192, 256}},
	}

	reader := NewTokenIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Token)
		if !ok {
			t.Fatalf("expected to find token %q", e.Token)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("token %q: expected %d positions, got %d", e.Token, len(e.Positions), len(positions))
		}
		for i, p := range positions {
			if p != e.Positions[i] {
				t.Fatalf("token %q pos %d: expected %d, got %d", e.Token, i, e.Positions[i], p)
			}
		}
	}
}

func TestTokenLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "error", Positions: []uint64{0}},
		{Token: "warning", Positions: []uint64{64}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("info")
	if ok {
		t.Fatalf("expected ok=false for missing token, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

func TestTokenLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewTokenIndexReader(id, nil)

	positions, ok := reader.Lookup("anything")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

func TestTokenLookupSingleEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "error", Positions: []uint64{42, 84}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("error")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(positions) != 2 || positions[0] != 42 || positions[1] != 84 {
		t.Fatalf("expected [42 84], got %v", positions)
	}
}

func TestTokenLookupFirstEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "aaa", Positions: []uint64{0}},
		{Token: "bbb", Positions: []uint64{64}},
		{Token: "ccc", Positions: []uint64{128}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("aaa")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(positions) != 1 || positions[0] != 0 {
		t.Fatalf("expected [0], got %v", positions)
	}
}

func TestTokenLookupLastEntry(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "aaa", Positions: []uint64{0}},
		{Token: "bbb", Positions: []uint64{64}},
		{Token: "zzz", Positions: []uint64{128}},
	}

	reader := NewTokenIndexReader(id, entries)

	positions, ok := reader.Lookup("zzz")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(positions) != 1 || positions[0] != 128 {
		t.Fatalf("expected [128], got %v", positions)
	}
}

func TestTokenLookupCaseSensitive(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []TokenIndexEntry{
		{Token: "error", Positions: []uint64{0}},
	}

	reader := NewTokenIndexReader(id, entries)

	// Tokens are stored lowercase by the indexer, so uppercase lookup should fail.
	_, ok := reader.Lookup("ERROR")
	if ok {
		t.Fatal("expected ok=false for case mismatch")
	}

	// Lowercase should work.
	_, ok = reader.Lookup("error")
	if !ok {
		t.Fatal("expected ok=true for exact match")
	}
}

// AttrKeyIndexReader tests

func TestAttrKeyLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []AttrKeyIndexEntry{
		{Key: "env", Positions: []uint64{0, 128}},
		{Key: "host", Positions: []uint64{64}},
		{Key: "service", Positions: []uint64{192, 256}},
	}

	reader := NewAttrKeyIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Key)
		if !ok {
			t.Fatalf("expected to find key %q", e.Key)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("key %q: expected %d positions, got %d", e.Key, len(e.Positions), len(positions))
		}
		for i, p := range positions {
			if p != e.Positions[i] {
				t.Fatalf("key %q pos %d: expected %d, got %d", e.Key, i, e.Positions[i], p)
			}
		}
	}
}

func TestAttrKeyLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []AttrKeyIndexEntry{
		{Key: "env", Positions: []uint64{0}},
		{Key: "host", Positions: []uint64{64}},
	}

	reader := NewAttrKeyIndexReader(id, entries)

	positions, ok := reader.Lookup("service")
	if ok {
		t.Fatalf("expected ok=false for missing key, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

func TestAttrKeyLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewAttrKeyIndexReader(id, nil)

	positions, ok := reader.Lookup("anything")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
	if positions != nil {
		t.Fatalf("expected nil positions, got %v", positions)
	}
}

// AttrValueIndexReader tests

func TestAttrValueLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []AttrValueIndexEntry{
		{Value: "dev", Positions: []uint64{0}},
		{Value: "prod", Positions: []uint64{64, 128}},
		{Value: "staging", Positions: []uint64{192}},
	}

	reader := NewAttrValueIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Value)
		if !ok {
			t.Fatalf("expected to find value %q", e.Value)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("value %q: expected %d positions, got %d", e.Value, len(e.Positions), len(positions))
		}
	}
}

func TestAttrValueLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []AttrValueIndexEntry{
		{Value: "prod", Positions: []uint64{0}},
	}

	reader := NewAttrValueIndexReader(id, entries)

	positions, ok := reader.Lookup("dev")
	if ok {
		t.Fatalf("expected ok=false for missing value, got positions %v", positions)
	}
}

func TestAttrValueLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewAttrValueIndexReader(id, nil)

	positions, ok := reader.Lookup("anything")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
}

// AttrKVIndexReader tests

func TestAttrKVLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []AttrKVIndexEntry{
		{Key: "env", Value: "dev", Positions: []uint64{0}},
		{Key: "env", Value: "prod", Positions: []uint64{64, 128}},
		{Key: "host", Value: "server1", Positions: []uint64{192}},
	}

	reader := NewAttrKVIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Key, e.Value)
		if !ok {
			t.Fatalf("expected to find key=%q value=%q", e.Key, e.Value)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("kv %q=%q: expected %d positions, got %d", e.Key, e.Value, len(e.Positions), len(positions))
		}
	}
}

func TestAttrKVLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []AttrKVIndexEntry{
		{Key: "env", Value: "prod", Positions: []uint64{0}},
	}

	reader := NewAttrKVIndexReader(id, entries)

	// Key exists but value doesn't.
	positions, ok := reader.Lookup("env", "dev")
	if ok {
		t.Fatalf("expected ok=false for missing kv, got positions %v", positions)
	}

	// Neither key nor value exists.
	positions, ok = reader.Lookup("host", "server1")
	if ok {
		t.Fatalf("expected ok=false for missing kv, got positions %v", positions)
	}
}

func TestAttrKVLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewAttrKVIndexReader(id, nil)

	positions, ok := reader.Lookup("env", "prod")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
}

func TestAttrKVLookupSortedByKeyThenValue(t *testing.T) {
	id := chunk.NewChunkID()
	// Entries must be sorted by (Key, Value) for binary search.
	entries := []AttrKVIndexEntry{
		{Key: "a", Value: "x", Positions: []uint64{0}},
		{Key: "a", Value: "y", Positions: []uint64{64}},
		{Key: "b", Value: "x", Positions: []uint64{128}},
		{Key: "b", Value: "z", Positions: []uint64{192}},
	}

	reader := NewAttrKVIndexReader(id, entries)

	// Test all combinations.
	tests := []struct {
		key, value string
		wantPos    uint64
		wantOK     bool
	}{
		{"a", "x", 0, true},
		{"a", "y", 64, true},
		{"a", "z", 0, false},
		{"b", "x", 128, true},
		{"b", "y", 0, false},
		{"b", "z", 192, true},
		{"c", "x", 0, false},
	}

	for _, tc := range tests {
		positions, ok := reader.Lookup(tc.key, tc.value)
		if ok != tc.wantOK {
			t.Errorf("Lookup(%q, %q): expected ok=%v, got ok=%v", tc.key, tc.value, tc.wantOK, ok)
			continue
		}
		if ok && (len(positions) != 1 || positions[0] != tc.wantPos) {
			t.Errorf("Lookup(%q, %q): expected pos %d, got %v", tc.key, tc.value, tc.wantPos, positions)
		}
	}
}

// KVKeyIndexReader tests

func TestKVKeyLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVKeyIndexEntry{
		{Key: "level", Positions: []uint64{0, 128}},
		{Key: "msg", Positions: []uint64{64}},
		{Key: "status", Positions: []uint64{192, 256}},
	}

	reader := NewKVKeyIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Key)
		if !ok {
			t.Fatalf("expected to find key %q", e.Key)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("key %q: expected %d positions, got %d", e.Key, len(e.Positions), len(positions))
		}
	}
}

func TestKVKeyLookupCaseInsensitive(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVKeyIndexEntry{
		{Key: "level", Positions: []uint64{0}}, // stored lowercase
	}

	reader := NewKVKeyIndexReader(id, entries)

	// All case variants should match.
	for _, key := range []string{"level", "LEVEL", "Level", "LeVeL"} {
		positions, ok := reader.Lookup(key)
		if !ok {
			t.Errorf("expected to find key %q (case insensitive)", key)
		}
		if len(positions) != 1 || positions[0] != 0 {
			t.Errorf("key %q: expected [0], got %v", key, positions)
		}
	}
}

func TestKVKeyLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVKeyIndexEntry{
		{Key: "level", Positions: []uint64{0}},
	}

	reader := NewKVKeyIndexReader(id, entries)

	positions, ok := reader.Lookup("status")
	if ok {
		t.Fatalf("expected ok=false for missing key, got positions %v", positions)
	}
}

func TestKVKeyLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewKVKeyIndexReader(id, nil)

	positions, ok := reader.Lookup("anything")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
}

// KVValueIndexReader tests

func TestKVValueLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVValueIndexEntry{
		{Value: "error", Positions: []uint64{0, 128}},
		{Value: "info", Positions: []uint64{64}},
		{Value: "warning", Positions: []uint64{192}},
	}

	reader := NewKVValueIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Value)
		if !ok {
			t.Fatalf("expected to find value %q", e.Value)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("value %q: expected %d positions, got %d", e.Value, len(e.Positions), len(positions))
		}
	}
}

func TestKVValueLookupCaseInsensitive(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVValueIndexEntry{
		{Value: "error", Positions: []uint64{0}}, // stored as-is (may be mixed case)
	}

	reader := NewKVValueIndexReader(id, entries)

	// All case variants should match.
	for _, value := range []string{"error", "ERROR", "Error"} {
		positions, ok := reader.Lookup(value)
		if !ok {
			t.Errorf("expected to find value %q (case insensitive)", value)
		}
		if len(positions) != 1 || positions[0] != 0 {
			t.Errorf("value %q: expected [0], got %v", value, positions)
		}
	}
}

func TestKVValueLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVValueIndexEntry{
		{Value: "error", Positions: []uint64{0}},
	}

	reader := NewKVValueIndexReader(id, entries)

	positions, ok := reader.Lookup("info")
	if ok {
		t.Fatalf("expected ok=false for missing value, got positions %v", positions)
	}
}

func TestKVValueLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewKVValueIndexReader(id, nil)

	positions, ok := reader.Lookup("anything")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
}

// KVIndexReader tests

func TestKVLookupFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVIndexEntry{
		{Key: "level", Value: "error", Positions: []uint64{0, 128}},
		{Key: "level", Value: "info", Positions: []uint64{64}},
		{Key: "status", Value: "200", Positions: []uint64{192}},
	}

	reader := NewKVIndexReader(id, entries)

	for _, e := range entries {
		positions, ok := reader.Lookup(e.Key, e.Value)
		if !ok {
			t.Fatalf("expected to find kv %q=%q", e.Key, e.Value)
		}
		if len(positions) != len(e.Positions) {
			t.Fatalf("kv %q=%q: expected %d positions, got %d", e.Key, e.Value, len(e.Positions), len(positions))
		}
	}
}

func TestKVLookupCaseInsensitive(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVIndexEntry{
		{Key: "level", Value: "error", Positions: []uint64{0}}, // stored lowercase
	}

	reader := NewKVIndexReader(id, entries)

	// All case variants should match.
	cases := []struct{ key, value string }{
		{"level", "error"},
		{"LEVEL", "ERROR"},
		{"Level", "Error"},
		{"LeVeL", "ErRoR"},
	}
	for _, tc := range cases {
		positions, ok := reader.Lookup(tc.key, tc.value)
		if !ok {
			t.Errorf("expected to find kv %q=%q (case insensitive)", tc.key, tc.value)
		}
		if len(positions) != 1 || positions[0] != 0 {
			t.Errorf("kv %q=%q: expected [0], got %v", tc.key, tc.value, positions)
		}
	}
}

func TestKVLookupNotFound(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVIndexEntry{
		{Key: "level", Value: "error", Positions: []uint64{0}},
	}

	reader := NewKVIndexReader(id, entries)

	// Key exists but value doesn't.
	positions, ok := reader.Lookup("level", "info")
	if ok {
		t.Fatalf("expected ok=false for missing kv, got positions %v", positions)
	}

	// Neither key nor value exists.
	positions, ok = reader.Lookup("status", "200")
	if ok {
		t.Fatalf("expected ok=false for missing kv, got positions %v", positions)
	}
}

func TestKVLookupEmptyIndex(t *testing.T) {
	id := chunk.NewChunkID()
	reader := NewKVIndexReader(id, nil)

	positions, ok := reader.Lookup("level", "error")
	if ok {
		t.Fatalf("expected ok=false for empty index, got positions %v", positions)
	}
}

func TestKVEntries(t *testing.T) {
	id := chunk.NewChunkID()
	entries := []KVIndexEntry{
		{Key: "level", Value: "error", Positions: []uint64{0}},
		{Key: "level", Value: "info", Positions: []uint64{64}},
	}

	reader := NewKVIndexReader(id, entries)

	got := reader.Entries()
	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
	for i, e := range got {
		if e.Key != entries[i].Key || e.Value != entries[i].Value {
			t.Errorf("entry %d: expected %q=%q, got %q=%q", i, entries[i].Key, entries[i].Value, e.Key, e.Value)
		}
	}
}
