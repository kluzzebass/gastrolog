package lookup

import (
	"testing"

	"gastrolog/internal/format"
)

func TestBinFormat_RoundTrip(t *testing.T) {
	columns := []string{"env", "hostname"}
	rows := []binRow{
		{key: "10.0.0.1", values: []string{"prod", "web-1"}},
		{key: "10.0.0.2", values: []string{"staging", "db-1"}},
		{key: "10.0.0.3", values: []string{"dev", "test-1"}},
	}

	encoded, dups, err := encodeBinLookup(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if dups != 0 {
		t.Errorf("expected 0 duplicates, got %d", dups)
	}

	// Decode header.
	bd, err := decodeBinHeader(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if bd.numRows != 3 {
		t.Errorf("numRows = %d, want 3", bd.numRows)
	}
	if bd.numCols != 2 {
		t.Errorf("numCols = %d, want 2", bd.numCols)
	}

	// Decode columns.
	cols, err := decodeBinColumns(encoded, bd.numCols)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || cols[0] != "env" || cols[1] != "hostname" {
		t.Errorf("columns = %v, want [env hostname]", cols)
	}

	// Lookup each key.
	for _, r := range rows {
		result := bd.lookupKey(encoded, r.key, cols)
		if result == nil {
			t.Fatalf("lookupKey(%q) returned nil", r.key)
		}
		if result["env"] != r.values[0] {
			t.Errorf("lookupKey(%q)[env] = %q, want %q", r.key, result["env"], r.values[0])
		}
		if result["hostname"] != r.values[1] {
			t.Errorf("lookupKey(%q)[hostname] = %q, want %q", r.key, result["hostname"], r.values[1])
		}
	}

	// Miss returns nil.
	if bd.lookupKey(encoded, "99.99.99.99", cols) != nil {
		t.Error("expected nil for missing key")
	}
}

func TestBinFormat_DuplicateKeys(t *testing.T) {
	columns := []string{"val"}
	rows := []binRow{
		{key: "a", values: []string{"first"}},
		{key: "a", values: []string{"second"}},
		{key: "b", values: []string{"only"}},
	}

	encoded, dups, err := encodeBinLookup(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if dups != 1 {
		t.Errorf("expected 1 duplicate, got %d", dups)
	}

	bd, _ := decodeBinHeader(encoded)
	cols, _ := decodeBinColumns(encoded, bd.numCols)

	if bd.numRows != 2 {
		t.Errorf("numRows = %d, want 2 (after dedup)", bd.numRows)
	}

	// First occurrence wins.
	result := bd.lookupKey(encoded, "a", cols)
	if result == nil || result["val"] != "first" {
		t.Errorf("expected first occurrence, got %v", result)
	}
}

func TestBinFormat_SortedKeys(t *testing.T) {
	columns := []string{"v"}
	rows := []binRow{
		{key: "zebra", values: []string{"z"}},
		{key: "apple", values: []string{"a"}},
		{key: "mango", values: []string{"m"}},
	}

	encoded, _, err := encodeBinLookup(columns, rows)
	if err != nil {
		t.Fatal(err)
	}

	bd, _ := decodeBinHeader(encoded)
	cols, _ := decodeBinColumns(encoded, bd.numCols)

	// All keys should be findable regardless of insertion order.
	for _, r := range rows {
		result := bd.lookupKey(encoded, r.key, cols)
		if result == nil || result["v"] != r.values[0] {
			t.Errorf("lookupKey(%q) = %v, want %q", r.key, result, r.values[0])
		}
	}
}

func TestBinFormat_SingleRow(t *testing.T) {
	columns := []string{"v"}
	rows := []binRow{{key: "only", values: []string{"one"}}}

	encoded, _, err := encodeBinLookup(columns, rows)
	if err != nil {
		t.Fatal(err)
	}

	bd, _ := decodeBinHeader(encoded)
	cols, _ := decodeBinColumns(encoded, bd.numCols)

	result := bd.lookupKey(encoded, "only", cols)
	if result == nil || result["v"] != "one" {
		t.Errorf("got %v, want {v: one}", result)
	}
	if bd.lookupKey(encoded, "other", cols) != nil {
		t.Error("expected nil for missing key")
	}
}

func TestBinFormat_EmptyValues(t *testing.T) {
	columns := []string{"a", "b"}
	rows := []binRow{
		{key: "k", values: []string{"", ""}},
	}

	encoded, _, err := encodeBinLookup(columns, rows)
	if err != nil {
		t.Fatal(err)
	}

	bd, _ := decodeBinHeader(encoded)
	cols, _ := decodeBinColumns(encoded, bd.numCols)

	result := bd.lookupKey(encoded, "k", cols)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["a"] != "" || result["b"] != "" {
		t.Errorf("expected empty values, got %v", result)
	}
}

func TestBinFormat_EmptyRows(t *testing.T) {
	_, _, err := encodeBinLookup([]string{"v"}, nil)
	if err == nil {
		t.Error("expected error for empty rows")
	}
}

func TestBinFormat_CorruptHeader(t *testing.T) {
	// Too small.
	if _, err := decodeBinHeader([]byte{1, 2, 3}); err == nil {
		t.Error("expected error for truncated header")
	}

	// Wrong signature.
	bad := make([]byte, binHeaderSize)
	bad[0] = 0xFF
	if _, err := decodeBinHeader(bad); err == nil {
		t.Error("expected error for wrong signature")
	}

	// Wrong type.
	hdr := format.Header{Type: 'X', Version: binVersion, Flags: format.FlagComplete}
	buf := hdr.Encode()
	copy(bad, buf[:])
	if _, err := decodeBinHeader(bad); err == nil {
		t.Error("expected error for wrong type")
	}

	// Wrong version.
	hdr2 := format.Header{Type: format.TypeLookupTable, Version: 99, Flags: format.FlagComplete}
	buf2 := hdr2.Encode()
	copy(bad, buf2[:])
	if _, err := decodeBinHeader(bad); err == nil {
		t.Error("expected error for wrong version")
	}
}

func TestBinFormat_TruncatedColumnNames(t *testing.T) {
	// Valid header but truncated column name section.
	columns := []string{"hostname"}
	rows := []binRow{{key: "k", values: []string{"v"}}}
	encoded, _, _ := encodeBinLookup(columns, rows)

	// Truncate after header.
	truncated := encoded[:binHeaderSize+1]
	bd, err := decodeBinHeader(truncated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeBinColumns(truncated, bd.numCols); err == nil {
		t.Error("expected error for truncated column names")
	}
}

func FuzzBinFormatDecode(f *testing.F) {
	// Seed with a valid encoded file.
	columns := []string{"v"}
	rows := []binRow{{key: "k", values: []string{"val"}}}
	encoded, _, _ := encodeBinLookup(columns, rows)
	f.Add(encoded)

	f.Fuzz(func(t *testing.T, data []byte) {
		bd, err := decodeBinHeader(data)
		if err != nil {
			return
		}
		cols, err := decodeBinColumns(data, bd.numCols)
		if err != nil {
			return
		}
		// Should not panic on any input.
		bd.lookupKey(data, "test", cols)
	})
}
