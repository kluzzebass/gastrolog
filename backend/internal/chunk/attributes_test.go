package chunk

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// =============================================================================
// Basic Encoding Tests
// =============================================================================

func TestAttributesEncodeEmpty(t *testing.T) {
	attrs := Attributes{}
	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}

	// Empty attrs should be exactly 2 bytes (count = 0)
	if len(encoded) != 2 {
		t.Fatalf("expected 2 bytes for empty attrs, got %d", len(encoded))
	}

	count := binary.LittleEndian.Uint16(encoded[0:2])
	if count != 0 {
		t.Fatalf("expected count 0, got %d", count)
	}
}

func TestAttributesEncodeNil(t *testing.T) {
	var attrs Attributes // nil
	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}

	if len(encoded) != 2 {
		t.Fatalf("expected 2 bytes for nil attrs, got %d", len(encoded))
	}

	count := binary.LittleEndian.Uint16(encoded[0:2])
	if count != 0 {
		t.Fatalf("expected count 0, got %d", count)
	}
}

func TestAttributesEncodeSinglePair(t *testing.T) {
	attrs := Attributes{"key": "value"}
	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Expected size: 2 (count) + 2 (keyLen) + 3 (key) + 2 (valLen) + 5 (val) = 14
	expectedSize := 2 + 2 + 3 + 2 + 5
	if len(encoded) != expectedSize {
		t.Fatalf("expected %d bytes, got %d", expectedSize, len(encoded))
	}

	count := binary.LittleEndian.Uint16(encoded[0:2])
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

func TestAttributesEncodeSortedKeys(t *testing.T) {
	// Test that keys are sorted lexicographically for deterministic output
	attrs := Attributes{
		"zebra":     "z",
		"alpha":     "a",
		"middle":    "m",
		"123":       "num",
		"UPPER":     "up",
		"_under":    "u",
		"zzz":       "end",
		"aaardvark": "start",
	}

	// Encode twice, should get identical output
	enc1, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode 1: %v", err)
	}

	enc2, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode 2: %v", err)
	}

	if !bytes.Equal(enc1, enc2) {
		t.Fatal("encoding is not deterministic")
	}

	// Verify count matches number of keys
	count := binary.LittleEndian.Uint16(enc1[0:2])
	if int(count) != len(attrs) {
		t.Fatalf("expected count %d, got %d", len(attrs), count)
	}
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestAttributesEncodeTooLarge(t *testing.T) {
	// Create attributes that exceed 65535 bytes
	// A single key-value pair with 32KB key and 32KB value would be > 64KB
	attrs := Attributes{
		strings.Repeat("k", 32768): strings.Repeat("v", 32768),
	}

	_, err := attrs.Encode()
	if err != ErrAttrsTooLarge {
		t.Fatalf("expected ErrAttrsTooLarge, got %v", err)
	}
}

// =============================================================================
// Copy Tests
// =============================================================================

func TestAttributesCopy(t *testing.T) {
	original := Attributes{"a": "1", "b": "2"}
	copied := original.Copy()

	// Modify original
	original["a"] = "modified"
	original["c"] = "3"

	// Copied should be unchanged
	if copied["a"] != "1" {
		t.Fatalf("copy was modified: a=%q", copied["a"])
	}
	if _, ok := copied["c"]; ok {
		t.Fatal("copy has key 'c' that was added after copy")
	}
}

func TestAttributesCopyNil(t *testing.T) {
	var attrs Attributes
	copied := attrs.Copy()
	if copied != nil {
		t.Fatalf("expected nil copy of nil attrs, got %v", copied)
	}
}

func TestAttributesCopyEmpty(t *testing.T) {
	attrs := Attributes{}
	copied := attrs.Copy()
	if copied == nil {
		t.Fatal("copy of empty attrs should not be nil")
	}
	if len(copied) != 0 {
		t.Fatalf("expected empty copy, got %d entries", len(copied))
	}
}

// =============================================================================
// Binary Format Verification Tests
// =============================================================================

func TestAttributesBinaryFormat(t *testing.T) {
	// Verify exact binary format for known input
	attrs := Attributes{"ab": "cd"}

	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Expected format:
	// [0:2]   count = 1 (little-endian)
	// [2:4]   keyLen = 2 (little-endian)
	// [4:6]   key = "ab"
	// [6:8]   valLen = 2 (little-endian)
	// [8:10]  val = "cd"
	expected := []byte{
		0x01, 0x00, // count = 1
		0x02, 0x00, // keyLen = 2
		'a', 'b', // key
		0x02, 0x00, // valLen = 2
		'c', 'd', // val
	}

	if !bytes.Equal(encoded, expected) {
		t.Fatalf("binary format mismatch:\nwant: %v\ngot:  %v", expected, encoded)
	}
}

func TestAttributesBinaryFormatMultiple(t *testing.T) {
	// Test with multiple pairs - keys should be sorted
	attrs := Attributes{"b": "2", "a": "1"}

	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Expected format (keys sorted: "a" before "b"):
	// [0:2]   count = 2
	// [2:4]   keyLen = 1
	// [4]     key = "a"
	// [5:7]   valLen = 1
	// [7]     val = "1"
	// [8:10]  keyLen = 1
	// [10]    key = "b"
	// [11:13] valLen = 1
	// [13]    val = "2"
	expected := []byte{
		0x02, 0x00, // count = 2
		0x01, 0x00, // keyLen = 1
		'a',        // key
		0x01, 0x00, // valLen = 1
		'1',        // val
		0x01, 0x00, // keyLen = 1
		'b',        // key
		0x01, 0x00, // valLen = 1
		'2', // val
	}

	if !bytes.Equal(encoded, expected) {
		t.Fatalf("binary format mismatch:\nwant: %v\ngot:  %v", expected, encoded)
	}
}

// =============================================================================
// Performance Benchmarks
// =============================================================================

func BenchmarkAttributesEncode(b *testing.B) {
	attrs := Attributes{
		"host":    "srv-001.example.com",
		"service": "api-gateway",
		"env":     "production",
		"region":  "us-east-1",
		"version": "1.2.3",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = attrs.Encode()
	}
}

func BenchmarkAttributesEncodeLarge(b *testing.B) {
	attrs := make(Attributes)
	for i := range 50 {
		key := strings.Repeat("key", 5) + string(rune('A'+i%26))
		attrs[key] = strings.Repeat("value", 20)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = attrs.Encode()
	}
}
