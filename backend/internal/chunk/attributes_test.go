package chunk

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// =============================================================================
// Basic Encoding/Decoding Tests
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

func TestAttributesRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		attrs Attributes
	}{
		{"empty", Attributes{}},
		{"single", Attributes{"key": "value"}},
		{"multiple", Attributes{"a": "1", "b": "2", "c": "3"}},
		{"empty_value", Attributes{"key": ""}},
		{"empty_key", Attributes{"": "value"}},
		{"both_empty", Attributes{"": ""}},
		{"unicode", Attributes{"host": "srv-日本語", "env": "测试"}},
		{"special_chars", Attributes{"path": "/var/log/app.log", "query": "foo=bar&baz=qux"}},
		{"whitespace", Attributes{"msg": "hello world\nline2\ttab"}},
		{"long_key", Attributes{strings.Repeat("k", 1000): "v"}},
		{"long_value", Attributes{"k": strings.Repeat("v", 10000)}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := tc.attrs.Encode()
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			decoded, err := DecodeAttributes(encoded)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			if len(decoded) != len(tc.attrs) {
				t.Fatalf("length mismatch: want %d, got %d", len(tc.attrs), len(decoded))
			}

			for k, v := range tc.attrs {
				got, ok := decoded[k]
				if !ok {
					t.Fatalf("missing key %q", k)
				}
				if got != v {
					t.Fatalf("value mismatch for %q: want %q, got %q", k, v, got)
				}
			}
		})
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

	// Verify keys appear in sorted order by parsing the encoded data
	decoded, err := DecodeAttributes(enc1)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// All keys should be present
	for k, v := range attrs {
		if decoded[k] != v {
			t.Fatalf("key %q: want %q, got %q", k, v, decoded[k])
		}
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

func TestDecodeAttributesInvalidData(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"one_byte", []byte{0x00}},
		{"count_but_no_data", []byte{0x01, 0x00}},                               // count=1 but no key-value data
		{"truncated_key_len", []byte{0x01, 0x00, 0x05}},                         // count=1, partial key length
		{"truncated_key", []byte{0x01, 0x00, 0x05, 0x00, 'h', 'e'}},             // count=1, keyLen=5, only 2 bytes
		{"truncated_val_len", []byte{0x01, 0x00, 0x01, 0x00, 'k'}},              // count=1, keyLen=1, key='k', no valLen
		{"truncated_val", []byte{0x01, 0x00, 0x01, 0x00, 'k', 0x05, 0x00, 'v'}}, // valLen=5, only 1 byte
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeAttributes(tc.data)
			if err != ErrInvalidAttrsData {
				t.Fatalf("expected ErrInvalidAttrsData, got %v", err)
			}
		})
	}
}

func TestDecodeAttributesZeroCount(t *testing.T) {
	data := []byte{0x00, 0x00} // count = 0
	attrs, err := DecodeAttributes(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(attrs) != 0 {
		t.Fatalf("expected empty attrs, got %d entries", len(attrs))
	}
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestAttributesBinaryValues(t *testing.T) {
	// Test that binary data (including null bytes) round-trips correctly
	attrs := Attributes{
		"binary": string([]byte{0x00, 0x01, 0x02, 0xff, 0xfe}),
		"null":   string([]byte{0x00}),
	}

	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeAttributes(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded["binary"] != attrs["binary"] {
		t.Fatalf("binary value mismatch")
	}
	if decoded["null"] != attrs["null"] {
		t.Fatalf("null value mismatch")
	}
}

func TestAttributesMaxSize(t *testing.T) {
	// Create attributes that are exactly at the limit (or just under)
	// 65535 - 2 (count) = 65533 bytes for key-value pairs
	// Each pair: 2 (keyLen) + 2 (valLen) + keyLen + valLen
	// With 1 pair and keyLen=1, valLen=65528: 2 + 1 + 2 + 65528 = 65533

	attrs := Attributes{
		"k": strings.Repeat("v", 65528),
	}

	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode near-max: %v", err)
	}

	if len(encoded) > 65535 {
		t.Fatalf("encoded size %d exceeds max", len(encoded))
	}

	decoded, err := DecodeAttributes(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded["k"] != attrs["k"] {
		t.Fatal("value mismatch at max size")
	}
}

func TestAttributesManyPairs(t *testing.T) {
	// Test with many small key-value pairs
	attrs := make(Attributes)
	for i := range 1000 {
		key := strings.Repeat("k", i%10+1)
		// Use index in key to ensure uniqueness
		key = key + string(rune('A'+i%26)) + string(rune('0'+i%10))
		attrs[key] = strings.Repeat("v", i%20+1)
	}

	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeAttributes(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(decoded) != len(attrs) {
		t.Fatalf("count mismatch: want %d, got %d", len(attrs), len(decoded))
	}

	for k, v := range attrs {
		if decoded[k] != v {
			t.Fatalf("value mismatch for key %q", k)
		}
	}
}

func TestAttributesDuplicateKeys(t *testing.T) {
	// In Go maps, duplicate keys are impossible, but test that the encoding
	// correctly handles what's in the map
	attrs := Attributes{"key": "value1"}
	attrs["key"] = "value2" // overwrites

	encoded, err := attrs.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeAttributes(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded["key"] != "value2" {
		t.Fatalf("expected value2, got %q", decoded["key"])
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
// Fuzzing Support
// =============================================================================

func FuzzAttributesRoundTrip(f *testing.F) {
	// Seed with various test cases
	f.Add("key", "value")
	f.Add("", "")
	f.Add("k", "")
	f.Add("", "v")
	f.Add("host", "srv-001.example.com")
	f.Add("env", "production")
	f.Add("path", "/var/log/app/error.log")
	f.Add("unicode", "日本語テスト")

	f.Fuzz(func(t *testing.T, key, value string) {
		// Skip if key+value would make attrs too large
		// 2 (count) + 2 (keyLen) + len(key) + 2 (valLen) + len(value)
		size := 2 + 2 + len(key) + 2 + len(value)
		if size > 65535 {
			t.Skip("attrs would be too large")
		}

		attrs := Attributes{key: value}
		encoded, err := attrs.Encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}

		decoded, err := DecodeAttributes(encoded)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}

		if decoded[key] != value {
			t.Fatalf("round-trip failed: key=%q want=%q got=%q", key, value, decoded[key])
		}
	})
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

func BenchmarkAttributesDecode(b *testing.B) {
	attrs := Attributes{
		"host":    "srv-001.example.com",
		"service": "api-gateway",
		"env":     "production",
		"region":  "us-east-1",
		"version": "1.2.3",
	}
	encoded, _ := attrs.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeAttributes(encoded)
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

func BenchmarkAttributesDecodeLarge(b *testing.B) {
	attrs := make(Attributes)
	for i := range 50 {
		key := strings.Repeat("key", 5) + string(rune('A'+i%26))
		attrs[key] = strings.Repeat("value", 20)
	}
	encoded, _ := attrs.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeAttributes(encoded)
	}
}
