package chunk

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestStringDictAddAndLookup(t *testing.T) {
	d := NewStringDict()

	id0, err := d.Add("host")
	if err != nil {
		t.Fatalf("add host: %v", err)
	}
	if id0 != 0 {
		t.Fatalf("first entry should get ID 0, got %d", id0)
	}

	id1, err := d.Add("service")
	if err != nil {
		t.Fatalf("add service: %v", err)
	}
	if id1 != 1 {
		t.Fatalf("second entry should get ID 1, got %d", id1)
	}

	if d.Len() != 2 {
		t.Fatalf("expected len 2, got %d", d.Len())
	}

	got, ok := d.Lookup("host")
	if !ok || got != 0 {
		t.Fatalf("lookup host: ok=%v id=%d", ok, got)
	}

	got, ok = d.Lookup("service")
	if !ok || got != 1 {
		t.Fatalf("lookup service: ok=%v id=%d", ok, got)
	}

	_, ok = d.Lookup("missing")
	if ok {
		t.Fatal("lookup missing: should return false")
	}
}

func TestStringDictIdempotentAdd(t *testing.T) {
	d := NewStringDict()

	id1, err := d.Add("host")
	if err != nil {
		t.Fatalf("add: %v", err)
	}

	id2, err := d.Add("host")
	if err != nil {
		t.Fatalf("add again: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("idempotent add: want %d, got %d", id1, id2)
	}

	if d.Len() != 1 {
		t.Fatalf("expected len 1 after duplicate add, got %d", d.Len())
	}
}

func TestStringDictGet(t *testing.T) {
	d := NewStringDict()
	d.Add("alpha")
	d.Add("beta")

	s, err := d.Get(0)
	if err != nil || s != "alpha" {
		t.Fatalf("Get(0): %q, %v", s, err)
	}

	s, err = d.Get(1)
	if err != nil || s != "beta" {
		t.Fatalf("Get(1): %q, %v", s, err)
	}

	_, err = d.Get(2)
	if err != ErrDictEntryNotFound {
		t.Fatalf("Get(2): expected ErrDictEntryNotFound, got %v", err)
	}
}

func TestStringDictLen(t *testing.T) {
	d := NewStringDict()
	if d.Len() != 0 {
		t.Fatalf("empty dict len: %d", d.Len())
	}

	d.Add("a")
	d.Add("b")
	d.Add("a") // duplicate
	if d.Len() != 2 {
		t.Fatalf("expected 2, got %d", d.Len())
	}
}

func TestStringDictSharedKeyValueNamespace(t *testing.T) {
	// Keys and values share the same ID space. If a string appears as both
	// a key and a value, it gets the same ID.
	d := NewStringDict()

	id1, _ := d.Add("host")     // used as key
	id2, _ := d.Add("srv-001")  // used as value
	id3, _ := d.Add("host")     // same string, used as value this time

	if id1 != id3 {
		t.Fatalf("same string should get same ID: %d vs %d", id1, id3)
	}
	if id1 == id2 {
		t.Fatal("different strings should get different IDs")
	}
	if d.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", d.Len())
	}
}

func TestEncodeDictEntry(t *testing.T) {
	entry := EncodeDictEntry("host")

	// Expected: [4, 0, 'h', 'o', 's', 't']
	if len(entry) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(entry))
	}
	strLen := binary.LittleEndian.Uint16(entry[0:2])
	if strLen != 4 {
		t.Fatalf("strLen: want 4, got %d", strLen)
	}
	if string(entry[2:]) != "host" {
		t.Fatalf("string: want 'host', got %q", string(entry[2:]))
	}
}

func TestEncodeDictEntryEmpty(t *testing.T) {
	entry := EncodeDictEntry("")
	if len(entry) != 2 {
		t.Fatalf("expected 2 bytes for empty string, got %d", len(entry))
	}
	strLen := binary.LittleEndian.Uint16(entry[0:2])
	if strLen != 0 {
		t.Fatalf("strLen: want 0, got %d", strLen)
	}
}

func TestEncodeDecodeWithDictRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		attrs Attributes
	}{
		{"empty", Attributes{}},
		{"single", Attributes{"key": "value"}},
		{"multiple", Attributes{"host": "srv-001", "env": "prod", "service": "api"}},
		{"empty_value", Attributes{"key": ""}},
		{"empty_key", Attributes{"": "value"}},
		{"unicode", Attributes{"host": "srv-日本語", "env": "测试"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dict := NewStringDict()
			encoded, _, err := EncodeWithDict(tc.attrs, dict)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			decoded, err := DecodeWithDict(encoded, dict)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			if len(decoded) != len(tc.attrs) {
				t.Fatalf("length mismatch: want %d, got %d", len(tc.attrs), len(decoded))
			}
			for k, v := range tc.attrs {
				if decoded[k] != v {
					t.Fatalf("value mismatch for %q: want %q, got %q", k, v, decoded[k])
				}
			}
		})
	}
}

func TestEncodeWithDictNewEntries(t *testing.T) {
	dict := NewStringDict()

	// First encode: all keys and values are new.
	attrs1 := Attributes{"host": "srv-001", "env": "prod"}
	_, new1, err := EncodeWithDict(attrs1, dict)
	if err != nil {
		t.Fatalf("encode 1: %v", err)
	}
	// 4 new entries: "env", "prod", "host", "srv-001" (keys sorted, so env first)
	if len(new1) != 4 {
		t.Fatalf("expected 4 new entries, got %d: %v", len(new1), new1)
	}

	// Second encode with same keys but different values: only new values.
	attrs2 := Attributes{"host": "srv-002", "env": "staging"}
	_, new2, err := EncodeWithDict(attrs2, dict)
	if err != nil {
		t.Fatalf("encode 2: %v", err)
	}
	// 2 new entries: "staging" and "srv-002" (keys already known)
	if len(new2) != 2 {
		t.Fatalf("expected 2 new entries, got %d: %v", len(new2), new2)
	}

	// Third encode with all known strings: no new entries.
	attrs3 := Attributes{"host": "srv-001", "env": "prod"}
	_, new3, err := EncodeWithDict(attrs3, dict)
	if err != nil {
		t.Fatalf("encode 3: %v", err)
	}
	if len(new3) != 0 {
		t.Fatalf("expected 0 new entries, got %d: %v", len(new3), new3)
	}
}

func TestEncodeWithDictSmallerThanPlain(t *testing.T) {
	attrs := Attributes{
		"host":    "server-001.example.com",
		"service": "api-gateway",
		"env":     "production",
		"region":  "us-east-1",
		"version": "1.2.3",
	}

	// Plain encoding.
	plain, err := attrs.Encode()
	if err != nil {
		t.Fatalf("plain encode: %v", err)
	}

	// Dict encoding.
	dict := NewStringDict()
	dictEncoded, _, err := EncodeWithDict(attrs, dict)
	if err != nil {
		t.Fatalf("dict encode: %v", err)
	}

	if len(dictEncoded) >= len(plain) {
		t.Fatalf("dict encoding (%d) should be smaller than plain (%d)", len(dictEncoded), len(plain))
	}
}

func TestEncodeWithDictFixedSize(t *testing.T) {
	// With dict encoding, the encoded size is always 2 + count*8,
	// regardless of key/value string lengths.
	dict := NewStringDict()

	small := Attributes{"k": "v"}
	enc1, _, _ := EncodeWithDict(small, dict)
	if len(enc1) != 2+1*8 {
		t.Fatalf("single attr: want %d bytes, got %d", 2+1*8, len(enc1))
	}

	big := Attributes{"very-long-key-name": "very-long-value-string-here"}
	enc2, _, _ := EncodeWithDict(big, dict)
	if len(enc2) != 2+1*8 {
		t.Fatalf("single attr (long strings): want %d bytes, got %d", 2+1*8, len(enc2))
	}
}

func TestDecodeDictDataRoundTrip(t *testing.T) {
	// Build dict data manually.
	var buf []byte
	entries := []string{"host", "service", "env", "level", "prod", "srv-001"}
	for _, s := range entries {
		buf = append(buf, EncodeDictEntry(s)...)
	}

	dict, err := DecodeDictData(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if dict.Len() != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), dict.Len())
	}

	for i, s := range entries {
		id, ok := dict.Lookup(s)
		if !ok {
			t.Fatalf("entry %q not found", s)
		}
		if int(id) != i {
			t.Fatalf("entry %q: want ID %d, got %d", s, i, id)
		}
	}
}

func TestDecodeDictDataPartialEntry(t *testing.T) {
	// Complete entry for "host" + partial trailing entry.
	var buf []byte
	buf = append(buf, EncodeDictEntry("host")...)

	// Add partial entry: strLen says 10 but only 3 bytes follow.
	partial := make([]byte, 2)
	binary.LittleEndian.PutUint16(partial, 10)
	buf = append(buf, partial...)
	buf = append(buf, 'a', 'b', 'c') // Only 3 of 10 bytes.

	dict, err := DecodeDictData(buf)
	if err != nil {
		t.Fatalf("decode with partial: %v", err)
	}

	if dict.Len() != 1 {
		t.Fatalf("expected 1 entry (partial ignored), got %d", dict.Len())
	}

	s, err := dict.Get(0)
	if err != nil || s != "host" {
		t.Fatalf("Get(0): %q, %v", s, err)
	}
}

func TestDecodeDictDataPartialStrLen(t *testing.T) {
	// Complete entry + only 1 byte of next strLen (needs 2).
	var buf []byte
	buf = append(buf, EncodeDictEntry("ok")...)
	buf = append(buf, 0x05) // Only 1 of 2 bytes for strLen.

	dict, err := DecodeDictData(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dict.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", dict.Len())
	}
}

func TestDecodeDictDataEmpty(t *testing.T) {
	dict, err := DecodeDictData(nil)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if dict.Len() != 0 {
		t.Fatalf("expected 0 entries, got %d", dict.Len())
	}
}

func TestEncodeDecodeWithDictBinaryFormat(t *testing.T) {
	dict := NewStringDict()
	attrs := Attributes{"ab": "cd"}
	encoded, _, err := EncodeWithDict(attrs, dict)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Expected format:
	// [0:2]   count = 1
	// [2:6]   keyID = 0 (first entry: "ab")
	// [6:10]  valID = 1 (second entry: "cd")
	expected := []byte{
		0x01, 0x00,             // count = 1
		0x00, 0x00, 0x00, 0x00, // keyID = 0
		0x01, 0x00, 0x00, 0x00, // valID = 1
	}

	if !bytes.Equal(encoded, expected) {
		t.Fatalf("binary format mismatch:\nwant: %v\ngot:  %v", expected, encoded)
	}
}

func TestEncodeWithDictEmptyAttrs(t *testing.T) {
	dict := NewStringDict()
	encoded, newEntries, err := EncodeWithDict(Attributes{}, dict)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(newEntries) != 0 {
		t.Fatalf("expected no new entries, got %v", newEntries)
	}
	if !bytes.Equal(encoded, []byte{0, 0}) {
		t.Fatalf("expected [0,0], got %v", encoded)
	}
}

func TestDecodeWithDictInvalidData(t *testing.T) {
	dict := NewStringDict()
	dict.Add("key")
	dict.Add("val")

	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"one_byte", []byte{0x00}},
		{"count_but_no_data", []byte{0x01, 0x00}},
		{"truncated_val_id", []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00}}, // count=1, keyID=0, no valID
		{"bad_key_id", []byte{0x01, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}}, // keyID=5 doesn't exist
		{"bad_val_id", []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00}}, // valID=5 doesn't exist
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeWithDict(tc.data, dict)
			if err != ErrInvalidAttrsData {
				t.Fatalf("expected ErrInvalidAttrsData, got %v", err)
			}
		})
	}
}

func TestDecodeWithDictZeroCount(t *testing.T) {
	dict := NewStringDict()
	attrs, err := DecodeWithDict([]byte{0x00, 0x00}, dict)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(attrs) != 0 {
		t.Fatalf("expected empty attrs, got %d entries", len(attrs))
	}
}

func TestEncodeWithDictValueDedup(t *testing.T) {
	// When multiple records share the same values, the dict deduplicates them.
	dict := NewStringDict()

	attrs1 := Attributes{"env": "prod", "service": "api"}
	_, new1, _ := EncodeWithDict(attrs1, dict)
	// 4 new: "env", "prod", "service", "api"
	if len(new1) != 4 {
		t.Fatalf("expected 4 new, got %d", len(new1))
	}

	attrs2 := Attributes{"env": "prod", "service": "web"}
	_, new2, _ := EncodeWithDict(attrs2, dict)
	// Only 1 new: "web" (env, prod, service already known)
	if len(new2) != 1 {
		t.Fatalf("expected 1 new, got %d: %v", len(new2), new2)
	}
	if new2[0] != "web" {
		t.Fatalf("expected 'web', got %q", new2[0])
	}
}
