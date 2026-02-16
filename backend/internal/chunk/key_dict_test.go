package chunk

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestKeyDictAddAndLookup(t *testing.T) {
	d := NewKeyDict()

	id0, err := d.Add("host")
	if err != nil {
		t.Fatalf("add host: %v", err)
	}
	if id0 != 0 {
		t.Fatalf("first key should get ID 0, got %d", id0)
	}

	id1, err := d.Add("service")
	if err != nil {
		t.Fatalf("add service: %v", err)
	}
	if id1 != 1 {
		t.Fatalf("second key should get ID 1, got %d", id1)
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

func TestKeyDictIdempotentAdd(t *testing.T) {
	d := NewKeyDict()

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

func TestKeyDictKey(t *testing.T) {
	d := NewKeyDict()
	d.Add("alpha")
	d.Add("beta")

	key, err := d.Key(0)
	if err != nil || key != "alpha" {
		t.Fatalf("Key(0): %q, %v", key, err)
	}

	key, err = d.Key(1)
	if err != nil || key != "beta" {
		t.Fatalf("Key(1): %q, %v", key, err)
	}

	_, err = d.Key(2)
	if err != ErrKeyNotFound {
		t.Fatalf("Key(2): expected ErrKeyNotFound, got %v", err)
	}
}

func TestKeyDictLen(t *testing.T) {
	d := NewKeyDict()
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

func TestEncodeDictEntry(t *testing.T) {
	entry := EncodeDictEntry("host")

	// Expected: [4, 0, 'h', 'o', 's', 't']
	if len(entry) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(entry))
	}
	keyLen := binary.LittleEndian.Uint16(entry[0:2])
	if keyLen != 4 {
		t.Fatalf("keyLen: want 4, got %d", keyLen)
	}
	if string(entry[2:]) != "host" {
		t.Fatalf("key: want 'host', got %q", string(entry[2:]))
	}
}

func TestEncodeDictEntryEmpty(t *testing.T) {
	entry := EncodeDictEntry("")
	if len(entry) != 2 {
		t.Fatalf("expected 2 bytes for empty key, got %d", len(entry))
	}
	keyLen := binary.LittleEndian.Uint16(entry[0:2])
	if keyLen != 0 {
		t.Fatalf("keyLen: want 0, got %d", keyLen)
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
			dict := NewKeyDict()
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

func TestEncodeWithDictNewKeys(t *testing.T) {
	dict := NewKeyDict()

	// First encode: all keys are new.
	attrs1 := Attributes{"host": "srv-001", "env": "prod"}
	_, newKeys1, err := EncodeWithDict(attrs1, dict)
	if err != nil {
		t.Fatalf("encode 1: %v", err)
	}
	if len(newKeys1) != 2 {
		t.Fatalf("expected 2 new keys, got %d", len(newKeys1))
	}

	// Second encode with same keys: no new keys.
	attrs2 := Attributes{"host": "srv-002", "env": "staging"}
	_, newKeys2, err := EncodeWithDict(attrs2, dict)
	if err != nil {
		t.Fatalf("encode 2: %v", err)
	}
	if len(newKeys2) != 0 {
		t.Fatalf("expected 0 new keys, got %d", len(newKeys2))
	}

	// Third encode with one new key.
	attrs3 := Attributes{"host": "srv-003", "service": "api"}
	_, newKeys3, err := EncodeWithDict(attrs3, dict)
	if err != nil {
		t.Fatalf("encode 3: %v", err)
	}
	if len(newKeys3) != 1 || newKeys3[0] != "service" {
		t.Fatalf("expected [service], got %v", newKeys3)
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
	dict := NewKeyDict()
	dictEncoded, _, err := EncodeWithDict(attrs, dict)
	if err != nil {
		t.Fatalf("dict encode: %v", err)
	}

	if len(dictEncoded) >= len(plain) {
		t.Fatalf("dict encoding (%d) should be smaller than plain (%d)", len(dictEncoded), len(plain))
	}
}

func TestDecodeDictDataRoundTrip(t *testing.T) {
	// Build dict data manually.
	var buf []byte
	keys := []string{"host", "service", "env", "level"}
	for _, k := range keys {
		buf = append(buf, EncodeDictEntry(k)...)
	}

	dict, err := DecodeDictData(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if dict.Len() != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), dict.Len())
	}

	for i, k := range keys {
		id, ok := dict.Lookup(k)
		if !ok {
			t.Fatalf("key %q not found", k)
		}
		if int(id) != i {
			t.Fatalf("key %q: want ID %d, got %d", k, i, id)
		}
	}
}

func TestDecodeDictDataPartialEntry(t *testing.T) {
	// Complete entry for "host" + partial trailing entry.
	var buf []byte
	buf = append(buf, EncodeDictEntry("host")...)

	// Add partial entry: keyLen says 10 but only 3 bytes follow.
	partial := make([]byte, 2)
	binary.LittleEndian.PutUint16(partial, 10)
	buf = append(buf, partial...)
	buf = append(buf, 'a', 'b', 'c') // Only 3 of 10 bytes.

	dict, err := DecodeDictData(buf)
	if err != nil {
		t.Fatalf("decode with partial: %v", err)
	}

	if dict.Len() != 1 {
		t.Fatalf("expected 1 key (partial ignored), got %d", dict.Len())
	}

	key, err := dict.Key(0)
	if err != nil || key != "host" {
		t.Fatalf("Key(0): %q, %v", key, err)
	}
}

func TestDecodeDictDataPartialKeyLen(t *testing.T) {
	// Complete entry + only 1 byte of next keyLen (needs 2).
	var buf []byte
	buf = append(buf, EncodeDictEntry("ok")...)
	buf = append(buf, 0x05) // Only 1 of 2 bytes for keyLen.

	dict, err := DecodeDictData(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dict.Len() != 1 {
		t.Fatalf("expected 1 key, got %d", dict.Len())
	}
}

func TestDecodeDictDataEmpty(t *testing.T) {
	dict, err := DecodeDictData(nil)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if dict.Len() != 0 {
		t.Fatalf("expected 0 keys, got %d", dict.Len())
	}
}

func TestEncodeDecodeWithDictBinaryFormat(t *testing.T) {
	dict := NewKeyDict()
	attrs := Attributes{"ab": "cd"}
	encoded, _, err := EncodeWithDict(attrs, dict)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Expected format:
	// [0:2]   count = 1
	// [2:4]   keyID = 0 (first key in dict)
	// [4:6]   valLen = 2
	// [6:8]   val = "cd"
	expected := []byte{
		0x01, 0x00, // count = 1
		0x00, 0x00, // keyID = 0
		0x02, 0x00, // valLen = 2
		'c', 'd',   // val
	}

	if !bytes.Equal(encoded, expected) {
		t.Fatalf("binary format mismatch:\nwant: %v\ngot:  %v", expected, encoded)
	}
}

func TestEncodeWithDictEmptyAttrs(t *testing.T) {
	dict := NewKeyDict()
	encoded, newKeys, err := EncodeWithDict(Attributes{}, dict)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(newKeys) != 0 {
		t.Fatalf("expected no new keys, got %v", newKeys)
	}
	if !bytes.Equal(encoded, []byte{0, 0}) {
		t.Fatalf("expected [0,0], got %v", encoded)
	}
}

func TestDecodeWithDictInvalidData(t *testing.T) {
	dict := NewKeyDict()
	dict.Add("key")

	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"one_byte", []byte{0x00}},
		{"count_but_no_data", []byte{0x01, 0x00}},
		{"truncated_val_len", []byte{0x01, 0x00, 0x00, 0x00}}, // count=1, keyID=0, no valLen
		{"bad_key_id", []byte{0x01, 0x00, 0x05, 0x00, 0x00, 0x00}}, // keyID=5 doesn't exist
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
	dict := NewKeyDict()
	attrs, err := DecodeWithDict([]byte{0x00, 0x00}, dict)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(attrs) != 0 {
		t.Fatalf("expected empty attrs, got %d entries", len(attrs))
	}
}
