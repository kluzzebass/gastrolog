package safeutf8

import (
	"testing"
	"unicode/utf8"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/proto"
)

func TestString_Valid_ReturnsUnchanged(t *testing.T) {
	t.Parallel()
	s := "hello world æøå 日本語"
	got := String(s)
	if got != s {
		t.Fatalf("expected unchanged, got %q", got)
	}
}

func TestString_Invalid_ReplacesWithReplacementChar(t *testing.T) {
	t.Parallel()
	// 0xff is never a valid start byte in UTF-8.
	bad := string([]byte{'a', 0xff, 'b'})
	if utf8.ValidString(bad) {
		t.Fatal("fixture is unexpectedly valid UTF-8")
	}
	got := String(bad)
	if !utf8.ValidString(got) {
		t.Fatalf("sanitized output is not valid UTF-8: %q", got)
	}
	if got != "a�b" {
		t.Fatalf("expected 'a�b', got %q", got)
	}
}

func TestString_PartialUTF8AtBufferBoundary_IsRepaired(t *testing.T) {
	t.Parallel()
	// Leading byte of a 2-byte sequence (0xc3) followed by EOF (or
	// any byte that isn't a valid continuation) — the shape you get
	// when a UTF-8 codepoint is sliced mid-way.
	bad := string([]byte{'k', 0xc3})
	got := String(bad)
	if !utf8.ValidString(got) {
		t.Fatalf("sanitized output is not valid UTF-8: %q", got)
	}
}

func TestStrings_CleanInput_ReturnsSameSlice(t *testing.T) {
	t.Parallel()
	in := []string{"a", "b", "c"}
	out := Strings(in)
	// Identity check: same backing array when clean. Cast to uintptr
	// via a helper if you need, but &in[0] vs &out[0] is enough.
	if len(in) != len(out) {
		t.Fatalf("len mismatch: %d vs %d", len(in), len(out))
	}
	if &in[0] != &out[0] {
		t.Fatal("expected same slice (no copy) when all elements are valid")
	}
}

func TestStrings_DirtyInput_ReturnsSanitizedCopy(t *testing.T) {
	t.Parallel()
	in := []string{"a", string([]byte{0xff}), "c"}
	out := Strings(in)
	if &in[0] == &out[0] {
		t.Fatal("expected a fresh slice when sanitization occurs")
	}
	for i, v := range out {
		if !utf8.ValidString(v) {
			t.Errorf("out[%d] = %q still invalid", i, v)
		}
	}
}

func TestAttrs_CleanInput_ReturnsSameMap(t *testing.T) {
	t.Parallel()
	in := map[string]string{"host": "web-01", "level": "error"}
	out := Attrs(in)
	// Same reference: mutating out mutates in.
	out["new"] = "x"
	if in["new"] != "x" {
		t.Fatal("expected same map reference when all keys/values are valid")
	}
}

func TestAttrs_DirtyValue_ReturnsSanitizedCopy(t *testing.T) {
	t.Parallel()
	in := map[string]string{"host": string([]byte{'w', 0xff, 'b'})}
	out := Attrs(in)
	for k, v := range out {
		if !utf8.ValidString(k) {
			t.Errorf("key %q still invalid", k)
		}
		if !utf8.ValidString(v) {
			t.Errorf("value %q still invalid", v)
		}
	}
	// Fresh map, not aliased.
	out["new"] = "x"
	if _, ok := in["new"]; ok {
		t.Fatal("expected detached map after sanitization")
	}
}

func TestAttrs_DirtyKey_ReturnsSanitizedCopy(t *testing.T) {
	t.Parallel()
	in := map[string]string{string([]byte{'k', 0xff}): "v"}
	out := Attrs(in)
	for k, v := range out {
		if !utf8.ValidString(k) {
			t.Errorf("key %q still invalid", k)
		}
		if !utf8.ValidString(v) {
			t.Errorf("value %q still invalid", v)
		}
	}
}

// TestProtoMarshal_AfterSanitize exercises the actual end-to-end
// motivation: proto3 string fields refuse to marshal invalid UTF-8.
// Before sanitizing, FieldValue.Value with raw 0xff bytes causes
// "proto: field gastrolog.v1.FieldValue.value contains invalid UTF-8".
// After sanitizing, marshal succeeds.
func TestProtoMarshal_AfterSanitize(t *testing.T) {
	t.Parallel()
	bad := string([]byte{'v', 0xff, 'x'})

	// Before: proto marshal must fail.
	fv := &gastrologv1.FieldValue{Value: bad, Count: 1}
	if _, err := proto.Marshal(fv); err == nil {
		t.Fatal("expected marshal of invalid UTF-8 string to fail, got nil")
	}

	// After: sanitized value marshals cleanly.
	fv.Value = String(bad)
	if _, err := proto.Marshal(fv); err != nil {
		t.Fatalf("sanitized marshal failed: %v", err)
	}
}
