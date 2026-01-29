package format

import (
	"testing"
)

func TestHeaderEncode(t *testing.T) {
	h := Header{Type: TypeTimeIndex, Version: 1, Flags: 0}
	buf := h.Encode()

	if buf[0] != Signature {
		t.Errorf("expected signature 0x%02x, got 0x%02x", Signature, buf[0])
	}
	if buf[1] != TypeTimeIndex {
		t.Errorf("expected type 0x%02x, got 0x%02x", TypeTimeIndex, buf[1])
	}
	if buf[2] != 1 {
		t.Errorf("expected version 1, got %d", buf[2])
	}
	if buf[3] != 0 {
		t.Errorf("expected flags 0, got %d", buf[3])
	}
}

func TestHeaderEncodeInto(t *testing.T) {
	h := Header{Type: TypeSourceIndex, Version: 2, Flags: 0x0F}
	buf := make([]byte, 10)
	n := h.EncodeInto(buf)

	if n != HeaderSize {
		t.Errorf("expected %d bytes written, got %d", HeaderSize, n)
	}
	if buf[0] != Signature {
		t.Errorf("expected signature 0x%02x, got 0x%02x", Signature, buf[0])
	}
	if buf[1] != TypeSourceIndex {
		t.Errorf("expected type 0x%02x, got 0x%02x", TypeSourceIndex, buf[1])
	}
	if buf[2] != 2 {
		t.Errorf("expected version 2, got %d", buf[2])
	}
	if buf[3] != 0x0F {
		t.Errorf("expected flags 0x0F, got 0x%02x", buf[3])
	}
}

func TestDecode(t *testing.T) {
	buf := []byte{Signature, TypeTokenIndex, 3, 0x10}
	h, err := Decode(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Type != TypeTokenIndex {
		t.Errorf("expected type 0x%02x, got 0x%02x", TypeTokenIndex, h.Type)
	}
	if h.Version != 3 {
		t.Errorf("expected version 3, got %d", h.Version)
	}
	if h.Flags != 0x10 {
		t.Errorf("expected flags 0x10, got 0x%02x", h.Flags)
	}
}

func TestDecodeHeaderTooSmall(t *testing.T) {
	buf := []byte{Signature, TypeTimeIndex, 1} // only 3 bytes
	_, err := Decode(buf)
	if err != ErrHeaderTooSmall {
		t.Errorf("expected ErrHeaderTooSmall, got %v", err)
	}
}

func TestDecodeSignatureMismatch(t *testing.T) {
	buf := []byte{'x', TypeTimeIndex, 1, 0}
	_, err := Decode(buf)
	if err != ErrSignatureMismatch {
		t.Errorf("expected ErrSignatureMismatch, got %v", err)
	}
}

func TestDecodeAndValidate(t *testing.T) {
	buf := []byte{Signature, TypeChunkMeta, 1, 0}
	h, err := DecodeAndValidate(buf, TypeChunkMeta, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Type != TypeChunkMeta {
		t.Errorf("expected type 0x%02x, got 0x%02x", TypeChunkMeta, h.Type)
	}
}

func TestDecodeAndValidateTypeMismatch(t *testing.T) {
	buf := []byte{Signature, TypeTimeIndex, 1, 0}
	_, err := DecodeAndValidate(buf, TypeSourceIndex, 1)
	if err != ErrTypeMismatch {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestDecodeAndValidateVersionMismatch(t *testing.T) {
	buf := []byte{Signature, TypeTimeIndex, 1, 0}
	_, err := DecodeAndValidate(buf, TypeTimeIndex, 2)
	if err != ErrVersionMismatch {
		t.Errorf("expected ErrVersionMismatch, got %v", err)
	}
}

func TestRoundTrip(t *testing.T) {
	original := Header{Type: TypeSourceRegistry, Version: 5, Flags: 0xAB}
	buf := original.Encode()
	decoded, err := Decode(buf[:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decoded != original {
		t.Errorf("roundtrip failed: expected %+v, got %+v", original, decoded)
	}
}
