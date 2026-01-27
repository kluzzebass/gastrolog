package memory

import (
	"bytes"
	"testing"
)

func TestAppenderAppendAndBytes(t *testing.T) {
	a := NewAppender()
	n, err := a.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes written, got %d", n)
	}

	n, err = a.Append([]byte(" world"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if n != 6 {
		t.Fatalf("expected 6 bytes written, got %d", n)
	}

	got := a.Bytes()
	if !bytes.Equal(got, []byte("hello world")) {
		t.Fatalf("expected %q, got %q", "hello world", got)
	}
}

func TestAppenderSize(t *testing.T) {
	a := NewAppender()
	if a.Size() != 0 {
		t.Fatalf("expected size 0, got %d", a.Size())
	}

	a.Append([]byte("abc"))
	if a.Size() != 3 {
		t.Fatalf("expected size 3, got %d", a.Size())
	}

	a.Append([]byte("de"))
	if a.Size() != 5 {
		t.Fatalf("expected size 5, got %d", a.Size())
	}
}

func TestAppenderBytesReturnsCopy(t *testing.T) {
	a := NewAppender()
	a.Append([]byte("original"))

	got := a.Bytes()
	got[0] = 'X'

	// Internal buffer should be unmodified.
	internal := a.Bytes()
	if internal[0] != 'o' {
		t.Fatal("Bytes() should return a copy, but internal buffer was modified")
	}
}

func TestAppenderEmptyBytes(t *testing.T) {
	a := NewAppender()
	got := a.Bytes()
	if len(got) != 0 {
		t.Fatalf("expected empty bytes, got %d bytes", len(got))
	}
}

func TestAppenderAppendEmpty(t *testing.T) {
	a := NewAppender()
	n, err := a.Append([]byte{})
	if err != nil {
		t.Fatalf("append empty: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}
	if a.Size() != 0 {
		t.Fatalf("expected size 0, got %d", a.Size())
	}
}

func TestAppenderClose(t *testing.T) {
	a := NewAppender()
	a.Append([]byte("data"))

	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Append after close should fail.
	_, err := a.Append([]byte("more"))
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestAppenderCloseIdempotent(t *testing.T) {
	a := NewAppender()
	if err := a.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestAppenderSizeAfterClose(t *testing.T) {
	a := NewAppender()
	a.Append([]byte("hello"))
	a.Close()

	// Size should still report the buffered data.
	if a.Size() != 5 {
		t.Fatalf("expected size 5 after close, got %d", a.Size())
	}
}

func TestAppenderBytesAfterClose(t *testing.T) {
	a := NewAppender()
	a.Append([]byte("hello"))
	a.Close()

	got := a.Bytes()
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("expected %q after close, got %q", "hello", got)
	}
}
