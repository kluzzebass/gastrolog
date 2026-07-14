package cluster

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type scriptedRead struct {
	data []byte
	err  error
}

// scriptedReader replays an exact sequence of Read results.
type scriptedReader struct {
	script []scriptedRead
	pos    int
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.script) {
		return 0, io.EOF
	}
	step := r.script[r.pos]
	r.pos++
	if len(step.data) > len(p) {
		panic("scripted read larger than buffer")
	}
	copy(p, step.data)
	return len(step.data), step.err
}

func TestCopyFramesShortReadsAndDataWithEOF(t *testing.T) {
	t.Parallel()
	src := &scriptedReader{script: []scriptedRead{
		{data: []byte("abc")},
		{data: nil}, // zero-byte read must not emit an empty frame or stop the copy
		{data: []byte("defg")},
		{data: []byte("hi"), err: io.EOF}, // n>0 with EOF: bytes must still land
	}}
	var out bytes.Buffer
	n, err := copyFrames(&out, src, make([]byte, 16))
	if err != nil {
		t.Fatalf("copyFrames: %v", err)
	}
	if n != 9 || out.String() != "abcdefghi" {
		t.Fatalf("copied n=%d %q, want 9 %q", n, out.String(), "abcdefghi")
	}
}

func TestCopyFramesReaderError(t *testing.T) {
	t.Parallel()
	boom := errors.New("read failed")
	src := &scriptedReader{script: []scriptedRead{
		{data: []byte("abc")},
		{data: []byte("de"), err: boom}, // bytes before the error must be delivered
	}}
	var out bytes.Buffer
	n, err := copyFrames(&out, src, make([]byte, 16))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if n != 5 || out.String() != "abcde" {
		t.Fatalf("copied n=%d %q, want 5 %q", n, out.String(), "abcde")
	}
}

type failingWriter struct{ err error }

func (w *failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestCopyFramesWriterError(t *testing.T) {
	t.Parallel()
	boom := errors.New("send failed")
	n, err := copyFrames(&failingWriter{err: boom}, bytes.NewReader([]byte("abc")), make([]byte, 2))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if n != 0 {
		t.Fatalf("n = %d, want 0", n)
	}
}
