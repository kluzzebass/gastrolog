package docker

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"gastrolog/internal/ingester/limits"
)

// TestOversizeFrameHeaderIsRejected proves a Docker frame header claiming
// more than the frame ceiling is refused before the allocation. Docker's own
// log driver splits at 16 KB; a header near this ceiling means the stream is
// not what it claims to be.
func TestOversizeFrameHeaderIsRejected(t *testing.T) {
	t.Parallel()

	header := make([]byte, 8)
	header[0] = byte(streamStdout)
	binary.BigEndian.PutUint32(header[4:8], uint32(limits.MaxFrameBytes+1))

	entries := make(chan logEntry, 1)
	err := readMultiplexed(bytes.NewReader(header), entries)

	if err == nil {
		t.Fatal("an oversize frame header was accepted")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("rejection does not name the ceiling: %v", err)
	}
	select {
	case e := <-entries:
		t.Fatalf("an oversize frame produced an entry: %q", e.Line)
	default:
	}
}

// TestFramesUnderTheCeilingStillStream proves the bound did not change what
// a conforming Docker log stream produces.
func TestFramesUnderTheCeilingStillStream(t *testing.T) {
	t.Parallel()

	payload := []byte("2024-01-15T10:30:00.000000000Z hello world\n")
	frame := make([]byte, 8, 8+len(payload))
	frame[0] = byte(streamStdout)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(payload)))
	frame = append(frame, payload...)

	entries := make(chan logEntry, 4)
	_ = readMultiplexed(bytes.NewReader(frame), entries)
	close(entries)

	got := <-entries
	if string(got.Line) != "hello world" {
		t.Errorf("payload altered: %q", got.Line)
	}
	if got.Stream != "stdout" {
		t.Errorf("stream lost: %q", got.Stream)
	}
}
