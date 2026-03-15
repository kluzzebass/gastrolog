package docker

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// FuzzReadMultiplexed feeds random bytes into the Docker multiplexed log demuxer.
// The parser must never panic, regardless of input.
func FuzzReadMultiplexed(f *testing.F) {
	// Valid multiplexed frame: [stream_type(1)][padding(3)][size(4 BE)][payload]
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	validPayload := ts.Format(time.RFC3339Nano) + " hello world\n"
	validFrame := make([]byte, 8+len(validPayload))
	validFrame[0] = 1 // stdout
	binary.BigEndian.PutUint32(validFrame[4:8], uint32(len(validPayload)))
	copy(validFrame[8:], validPayload)
	f.Add(validFrame)

	// stderr frame
	stderrFrame := make([]byte, 8+len(validPayload))
	stderrFrame[0] = 2 // stderr
	binary.BigEndian.PutUint32(stderrFrame[4:8], uint32(len(validPayload)))
	copy(stderrFrame[8:], validPayload)
	f.Add(stderrFrame)

	// Zero-size payload (should be skipped).
	zeroFrame := make([]byte, 8)
	zeroFrame[0] = 1
	f.Add(zeroFrame)

	// Edge cases.
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})                         // all zeros
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}) // all 0xFF
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}) // no newline (partial)
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 1, '\n'})                     // just newline

	// Header claims huge payload but data is short.
	hugeHeader := make([]byte, 8)
	hugeHeader[0] = 1
	binary.BigEndian.PutUint32(hugeHeader[4:8], 0x7FFFFFFF)
	f.Add(append(hugeHeader, []byte("short")...))

	f.Fuzz(func(t *testing.T, data []byte) {
		entries := make(chan logEntry, 100)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range entries {
				// drain
			}
		}()

		// readMultiplexed blocks until EOF or error, so give it a reader
		// that will end.
		_ = readMultiplexed(bytes.NewReader(data), entries)
		close(entries)
		<-done
	})
}

// FuzzReadRaw feeds random bytes into the Docker TTY (raw) log parser.
func FuzzReadRaw(f *testing.F) {
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	f.Add([]byte(ts.Format(time.RFC3339Nano) + " hello world\n"))
	f.Add([]byte("just plain text\n"))
	f.Add([]byte{})
	f.Add([]byte("\n\n\n"))
	f.Add([]byte("no-newline-at-end"))

	f.Fuzz(func(t *testing.T, data []byte) {
		entries := make(chan logEntry, 100)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range entries {
			}
		}()

		_ = readRaw(bytes.NewReader(data), entries)
		close(entries)
		<-done
	})
}

// FuzzParseTimestamp feeds random byte slices into the Docker timestamp parser.
func FuzzParseTimestamp(f *testing.F) {
	f.Add([]byte("2025-01-15T10:30:00.123456789Z hello"))
	f.Add([]byte("2025-01-15T10:30:00Z hello"))
	f.Add([]byte("not a timestamp"))
	f.Add([]byte(""))
	f.Add([]byte("2025-01-15T10:30:00.123456789Z "))
	f.Add([]byte("2025-01-15T10:30:00.123456789Z"))
	f.Add([]byte("short ts"))

	f.Fuzz(func(t *testing.T, data []byte) {
		ts, rest := parseTimestamp(data)
		// Must never panic. If timestamp parsed, rest should be a suffix of data.
		if !ts.IsZero() && len(rest) > len(data) {
			t.Fatal("rest is longer than input")
		}
	})
}
