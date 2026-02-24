package docker

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"
)

// streamType identifies the source of a Docker log line.
type streamType byte

const (
	streamStdin  streamType = 0
	streamStdout streamType = 1
	streamStderr streamType = 2
)

func (s streamType) String() string {
	switch s {
	case streamStdout:
		return "stdout"
	case streamStderr:
		return "stderr"
	case streamStdin:
		return "stdin"
	default:
		return "stdin"
	}
}

// logEntry is a single parsed log line from a Docker container.
type logEntry struct {
	Timestamp time.Time
	Stream    string // "stdout", "stderr", or "tty"
	Line      []byte
}

// readMultiplexed reads Docker multiplexed log frames from a non-TTY container.
// Docker uses 8-byte frame headers: [stream_type(1)][padding(3)][size(4 BE)][payload]
func readMultiplexed(r io.Reader, entries chan<- logEntry) error {
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			return err
		}

		st := streamType(header[0])
		size := binary.BigEndian.Uint32(header[4:8])

		if size == 0 {
			continue
		}

		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil {
			return fmt.Errorf("read frame payload: %w", err)
		}

		// Payload may contain multiple lines separated by newlines.
		// Each line has a Docker timestamp prefix when timestamps=true.
		lines := splitLines(payload)
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			ts, rest := parseTimestamp(line)
			entries <- logEntry{
				Timestamp: ts,
				Stream:    st.String(),
				Line:      rest,
			}
		}
	}
}

// readRaw reads raw (non-multiplexed) log output from a TTY container.
// Each line has a Docker timestamp prefix when timestamps=true.
func readRaw(r io.Reader, entries chan<- logEntry) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		ts, rest := parseTimestamp(line)
		raw := make([]byte, len(rest))
		copy(raw, rest)
		entries <- logEntry{
			Timestamp: ts,
			Stream:    "tty",
			Line:      raw,
		}
	}
	return scanner.Err()
}

// parseTimestamp extracts the RFC3339Nano timestamp prefix from a Docker log line.
// Docker timestamp format: "2024-01-15T10:30:00.123456789Z " followed by the log content.
// Returns zero time and the full line if parsing fails.
func parseTimestamp(line []byte) (time.Time, []byte) {
	// Docker timestamps are RFC3339Nano, typically 30-35 chars long.
	// Find the first space after the timestamp.
	s := string(line)
	idx := strings.IndexByte(s, ' ')
	if idx < 20 { // RFC3339 minimum is ~20 chars
		return time.Time{}, line
	}

	ts, err := time.Parse(time.RFC3339Nano, s[:idx])
	if err != nil {
		return time.Time{}, line
	}

	rest := line[idx+1:]
	return ts, rest
}

// splitLines splits payload bytes by newline, trimming \r\n.
func splitLines(data []byte) [][]byte {
	var lines [][]byte
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Bytes()
		cp := make([]byte, len(line))
		copy(cp, line)
		lines = append(lines, cp)
	}
	return lines
}
