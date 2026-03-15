package tail

import (
	"bytes"
	"log/slog"
	"testing"
)

func FuzzParseConfig(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	// Seed: valid params.
	f.Add([]byte("paths\x00[\"/var/log/*.log\"]\x00poll_interval\x0030s"))
	f.Add([]byte("paths\x00[\"/tmp/a.log\",\"/tmp/b.log\"]"))
	f.Add([]byte("paths\x00[\"/var/log/syslog\"]\x00poll_interval\x001m"))
	// Seed: missing paths (error).
	f.Add([]byte("poll_interval\x0010s"))
	f.Add([]byte(""))
	// Seed: invalid JSON.
	f.Add([]byte("paths\x00not-json"))
	// Seed: empty array.
	f.Add([]byte("paths\x00[]"))
	// Seed: bad duration.
	f.Add([]byte("paths\x00[\"/a\"]\x00poll_interval\x00notaduration"))
	// Seed: negative duration.
	f.Add([]byte("paths\x00[\"/a\"]\x00poll_interval\x00-5s"))

	f.Fuzz(func(t *testing.T, data []byte) {
		params := splitParams(data)
		_, _ = parseConfig("fuzz-id", params, logger)
	})
}

func splitParams(data []byte) map[string]string {
	parts := bytes.Split(data, []byte{0})
	m := make(map[string]string, len(parts)/2)
	for i := 0; i+1 < len(parts); i += 2 {
		m[string(parts[i])] = string(parts[i+1])
	}
	return m
}
