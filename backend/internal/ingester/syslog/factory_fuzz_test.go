package syslog

import (
	"bytes"
	"gastrolog/internal/glid"
	"log/slog"
	"testing"
)

func FuzzNewFactory(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	factory := NewFactory()
	id := glid.New()

	// Seed: valid params.
	f.Add([]byte("udp_addr\x00:514\x00tcp_addr\x00:601"))
	f.Add([]byte("udp_addr\x00127.0.0.1:514"))
	f.Add([]byte("tcp_addr\x000.0.0.0:601"))
	// Seed: empty (both missing → error).
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		params := splitParams(data)
		ing, err := factory(id, params, logger)
		if err != nil {
			return
		}
		if ing == nil {
			t.Fatal("nil ingester without error")
		}
	})
}

// splitParams splits fuzz bytes on null into alternating key/value pairs.
func splitParams(data []byte) map[string]string {
	parts := bytes.Split(data, []byte{0})
	m := make(map[string]string, len(parts)/2)
	for i := 0; i+1 < len(parts); i += 2 {
		m[string(parts[i])] = string(parts[i+1])
	}
	return m
}
