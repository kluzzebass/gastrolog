package otlp

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/google/uuid"
)

func FuzzNewFactory(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	factory := NewFactory()
	id := uuid.New()

	// Seed: valid params.
	f.Add([]byte("http_addr\x00:4318\x00grpc_addr\x00:4317"))
	f.Add([]byte("http_addr\x00127.0.0.1:4318\x00grpc_addr\x00[::1]:4317"))
	// Seed: empty (uses defaults).
	f.Add([]byte(""))
	// Seed: invalid addr without colon.
	f.Add([]byte("http_addr\x00localhost"))
	f.Add([]byte("grpc_addr\x00badaddr"))

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

func splitParams(data []byte) map[string]string {
	parts := bytes.Split(data, []byte{0})
	m := make(map[string]string, len(parts)/2)
	for i := 0; i+1 < len(parts); i += 2 {
		m[string(parts[i])] = string(parts[i+1])
	}
	return m
}
