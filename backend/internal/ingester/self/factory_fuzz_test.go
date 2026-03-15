package self

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"gastrolog/internal/logging"
)

func FuzzNewFactory(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	ch := make(chan logging.CapturedRecord, 1)
	// nil CaptureHandler — SetMinCaptureLevel is guarded by nil check.
	factory := NewFactory(ch, nil)
	id := uuid.New()

	// Seed: valid params.
	f.Add([]byte("min_level\x00warn"))
	f.Add([]byte("min_level\x00debug"))
	f.Add([]byte("min_level\x00info"))
	f.Add([]byte("min_level\x00error"))
	f.Add([]byte("min_level\x00warning"))
	// Seed: empty (uses defaults).
	f.Add([]byte(""))
	// Seed: unknown level (falls back to warn).
	f.Add([]byte("min_level\x00garbage"))

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
