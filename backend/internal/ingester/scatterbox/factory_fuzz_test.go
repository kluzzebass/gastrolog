package scatterbox

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/google/uuid"
)

func FuzzNewIngester(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	id := uuid.New()

	// Seed: valid with defaults.
	f.Add([]byte(""))
	// Seed: valid custom params.
	f.Add([]byte("interval\x00200ms\x00burst\x0010"))
	f.Add([]byte("interval\x001s\x00burst\x001"))
	f.Add([]byte("interval\x000s"))
	// Seed: invalid params.
	f.Add([]byte("interval\x00notaduration"))
	f.Add([]byte("interval\x00-1s"))
	f.Add([]byte("burst\x000"))
	f.Add([]byte("burst\x00-5"))
	f.Add([]byte("burst\x00abc"))

	f.Fuzz(func(t *testing.T, data []byte) {
		params := splitParams(data)
		ing, err := NewIngester(id, params, logger)
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
