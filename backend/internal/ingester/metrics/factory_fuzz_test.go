package metrics

import (
	"gastrolog/internal/glid"
	"bytes"
	"log/slog"
	"testing"

)

func FuzzNewFactory(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	// nil StatsSource is fine — it's stored but not called during construction.
	factory := NewFactory(nil)
	id := glid.New()

	// Seed: valid with defaults.
	f.Add([]byte(""))
	// Seed: valid custom params.
	f.Add([]byte("interval\x0010s\x00vault_interval\x0030s"))
	f.Add([]byte("interval\x001m\x00vault_interval\x005m"))
	f.Add([]byte("interval\x00500ms"))
	// Seed: invalid params.
	f.Add([]byte("interval\x00notaduration"))
	f.Add([]byte("interval\x00-1s"))
	f.Add([]byte("interval\x000s"))
	f.Add([]byte("vault_interval\x00bad"))
	f.Add([]byte("vault_interval\x000s"))

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
