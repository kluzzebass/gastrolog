package relp

import (
	"bytes"
	"gastrolog/internal/glid"
	"log/slog"
	"testing"
)

func FuzzNewFactory(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	// Pass nil certMgr — BuildTLSConfig only uses it when tls=true AND
	// tls_cert is set, so we seed without those to test the rest of parsing.
	// The fuzzer may also hit the nil certMgr path, which returns a clear error.
	factory := NewFactory(nil)
	id := glid.New()

	// Seed: valid params.
	f.Add([]byte("addr\x00:2514"))
	f.Add([]byte("addr\x00127.0.0.1:2514"))
	// Seed: empty (uses default addr).
	f.Add([]byte(""))
	// Seed: TLS enabled but no cert manager → error.
	f.Add([]byte("tls\x00true\x00tls_cert\x00mycert"))
	// Seed: TLS disabled explicitly.
	f.Add([]byte("addr\x00:2514\x00tls\x00false"))

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
