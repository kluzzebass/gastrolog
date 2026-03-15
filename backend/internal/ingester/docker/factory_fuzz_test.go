package docker

import (
	"bytes"
	"log/slog"
	"testing"
)

func FuzzParseConfig(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	// Seed: valid params.
	f.Add([]byte("host\x00unix:///var/run/docker.sock\x00poll_interval\x0030s\x00stdout\x00true\x00stderr\x00true"))
	f.Add([]byte("host\x00tcp://localhost:2376\x00tls\x00false"))
	f.Add([]byte("host\x00unix:///var/run/docker.sock\x00filter\x00name=~web"))
	f.Add([]byte("poll_interval\x001m\x00stdout\x00false"))
	// Seed: both streams disabled (error).
	f.Add([]byte("stdout\x00false\x00stderr\x00false"))
	// Seed: bad duration.
	f.Add([]byte("poll_interval\x00notaduration"))
	// Seed: empty.
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		params := splitParams(data)
		// Pass nil cfgStore — TLS cert resolution will fail gracefully when
		// tls_ca or tls_cert params are set, because resolveTLS checks for
		// empty names first and only calls cfgStore.ListCertificates when
		// a name is provided. With nil cfgStore the call will panic, so we
		// remove TLS cert params to test only the parsing logic.
		delete(params, "tls_ca")
		delete(params, "tls_cert")
		_, _ = parseConfig("fuzz-id", params, nil, logger)
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
