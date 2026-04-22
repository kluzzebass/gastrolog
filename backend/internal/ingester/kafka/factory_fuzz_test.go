package kafka

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
	f.Add([]byte("brokers\x00localhost:9092\x00topic\x00logs\x00group\x00gastrolog"))
	f.Add([]byte("brokers\x00a:9092,b:9092\x00topic\x00test\x00tls\x00true\x00sasl_mechanism\x00plain\x00sasl_user\x00u\x00sasl_password\x00p"))
	f.Add([]byte("brokers\x00host:9092\x00topic\x00t\x00sasl_mechanism\x00scram-sha-256"))
	f.Add([]byte("brokers\x00host:9092\x00topic\x00t\x00sasl_mechanism\x00scram-sha-512"))
	// Seed: missing required.
	f.Add([]byte("topic\x00logs"))
	f.Add([]byte("brokers\x00localhost:9092"))
	f.Add([]byte(""))
	// Seed: bad SASL mechanism.
	f.Add([]byte("brokers\x00h:1\x00topic\x00t\x00sasl_mechanism\x00badmech"))

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
