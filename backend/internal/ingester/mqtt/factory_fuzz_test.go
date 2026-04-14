package mqtt

import (
	"gastrolog/internal/glid"
	"bytes"
	"log/slog"
	"testing"

)

func FuzzNewFactory(f *testing.F) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	factory := NewFactory()
	id := glid.New()

	// Seed: valid params.
	f.Add([]byte("broker\x00tcp://localhost:1883\x00topics\x00logs/#\x00version\x003"))
	f.Add([]byte("broker\x00ssl://mqtt:8883\x00topics\x00a,b,c\x00tls\x00true\x00version\x005"))
	f.Add([]byte("broker\x00tcp://h:1883\x00topics\x00t\x00clean_session\x00false\x00client_id\x00myid"))
	f.Add([]byte("broker\x00tcp://h:1883\x00topics\x00t\x00username\x00u\x00password\x00p"))
	// Seed: missing required.
	f.Add([]byte("topics\x00logs"))
	f.Add([]byte("broker\x00tcp://h:1883"))
	f.Add([]byte(""))
	// Seed: bad version.
	f.Add([]byte("broker\x00tcp://h:1\x00topics\x00t\x00version\x004"))

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
