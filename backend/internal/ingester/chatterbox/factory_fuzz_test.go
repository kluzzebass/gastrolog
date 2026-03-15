package chatterbox

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
	f.Add([]byte("minInterval\x00200ms\x00maxInterval\x002s\x00hostCount\x005\x00serviceCount\x003"))
	f.Add([]byte("formats\x00json,plain\x00formatWeights\x00json=10,plain=5"))
	f.Add([]byte("minInterval\x001s\x00maxInterval\x001s"))
	f.Add([]byte("formats\x00access"))
	f.Add([]byte("formats\x00plain,kv,json,access,syslog,weird,multirecord"))
	// Seed: invalid params.
	f.Add([]byte("minInterval\x00notaduration"))
	f.Add([]byte("maxInterval\x00-1s"))
	f.Add([]byte("minInterval\x005s\x00maxInterval\x001s"))
	f.Add([]byte("hostCount\x000"))
	f.Add([]byte("hostCount\x00abc"))
	f.Add([]byte("serviceCount\x00-1"))
	f.Add([]byte("formats\x00unknown"))
	f.Add([]byte("formatWeights\x00badformat"))

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
