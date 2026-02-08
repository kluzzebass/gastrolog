package tokenizer

import (
	"sort"
	"testing"
)

func TestExtractJSON(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		expected []KeyValue
	}{
		{
			name:     "flat string fields",
			msg:      `{"level":"info","msg":"hello"}`,
			expected: []KeyValue{{Key: "level", Value: "info"}, {Key: "msg", Value: "hello"}},
		},
		{
			name:     "numeric field",
			msg:      `{"status":200}`,
			expected: []KeyValue{{Key: "status", Value: "200"}},
		},
		{
			name:     "float field",
			msg:      `{"duration":1.5}`,
			expected: []KeyValue{{Key: "duration", Value: "1.5"}},
		},
		{
			name:     "boolean fields",
			msg:      `{"active":true,"deleted":false}`,
			expected: []KeyValue{{Key: "active", Value: "true"}, {Key: "deleted", Value: "false"}},
		},
		{
			name:     "nested object",
			msg:      `{"http":{"method":"GET","status":200}}`,
			expected: []KeyValue{{Key: "http.method", Value: "get"}, {Key: "http.status", Value: "200"}},
		},
		{
			name:     "deeply nested",
			msg:      `{"a":{"b":{"c":{"d":"deep"}}}}`,
			expected: []KeyValue{{Key: "a.b.c.d", Value: "deep"}},
		},
		{
			name:     "depth 4 capped",
			msg:      `{"a":{"b":{"c":{"d":{"e":"too deep"}}}}}`,
			expected: nil, // depth 4 object has nested object, won't recurse into it
		},
		{
			name:     "array of strings",
			msg:      `{"tags":["web","api"]}`,
			expected: []KeyValue{{Key: "tags", Value: "web"}, {Key: "tags", Value: "api"}},
		},
		{
			name:     "array of numbers",
			msg:      `{"ports":[80,443]}`,
			expected: []KeyValue{{Key: "ports", Value: "80"}, {Key: "ports", Value: "443"}},
		},
		{
			name:     "mixed types",
			msg:      `{"level":"info","status":200,"active":true,"tags":["web"]}`,
			expected: []KeyValue{{Key: "level", Value: "info"}, {Key: "status", Value: "200"}, {Key: "active", Value: "true"}, {Key: "tags", Value: "web"}},
		},
		{
			name:     "null value skipped",
			msg:      `{"key":null,"level":"info"}`,
			expected: []KeyValue{{Key: "level", Value: "info"}},
		},
		{
			name:     "empty string skipped",
			msg:      `{"key":"","level":"info"}`,
			expected: []KeyValue{{Key: "level", Value: "info"}},
		},
		{
			name:     "case normalization",
			msg:      `{"Method":"GET","Path":"/Login"}`,
			expected: []KeyValue{{Key: "method", Value: "get"}, {Key: "path", Value: "/login"}},
		},

		// Rejection cases
		{
			name:     "not json",
			msg:      "just plain text",
			expected: nil,
		},
		{
			name:     "json array at top level",
			msg:      `["a","b"]`,
			expected: nil,
		},
		{
			name:     "empty object",
			msg:      `{}`,
			expected: nil,
		},
		{
			name:     "empty message",
			msg:      "",
			expected: nil,
		},
		{
			name:     "invalid json",
			msg:      `{"key":}`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractJSON([]byte(tt.msg))

			// Sort both expected and result by key+value for stable comparison
			// since JSON map iteration order is non-deterministic.
			sortKV := func(kvs []KeyValue) {
				sort.Slice(kvs, func(i, j int) bool {
					if kvs[i].Key != kvs[j].Key {
						return kvs[i].Key < kvs[j].Key
					}
					return kvs[i].Value < kvs[j].Value
				})
			}

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d pairs, got %d: %v", len(tt.expected), len(result), result)
				return
			}

			if len(result) > 0 {
				sortKV(result)
				expected := make([]KeyValue, len(tt.expected))
				copy(expected, tt.expected)
				sortKV(expected)

				for i, kv := range result {
					if kv.Key != expected[i].Key || kv.Value != expected[i].Value {
						t.Errorf("pair %d: expected {%q, %q}, got {%q, %q}",
							i, expected[i].Key, expected[i].Value, kv.Key, kv.Value)
					}
				}
			}
		})
	}
}
