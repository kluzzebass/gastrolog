package tokenizer

import "testing"

func TestExtractLogfmt(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		expected []KeyValue
	}{
		{
			name:     "standard logfmt",
			msg:      "level=info msg=hello duration=12ms",
			expected: []KeyValue{{Key: "level", Value: "info"}, {Key: "msg", Value: "hello"}, {Key: "duration", Value: "12ms"}},
		},
		{
			name:     "quoted value with spaces",
			msg:      `level=info msg="hello world" duration=12ms`,
			expected: []KeyValue{{Key: "level", Value: "info"}, {Key: "msg", Value: "hello world"}, {Key: "duration", Value: "12ms"}},
		},
		{
			name:     "hyphenated keys",
			msg:      "pod-name=api-server container-id=abc123",
			expected: []KeyValue{{Key: "pod-name", Value: "api-server"}, {Key: "container-id", Value: "abc123"}},
		},
		{
			name:     "bare boolean keys",
			msg:      "level=info active debug",
			expected: []KeyValue{{Key: "level", Value: "info"}, {Key: "active", Value: "true"}, {Key: "debug", Value: "true"}},
		},
		{
			name:     "escaped quotes in value",
			msg:      `msg="has \"quotes\" inside" level=info`,
			expected: []KeyValue{{Key: "msg", Value: `has "quotes" inside`}, {Key: "level", Value: "info"}},
		},
		{
			name:     "escaped backslash",
			msg:      `path="C:\\Users\\test" level=info`,
			expected: []KeyValue{{Key: "path", Value: `c:\users\test`}, {Key: "level", Value: "info"}},
		},
		{
			name:     "colons in keys",
			msg:      "service:name=api-gateway service:port=8080",
			expected: []KeyValue{{Key: "service:name", Value: "api-gateway"}, {Key: "service:port", Value: "8080"}},
		},
		{
			name:     "slashes in keys",
			msg:      "k8s.io/name=myapp",
			expected: []KeyValue{{Key: "k8s.io/name", Value: "myapp"}},
		},
		{
			name:     "empty value skipped",
			msg:      "key= next=val",
			expected: []KeyValue{{Key: "next", Value: "val"}},
		},
		{
			name:     "case normalization",
			msg:      "Level=INFO Method=GET",
			expected: []KeyValue{{Key: "level", Value: "info"}, {Key: "method", Value: "get"}},
		},
		{
			name:     "dedupe within message",
			msg:      "key=val key=val",
			expected: []KeyValue{{Key: "key", Value: "val"}},
		},

		// Rejection cases
		{
			name:     "json rejected",
			msg:      `{"level":"info","msg":"hello"}`,
			expected: nil,
		},
		{
			name:     "json array rejected",
			msg:      `["level","info"]`,
			expected: nil,
		},
		{
			name:     "xml/html rejected",
			msg:      `<html><body>test</body></html>`,
			expected: nil,
		},
		{
			name:     "plain text no equals",
			msg:      "just some plain text",
			expected: nil,
		},
		{
			name:     "empty message",
			msg:      "",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractLogfmt([]byte(tt.msg))
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d pairs, got %d: %v", len(tt.expected), len(result), result)
				return
			}
			for i, kv := range result {
				if kv.Key != tt.expected[i].Key || kv.Value != tt.expected[i].Value {
					t.Errorf("pair %d: expected {%q, %q}, got {%q, %q}",
						i, tt.expected[i].Key, tt.expected[i].Value, kv.Key, kv.Value)
				}
			}
		})
	}
}
