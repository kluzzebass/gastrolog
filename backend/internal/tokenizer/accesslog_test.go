package tokenizer

import "testing"

func TestExtractAccessLog(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		expected []KeyValue
	}{
		{
			name: "common log format",
			msg:  `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.1" 200 2326`,
			expected: []KeyValue{
				{Key: "remote_host", Value: "127.0.0.1"},
				{Key: "remote_user", Value: "frank"},
				{Key: "method", Value: "get"},
				{Key: "path", Value: "/apache_pb.gif"},
				{Key: "protocol", Value: "http/1.1"},
				{Key: "status", Value: "200"},
				{Key: "body_bytes", Value: "2326"},
			},
		},
		{
			name: "combined log format",
			msg:  `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08"`,
			expected: []KeyValue{
				{Key: "remote_host", Value: "127.0.0.1"},
				{Key: "remote_user", Value: "frank"},
				{Key: "method", Value: "get"},
				{Key: "path", Value: "/index.html"},
				{Key: "protocol", Value: "http/1.1"},
				{Key: "status", Value: "200"},
				{Key: "body_bytes", Value: "2326"},
				{Key: "referer", Value: "http://www.example.com/start.html"},
				{Key: "user_agent", Value: "mozilla/4.08"},
			},
		},
		{
			name: "dash user and zero bytes",
			msg:  `10.0.0.1 - - [08/Feb/2026:12:00:00 +0000] "POST /api/v1/users HTTP/1.1" 201 0`,
			expected: []KeyValue{
				{Key: "remote_host", Value: "10.0.0.1"},
				{Key: "method", Value: "post"},
				{Key: "path", Value: "/api/v1/users"},
				{Key: "protocol", Value: "http/1.1"},
				{Key: "status", Value: "201"},
			},
		},
		{
			name: "dash bytes",
			msg:  `10.0.0.1 - - [08/Feb/2026:12:00:00 +0000] "HEAD / HTTP/1.1" 304 -`,
			expected: []KeyValue{
				{Key: "remote_host", Value: "10.0.0.1"},
				{Key: "method", Value: "head"},
				{Key: "path", Value: "/"},
				{Key: "protocol", Value: "http/1.1"},
				{Key: "status", Value: "304"},
			},
		},
		{
			name: "path with query string",
			msg:  `10.0.0.1 - - [08/Feb/2026:12:00:00 +0000] "GET /api/v1/users?page=2&limit=10 HTTP/1.1" 200 4096`,
			expected: []KeyValue{
				{Key: "remote_host", Value: "10.0.0.1"},
				{Key: "method", Value: "get"},
				{Key: "path", Value: "/api/v1/users?page=2&limit=10"},
				{Key: "protocol", Value: "http/1.1"},
				{Key: "status", Value: "200"},
				{Key: "body_bytes", Value: "4096"},
			},
		},
		{
			name: "combined with dash referer",
			msg:  `10.0.0.1 - - [08/Feb/2026:12:00:00 +0000] "GET / HTTP/1.1" 200 1234 "-" "curl/7.64.1"`,
			expected: []KeyValue{
				{Key: "remote_host", Value: "10.0.0.1"},
				{Key: "method", Value: "get"},
				{Key: "path", Value: "/"},
				{Key: "protocol", Value: "http/1.1"},
				{Key: "status", Value: "200"},
				{Key: "body_bytes", Value: "1234"},
				{Key: "user_agent", Value: "curl/7.64.1"},
			},
		},

		// Rejection cases
		{
			name:     "plain text",
			msg:      "just some log message",
			expected: nil,
		},
		{
			name:     "json",
			msg:      `{"level":"info","msg":"hello"}`,
			expected: nil,
		},
		{
			name:     "logfmt",
			msg:      "level=info msg=hello",
			expected: nil,
		},
		{
			name:     "empty message",
			msg:      "",
			expected: nil,
		},
		{
			name:     "missing request line quotes",
			msg:      `10.0.0.1 - - [08/Feb/2026:12:00:00 +0000] GET /index.html 200 1234`,
			expected: nil,
		},
		{
			name:     "non-numeric status",
			msg:      `10.0.0.1 - - [08/Feb/2026:12:00:00 +0000] "GET / HTTP/1.1" OK 1234`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractAccessLog([]byte(tt.msg))
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
