package tokenizer

import (
	"testing"
)

func TestExtractKeyValues(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		expected []KeyValue
	}{
		{
			name:     "simple key=value",
			msg:      "status=500",
			expected: []KeyValue{{Key: "status", Value: "500"}},
		},
		{
			name:     "dotted key",
			msg:      "http.status=200",
			expected: []KeyValue{{Key: "http.status", Value: "200"}},
		},
		{
			name:     "multiple pairs",
			msg:      "status=500 user_id=123",
			expected: []KeyValue{{Key: "status", Value: "500"}, {Key: "user_id", Value: "123"}},
		},
		{
			name:     "key with underscore",
			msg:      "user_id=abc",
			expected: []KeyValue{{Key: "user_id", Value: "abc"}},
		},
		{
			name:     "mixed case key normalized",
			msg:      "Status=OK",
			expected: []KeyValue{{Key: "status", Value: "ok"}},
		},
		{
			name:     "value normalized to lowercase",
			msg:      "path=/Login",
			expected: []KeyValue{{Key: "path", Value: "/login"}},
		},
		{
			name:     "complex dotted key",
			msg:      "some.Kind_of.key=value",
			expected: []KeyValue{{Key: "some.kind_of.key", Value: "value"}},
		},
		{
			name:     "in context",
			msg:      "ERROR: request failed status=500 method=GET",
			expected: []KeyValue{{Key: "status", Value: "500"}, {Key: "method", Value: "get"}},
		},
		{
			name:     "with comma delimiter",
			msg:      "status=200,method=POST",
			expected: []KeyValue{{Key: "status", Value: "200"}, {Key: "method", Value: "post"}},
		},
		{
			name:     "value with path",
			msg:      "path=/api/v1/users",
			expected: []KeyValue{{Key: "path", Value: "/api/v1/users"}},
		},

		// Rejected cases
		{
			name:     "hyphen in key rejected",
			msg:      "foo-bar=value",
			expected: nil,
		},
		{
			name:     "empty segment rejected",
			msg:      "foo..bar=value",
			expected: nil,
		},
		{
			name:     "leading dot rejected",
			msg:      ".foo=value",
			expected: nil,
		},
		{
			name:     "trailing dot rejected",
			msg:      "foo.=value",
			expected: nil,
		},
		{
			name:     "colon acts as delimiter",
			msg:      "foo:bar=value",
			expected: []KeyValue{{Key: "bar", Value: "value"}}, // colon is delimiter, so bar=value is extracted
		},
		{
			name:     "bracket in key rejected",
			msg:      "foo[0]=value",
			expected: nil,
		},
		{
			name:     "json value rejected",
			msg:      `data={"a":1}`,
			expected: nil,
		},
		{
			name:     "nested equals rejected",
			msg:      "x=a=b",
			expected: nil,
		},
		{
			name:     "url params rejected",
			msg:      "params=x=y&z=w",
			expected: nil,
		},
		{
			name:     "empty value rejected",
			msg:      "key=",
			expected: nil,
		},
		{
			name:     "quoted value rejected",
			msg:      `key="value"`,
			expected: nil,
		},
		{
			name:     "key starting with digit rejected",
			msg:      "123=value",
			expected: nil,
		},
		{
			name:     "key too long rejected",
			msg:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=v", // 65 chars
			expected: nil,
		},

		// Edge cases
		{
			name:     "empty message",
			msg:      "",
			expected: nil,
		},
		{
			name:     "no equals",
			msg:      "just some text",
			expected: nil,
		},
		{
			name:     "only equals",
			msg:      "=",
			expected: nil,
		},
		{
			name:     "dedupe within message",
			msg:      "key=value key=value",
			expected: []KeyValue{{Key: "key", Value: "value"}},
		},
		{
			name:     "same key different values",
			msg:      "key=val1 key=val2",
			expected: []KeyValue{{Key: "key", Value: "val1"}, {Key: "key", Value: "val2"}},
		},
		{
			name:     "key at start of message",
			msg:      "level=INFO some message",
			expected: []KeyValue{{Key: "level", Value: "info"}},
		},
		{
			name:     "value at end of message",
			msg:      "some message level=INFO",
			expected: []KeyValue{{Key: "level", Value: "info"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractKeyValues([]byte(tt.msg))
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

func TestIsValidKey(t *testing.T) {
	valid := []string{
		"status",
		"user_id",
		"http.status",
		"some.Kind_of.key",
		"_private",
		"a",
		"A",
		"_",
	}

	invalid := []string{
		"",
		"foo-bar",
		"foo..bar",
		".foo",
		"foo.",
		"123",
		"1abc",
		"foo:bar",
		"foo[0]",
	}

	for _, k := range valid {
		if !isValidKey([]byte(k)) {
			t.Errorf("expected %q to be valid", k)
		}
	}

	for _, k := range invalid {
		if isValidKey([]byte(k)) {
			t.Errorf("expected %q to be invalid", k)
		}
	}
}

func TestIsValidValue(t *testing.T) {
	valid := []string{
		"500",
		"/login",
		"EOF",
		"abc123",
		"hello-world",
		"/api/v1/users",
	}

	invalid := []string{
		"",
		`{"a":1}`,
		"a=b",
		"x&y",
		`"quoted"`,
		"[array]",
	}

	for _, v := range valid {
		if !isValidValue([]byte(v)) {
			t.Errorf("expected %q to be valid", v)
		}
	}

	for _, v := range invalid {
		if isValidValue([]byte(v)) {
			t.Errorf("expected %q to be invalid", v)
		}
	}
}
