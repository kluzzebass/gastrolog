package tokenizer

import "testing"

// FuzzExtractKeyValues verifies that the heuristic key=value parser never panics
// and always produces valid (bounded) key-value pairs.
func FuzzExtractKeyValues(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("key=value"))
	f.Add([]byte("a=b c=d e=f"))
	f.Add([]byte(`level=ERROR status=500 msg="request failed"`))
	f.Add([]byte("host=server-1,env=prod;region=us-east"))
	f.Add([]byte("nested.key.path=deep_value"))
	f.Add([]byte("=value"))            // missing key
	f.Add([]byte("key="))              // empty value
	f.Add([]byte("==="))               // degenerate
	f.Add([]byte("a=b=c=d"))           // chained equals
	f.Add([]byte(`key="quoted value"`))
	f.Add([]byte(`key='single quoted'`))
	f.Add([]byte(`key="unclosed`))
	f.Add([]byte("k=v&a=b"))           // URL params
	f.Add([]byte("k={json}"))          // structured value
	f.Add([]byte("k=[array]"))
	f.Add([]byte("\x00=\x01"))         // binary
	f.Add([]byte("_private=true"))     // underscore-prefixed key

	f.Fuzz(func(t *testing.T, data []byte) {
		result := ExtractKeyValues(data)
		for _, kv := range result {
			if len(kv.Key) == 0 {
				t.Fatal("empty key in result")
			}
			if len(kv.Key) > MaxKeyLength {
				t.Fatalf("key too long: %d > %d", len(kv.Key), MaxKeyLength)
			}
			if len(kv.Value) == 0 {
				t.Fatal("empty value in result")
			}
			if len(kv.Value) > MaxValueLength {
				t.Fatalf("value too long: %d > %d", len(kv.Value), MaxValueLength)
			}
		}
	})
}

// FuzzExtractLogfmt verifies that the logfmt parser never panics
// and always produces bounded results.
func FuzzExtractLogfmt(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("key=value"))
	f.Add([]byte("ts=2024-01-15T10:30:00Z level=info msg=ok"))
	f.Add([]byte(`msg="hello world" err="connection refused"`))
	f.Add([]byte("a=1 b=2 c=3"))
	f.Add([]byte("=nokey"))
	f.Add([]byte("novalue="))
	f.Add([]byte("\x00\xff\xfe"))

	f.Fuzz(func(t *testing.T, data []byte) {
		result := ExtractLogfmt(data)
		for _, kv := range result {
			if len(kv.Key) == 0 {
				t.Fatal("empty key in result")
			}
			if len(kv.Key) > MaxKeyLength {
				t.Fatalf("key too long: %d > %d", len(kv.Key), MaxKeyLength)
			}
			if len(kv.Value) > MaxValueLength {
				t.Fatalf("value too long: %d > %d", len(kv.Value), MaxValueLength)
			}
		}
	})
}

// FuzzCombinedExtract verifies that the combined extractor pipeline never panics.
func FuzzCombinedExtract(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("key=value"))
	f.Add([]byte(`{"level":"info","msg":"ok"}`))
	f.Add([]byte("GET /api/v1/users HTTP/1.1 200 1234"))
	f.Add([]byte("ts=2024-01-15 level=error msg=fail"))

	f.Fuzz(func(t *testing.T, data []byte) {
		extractors := DefaultExtractors()
		_ = CombinedExtract(data, extractors)
	})
}
