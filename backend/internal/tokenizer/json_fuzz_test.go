package tokenizer

import "testing"

// FuzzExtractJSON verifies that the JSON field extractor never panics on
// arbitrary byte input and always produces bounded key-value pairs.
func FuzzExtractJSON(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("not json"))
	f.Add([]byte("{}"))
	f.Add([]byte(`{"key":"value"}`))
	f.Add([]byte(`{"a":1,"b":true,"c":null,"d":"str"}`))
	f.Add([]byte(`{"nested":{"deep":{"deeper":"val"}}}`))
	f.Add([]byte(`{"arr":[1,2,3]}`))
	f.Add([]byte(`{"arr":["a","b","c"]}`))
	f.Add([]byte(`{"mixed":[1,"two",true,null,{"k":"v"}]}`))
	f.Add([]byte(`  { "spaced" : "value" }  `))
	f.Add([]byte(`{"":"empty_key"}`)) // JSON allows empty string keys
	f.Add([]byte(`{"k":""}`))        // empty value — filtered by ExtractJSON
	f.Add([]byte(`{"dup":"a","dup":"b"}`))
	f.Add([]byte(`{"float":3.14159265358979323846}`))
	f.Add([]byte(`{"big":99999999999999999}`))
	f.Add([]byte(`{"escape":"hello\nworld"}`))
	f.Add([]byte(`{"unicode":"\u0041\u0042"}`))
	f.Add([]byte("[1,2,3]")) // array, not object
	f.Add([]byte(`{"a":{"b":{"c":{"d":{"e":"deep"}}}}}`)) // exceeds maxJSONDepth

	f.Fuzz(func(t *testing.T, data []byte) {
		result := ExtractJSON(data)
		for _, kv := range result {
			if len(kv.Key) > MaxKeyLength {
				t.Fatalf("key too long: %d > %d", len(kv.Key), MaxKeyLength)
			}
			if len(kv.Value) > MaxValueLength {
				t.Fatalf("value too long: %d > %d", len(kv.Value), MaxValueLength)
			}
		}
	})
}

// FuzzWalkJSON verifies that WalkJSON never panics on arbitrary byte input.
func FuzzWalkJSON(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("{}"))
	f.Add([]byte(`{"service":{"name":"web"}}`))
	f.Add([]byte(`{"spans":[{"name":"x"},{"name":"y"}]}`))
	f.Add([]byte(`{"a.b":"dotted_key"}`))
	f.Add([]byte(`{"deep":{"nesting":{"goes":{"very":"far"}}}}`))
	f.Add([]byte("not json at all"))
	f.Add([]byte(`{"k":"\u0000"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var pathCount, leafCount int
		WalkJSON(data,
			func(path []byte) {
				pathCount++
				if len(path) > MaxPathLength {
					t.Fatalf("path too long: %d > %d", len(path), MaxPathLength)
				}
			},
			func(path []byte, value []byte) {
				leafCount++
				if len(value) > MaxValueLength {
					t.Fatalf("leaf value too long: %d > %d", len(value), MaxValueLength)
				}
			},
		)
	})
}
