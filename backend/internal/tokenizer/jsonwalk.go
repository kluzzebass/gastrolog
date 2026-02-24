package tokenizer

import (
	"encoding/json"
	"strconv"
)

// MaxPathLength is the maximum total byte length of a null-separated JSON path.
// Paths exceeding this are skipped.
const MaxPathLength = 256

// JSONPathCallback is called for each path discovered during a JSON walk.
// path is the null-byte separated path (e.g. "service\x00name").
type JSONPathCallback func(path []byte)

// JSONLeafCallback is called for each leaf scalar discovered during a JSON walk.
// path is the null-byte separated path, value is the lowercased scalar value.
type JSONLeafCallback func(path []byte, value []byte)

// WalkJSON parses msg as a JSON object and walks the structure recursively,
// calling onPath for every path encountered and onLeaf for every leaf scalar.
//
// Path segments are separated by null bytes (\x00). Array elements insert a
// literal "[*]" segment. This encoding is unambiguous:
//
//	{"service": {"name": "x"}}  → path: service\x00name
//	{"service.name": "x"}       → path: service.name
//	{"spans": [{"name": "x"}]}  → path: spans\x00[*]\x00name
//
// Values are lowercased and capped at MaxValueLength bytes.
// Returns false if msg is not a JSON object.
func WalkJSON(msg []byte, onPath JSONPathCallback, onLeaf JSONLeafCallback) bool {
	if len(msg) == 0 {
		return false
	}

	// Quick check: must start with '{' after optional whitespace.
	i := skipSpaces(msg)
	if i >= len(msg) || msg[i] != '{' {
		return false
	}

	var obj map[string]any
	if err := json.Unmarshal(msg, &obj); err != nil {
		return false
	}

	if len(obj) == 0 {
		return false
	}

	// Reusable path buffer.
	pathBuf := make([]byte, 0, 128)
	walkJSONObject(pathBuf, obj, onPath, onLeaf)
	return true
}

func walkJSONObject(prefix []byte, obj map[string]any, onPath JSONPathCallback, onLeaf JSONLeafCallback) {
	for k, v := range obj {
		// Build path: prefix + \x00 + key (or just key if prefix is empty).
		var path []byte
		if len(prefix) == 0 {
			path = append(path, k...)
		} else {
			path = make([]byte, 0, len(prefix)+1+len(k))
			path = append(path, prefix...)
			path = append(path, 0)
			path = append(path, k...)
		}

		if len(path) > MaxPathLength {
			continue
		}

		walkJSONValue(path, v, onPath, onLeaf)
	}
}

func walkJSONValue(path []byte, v any, onPath JSONPathCallback, onLeaf JSONLeafCallback) {
	switch val := v.(type) {
	case string:
		emitPath(path, onPath)
		walkJSONString(path, val, onLeaf)
	case float64:
		emitPath(path, onPath)
		walkJSONFloat(path, val, onLeaf)
	case bool:
		emitPath(path, onPath)
		walkJSONBool(path, val, onLeaf)
	case map[string]any:
		emitPath(path, onPath)
		walkJSONObject(path, val, onPath, onLeaf)
	case []any:
		emitPath(path, onPath)
		walkJSONArray(path, val, onPath, onLeaf)
	case nil:
		emitPath(path, onPath)
	}
}

func emitPath(path []byte, onPath JSONPathCallback) {
	if onPath != nil {
		onPath(path)
	}
}

func walkJSONString(path []byte, val string, onLeaf JSONLeafCallback) {
	if onLeaf == nil || len(val) == 0 || len(val) > MaxValueLength {
		return
	}
	onLeaf(path, toLowerASCIIBytes([]byte(val)))
}

func walkJSONFloat(path []byte, val float64, onLeaf JSONLeafCallback) {
	if onLeaf == nil {
		return
	}
	var s string
	if val == float64(int64(val)) {
		s = strconv.FormatInt(int64(val), 10)
	} else {
		s = strconv.FormatFloat(val, 'f', -1, 64)
	}
	if len(s) <= MaxValueLength {
		onLeaf(path, []byte(s))
	}
}

func walkJSONBool(path []byte, val bool, onLeaf JSONLeafCallback) {
	if onLeaf == nil {
		return
	}
	if val {
		onLeaf(path, []byte("true"))
	} else {
		onLeaf(path, []byte("false"))
	}
}

func walkJSONArray(path []byte, val []any, onPath JSONPathCallback, onLeaf JSONLeafCallback) {
	arrayPath := make([]byte, 0, len(path)+4)
	arrayPath = append(arrayPath, path...)
	arrayPath = append(arrayPath, 0)
	arrayPath = append(arrayPath, "[*]"...)
	if len(arrayPath) > MaxPathLength {
		return
	}
	for _, elem := range val {
		walkJSONValue(arrayPath, elem, onPath, onLeaf)
	}
}

// toLowerASCIIBytes lowercases ASCII bytes in place and returns the slice.
func toLowerASCIIBytes(b []byte) []byte {
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return b
}
