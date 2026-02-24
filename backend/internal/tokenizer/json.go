package tokenizer

import (
	"encoding/json"
	"strconv"
)

// maxJSONDepth is the maximum nesting depth for JSON extraction.
// Prevents pathological deep structures from consuming too much time.
const maxJSONDepth = 4

// ExtractJSON parses a log message as a JSON object and extracts key=value
// pairs for scalar fields. Nested objects produce dot-separated key paths.
// Array elements are emitted with the array's key (e.g., tags=web, tags=api).
// Returns nil if the message is not a JSON object.
func ExtractJSON(msg []byte) []KeyValue {
	if len(msg) == 0 {
		return nil
	}

	// Quick check: must start with '{' after optional whitespace.
	i := skipSpaces(msg)
	if i >= len(msg) || msg[i] != '{' {
		return nil
	}

	// Parse as map[string]any.
	var obj map[string]any
	if err := json.Unmarshal(msg, &obj); err != nil {
		return nil
	}

	if len(obj) == 0 {
		return nil
	}

	var result []KeyValue
	seen := make(map[string]struct{})
	extractJSONObject(&result, seen, "", obj, 0)

	if len(result) == 0 {
		return nil
	}
	return result
}

func extractJSONObject(result *[]KeyValue, seen map[string]struct{}, prefix string, obj map[string]any, depth int) {
	if depth >= maxJSONDepth {
		return
	}

	for k, v := range obj {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}

		if len(fullKey) > MaxKeyLength {
			continue
		}

		extractJSONValue(result, seen, fullKey, v, depth)
	}
}

func extractJSONValue(result *[]KeyValue, seen map[string]struct{}, key string, v any, depth int) {
	switch val := v.(type) {
	case string:
		if len(val) > 0 && len(val) <= MaxValueLength {
			addJSONPair(result, seen, key, val)
		}
	case float64:
		extractJSONFloat(result, seen, key, val)
	case bool:
		addJSONPair(result, seen, key, formatBool(val))
	case map[string]any:
		extractJSONObject(result, seen, key, val, depth+1)
	case []any:
		extractJSONArray(result, seen, key, val, depth)
	case nil:
	}
}

func extractJSONFloat(result *[]KeyValue, seen map[string]struct{}, key string, val float64) {
	s := formatFloat(val)
	if len(s) <= MaxValueLength {
		addJSONPair(result, seen, key, s)
	}
}

func extractJSONArray(result *[]KeyValue, seen map[string]struct{}, key string, val []any, depth int) {
	if depth+1 >= maxJSONDepth {
		return
	}
	for _, elem := range val {
		extractJSONArrayElem(result, seen, key, elem)
	}
}

func extractJSONArrayElem(result *[]KeyValue, seen map[string]struct{}, key string, elem any) {
	switch ev := elem.(type) {
	case string:
		if len(ev) > 0 && len(ev) <= MaxValueLength {
			addJSONPair(result, seen, key, ev)
		}
	case float64:
		addJSONPair(result, seen, key, formatFloat(ev))
	case bool:
		addJSONPair(result, seen, key, formatBool(ev))
	}
}

func formatFloat(val float64) string {
	if val == float64(int64(val)) {
		return strconv.FormatInt(int64(val), 10)
	}
	return strconv.FormatFloat(val, 'f', -1, 64)
}

func formatBool(val bool) string {
	if val {
		return "true"
	}
	return "false"
}

func addJSONPair(result *[]KeyValue, seen map[string]struct{}, key, value string) {
	keyLower := ToLowerASCII([]byte(key))
	valLower := ToLowerASCII([]byte(value))

	dedup := keyLower + "\x00" + valLower
	if _, ok := seen[dedup]; ok {
		return
	}
	seen[dedup] = struct{}{}
	*result = append(*result, KeyValue{Key: keyLower, Value: valLower})
}
