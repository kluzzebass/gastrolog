package repl

import (
	"fmt"
	"strings"
	"time"

	"gastrolog/internal/query"
)

// parseQueryArgs parses command-line arguments into a Query.
// Returns the parsed query and any error message (empty if successful).
//
// Supported arguments:
//   - Bare words: treated as token searches (AND semantics)
//   - start=TIME: start time bound (RFC3339 or Unix timestamp)
//   - end=TIME: end time bound (RFC3339 or Unix timestamp)
//   - limit=N: maximum results
//   - key=value: filter by key=value in attrs or message body
//   - key=*: filter by key existence
//   - *=value: filter by value existence
func parseQueryArgs(args []string) (query.Query, string) {
	q := query.Query{}
	var tokens []string
	var kvFilters []query.KeyValueFilter

	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			// Bare word without '=' - treat as token search
			tokens = append(tokens, arg)
			continue
		}

		switch k {
		case "start":
			t, err := parseTime(v)
			if err != nil {
				return q, fmt.Sprintf("Invalid start time: %v", err)
			}
			q.Start = t
		case "end":
			t, err := parseTime(v)
			if err != nil {
				return q, fmt.Sprintf("Invalid end time: %v", err)
			}
			q.End = t
		case "token":
			tokens = append(tokens, v)
		case "limit":
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
				return q, fmt.Sprintf("Invalid limit: %v", err)
			}
			q.Limit = n
		default:
			// Treat as key=value filter (searches both attrs and message body)
			// Handle wildcard patterns: key=* and *=value
			key := k
			value := v
			if k == "*" {
				key = "" // *=value pattern
			}
			if v == "*" {
				value = "" // key=* pattern
			}
			kvFilters = append(kvFilters, query.KeyValueFilter{Key: key, Value: value})
		}
	}

	if len(tokens) > 0 {
		q.Tokens = tokens
	}
	if len(kvFilters) > 0 {
		q.KV = kvFilters
	}

	return q, ""
}

// parseTime parses a time string in RFC3339 format or as a Unix timestamp.
func parseTime(s string) (time.Time, error) {
	// Try RFC3339 (with timezone)
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	// Try RFC3339Nano (with timezone)
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	// Try Unix timestamp (must be all digits)
	var unix int64
	if n, err := fmt.Sscanf(s, "%d", &unix); err == nil && n == 1 && fmt.Sprintf("%d", unix) == s {
		return time.Unix(unix, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %s (use RFC3339: 2006-01-02T15:04:05Z or 2006-01-02T15:04:05+01:00)", s)
}
