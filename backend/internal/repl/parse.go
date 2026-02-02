package repl

import (
	"fmt"
	"strings"
	"time"

	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// parseQueryArgs parses command-line arguments into a Query.
// Returns the parsed query and any error message (empty if successful).
//
// Supported arguments (legacy mode):
//   - Bare words: treated as token searches (AND semantics)
//   - start=TIME: start time bound (RFC3339 or Unix timestamp)
//   - end=TIME: end time bound (RFC3339 or Unix timestamp)
//   - limit=N: maximum results
//   - key=value: filter by key=value in attrs or message body
//   - key=*: filter by key existence
//   - *=value: filter by value existence
//
// Boolean query mode (triggered by parentheses, OR, or NOT):
//   - (error OR warn) AND NOT debug
//   - error OR "disk full"
//   - message="out of memory" OR level=error
//
// Time bounds and limit are extracted first, then the rest is parsed as a boolean expression.
func parseQueryArgs(args []string) (query.Query, string) {
	if len(args) == 0 {
		return query.Query{}, ""
	}

	// First pass: extract control arguments (start, end, limit) and collect the rest.
	q := query.Query{}
	var filterArgs []string

	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if ok {
			switch k {
			case "start":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Sprintf("Invalid start time: %v", err)
				}
				q.Start = t
				continue
			case "end":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Sprintf("Invalid end time: %v", err)
				}
				q.End = t
				continue
			case "limit":
				var n int
				if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
					return q, fmt.Sprintf("Invalid limit: %v", err)
				}
				q.Limit = n
				continue
			}
		}
		// Not a control argument, collect for filter parsing.
		filterArgs = append(filterArgs, arg)
	}

	if len(filterArgs) == 0 {
		return q, ""
	}

	// Join remaining args and check if it looks like a boolean query.
	filterInput := strings.Join(filterArgs, " ")

	if looksLikeBoolean(filterInput) {
		// Parse as boolean expression.
		expr, err := querylang.Parse(filterInput)
		if err != nil {
			return q, fmt.Sprintf("Parse error: %v", err)
		}
		q.BoolExpr = expr
		return q, ""
	}

	// Legacy mode: parse as simple token and KV filters.
	return parseLegacyFilters(q, filterArgs)
}

// looksLikeBoolean returns true if the input appears to use boolean query syntax.
// Detection is based on presence of parentheses or boolean keywords.
func looksLikeBoolean(input string) bool {
	// Check for parentheses.
	if strings.Contains(input, "(") || strings.Contains(input, ")") {
		return true
	}

	// Check for boolean keywords as separate words.
	// We need to be careful not to match "error" as containing "or".
	lower := strings.ToLower(input)
	words := strings.Fields(lower)
	for _, word := range words {
		if word == "or" || word == "and" || word == "not" {
			return true
		}
	}

	// Check for quoted strings (indicates advanced syntax).
	if strings.Contains(input, `"`) || strings.Contains(input, `'`) {
		return true
	}

	return false
}

// parseLegacyFilters parses filter arguments using the legacy (non-boolean) syntax.
func parseLegacyFilters(q query.Query, args []string) (query.Query, string) {
	var tokens []string
	var kvFilters []query.KeyValueFilter

	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			// Bare word without '=' - treat as token search
			tokens = append(tokens, arg)
			continue
		}

		// Handle token= prefix for explicit token specification.
		if k == "token" {
			tokens = append(tokens, v)
			continue
		}

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
