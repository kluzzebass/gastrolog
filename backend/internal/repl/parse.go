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
// All filter expressions are parsed through the querylang parser.
// Control arguments (start, end, limit) are extracted first.
//
// Examples:
//   - error warn                      → AND of two token predicates
//   - error OR warn                   → OR of two token predicates
//   - (error OR warn) AND NOT debug   → complex boolean expression
//   - level=error host=*              → AND of KV predicates
//   - start=2024-01-01T00:00:00Z error → time-bounded token search
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
			case "source_start":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Sprintf("Invalid source_start time: %v", err)
				}
				q.SourceStart = t
				continue
			case "source_end":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Sprintf("Invalid source_end time: %v", err)
				}
				q.SourceEnd = t
				continue
			case "ingest_start":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Sprintf("Invalid ingest_start time: %v", err)
				}
				q.IngestStart = t
				continue
			case "ingest_end":
				t, err := parseTime(v)
				if err != nil {
					return q, fmt.Sprintf("Invalid ingest_end time: %v", err)
				}
				q.IngestEnd = t
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

	// All filter expressions go through the querylang parser.
	// Simple expressions like "error warn level=info" become AND-only ASTs.
	filterInput := strings.Join(filterArgs, " ")
	expr, err := querylang.Parse(filterInput)
	if err != nil {
		return q, fmt.Sprintf("Parse error: %v", err)
	}
	q.BoolExpr = expr

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
