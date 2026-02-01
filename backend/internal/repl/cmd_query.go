package repl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
	"gastrolog/internal/tokenizer"
)

func (r *REPL) cmdQuery(out *strings.Builder, args []string, follow bool) {
	q := query.Query{}
	var tokens []string
	var kvFilters []query.KeyValueFilter

	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			fmt.Fprintf(out, "Invalid filter: %s (expected key=value)\n", arg)
			return
		}

		switch k {
		case "start":
			t, err := parseTime(v)
			if err != nil {
				fmt.Fprintf(out, "Invalid start time: %v\n", err)
				return
			}
			q.Start = t
		case "end":
			t, err := parseTime(v)
			if err != nil {
				fmt.Fprintf(out, "Invalid end time: %v\n", err)
				return
			}
			q.End = t
		case "token":
			tokens = append(tokens, v)
		case "limit":
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
				fmt.Fprintf(out, "Invalid limit: %v\n", err)
				return
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

	// Cancel any previous query goroutine
	if r.queryCancel != nil {
		r.queryCancel()
	}

	// Create cancellable context for this query
	queryCtx, queryCancel := context.WithCancel(r.ctx)
	r.queryCancel = queryCancel

	// Create channel and start goroutine to feed records.
	// Records are copied because Raw may point to mmap'd memory.
	ch := make(chan recordResult, 100)
	r.resultChan = ch

	if follow {
		// Follow mode: stream records from the active chunk in WriteTS order.
		// This is like "tail -f" - we only watch the active chunk where new
		// records arrive, and we track position to avoid re-sending records.
		go r.runFollowMode(queryCtx, ch, q)
	} else {
		// Execute query
		seq, getToken, err := r.orch.Search(r.ctx, r.store, q, nil)
		if err != nil {
			fmt.Fprintf(out, "Query error: %v\n", err)
			return
		}

		// Store query state
		r.lastQuery = &q
		r.getToken = getToken
		r.resumeToken = nil

		go func() {
			defer close(ch)
			for rec, err := range seq {
				select {
				case <-queryCtx.Done():
					return
				default:
				}
				if err != nil {
					ch <- recordResult{err: err}
					continue
				}
				ch <- recordResult{rec: rec.Copy()}
			}
		}()

		r.fetchAndPrint(out)
	}
}

// runFollowMode streams new records from the active chunk as they arrive (like tail -f).
// It does NOT show existing records - use 'query' for that.
func (r *REPL) runFollowMode(ctx context.Context, ch chan<- recordResult, q query.Query) {
	defer close(ch)

	cm := r.orch.ChunkManager(r.store)
	if cm == nil {
		ch <- recordResult{err: errors.New("chunk manager not found for store")}
		return
	}

	// Start from current end of active chunk - only show NEW records
	var currentChunkID chunk.ChunkID
	var nextPos uint64

	if active := cm.Active(); active != nil {
		currentChunkID = active.ID
		// Find current end position
		if cursor, err := cm.OpenCursor(active.ID); err == nil {
			for {
				_, ref, err := cursor.Next()
				if errors.Is(err, chunk.ErrNoMoreRecords) {
					break
				}
				if err != nil {
					break
				}
				nextPos = ref.Pos + 1
			}
			cursor.Close()
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Get the active chunk
		active := cm.Active()
		if active == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		// If chunk changed (sealed and new one created), start from beginning of new chunk
		if active.ID != currentChunkID {
			currentChunkID = active.ID
			nextPos = 0
		}

		// Open cursor and seek to our position
		cursor, err := cm.OpenCursor(currentChunkID)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		if nextPos > 0 {
			if err := cursor.Seek(chunk.RecordRef{ChunkID: currentChunkID, Pos: nextPos}); err != nil {
				cursor.Close()
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
		}

		// Read new records
		for {
			rec, ref, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				ch <- recordResult{err: err}
				break
			}

			nextPos = ref.Pos + 1

			// Apply token filter (AND semantics)
			if len(q.Tokens) > 0 && !matchesAllTokens(rec.Raw, q.Tokens) {
				continue
			}

			// Apply key=value filter (AND semantics, OR within each filter for attrs vs message)
			if len(q.KV) > 0 && !matchesAllKeyValues(rec.Attrs, rec.Raw, q.KV) {
				continue
			}

			select {
			case <-ctx.Done():
				cursor.Close()
				return
			case ch <- recordResult{rec: rec.Copy()}:
			}
		}

		cursor.Close()

		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// matchesAllTokens checks if the raw data contains all query tokens.
func matchesAllTokens(raw []byte, tokens []string) bool {
	rawLower := strings.ToLower(string(raw))
	for _, tok := range tokens {
		if !strings.Contains(rawLower, strings.ToLower(tok)) {
			return false
		}
	}
	return true
}

// matchesAllKeyValues checks if all query key=value pairs are found in either
// the record's attributes or the message body (OR semantics per pair, AND across pairs).
//
// Supports wildcard patterns:
//   - Key="foo", Value="bar" - exact match for foo=bar
//   - Key="foo", Value=""    - match if key "foo" exists (any value)
//   - Key="", Value="bar"    - match if any key has value "bar"
func matchesAllKeyValues(recAttrs chunk.Attributes, raw []byte, queryFilters []query.KeyValueFilter) bool {
	if len(queryFilters) == 0 {
		return true
	}

	// Lazily extract key=value pairs from message body only if needed.
	var msgPairs map[string]map[string]struct{}
	var msgValues map[string]struct{} // all values (for *=value pattern)
	getMsgPairs := func() map[string]map[string]struct{} {
		if msgPairs == nil {
			pairs := tokenizer.ExtractKeyValues(raw)
			msgPairs = make(map[string]map[string]struct{})
			msgValues = make(map[string]struct{})
			for _, kv := range pairs {
				if msgPairs[kv.Key] == nil {
					msgPairs[kv.Key] = make(map[string]struct{})
				}
				// Keys are already lowercase from extractor, values are preserved.
				// For matching, we lowercase both.
				valLower := strings.ToLower(kv.Value)
				msgPairs[kv.Key][valLower] = struct{}{}
				msgValues[valLower] = struct{}{}
			}
		}
		return msgPairs
	}
	getMsgValues := func() map[string]struct{} {
		getMsgPairs() // ensure msgValues is populated
		return msgValues
	}

	// Check all filters (AND semantics across filters).
	for _, f := range queryFilters {
		keyLower := strings.ToLower(f.Key)
		valLower := strings.ToLower(f.Value)

		if f.Key == "" && f.Value == "" {
			// Both empty - matches everything, skip this filter
			continue
		} else if f.Value == "" {
			// Key only: key=* pattern (key exists with any value)
			// Check attrs
			found := false
			for k := range recAttrs {
				if strings.EqualFold(k, f.Key) {
					found = true
					break
				}
			}
			if found {
				continue
			}
			// Check message body
			pairs := getMsgPairs()
			if _, ok := pairs[keyLower]; ok {
				continue
			}
			return false // Key not found in either location
		} else if f.Key == "" {
			// Value only: *=value pattern (any key has this value)
			// Check attrs
			found := false
			for _, v := range recAttrs {
				if strings.EqualFold(v, f.Value) {
					found = true
					break
				}
			}
			if found {
				continue
			}
			// Check message body
			values := getMsgValues()
			if _, ok := values[valLower]; ok {
				continue
			}
			return false // Value not found in either location
		} else {
			// Both key and value: exact key=value match
			// Check attributes first (cheaper).
			if v, ok := recAttrs[f.Key]; ok && strings.EqualFold(v, f.Value) {
				continue // Found in attrs, this filter passes.
			}

			// Check message body.
			pairs := getMsgPairs()
			if values, ok := pairs[keyLower]; ok {
				if _, found := values[valLower]; found {
					continue // Found in message, this filter passes.
				}
			}

			return false // Not found in either location.
		}
	}
	return true
}

func (r *REPL) cmdNext(out *strings.Builder, args []string) {
	if r.resultChan == nil {
		out.WriteString("No active query. Use 'query' first.\n")
		return
	}

	// Allow override for this call only
	if len(args) > 0 {
		var count int
		if _, err := fmt.Sscanf(args[0], "%d", &count); err != nil {
			fmt.Fprintf(out, "Invalid count: %v\n", err)
			return
		}
		r.fetchAndPrintN(out, count)
		return
	}

	r.fetchAndPrint(out)
}

func (r *REPL) fetchAndPrint(out *strings.Builder) {
	if r.pageSize == 0 {
		// No paging - fetch all
		r.fetchAndPrintN(out, 0)
	} else {
		r.fetchAndPrintN(out, r.pageSize)
	}
}

func (r *REPL) fetchAndPrintN(out *strings.Builder, count int) {
	if r.resultChan == nil {
		out.WriteString("No active query.\n")
		return
	}

	printed := 0
	for {
		if count > 0 && printed >= count {
			break
		}
		result, ok := <-r.resultChan
		if !ok {
			if printed == 0 {
				out.WriteString("No more results.\n")
			} else {
				fmt.Fprintf(out, "--- %d records (end of results) ---\n", printed)
			}
			r.resultChan = nil
			return
		}
		if result.err != nil {
			if errors.Is(result.err, query.ErrInvalidResumeToken) {
				out.WriteString("Resume token invalid (chunk deleted). Use 'reset' and re-query.\n")
				r.resultChan = nil
				return
			}
			fmt.Fprintf(out, "Error: %v\n", result.err)
			return
		}

		r.printRecord(out, result.rec)
		printed++
	}

	if printed > 0 {
		fmt.Fprintf(out, "--- %d records shown. Use 'next' for more. ---\n", printed)
	}
}

func (r *REPL) formatRecord(rec chunk.Record) string {
	// Format: TIMESTAMP ATTRS RAW
	ts := rec.IngestTS.Format(time.RFC3339Nano)

	// Format attributes
	var attrStr string
	if len(rec.Attrs) > 0 {
		keys := make([]string, 0, len(rec.Attrs))
		for k := range rec.Attrs {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		var attrs []string
		for _, k := range keys {
			attrs = append(attrs, k+"="+rec.Attrs[k])
		}
		attrStr = strings.Join(attrs, ",")
	} else {
		attrStr = "-"
	}

	return fmt.Sprintf("%s %s %s", ts, attrStr, string(rec.Raw))
}

func (r *REPL) printRecord(out *strings.Builder, rec chunk.Record) {
	out.WriteString(r.formatRecord(rec))
	out.WriteByte('\n')
}

func (r *REPL) cmdReset(out *strings.Builder) {
	r.lastQuery = nil
	r.resumeToken = nil
	r.resultChan = nil
	r.getToken = nil
	out.WriteString("Query state cleared.\n")
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
