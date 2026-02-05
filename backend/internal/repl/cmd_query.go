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
	"gastrolog/internal/querylang"
)

func (r *REPL) cmdQuery(out *strings.Builder, args []string, follow bool) {
	q, errMsg := parseQueryArgs(args)
	if errMsg != "" {
		out.WriteString(errMsg + "\n")
		return
	}

	// Cancel any previous query goroutine (thread-safe)
	r.cancelQuery()

	// Create cancellable context for this query
	queryCtx, queryCancel := context.WithCancel(r.ctx)

	// Create channel and start goroutine to feed records.
	// Records are copied because Raw may point to mmap'd memory.
	ch := make(chan recordResult, 100)

	// Update query state under lock
	r.queryMu.Lock()
	r.queryCancel = queryCancel
	r.resultChan = ch
	r.queryMu.Unlock()

	if follow {
		// Follow mode: stream records from the active chunk in WriteTS order.
		// This is like "tail -f" - we only watch the active chunk where new
		// records arrive, and we track position to avoid re-sending records.
		go r.runFollowMode(queryCtx, ch, q)
	} else {
		// Execute query using the query context (not the REPL lifetime context)
		// so that cancelling the query stops the search.
		seq, getToken, err := r.client.Search(queryCtx, "", q, nil)
		if err != nil {
			fmt.Fprintf(out, "Query error: %v\n", err)
			return
		}

		// Store query state under lock
		r.queryMu.Lock()
		r.lastQuery = &q
		r.getToken = getToken
		r.resumeToken = nil
		r.queryMu.Unlock()

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

	cm := r.client.ChunkManager("")
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

			// Apply filter expression if present
			if q.BoolExpr != nil && !matchesBoolExpr(q.BoolExpr, rec) {
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

// matchesBoolExpr checks if a record matches a boolean expression using DNF evaluation.
// This evaluates primitive predicates only, not recursive AST evaluation.
func matchesBoolExpr(expr querylang.Expr, rec chunk.Record) bool {
	dnf := querylang.ToDNF(expr)
	for _, branch := range dnf.Branches {
		if matchesBranch(&branch, rec) {
			return true
		}
	}
	return false
}

// matchesBranch checks if a record matches a single DNF branch.
func matchesBranch(branch *querylang.Conjunction, rec chunk.Record) bool {
	// Check all positive predicates (AND semantics)
	for _, p := range branch.Positive {
		if !matchesPredicate(p, rec) {
			return false
		}
	}
	// Check all negative predicates (must NOT match any)
	for _, p := range branch.Negative {
		if matchesPredicate(p, rec) {
			return false
		}
	}
	return true
}

// matchesPredicate checks if a record matches a single predicate.
func matchesPredicate(pred *querylang.PredicateExpr, rec chunk.Record) bool {
	switch pred.Kind {
	case querylang.PredToken:
		return matchesToken(rec.Raw, pred.Value)
	case querylang.PredKV:
		return matchesKV(rec.Attrs, rec.Raw, pred.Key, pred.Value)
	case querylang.PredKeyExists:
		return matchesKeyExists(rec.Attrs, rec.Raw, pred.Key)
	case querylang.PredValueExists:
		return matchesValueExists(rec.Attrs, rec.Raw, pred.Value)
	default:
		return false
	}
}

// matchesToken checks if raw data contains a token (case-insensitive substring match).
func matchesToken(raw []byte, token string) bool {
	return strings.Contains(strings.ToLower(string(raw)), strings.ToLower(token))
}

// matchesKV checks if a record has a key=value pair in attrs or message body.
func matchesKV(attrs chunk.Attributes, raw []byte, key, value string) bool {
	// Check attributes
	for k, v := range attrs {
		if strings.EqualFold(k, key) && strings.EqualFold(v, value) {
			return true
		}
	}
	// Check message body (simple substring for now)
	rawLower := strings.ToLower(string(raw))
	pattern := strings.ToLower(key) + "=" + strings.ToLower(value)
	return strings.Contains(rawLower, pattern)
}

// matchesKeyExists checks if a record has a key in attrs or message body.
func matchesKeyExists(attrs chunk.Attributes, raw []byte, key string) bool {
	// Check attributes
	for k := range attrs {
		if strings.EqualFold(k, key) {
			return true
		}
	}
	// Check message body
	rawLower := strings.ToLower(string(raw))
	pattern := strings.ToLower(key) + "="
	return strings.Contains(rawLower, pattern)
}

// matchesValueExists checks if a record has a value in attrs or message body.
func matchesValueExists(attrs chunk.Attributes, raw []byte, value string) bool {
	// Check attributes
	for _, v := range attrs {
		if strings.EqualFold(v, value) {
			return true
		}
	}
	// Check message body
	rawLower := strings.ToLower(string(raw))
	pattern := "=" + strings.ToLower(value)
	return strings.Contains(rawLower, pattern)
}

func (r *REPL) cmdNext(out *strings.Builder, args []string) {
	if !r.hasActiveQuery() {
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
	// Get the channel under lock to avoid race
	ch := r.getResultChan()
	if ch == nil {
		out.WriteString("No active query.\n")
		return
	}

	printed := 0
	for {
		if count > 0 && printed >= count {
			break
		}
		result, ok := <-ch
		if !ok {
			if printed == 0 {
				out.WriteString("No more results.\n")
			} else {
				fmt.Fprintf(out, "--- %d records (end of results) ---\n", printed)
			}
			r.setResultChan(nil)
			return
		}
		if result.err != nil {
			if errors.Is(result.err, query.ErrInvalidResumeToken) {
				out.WriteString("Resume token invalid (chunk deleted). Use 'reset' and re-query.\n")
				r.setResultChan(nil)
				return
			}
			fmt.Fprintf(out, "Error: %v\n", result.err)
			return
		}

		r.printRecord(out, result.rec)
		printed++
	}

	if printed > 0 {
		fmt.Fprintf(out, "--- %d records shown. Press Enter for more. ---\n", printed)
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
	r.queryMu.Lock()
	r.lastQuery = nil
	r.resumeToken = nil
	r.resultChan = nil
	r.getToken = nil
	r.queryMu.Unlock()
	out.WriteString("Query state cleared.\n")
}
