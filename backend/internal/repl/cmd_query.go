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
	ch := make(chan recordResult, 100)

	// Update query state under lock
	r.queryMu.Lock()
	r.queryCancel = queryCancel
	r.resultChan = ch
	r.queryMu.Unlock()

	// Execute query
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

// cmdFollow runs a query in follow mode, streaming results until interrupted.
func (r *REPL) cmdFollow(out *strings.Builder, args []string) {
	q, errMsg := parseQueryArgs(args)
	if errMsg != "" {
		out.WriteString(errMsg + "\n")
		return
	}

	// Cancel any previous query
	r.cancelQuery()

	// Create cancellable context
	queryCtx, queryCancel := context.WithCancel(r.ctx)

	r.queryMu.Lock()
	r.queryCancel = queryCancel
	r.queryMu.Unlock()

	out.WriteString("Following... (Ctrl+C to stop)\n")

	// Run follow mode synchronously (blocking)
	r.runFollowModeBlocking(queryCtx, q, out)

	out.WriteString("Follow stopped.\n")
}

// runFollowModeBlocking streams new records synchronously until context is cancelled.
func (r *REPL) runFollowModeBlocking(ctx context.Context, q query.Query, out *strings.Builder) {
	cm := r.client.ChunkManager("")
	if cm == nil {
		out.WriteString("Error: chunk manager not found\n")
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
				fmt.Fprintf(out, "Error: %v\n", err)
				break
			}

			nextPos = ref.Pos + 1

			// Apply filter expression if present
			if q.BoolExpr != nil && !matchesBoolExpr(q.BoolExpr, rec) {
				continue
			}

			// Print immediately
			fmt.Print(r.formatRecord(rec) + "\n")
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
	for _, p := range branch.Positive {
		if !matchesPredicate(p, rec) {
			return false
		}
	}
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

func matchesToken(raw []byte, token string) bool {
	return strings.Contains(strings.ToLower(string(raw)), strings.ToLower(token))
}

func matchesKV(attrs chunk.Attributes, raw []byte, key, value string) bool {
	for k, v := range attrs {
		if strings.EqualFold(k, key) && strings.EqualFold(v, value) {
			return true
		}
	}
	rawLower := strings.ToLower(string(raw))
	pattern := strings.ToLower(key) + "=" + strings.ToLower(value)
	return strings.Contains(rawLower, pattern)
}

func matchesKeyExists(attrs chunk.Attributes, raw []byte, key string) bool {
	for k := range attrs {
		if strings.EqualFold(k, key) {
			return true
		}
	}
	rawLower := strings.ToLower(string(raw))
	pattern := strings.ToLower(key) + "="
	return strings.Contains(rawLower, pattern)
}

func matchesValueExists(attrs chunk.Attributes, raw []byte, value string) bool {
	for _, v := range attrs {
		if strings.EqualFold(v, value) {
			return true
		}
	}
	rawLower := strings.ToLower(string(raw))
	pattern := "=" + strings.ToLower(value)
	return strings.Contains(rawLower, pattern)
}

func (r *REPL) cmdNext(out *strings.Builder, args []string) {
	if !r.hasActiveQuery() {
		out.WriteString("No active query. Use 'query' first.\n")
		return
	}

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

func (r *REPL) cmdReset(out *strings.Builder) {
	r.cancelQuery()
	r.clearQueryState()
	out.WriteString("Query state cleared.\n")
}

func (r *REPL) formatRecord(rec chunk.Record) string {
	ts := rec.IngestTS.Format(time.RFC3339Nano)

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
