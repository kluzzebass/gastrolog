package repl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"gastrolog/internal/chunk"
)

// cmdQuery executes a query. If usePager is true, results are displayed in an
// interactive pager with fetch-more support. Otherwise, results are written to
// out and the query state is preserved for subsequent 'next' commands.
func (r *REPL) cmdQuery(out *strings.Builder, args []string, usePager bool) {
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

	if usePager {
		// Interactive mode: use pager with fetch-more support
		r.queryWithPager()
	} else {
		// Non-interactive mode: fetch and print, keep query state for 'next'
		r.fetchAndPrint(out)
	}
}

// cmdFollow runs a query in follow mode, streaming results in a live pager.
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

	// Start the follow stream
	seq, err := r.client.Follow(queryCtx, "", q)
	if err != nil {
		queryCancel()
		fmt.Fprintf(out, "Follow error: %v\n", err)
		return
	}

	// Create channel for live pager
	linesChan := make(chan string, 100)

	// Feed records to channel
	go func() {
		defer close(linesChan)
		defer queryCancel()
		for rec, err := range seq {
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				linesChan <- fmt.Sprintf("Error: %v", err)
				continue
			}
			linesChan <- r.formatRecord(rec)
		}
	}()

	// Run live pager (blocks until user quits)
	r.pagerLive(linesChan)

	// Cancel the query when pager exits
	queryCancel()
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
	// Format timestamp with fixed width (always 30 chars: 2006-01-02T15:04:05.000000000Z)
	ts := rec.IngestTS.Format("2006-01-02T15:04:05.000000000Z07:00")

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

	raw := strings.TrimRight(string(rec.Raw), "\r\n")

	// Handle embedded newlines: indent continuation lines to align with the message start
	// Prefix is: "timestamp attrs " - we'll pad continuation lines with spaces
	prefix := ts + " " + attrStr + " "
	if strings.ContainsAny(raw, "\r\n") {
		indent := strings.Repeat(" ", len(prefix))
		lines := strings.Split(raw, "\n")
		for i := range lines {
			lines[i] = strings.TrimRight(lines[i], "\r")
		}
		raw = strings.Join(lines, "\n"+indent)
	}

	return prefix + raw
}
