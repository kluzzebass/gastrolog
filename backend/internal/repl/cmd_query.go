package repl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
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
	defer queryCancel()

	r.queryMu.Lock()
	r.queryCancel = queryCancel
	r.queryMu.Unlock()

	// Set up signal handler for Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	go func() {
		select {
		case <-sigCh:
			queryCancel()
		case <-queryCtx.Done():
		}
	}()

	fmt.Println("Following... (Ctrl+C to stop)")

	// Start the follow stream
	seq, err := r.client.Follow(queryCtx, "", q)
	if err != nil {
		fmt.Printf("Follow error: %v\n", err)
		return
	}

	// Stream records until cancelled
	for rec, err := range seq {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fmt.Println(r.formatRecord(rec))
	}

	fmt.Println("\nFollow stopped.")
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
