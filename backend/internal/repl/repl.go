// Package repl provides an in-process REPL for interacting with a running
// GastroLog system. The REPL is a client of the orchestrator and query engine,
// not their owner. It only observes and queries via public APIs.
//
// The REPL does not control the system. It does not start components, stop
// components, manage lifecycles, own goroutines, or coordinate ingestion.
package repl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/source"
)

// REPL provides an interactive read-eval-print loop for querying a running
// GastroLog system. It interacts only through exported, stable interfaces.
type REPL struct {
	orch    *orchestrator.Orchestrator
	sources *source.Registry

	// I/O
	in  *bufio.Scanner
	out io.Writer

	// Query state
	store       string                             // target store for queries
	lastQuery   *query.Query                       // last executed query
	resumeToken *query.ResumeToken                 // resume token for pagination
	resultIter  func() (chunk.Record, error, bool) // current result iterator
	getToken    func() *query.ResumeToken          // get resume token function

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a REPL attached to an already-running system.
// All components must be live and concurrent.
func New(orch *orchestrator.Orchestrator, sources *source.Registry, in io.Reader, out io.Writer) *REPL {
	ctx, cancel := context.WithCancel(context.Background())
	return &REPL{
		orch:    orch,
		sources: sources,
		in:      bufio.NewScanner(in),
		out:     out,
		store:   "default",
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Run starts the REPL loop. It blocks until the user exits or context is cancelled.
func (r *REPL) Run() error {
	r.printf("GastroLog REPL. Type 'help' for commands.\n")
	r.printf("> ")

	for r.in.Scan() {
		if err := r.ctx.Err(); err != nil {
			return err
		}

		line := strings.TrimSpace(r.in.Text())
		if line == "" {
			r.printf("> ")
			continue
		}

		if exit := r.execute(line); exit {
			return nil
		}

		r.printf("> ")
	}

	if err := r.in.Err(); err != nil {
		return err
	}
	return nil
}

// execute parses and executes a single command. Returns true if REPL should exit.
func (r *REPL) execute(line string) bool {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return false
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "help":
		r.cmdHelp()
	case "sources":
		r.cmdSources(args)
	case "query":
		r.cmdQuery(args)
	case "next":
		r.cmdNext(args)
	case "reset":
		r.cmdReset()
	case "store":
		r.cmdStore(args)
	case "exit", "quit":
		return true
	default:
		r.printf("Unknown command: %s. Type 'help' for commands.\n", cmd)
	}

	return false
}

func (r *REPL) cmdHelp() {
	r.printf(`Commands:
  help                     Show this help
  sources [key=value ...]  List sources matching filters
  store [name]             Get or set target store (default: "default")
  query key=value ...      Execute a query with filters
  next [count]             Fetch next page of results (default: 10)
  reset                    Clear current query state
  exit                     Exit the REPL

Query filters:
  start=TIME               Start time (RFC3339 or Unix timestamp)
  end=TIME                 End time (RFC3339 or Unix timestamp)
  source=ID                Filter by source ID (can repeat)
  token=WORD               Filter by token (can repeat, AND semantics)
  limit=N                  Maximum results

Examples:
  sources env=prod
  query start=2024-01-01T00:00:00Z end=2024-01-02T00:00:00Z token=error
  next 20
`)
}

func (r *REPL) cmdSources(args []string) {
	filters := make(map[string]string)
	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			r.printf("Invalid filter: %s (expected key=value)\n", arg)
			return
		}
		filters[k] = v
	}

	ids := r.sources.Query(filters)
	if len(ids) == 0 {
		r.printf("No sources found.\n")
		return
	}

	for _, id := range ids {
		src, ok := r.sources.Get(id)
		if !ok {
			continue
		}
		r.printf("%s", id.String())
		if len(src.Attributes) > 0 {
			var attrs []string
			for k, v := range src.Attributes {
				attrs = append(attrs, fmt.Sprintf("%s=%s", k, v))
			}
			r.printf(" %s", strings.Join(attrs, " "))
		}
		r.printf("\n")
	}
}

func (r *REPL) cmdStore(args []string) {
	if len(args) == 0 {
		r.printf("Current store: %s\n", r.store)
		return
	}
	r.store = args[0]
	r.printf("Store set to: %s\n", r.store)
}

func (r *REPL) cmdQuery(args []string) {
	q := query.Query{}
	var sourceIDs []chunk.SourceID
	var tokens []string

	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			r.printf("Invalid filter: %s (expected key=value)\n", arg)
			return
		}

		switch k {
		case "start":
			t, err := parseTime(v)
			if err != nil {
				r.printf("Invalid start time: %v\n", err)
				return
			}
			q.Start = t
		case "end":
			t, err := parseTime(v)
			if err != nil {
				r.printf("Invalid end time: %v\n", err)
				return
			}
			q.End = t
		case "source":
			id, err := chunk.ParseSourceID(v)
			if err != nil {
				r.printf("Invalid source ID: %v\n", err)
				return
			}
			sourceIDs = append(sourceIDs, id)
		case "token":
			tokens = append(tokens, v)
		case "limit":
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
				r.printf("Invalid limit: %v\n", err)
				return
			}
			q.Limit = n
		default:
			r.printf("Unknown filter: %s\n", k)
			return
		}
	}

	if len(sourceIDs) > 0 {
		q.Sources = sourceIDs
	}
	if len(tokens) > 0 {
		q.Tokens = tokens
	}

	// Execute query
	seq, getToken, err := r.orch.Search(r.ctx, r.store, q, nil)
	if err != nil {
		r.printf("Query error: %v\n", err)
		return
	}

	// Store query state
	r.lastQuery = &q
	r.getToken = getToken
	r.resumeToken = nil

	// Create iterator wrapper
	next, stop := pullIter(seq)
	r.resultIter = next
	_ = stop // We don't call stop; iterator exhausts naturally or on reset

	// Fetch first page
	r.fetchAndPrint(10)
}

func (r *REPL) cmdNext(args []string) {
	if r.resultIter == nil {
		r.printf("No active query. Use 'query' first.\n")
		return
	}

	count := 10
	if len(args) > 0 {
		if _, err := fmt.Sscanf(args[0], "%d", &count); err != nil {
			r.printf("Invalid count: %v\n", err)
			return
		}
	}

	r.fetchAndPrint(count)
}

func (r *REPL) fetchAndPrint(count int) {
	if r.resultIter == nil {
		r.printf("No active query.\n")
		return
	}

	printed := 0
	for i := 0; i < count; i++ {
		rec, err, ok := r.resultIter()
		if !ok {
			if printed == 0 {
				r.printf("No more results.\n")
			}
			r.resultIter = nil
			return
		}
		if err != nil {
			if errors.Is(err, query.ErrInvalidResumeToken) {
				r.printf("Resume token invalid (chunk deleted). Use 'reset' and re-query.\n")
				r.resultIter = nil
				return
			}
			r.printf("Error: %v\n", err)
			return
		}

		r.printRecord(rec)
		printed++
	}

	if printed > 0 {
		r.printf("--- %d records shown. Use 'next' for more. ---\n", printed)
	}
}

func (r *REPL) printRecord(rec chunk.Record) {
	// Format: TIMESTAMP SOURCE_ID RAW
	ts := rec.WriteTS.Format(time.RFC3339Nano)
	r.printf("%s %s %s\n", ts, rec.SourceID.String(), string(rec.Raw))
}

func (r *REPL) cmdReset() {
	r.lastQuery = nil
	r.resumeToken = nil
	r.resultIter = nil
	r.getToken = nil
	r.printf("Query state cleared.\n")
}

func (r *REPL) printf(format string, args ...any) {
	fmt.Fprintf(r.out, format, args...)
}

// parseTime parses a time string in RFC3339 format or as a Unix timestamp.
func parseTime(s string) (time.Time, error) {
	// Try RFC3339 first
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	// Try RFC3339Nano
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	// Try Unix timestamp (seconds)
	var unix int64
	if _, err := fmt.Sscanf(s, "%d", &unix); err == nil {
		return time.Unix(unix, 0), nil
	}
	return time.Time{}, fmt.Errorf("cannot parse time: %s", s)
}

// pullIter converts an iter.Seq2 into a pull-style iterator.
// Returns a next function and a stop function.
func pullIter[T any, E error](seq func(yield func(T, E) bool)) (next func() (T, E, bool), stop func()) {
	ch := make(chan struct {
		val T
		err E
	})
	done := make(chan struct{})

	go func() {
		defer close(ch)
		seq(func(val T, err E) bool {
			select {
			case ch <- struct {
				val T
				err E
			}{val, err}:
				return true
			case <-done:
				return false
			}
		})
	}()

	return func() (T, E, bool) {
			result, ok := <-ch
			return result.val, result.err, ok
		}, func() {
			close(done)
		}
}
