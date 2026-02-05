// Package repl provides an interactive REPL for interacting with a GastroLog
// system. The REPL communicates through a Client interface, which can be
// backed by either a direct in-process connection (embedded mode) or a
// remote gRPC connection (client-server mode).
//
// The REPL does not control the system. It only observes and queries.
package repl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/chzyer/readline"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
)

const defaultPageSize = 50

// recordResult holds a record or error from the iterator channel.
type recordResult struct {
	rec chunk.Record
	err error
}

// REPL provides an interactive read-eval-print loop for querying a GastroLog
// system. It communicates through a Client interface.
type REPL struct {
	client Client

	// Query state (protected by queryMu)
	queryMu     sync.Mutex
	lastQuery   *query.Query              // last executed query
	resumeToken *query.ResumeToken        // resume token for pagination
	resultChan  chan recordResult         // channel for receiving records
	getToken    func() *query.ResumeToken // get resume token function
	queryCancel context.CancelFunc        // cancels the active query goroutine

	// Config
	pageSize int // records per page (0 = no paging, show all)

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// commands is the list of available REPL commands for tab completion.
var commands = []string{"help", "stores", "query", "follow", "next", "reset", "set", "chunks", "chunk", "indexes", "analyze", "explain", "stats", "status", "exit", "quit"}

// queryFilters is the list of query filter keys for tab completion.
var queryFilters = []string{"start=", "end=", "limit=", "store="}

// New creates a REPL using the provided client.
func New(client Client) *REPL {
	ctx, cancel := context.WithCancel(context.Background())

	return &REPL{
		client:   client,
		pageSize: defaultPageSize,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// completer provides tab completion for the REPL.
type completer struct{}

func (c *completer) Do(line []rune, pos int) (newLine [][]rune, length int) {
	lineStr := string(line[:pos])
	parts := strings.Fields(lineStr)

	// If empty or still typing first word, complete commands
	if len(parts) == 0 || (len(parts) == 1 && !strings.HasSuffix(lineStr, " ")) {
		prefix := ""
		if len(parts) == 1 {
			prefix = parts[0]
		}
		for _, cmd := range commands {
			if strings.HasPrefix(cmd, prefix) {
				newLine = append(newLine, []rune(cmd[len(prefix):]))
			}
		}
		return newLine, len(prefix)
	}

	// After command, suggest based on command type
	cmd := parts[0]
	switch cmd {
	case "query", "follow", "explain":
		// Complete filter keys
		lastPart := ""
		if !strings.HasSuffix(lineStr, " ") && len(parts) > 1 {
			lastPart = parts[len(parts)-1]
		}
		for _, filter := range queryFilters {
			if strings.HasPrefix(filter, lastPart) {
				newLine = append(newLine, []rune(filter[len(lastPart):]))
			}
		}
		return newLine, len(lastPart)
	}

	return nil, 0
}

// Run starts the REPL loop.
func (r *REPL) Run() error {
	historyFile := ""
	if home, err := os.UserHomeDir(); err == nil {
		historyFile = filepath.Join(home, ".gastrolog_history")
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "> ",
		HistoryFile:     historyFile,
		AutoComplete:    &completer{},
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return err
	}
	defer rl.Close()

	fmt.Println("GastroLog REPL. Type 'help' for commands.")

	for {
		// Update prompt based on query state
		if r.hasActiveQuery() {
			rl.SetPrompt("[query] > ")
		} else {
			rl.SetPrompt("> ")
		}

		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if r.hasActiveQuery() {
					r.cancelQuery()
					r.clearQueryState()
					fmt.Println("Query cancelled.")
					continue
				}
				continue
			}
			if err == io.EOF {
				return nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		exit := r.execute(line)
		if exit {
			return nil
		}
	}
}

// execute parses and executes a single command.
func (r *REPL) execute(line string) (exit bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return false
	}

	cmd := parts[0]
	args := parts[1:]

	var out strings.Builder

	switch cmd {
	case "help", "?":
		r.cmdHelp(&out)
	case "query":
		r.cmdQuery(&out, args, true) // interactive: use pager
	case "follow":
		r.cmdFollow(&out, args)
	case "next":
		r.cmdNext(&out, args)
	case "reset":
		r.cmdReset(&out)
	case "set":
		r.cmdSet(&out, args)
	case "stores":
		r.cmdStores(&out)
	case "chunks":
		r.cmdChunks(&out)
	case "chunk":
		r.cmdChunk(&out, args)
	case "indexes":
		r.cmdIndexes(&out, args)
	case "analyze":
		r.cmdAnalyze(&out, args)
	case "explain":
		r.cmdExplain(&out, args)
	case "stats":
		r.cmdStats(&out)
	case "status":
		r.cmdStatus(&out)
	case "exit", "quit":
		return true
	default:
		fmt.Fprintf(&out, "Unknown command: %s. Type 'help' for commands.\n", cmd)
	}

	output := out.String()
	if output != "" {
		r.printOutput(output)
	}

	return false
}

// printOutput prints output, using a pager if it exceeds terminal height.
func (r *REPL) printOutput(output string) {
	lines := strings.Count(output, "\n")
	termHeight := getTerminalHeight()

	// Use pager if output exceeds terminal height
	if lines >= termHeight {
		r.pager(output)
	} else {
		fmt.Print(output)
	}
}

// hasActiveQuery returns true if there's an active query with pending results.
func (r *REPL) hasActiveQuery() bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	return r.resultChan != nil
}

// clearQueryState clears all query state.
func (r *REPL) clearQueryState() {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	r.lastQuery = nil
	r.resumeToken = nil
	r.resultChan = nil
	r.getToken = nil
}

// cancelQuery cancels any running query.
func (r *REPL) cancelQuery() {
	r.queryMu.Lock()
	cancel := r.queryCancel
	r.queryCancel = nil
	r.queryMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// getResultChan returns the current result channel.
func (r *REPL) getResultChan() chan recordResult {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	return r.resultChan
}

// setResultChan sets the result channel.
func (r *REPL) setResultChan(ch chan recordResult) {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	r.resultChan = ch
}

// fetchAndPrint fetches records and prints them.
func (r *REPL) fetchAndPrint(out *strings.Builder) {
	if r.pageSize == 0 {
		r.fetchAndPrintN(out, 0)
	} else {
		r.fetchAndPrintN(out, r.pageSize)
	}
}

// queryWithPager fetches query results and displays them in an interactive pager.
// The pager allows fetching more results by pressing 'n'.
func (r *REPL) queryWithPager() {
	// Fetch first batch
	var out strings.Builder
	hasMore := r.fetchBatch(&out)

	output := out.String()
	if output == "" {
		fmt.Println("No results.")
		return
	}

	// If output fits on screen, just print it
	lines := strings.Count(output, "\n")
	termHeight := getTerminalHeight()
	if lines < termHeight && !hasMore {
		fmt.Print(output)
		return
	}

	// Use pager with fetch-more support
	var fetchMore func() string
	if hasMore {
		fetchMore = func() string {
			var more strings.Builder
			if r.fetchBatch(&more) {
				// Still more available
			} else {
				// No more results - return empty to signal end
				result := more.String()
				if result == "" || result == "No more results.\n" {
					return ""
				}
				return result
			}
			return more.String()
		}
	}

	r.pagerWithFetch(output, fetchMore)

	// Clear query state when done with pager
	r.cancelQuery()
	r.clearQueryState()
}

// fetchBatch fetches up to pageSize records into out.
// Returns true if there may be more results available.
func (r *REPL) fetchBatch(out *strings.Builder) bool {
	ch := r.getResultChan()
	if ch == nil {
		return false
	}

	count := r.pageSize
	if count == 0 {
		count = 50 // default batch size
	}

	printed := 0
	for {
		if printed >= count {
			return true // more may be available
		}

		select {
		case result, ok := <-ch:
			if !ok {
				if printed == 0 {
					out.WriteString("No more results.\n")
				}
				r.setResultChan(nil)
				return false
			}
			if result.err != nil {
				if errors.Is(result.err, query.ErrInvalidResumeToken) {
					out.WriteString("Resume token invalid (chunk deleted).\n")
					r.setResultChan(nil)
					return false
				}
				fmt.Fprintf(out, "Error: %v\n", result.err)
				return false
			}

			r.printRecord(out, result.rec)
			printed++
		}
	}
}

// fetchAndPrintN fetches up to count records and prints them.
func (r *REPL) fetchAndPrintN(out *strings.Builder, count int) {
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
		fmt.Fprintf(out, "--- %d records (more available, use 'next') ---\n", printed)
	}
}

// printRecord formats and prints a single record.
func (r *REPL) printRecord(out *strings.Builder, rec chunk.Record) {
	out.WriteString(r.formatRecord(rec))
	out.WriteByte('\n')
}

// Close shuts down the REPL.
func (r *REPL) Close() {
	r.cancelQuery()
	r.cancel()
}

// simpleREPL is a non-interactive REPL for testing.
type simpleREPL struct {
	repl *REPL
	in   io.Reader
	out  io.Writer
}

// NewSimple creates a REPL for testing without readline.
func NewSimple(client Client, in io.Reader, out io.Writer) *simpleREPL {
	ctx, cancel := context.WithCancel(context.Background())

	return &simpleREPL{
		repl: &REPL{
			client:   client,
			pageSize: defaultPageSize,
			ctx:      ctx,
			cancel:   cancel,
		},
		in:  in,
		out: out,
	}
}

// Run executes commands from the input reader.
func (s *simpleREPL) Run() error {
	buf := make([]byte, 4096)
	var lines []string

	for {
		n, err := s.in.Read(buf)
		if n > 0 {
			lines = append(lines, strings.Split(string(buf[:n]), "\n")...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := parts[0]
		args := parts[1:]

		var out strings.Builder

		switch cmd {
		case "help", "?":
			s.repl.cmdHelp(&out)
		case "query":
			s.repl.cmdQuery(&out, args, false) // non-interactive: no pager
		case "next":
			s.repl.cmdNext(&out, args)
		case "reset":
			s.repl.cmdReset(&out)
		case "set":
			s.repl.cmdSet(&out, args)
		case "stores":
			s.repl.cmdStores(&out)
		case "chunks":
			s.repl.cmdChunks(&out)
		case "chunk":
			s.repl.cmdChunk(&out, args)
		case "indexes":
			s.repl.cmdIndexes(&out, args)
		case "analyze":
			s.repl.cmdAnalyze(&out, args)
		case "explain":
			s.repl.cmdExplain(&out, args)
		case "stats":
			s.repl.cmdStats(&out)
		case "status":
			s.repl.cmdStatus(&out)
		case "exit", "quit":
			return nil
		default:
			fmt.Fprintf(&out, "Unknown command: %s. Type 'help' for commands.\n", cmd)
		}

		s.out.Write([]byte(out.String()))
	}

	return nil
}
