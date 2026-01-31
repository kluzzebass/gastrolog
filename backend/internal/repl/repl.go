// Package repl provides an in-process REPL for interacting with a running
// GastroLog system. The REPL is a client of the orchestrator and query engine,
// not their owner. It only observes and queries via public APIs.
//
// The REPL does not control the system. It does not start components, stop
// components, manage lifecycles, own goroutines, or coordinate ingestion.
package repl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

// recordResult holds a record or error from the iterator channel.
type recordResult struct {
	rec chunk.Record
	err error
}

// REPL provides an interactive read-eval-print loop for querying a running
// GastroLog system. It interacts only through exported, stable interfaces.
type REPL struct {
	orch *orchestrator.Orchestrator

	// Query state
	store       string                    // target store for queries
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

	// History
	history      []string
	historyIndex int
	historyFile  string
}

// model is the bubbletea model for the REPL.
type model struct {
	repl      *REPL
	textInput textinput.Model
	output    *strings.Builder
	quitting  bool
	following bool // true when in follow mode
}

// tickMsg is sent periodically during follow mode to poll for new records.
type tickMsg time.Time

// followTick returns a command that sends a tick after a delay.
func followTick() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// New creates a REPL attached to an already-running system.
// All components must be live and concurrent.
func New(orch *orchestrator.Orchestrator) *REPL {
	ctx, cancel := context.WithCancel(context.Background())

	r := &REPL{
		orch:         orch,
		store:        "default",
		pageSize:     defaultPageSize,
		ctx:          ctx,
		cancel:       cancel,
		history:      make([]string, 0),
		historyIndex: -1,
	}

	// Set up history file in user's home directory.
	if home, err := os.UserHomeDir(); err == nil {
		r.historyFile = filepath.Join(home, ".gastrolog_history")
		r.loadHistory()
	}

	return r
}

// NewSimple creates a REPL for testing without bubbletea.
// This version reads commands from the provided input and writes output to out.
func NewSimple(orch *orchestrator.Orchestrator, in io.Reader, out io.Writer) *simpleREPL {
	ctx, cancel := context.WithCancel(context.Background())

	return &simpleREPL{
		repl: &REPL{
			orch:         orch,
			store:        "default",
			ctx:          ctx,
			cancel:       cancel,
			history:      make([]string, 0),
			historyIndex: -1,
		},
		in:  in,
		out: out,
	}
}

// simpleREPL is a test-friendly REPL that doesn't use bubbletea.
type simpleREPL struct {
	repl *REPL
	in   io.Reader
	out  io.Writer
}

// Run starts the simple REPL loop for testing.
func (s *simpleREPL) Run() error {
	fmt.Fprintln(s.out, "GastroLog REPL. Type 'help' for commands.")

	scanner := newLineScanner(s.in)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		output, exit, _ := s.repl.execute(line)
		if output != "" {
			fmt.Fprint(s.out, output)
		}
		if exit {
			return nil
		}
		// Note: follow mode not supported in simple REPL (for testing)
	}

	return scanner.Err()
}

// lineScanner wraps bufio.Scanner-like behavior for io.Reader
type lineScanner struct {
	reader io.Reader
	buf    []byte
	line   string
	err    error
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{reader: r, buf: make([]byte, 0, 4096)}
}

func (s *lineScanner) Scan() bool {
	for {
		// Check for newline in buffer
		if idx := strings.IndexByte(string(s.buf), '\n'); idx >= 0 {
			s.line = string(s.buf[:idx])
			s.buf = s.buf[idx+1:]
			return true
		}

		// Read more data
		tmp := make([]byte, 1024)
		n, err := s.reader.Read(tmp)
		if n > 0 {
			s.buf = append(s.buf, tmp[:n]...)
		}
		if err != nil {
			if err == io.EOF && len(s.buf) > 0 {
				s.line = string(s.buf)
				s.buf = nil
				return true
			}
			if err != io.EOF {
				s.err = err
			}
			return false
		}
	}
}

func (s *lineScanner) Text() string { return s.line }
func (s *lineScanner) Err() error   { return s.err }

// defaultPageSize is the number of records shown per page in query results.
const defaultPageSize = 10

// commands is the list of available REPL commands for tab completion.
var commands = []string{"help", "store", "query", "follow", "next", "reset", "set", "chunks", "chunk", "indexes", "stats", "status", "exit", "quit"}

// queryFilters is the list of query filter keys for tab completion.
var queryFilters = []string{"start=", "end=", "token=", "limit="}

// Run starts the REPL loop using bubbletea.
func (r *REPL) Run() error {
	ti := textinput.New()
	ti.Placeholder = ""
	ti.Focus()
	ti.Prompt = "> "
	ti.CharLimit = 1024
	ti.Width = 80
	ti.ShowSuggestions = true
	ti.SetSuggestions(commands)

	output := &strings.Builder{}
	output.WriteString("GastroLog REPL. Type 'help' for commands.\n")

	m := model{
		repl:      r,
		textInput: ti,
		output:    output,
	}

	p := tea.NewProgram(m)
	_, err := p.Run()
	return err
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tickMsg:
		// Poll for new records in follow mode
		if !m.following {
			return m, nil
		}
		if m.repl.resultChan == nil {
			// Channel gone but still in follow mode - shouldn't happen
			m.following = false
			m.output.WriteString("--- Follow ended (no channel) ---\n")
			return m, nil
		}
		// Drain all available records without blocking
		for {
			select {
			case result, ok := <-m.repl.resultChan:
				if !ok {
					// Channel closed
					m.following = false
					m.repl.resultChan = nil
					m.output.WriteString("--- End of follow ---\n")
					return m, nil
				}
				if result.err != nil {
					m.output.WriteString("--- Error: " + result.err.Error() + " ---\n")
					continue
				}
				m.output.WriteString(m.repl.formatRecord(result.rec))
				m.output.WriteByte('\n')
			default:
				// No more records right now, schedule next tick
				return m, followTick()
			}
		}

	case tea.KeyMsg:
		// Any key press stops follow mode
		if m.following {
			m.following = false
			if m.repl.queryCancel != nil {
				m.repl.queryCancel()
			}
			m.output.WriteString("--- Follow stopped ---\n")
			return m, nil
		}

		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyCtrlD:
			m.quitting = true
			return m, tea.Quit

		case tea.KeyEnter:
			line := strings.TrimSpace(m.textInput.Value())
			if line != "" {
				m.repl.addHistory(line)
				m.output.WriteString("> " + line + "\n")
				output, exit, follow := m.repl.execute(line)
				if output != "" {
					m.output.WriteString(output)
				}
				if exit {
					m.quitting = true
					return m, tea.Quit
				}
				if follow {
					m.following = true
					m.textInput.SetValue("")
					m.textInput.SetSuggestions(commands)
					m.repl.historyIndex = -1
					m.output.WriteString("--- Following (press any key to stop) ---\n")
					return m, followTick()
				}
			}
			m.textInput.SetValue("")
			m.textInput.SetSuggestions(commands)
			m.repl.historyIndex = -1
			return m, nil

		case tea.KeyUp:
			// Navigate history backward
			if len(m.repl.history) > 0 {
				if m.repl.historyIndex < len(m.repl.history)-1 {
					m.repl.historyIndex++
					idx := len(m.repl.history) - 1 - m.repl.historyIndex
					m.textInput.SetValue(m.repl.history[idx])
					m.textInput.CursorEnd()
				}
			}
			return m, nil

		case tea.KeyDown:
			// Navigate history forward
			if m.repl.historyIndex > 0 {
				m.repl.historyIndex--
				idx := len(m.repl.history) - 1 - m.repl.historyIndex
				m.textInput.SetValue(m.repl.history[idx])
				m.textInput.CursorEnd()
			} else if m.repl.historyIndex == 0 {
				m.repl.historyIndex = -1
				m.textInput.SetValue("")
			}
			return m, nil
		}
	}

	m.textInput, cmd = m.textInput.Update(msg)

	// Update suggestions based on current input
	m.updateSuggestions()

	return m, cmd
}

// updateSuggestions sets appropriate suggestions based on current input.
func (m *model) updateSuggestions() {
	val := m.textInput.Value()
	parts := strings.Fields(val)

	if len(parts) == 0 {
		// No input yet - suggest commands
		m.textInput.SetSuggestions(commands)
		return
	}

	cmd := parts[0]

	// If we're still typing the first word, suggest commands
	if len(parts) == 1 && !strings.HasSuffix(val, " ") {
		m.textInput.SetSuggestions(commands)
		return
	}

	// After command, suggest based on command type
	switch cmd {
	case "query", "sources":
		// Suggest filter keys, prefixed with current input
		var suggestions []string
		prefix := val
		if !strings.HasSuffix(prefix, " ") {
			// Still typing a filter, find the last space
			lastSpace := strings.LastIndex(prefix, " ")
			if lastSpace >= 0 {
				prefix = prefix[:lastSpace+1]
			}
		}
		for _, f := range queryFilters {
			suggestions = append(suggestions, prefix+f)
		}
		m.textInput.SetSuggestions(suggestions)
	default:
		// No suggestions for other commands
		m.textInput.SetSuggestions(nil)
	}
}

func (m model) View() string {
	if m.quitting {
		return ""
	}
	if m.following {
		return m.output.String()
	}
	return m.output.String() + m.textInput.View()
}

// loadHistory reads history from file.
func (r *REPL) loadHistory() {
	if r.historyFile == "" {
		return
	}
	data, err := os.ReadFile(r.historyFile)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			r.history = append(r.history, line)
		}
	}
}

// addHistory adds a command to history and saves to file.
func (r *REPL) addHistory(cmd string) {
	// Don't add duplicates of the last command
	if len(r.history) > 0 && r.history[len(r.history)-1] == cmd {
		return
	}
	r.history = append(r.history, cmd)
	r.saveHistory()
}

// saveHistory writes history to file.
func (r *REPL) saveHistory() {
	if r.historyFile == "" {
		return
	}
	// Keep last 1000 entries
	start := 0
	if len(r.history) > 1000 {
		start = len(r.history) - 1000
	}
	data := strings.Join(r.history[start:], "\n") + "\n"
	_ = os.WriteFile(r.historyFile, []byte(data), 0600)
}

// execute parses and executes a single command. Returns output and whether to exit.
func (r *REPL) execute(line string) (output string, exit bool, follow bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", false, false
	}

	cmd := parts[0]
	args := parts[1:]

	var out strings.Builder

	switch cmd {
	case "help", "?":
		r.cmdHelp(&out)
	case "query":
		r.cmdQuery(&out, args, false)
	case "follow":
		r.cmdQuery(&out, args, true)
		return out.String(), false, true
	case "next":
		r.cmdNext(&out, args)
	case "reset":
		r.cmdReset(&out)
	case "set":
		r.cmdSet(&out, args)
	case "store":
		r.cmdStore(&out, args)
	case "chunks":
		r.cmdChunks(&out)
	case "chunk":
		r.cmdChunk(&out, args)
	case "indexes":
		r.cmdIndexes(&out, args)
	case "stats":
		r.cmdStats(&out)
	case "status":
		r.cmdStatus(&out)
	case "exit", "quit":
		return "", true, false
	default:
		fmt.Fprintf(&out, "Unknown command: %s. Type 'help' for commands.\n", cmd)
	}

	return out.String(), false, false
}

func (r *REPL) cmdHelp(out *strings.Builder) {
	out.WriteString(`Commands:
  help                     Show this help
  store [name]             Get or set target store (default: "default")
  query key=value ...      Execute a query with filters
  follow key=value ...     Continuously stream new results (press any key to stop)
  next [count]             Fetch next page of results
  reset                    Clear current query state
  set [key=value]          Get or set config (no args shows current settings)

Inspection:
  chunks                   List all chunks with metadata
  chunk <id>               Show details for a specific chunk
  indexes <chunk-id>       Show index status for a chunk
  stats                    Show overall system statistics
  status                   Show live system state

Query filters:
  start=TIME               Start time (RFC3339 or Unix timestamp)
  end=TIME                 End time (RFC3339 or Unix timestamp)
  token=WORD               Filter by token (can repeat, AND semantics)
  limit=N                  Maximum total results

Settings:
  pager=N                  Records per page (0 = no paging, show all)

Examples:
  query start=2024-01-01T00:00:00Z end=2024-01-02T00:00:00Z token=error
  set pager=50
  chunks
  chunk 019c10bb-a3a8-7ad9-9e8e-890bf77a84d3
`)
}

func (r *REPL) cmdStore(out *strings.Builder, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(out, "Current store: %s\n", r.store)
		return
	}
	r.store = args[0]
	fmt.Fprintf(out, "Store set to: %s\n", r.store)
}

func (r *REPL) cmdQuery(out *strings.Builder, args []string, follow bool) {
	q := query.Query{}
	var tokens []string

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
			fmt.Fprintf(out, "Unknown filter: %s\n", k)
			return
		}
	}

	if len(tokens) > 0 {
		q.Tokens = tokens
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

func (r *REPL) cmdSet(out *strings.Builder, args []string) {
	if len(args) == 0 {
		// Show current settings
		fmt.Fprintf(out, "pager=%d\n", r.pageSize)
		return
	}

	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			fmt.Fprintf(out, "Invalid setting: %s (expected key=value)\n", arg)
			return
		}

		switch k {
		case "pager":
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n < 0 {
				fmt.Fprintf(out, "Invalid pager value: %s (expected non-negative integer)\n", v)
				return
			}
			r.pageSize = n
			if n == 0 {
				out.WriteString("Pager disabled (showing all results).\n")
			} else {
				fmt.Fprintf(out, "Pager set to %d.\n", n)
			}
		default:
			fmt.Fprintf(out, "Unknown setting: %s\n", k)
		}
	}
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

// cmdChunks lists all chunks across all stores with their metadata.
func (r *REPL) cmdChunks(out *strings.Builder) {
	stores := r.orch.ChunkManagers()
	if len(stores) == 0 {
		out.WriteString("No chunk managers registered.\n")
		return
	}

	slices.Sort(stores)

	for _, store := range stores {
		cm := r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}

		chunks, err := cm.List()
		if err != nil {
			fmt.Fprintf(out, "[%s] Error listing chunks: %v\n", store, err)
			continue
		}

		if len(chunks) == 0 {
			fmt.Fprintf(out, "[%s] No chunks\n", store)
			continue
		}

		// Sort by StartTS
		slices.SortFunc(chunks, func(a, b chunk.ChunkMeta) int {
			return a.StartTS.Compare(b.StartTS)
		})

		fmt.Fprintf(out, "[%s] %d chunks:\n", store, len(chunks))

		// Check which is the active chunk
		active := cm.Active()
		var activeID chunk.ChunkID
		if active != nil {
			activeID = active.ID
		}

		for _, meta := range chunks {
			status := "sealed"
			if meta.ID == activeID {
				status = "active"
			} else if !meta.Sealed {
				status = "open"
			}

			// Format time range
			timeRange := fmt.Sprintf("%s - %s",
				meta.StartTS.Format("2006-01-02 15:04:05"),
				meta.EndTS.Format("2006-01-02 15:04:05"))

			fmt.Fprintf(out, "  %s  %s  %d records  [%s]\n",
				meta.ID.String()[:8], timeRange, meta.RecordCount, status)
		}
	}
}

// cmdChunk shows detailed information about a specific chunk.
func (r *REPL) cmdChunk(out *strings.Builder, args []string) {
	if len(args) == 0 {
		out.WriteString("Usage: chunk <chunk-id>\n")
		return
	}

	chunkID, err := chunk.ParseChunkID(args[0])
	if err != nil {
		fmt.Fprintf(out, "Invalid chunk ID: %v\n", err)
		return
	}

	// Find the chunk across all stores
	stores := r.orch.ChunkManagers()
	var foundStore string
	var meta chunk.ChunkMeta
	var cm chunk.ChunkManager

	for _, store := range stores {
		cm = r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}
		m, err := cm.Meta(chunkID)
		if err == nil {
			foundStore = store
			meta = m
			break
		}
	}

	if foundStore == "" {
		fmt.Fprintf(out, "Chunk not found: %s\n", args[0])
		return
	}

	// Determine status
	status := "sealed"
	if active := cm.Active(); active != nil && active.ID == chunkID {
		status = "active"
	} else if !meta.Sealed {
		status = "open"
	}

	fmt.Fprintf(out, "Chunk: %s\n", meta.ID.String())
	fmt.Fprintf(out, "  Store:    %s\n", foundStore)
	fmt.Fprintf(out, "  Status:   %s\n", status)
	fmt.Fprintf(out, "  StartTS:  %s\n", meta.StartTS.Format(time.RFC3339Nano))
	fmt.Fprintf(out, "  EndTS:    %s\n", meta.EndTS.Format(time.RFC3339Nano))
	fmt.Fprintf(out, "  Records:  %d\n", meta.RecordCount)

	// Show index status if sealed
	if meta.Sealed {
		im := r.orch.IndexManager(foundStore)
		if im != nil {
			complete, err := im.IndexesComplete(chunkID)
			if err != nil {
				fmt.Fprintf(out, "  Indexes:  error checking: %v\n", err)
			} else if complete {
				fmt.Fprintf(out, "  Indexes:  complete\n")
			} else {
				fmt.Fprintf(out, "  Indexes:  incomplete\n")
			}
		}
	} else {
		fmt.Fprintf(out, "  Indexes:  n/a (not sealed)\n")
	}
}

// cmdIndexes shows index details for a specific chunk.
func (r *REPL) cmdIndexes(out *strings.Builder, args []string) {
	if len(args) == 0 {
		out.WriteString("Usage: indexes <chunk-id>\n")
		return
	}

	chunkID, err := chunk.ParseChunkID(args[0])
	if err != nil {
		fmt.Fprintf(out, "Invalid chunk ID: %v\n", err)
		return
	}

	// Find the chunk and its index manager
	stores := r.orch.ChunkManagers()
	var foundStore string
	var cm chunk.ChunkManager
	var im index.IndexManager

	for _, store := range stores {
		cm = r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}
		if _, err := cm.Meta(chunkID); err == nil {
			foundStore = store
			im = r.orch.IndexManager(store)
			break
		}
	}

	if foundStore == "" {
		fmt.Fprintf(out, "Chunk not found: %s\n", args[0])
		return
	}

	if im == nil {
		fmt.Fprintf(out, "No index manager for store: %s\n", foundStore)
		return
	}

	fmt.Fprintf(out, "Indexes for chunk %s:\n", chunkID.String()[:8])

	// Time index
	if timeIdx, err := im.OpenTimeIndex(chunkID); err != nil {
		fmt.Fprintf(out, "  time:   not available (%v)\n", err)
	} else {
		entries := timeIdx.Entries()
		if len(entries) > 0 {
			fmt.Fprintf(out, "  time:   %d entries (sparsity ~%d)\n",
				len(entries), estimateSparsity(entries))
		} else {
			fmt.Fprintf(out, "  time:   0 entries\n")
		}
	}

	// Token index
	if tokIdx, err := im.OpenTokenIndex(chunkID); err != nil {
		fmt.Fprintf(out, "  token:  not available (%v)\n", err)
	} else {
		entries := tokIdx.Entries()
		totalPositions := 0
		for _, e := range entries {
			totalPositions += len(e.Positions)
		}
		fmt.Fprintf(out, "  token:  %d tokens, %d positions\n", len(entries), totalPositions)
	}
}

// cmdStats shows overall system statistics.
func (r *REPL) cmdStats(out *strings.Builder) {
	stores := r.orch.ChunkManagers()

	var totalChunks, totalSealed, totalActive int
	var totalRecords int64

	for _, store := range stores {
		cm := r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}

		chunks, err := cm.List()
		if err != nil {
			continue
		}

		active := cm.Active()

		for _, meta := range chunks {
			totalChunks++
			totalRecords += meta.RecordCount

			if active != nil && meta.ID == active.ID {
				totalActive++
			} else if meta.Sealed {
				totalSealed++
			}
		}
	}

	fmt.Fprintf(out, "System Statistics:\n")
	fmt.Fprintf(out, "  Stores:      %d\n", len(stores))
	fmt.Fprintf(out, "  Chunks:      %d total (%d sealed, %d active)\n",
		totalChunks, totalSealed, totalActive)
	fmt.Fprintf(out, "  Records:     %d\n", totalRecords)

	// Receiver stats
	receivers := r.orch.Receivers()
	fmt.Fprintf(out, "  Receivers:   %d\n", len(receivers))
}

// cmdStatus shows the live system state.
func (r *REPL) cmdStatus(out *strings.Builder) {
	fmt.Fprintf(out, "System Status:\n")

	// Orchestrator running status
	if r.orch.Running() {
		fmt.Fprintf(out, "  Orchestrator: running\n")
	} else {
		fmt.Fprintf(out, "  Orchestrator: stopped\n")
	}

	// Receivers
	receivers := r.orch.Receivers()
	if len(receivers) > 0 {
		slices.Sort(receivers)
		fmt.Fprintf(out, "  Receivers:    %s\n", strings.Join(receivers, ", "))
	} else {
		fmt.Fprintf(out, "  Receivers:    none\n")
	}

	// Stores and their active chunks
	stores := r.orch.ChunkManagers()
	slices.Sort(stores)

	fmt.Fprintf(out, "  Stores:\n")
	for _, store := range stores {
		cm := r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}

		active := cm.Active()
		if active != nil {
			fmt.Fprintf(out, "    [%s] active chunk: %s\n",
				store, active.ID.String()[:8])
		} else {
			fmt.Fprintf(out, "    [%s] no active chunk\n", store)
		}

		// Check for pending indexes on sealed chunks
		im := r.orch.IndexManager(store)
		if im != nil {
			chunks, err := cm.List()
			if err == nil {
				pendingIndexes := 0
				for _, meta := range chunks {
					if meta.Sealed {
						if complete, err := im.IndexesComplete(meta.ID); err == nil && !complete {
							pendingIndexes++
						}
					}
				}
				if pendingIndexes > 0 {
					fmt.Fprintf(out, "    [%s] pending indexes: %d chunks\n", store, pendingIndexes)
				}
			}
		}
	}
}

// estimateSparsity estimates the sparsity factor from time index entries.
func estimateSparsity(entries []index.TimeIndexEntry) int {
	if len(entries) < 2 {
		return 1
	}
	// Look at position gaps between entries
	totalGap := uint64(0)
	for i := 1; i < len(entries); i++ {
		gap := entries[i].RecordPos - entries[i-1].RecordPos
		totalGap += gap
	}
	avgGap := totalGap / uint64(len(entries)-1)
	if avgGap < 1 {
		return 1
	}
	return int(avgGap)
}
