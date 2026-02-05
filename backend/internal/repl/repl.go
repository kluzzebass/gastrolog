// Package repl provides an interactive REPL for interacting with a GastroLog
// system. The REPL communicates through a Client interface, which can be
// backed by either a direct in-process connection (embedded mode) or a
// remote gRPC connection (client-server mode).
//
// The REPL does not control the system. It only observes and queries.
package repl

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
)

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

// New creates a REPL using the provided client.
func New(client Client) *REPL {
	ctx, cancel := context.WithCancel(context.Background())

	r := &REPL{
		client:       client,
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
func NewSimple(client Client, in io.Reader, out io.Writer) *simpleREPL {
	ctx, cancel := context.WithCancel(context.Background())

	return &simpleREPL{
		repl: &REPL{
			client:       client,
			store:        "default",
			pageSize:     defaultPageSize,
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

// getResultChan returns the current result channel under lock.
func (r *REPL) getResultChan() chan recordResult {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	return r.resultChan
}

// setResultChan sets the result channel under lock.
func (r *REPL) setResultChan(ch chan recordResult) {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	r.resultChan = ch
}

// cancelQuery cancels the current query if one exists.
func (r *REPL) cancelQuery() {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if r.queryCancel != nil {
		r.queryCancel()
	}
}

// hasActiveQuery returns true if there's an active query.
func (r *REPL) hasActiveQuery() bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	return r.resultChan != nil
}

// commands is the list of available REPL commands for tab completion.
var commands = []string{"help", "store", "query", "follow", "next", "reset", "set", "chunks", "chunk", "indexes", "analyze", "explain", "stats", "status", "exit", "quit"}

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

	// Disable cursor blink to prevent redraws that clear terminal text selection.
	ti.Cursor.SetMode(cursor.CursorStatic)

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
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tickMsg:
		// Poll for new records in follow mode
		if !m.following {
			return m, nil
		}
		// Get channel under lock to avoid race
		ch := m.repl.getResultChan()
		if ch == nil {
			// Channel gone but still in follow mode - shouldn't happen
			m.following = false
			m.output.WriteString("--- Follow ended (no channel) ---\n")
			return m, nil
		}
		// Drain all available records without blocking.
		// We use the local ch variable to avoid race conditions.
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					// Channel closed
					m.following = false
					m.repl.setResultChan(nil)
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
			m.repl.cancelQuery()
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
			// If there are suggestions, let textinput handle cycling; otherwise navigate history
			if len(m.textInput.MatchedSuggestions()) > 0 {
				// Let textinput handle suggestion cycling
				break
			}
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
			// If there are suggestions, let textinput handle cycling; otherwise navigate history
			if len(m.textInput.MatchedSuggestions()) > 0 {
				// Let textinput handle suggestion cycling
				break
			}
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

	// Update prompt with current status
	m.textInput.Prompt = m.repl.buildPrompt()

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
	case "query", "follow", "explain":
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
	case "chunk", "indexes", "analyze":
		// Suggest chunk IDs
		m.textInput.SetSuggestions(m.getChunkIDSuggestions(val))
	default:
		// No suggestions for other commands
		m.textInput.SetSuggestions(nil)
	}
}

// getChunkIDSuggestions returns chunk ID suggestions for commands that take a chunk ID argument.
func (m *model) getChunkIDSuggestions(currentInput string) []string {
	parts := strings.Fields(currentInput)
	cmd := parts[0]

	// Get partial ID being typed (if any)
	var partial string
	if len(parts) > 1 {
		partial = strings.ToLower(parts[1])
	}

	// Collect all chunk IDs across all stores
	var suggestions []string
	stores := m.repl.client.ListStores()

	for _, store := range stores {
		cm := m.repl.client.ChunkManager(store)
		if cm == nil {
			continue
		}
		chunks, err := cm.List()
		if err != nil {
			continue
		}
		for _, meta := range chunks {
			id := meta.ID.String()
			// Filter by partial match if user is typing
			if partial == "" || strings.HasPrefix(strings.ToLower(id), partial) {
				suggestions = append(suggestions, cmd+" "+id)
			}
		}
	}

	// Sort suggestions (most recent chunks first based on UUID v7 ordering)
	slices.Sort(suggestions)
	slices.Reverse(suggestions)

	return suggestions
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

// buildPrompt constructs the REPL prompt showing current status.
// Format: [store] or [store|query] or [store|query:N pending]
func (r *REPL) buildPrompt() string {
	r.queryMu.Lock()
	store := r.store
	hasQuery := r.resultChan != nil
	lastQuery := r.lastQuery
	r.queryMu.Unlock()

	var parts []string

	// Always show store
	parts = append(parts, store)

	// Show query status if there's an active query
	if hasQuery {
		if lastQuery != nil {
			queryDesc := r.describeQuery(lastQuery)
			parts = append(parts, queryDesc)
		} else {
			parts = append(parts, "query")
		}
	}

	return "[" + strings.Join(parts, "|") + "] > "
}

// describeQuery returns a short description of a query for the prompt.
func (r *REPL) describeQuery(q *query.Query) string {
	// If BoolExpr is set, use its string representation
	if q.BoolExpr != nil {
		desc := q.BoolExpr.String()
		// Truncate if too long
		if len(desc) > 30 {
			desc = desc[:27] + "..."
		}
		return desc
	}

	// Legacy API: use Tokens and KV fields
	var parts []string

	if len(q.Tokens) > 0 {
		if len(q.Tokens) == 1 {
			parts = append(parts, q.Tokens[0])
		} else {
			parts = append(parts, fmt.Sprintf("%d tokens", len(q.Tokens)))
		}
	}

	if len(q.KV) > 0 {
		if len(q.KV) == 1 {
			f := q.KV[0]
			key := f.Key
			if key == "" {
				key = "*"
			}
			val := f.Value
			if val == "" {
				val = "*"
			}
			parts = append(parts, key+"="+val)
		} else {
			parts = append(parts, fmt.Sprintf("%d filters", len(q.KV)))
		}
	}

	if len(parts) == 0 {
		return "query"
	}

	desc := strings.Join(parts, ",")
	// Truncate if too long
	if len(desc) > 20 {
		desc = desc[:17] + "..."
	}
	return desc
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
	case "analyze":
		r.cmdAnalyze(&out, args)
	case "explain":
		r.cmdExplain(&out, args)
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
