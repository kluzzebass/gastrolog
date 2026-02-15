package repl

import (
	"fmt"
	"os"
	"strings"
	"unicode"

	tea "github.com/charmbracelet/bubbletea"
	"golang.org/x/term"
)

// sanitizeLine replaces non-printable and control characters with the
// replacement character (U+FFFD) to prevent terminal corruption.
func sanitizeLine(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for _, r := range s {
		if r == '\t' {
			// Keep tabs, they're useful
			sb.WriteRune(r)
		} else if r < 32 || r == 127 || (r >= 0x80 && r < 0xA0) {
			// Control characters and C1 control codes
			sb.WriteRune(unicode.ReplacementChar)
		} else if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			// Other non-printable characters
			sb.WriteRune(unicode.ReplacementChar)
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// pager displays output with full navigation support using bubbletea.
func (r *REPL) pager(output string) {
	r.pagerWithFetch(output, nil)
}

// pagerWithFetch displays output with navigation and optional "fetch more" support.
func (r *REPL) pagerWithFetch(output string, fetchMore func() string) {
	rawLines := strings.Split(output, "\n")
	if len(rawLines) > 0 && rawLines[len(rawLines)-1] == "" {
		rawLines = rawLines[:len(rawLines)-1]
	}
	if len(rawLines) == 0 {
		return
	}

	// Sanitize lines to prevent terminal corruption
	lines := make([]string, len(rawLines))
	for i, line := range rawLines {
		lines[i] = sanitizeLine(line)
	}

	// Save terminal state before bubbletea takes over
	oldState, err := term.GetState(int(os.Stdin.Fd()))
	if err != nil {
		// Can't save state, just print
		fmt.Print(output)
		return
	}

	m := &pagerModel{
		lines:     lines,
		fetchMore: fetchMore,
		hasMore:   fetchMore != nil,
	}

	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())
	p.Run()

	// Restore terminal state after bubbletea exits
	term.Restore(int(os.Stdin.Fd()), oldState)
}

// pagerLive displays a live stream of lines using bubbletea.
func (r *REPL) pagerLive(linesChan <-chan string) {
	// Save terminal state before bubbletea takes over
	oldState, err := term.GetState(int(os.Stdin.Fd()))
	if err != nil {
		// Can't save state, just print lines as they come
		for line := range linesChan {
			fmt.Println(line)
		}
		return
	}

	m := &livePagerModel{
		linesChan: linesChan,
	}

	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())
	p.Run()

	// Restore terminal state after bubbletea exits
	term.Restore(int(os.Stdin.Fd()), oldState)
}

// getTerminalHeight returns the terminal height, or a default if unavailable.
func getTerminalHeight() int {
	_, height, err := term.GetSize(0)
	if err != nil {
		return 24
	}
	return height
}

// pagerModel is the bubbletea model for the static pager.
type pagerModel struct {
	lines     []string
	topLine   int
	leftCol   int
	width     int
	height    int
	fetchMore func() string
	hasMore   bool
}

func (m *pagerModel) Init() tea.Cmd {
	return nil
}

func (m *pagerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height - 1 // Reserve for status bar

	case tea.MouseMsg:
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			if msg.Shift {
				m.leftCol -= 8
			} else {
				m.topLine -= 3
			}
		case tea.MouseButtonWheelDown:
			if msg.Shift {
				m.leftCol += 8
			} else {
				m.topLine += 3
			}
		case tea.MouseButtonWheelLeft:
			m.leftCol -= 8
		case tea.MouseButtonWheelRight:
			m.leftCol += 8
		}
		m.clamp()

	case tea.KeyMsg:
		viewHeight := m.height
		if viewHeight < 1 {
			viewHeight = 1
		}

		switch msg.String() {
		case "q", "Q", "esc":
			return m, tea.Quit

		case "j", "down":
			m.topLine++
		case "k", "up":
			m.topLine--
		case " ", "pgdown":
			m.topLine += viewHeight
		case "b", "pgup":
			m.topLine -= viewHeight
		case "h", "left":
			m.leftCol -= 8
		case "l", "right":
			m.leftCol += 8
		case "g", "home", "alt+up":
			m.topLine = 0
		case "G", "end", "alt+down":
			m.topLine = len(m.lines) - viewHeight
		case "0", "ctrl+a":
			m.leftCol = 0
		case "$", "ctrl+e":
			m.leftCol = m.maxLineWidth() - m.width
		case "alt+b", "alt+left":
			m.leftCol = 0
		case "alt+f", "alt+right":
			m.leftCol = m.maxLineWidth() - m.width

		case "n", "N":
			if m.hasMore && m.atBottom() {
				more := m.fetchMore()
				if more == "" {
					m.hasMore = false
				} else {
					moreLines := strings.Split(more, "\n")
					if len(moreLines) > 0 && moreLines[len(moreLines)-1] == "" {
						moreLines = moreLines[:len(moreLines)-1]
					}
					m.topLine = len(m.lines)
					for _, line := range moreLines {
						m.lines = append(m.lines, sanitizeLine(line))
					}
				}
			}
		}

		m.clamp()
	}

	return m, nil
}

func (m *pagerModel) View() string {
	if m.width == 0 || m.height == 0 {
		return "Loading..."
	}

	var sb strings.Builder
	viewHeight := m.height

	for i := range viewHeight {
		lineIdx := m.topLine + i
		var line string
		if lineIdx < len(m.lines) {
			line = m.lines[lineIdx]
			if m.leftCol < len(line) {
				line = line[m.leftCol:]
			} else {
				line = ""
			}
			if len(line) > m.width {
				line = line[:m.width]
			}
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}

	// Status bar
	sb.WriteString(m.statusBar())

	return sb.String()
}

func (m *pagerModel) statusBar() string {
	endLine := m.topLine + m.height
	if endLine > len(m.lines) {
		endLine = len(m.lines)
	}
	percent := 100
	if len(m.lines) > m.height {
		percent = (m.topLine + m.height) * 100 / len(m.lines)
		if percent > 100 {
			percent = 100
		}
	}

	status := fmt.Sprintf(" lines %d-%d of %d (%d%%)", m.topLine+1, endLine, len(m.lines), percent)
	if m.leftCol > 0 {
		status += fmt.Sprintf(" [col %d]", m.leftCol+1)
	}
	status += " | j/k PgUp/Dn h/l g/G"
	if m.hasMore && m.atBottom() {
		status += " n:more"
	}
	status += " q:quit"

	// Pad to width
	if len(status) < m.width {
		status += strings.Repeat(" ", m.width-len(status))
	} else if len(status) > m.width {
		status = status[:m.width]
	}

	return fmt.Sprintf("\033[7m%s\033[0m", status)
}

func (m *pagerModel) clamp() {
	maxTop := len(m.lines) - m.height
	if maxTop < 0 {
		maxTop = 0
	}
	if m.topLine > maxTop {
		m.topLine = maxTop
	}
	if m.topLine < 0 {
		m.topLine = 0
	}
	if m.leftCol < 0 {
		m.leftCol = 0
	}
}

func (m *pagerModel) atBottom() bool {
	maxTop := len(m.lines) - m.height
	if maxTop < 0 {
		maxTop = 0
	}
	return m.topLine >= maxTop
}

func (m *pagerModel) maxLineWidth() int {
	maxW := 0
	for _, line := range m.lines {
		if len(line) > maxW {
			maxW = len(line)
		}
	}
	return maxW
}

// livePagerModel is the bubbletea model for the live/follow pager.
type livePagerModel struct {
	lines     []string
	linesChan <-chan string
	topLine   int
	leftCol   int
	width     int
	height    int
	done      bool
}

// lineMsg is sent when a new line arrives from the channel.
type lineMsg string

// doneMsg is sent when the channel is closed.
type doneMsg struct{}

func (m *livePagerModel) Init() tea.Cmd {
	return m.waitForLine()
}

func (m *livePagerModel) waitForLine() tea.Cmd {
	return func() tea.Msg {
		line, ok := <-m.linesChan
		if !ok {
			return doneMsg{}
		}
		return lineMsg(line)
	}
}

func (m *livePagerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height - 1

	case tea.MouseMsg:
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			if msg.Shift {
				m.leftCol -= 8
			} else {
				m.topLine -= 3
			}
		case tea.MouseButtonWheelDown:
			if msg.Shift {
				m.leftCol += 8
			} else {
				m.topLine += 3
			}
		case tea.MouseButtonWheelLeft:
			m.leftCol -= 8
		case tea.MouseButtonWheelRight:
			m.leftCol += 8
		}
		m.clamp()

	case lineMsg:
		wasAtBottom := m.atBottom()
		m.lines = append(m.lines, sanitizeLine(string(msg)))
		if wasAtBottom {
			m.scrollToBottom()
		}
		return m, m.waitForLine()

	case doneMsg:
		m.done = true
		return m, nil

	case tea.KeyMsg:
		viewHeight := m.height
		if viewHeight < 1 {
			viewHeight = 1
		}

		switch msg.String() {
		case "q", "Q", "esc":
			return m, tea.Quit

		case "j", "down":
			m.topLine++
		case "k", "up":
			m.topLine--
		case " ", "pgdown":
			m.topLine += viewHeight
		case "b", "pgup":
			m.topLine -= viewHeight
		case "h", "left":
			m.leftCol -= 8
		case "l", "right":
			m.leftCol += 8
		case "g", "home", "alt+up":
			m.topLine = 0
		case "G", "end", "alt+down":
			m.scrollToBottom()
		case "0", "ctrl+a":
			m.leftCol = 0
		case "$", "ctrl+e":
			m.leftCol = m.maxLineWidth() - m.width
		case "alt+b", "alt+left":
			m.leftCol = 0
		case "alt+f", "alt+right":
			m.leftCol = m.maxLineWidth() - m.width
		}

		m.clamp()
	}

	return m, nil
}

func (m *livePagerModel) View() string {
	if m.width == 0 || m.height == 0 {
		return "Loading..."
	}

	var sb strings.Builder
	viewHeight := m.height

	for i := range viewHeight {
		lineIdx := m.topLine + i
		var line string
		if lineIdx < len(m.lines) {
			line = m.lines[lineIdx]
			if m.leftCol < len(line) {
				line = line[m.leftCol:]
			} else {
				line = ""
			}
			if len(line) > m.width {
				line = line[:m.width]
			}
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}

	// Status bar
	sb.WriteString(m.statusBar())

	return sb.String()
}

func (m *livePagerModel) statusBar() string {
	total := len(m.lines)
	endLine := m.topLine + m.height
	if endLine > total {
		endLine = total
	}

	var status string
	if total == 0 {
		status = " Waiting for records..."
	} else {
		status = fmt.Sprintf(" lines %d-%d of %d", m.topLine+1, endLine, total)
	}
	if m.leftCol > 0 {
		status += fmt.Sprintf(" [col %d]", m.leftCol+1)
	}
	if m.done {
		status += " [END]"
	}
	status += " | j/k h/l g/G 0/$ q:quit"

	if len(status) < m.width {
		status += strings.Repeat(" ", m.width-len(status))
	} else if len(status) > m.width {
		status = status[:m.width]
	}

	return fmt.Sprintf("\033[7m%s\033[0m", status)
}

func (m *livePagerModel) clamp() {
	maxTop := len(m.lines) - m.height
	if maxTop < 0 {
		maxTop = 0
	}
	if m.topLine > maxTop {
		m.topLine = maxTop
	}
	if m.topLine < 0 {
		m.topLine = 0
	}
	if m.leftCol < 0 {
		m.leftCol = 0
	}
}

func (m *livePagerModel) scrollToBottom() {
	maxTop := len(m.lines) - m.height
	if maxTop < 0 {
		maxTop = 0
	}
	m.topLine = maxTop
}

func (m *livePagerModel) atBottom() bool {
	maxTop := len(m.lines) - m.height
	if maxTop < 0 {
		maxTop = 0
	}
	return m.topLine >= maxTop
}

func (m *livePagerModel) maxLineWidth() int {
	maxW := 0
	for _, line := range m.lines {
		if len(line) > maxW {
			maxW = len(line)
		}
	}
	return maxW
}
