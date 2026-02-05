package repl

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

// pager displays output with full navigation support.
// Navigation:
//   - j/Down: scroll down one line
//   - k/Up: scroll up one line
//   - Space/PageDown: scroll down one page
//   - b/PageUp: scroll up one page
//   - Left/Right or h/l: scroll horizontally
//   - Home/g: go to start
//   - End/G: go to end
//   - q: quit
func (r *REPL) pager(output string) {
	lines := strings.Split(output, "\n")
	if len(lines) == 0 {
		return
	}

	// Remove trailing empty line from Split
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return
	}

	// Get terminal size
	width, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		// Fallback: just print everything
		fmt.Print(output)
		return
	}

	// Reserve one line for the status bar
	viewHeight := height - 1
	if viewHeight < 1 {
		viewHeight = 1
	}

	// Put terminal in raw mode to read single keys
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		// Fallback: just print everything
		fmt.Print(output)
		return
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	// Viewport state
	topLine := 0     // first visible line
	leftCol := 0     // horizontal scroll offset
	hScrollStep := 8 // horizontal scroll amount

	// Hide cursor during paging
	fmt.Print("\033[?25l")
	defer fmt.Print("\033[?25h")

	// Clear screen and draw initial view
	fmt.Print("\033[2J\033[H")

	for {
		// Clamp topLine
		maxTop := len(lines) - viewHeight
		if maxTop < 0 {
			maxTop = 0
		}
		if topLine > maxTop {
			topLine = maxTop
		}
		if topLine < 0 {
			topLine = 0
		}

		// Clamp leftCol
		if leftCol < 0 {
			leftCol = 0
		}

		// Move cursor to top-left
		fmt.Print("\033[H")

		// Draw visible lines
		for i := 0; i < viewHeight; i++ {
			lineIdx := topLine + i
			if lineIdx < len(lines) {
				line := lines[lineIdx]
				// Apply horizontal scroll
				if leftCol < len(line) {
					line = line[leftCol:]
				} else {
					line = ""
				}
				// Truncate to terminal width
				if len(line) > width {
					line = line[:width]
				}
				fmt.Print(line)
			}
			// Clear rest of line and move to next
			fmt.Print("\033[K\n")
		}

		// Draw status bar (inverted colors)
		endLine := topLine + viewHeight
		if endLine > len(lines) {
			endLine = len(lines)
		}
		percent := 100
		if len(lines) > viewHeight {
			percent = (topLine + viewHeight) * 100 / len(lines)
			if percent > 100 {
				percent = 100
			}
		}

		status := fmt.Sprintf(" lines %d-%d of %d (%d%%)", topLine+1, endLine, len(lines), percent)
		if leftCol > 0 {
			status += fmt.Sprintf(" [col %d]", leftCol+1)
		}
		status += " | j/k:line PgUp/Dn:page h/l:scroll g/G:start/end q:quit "

		// Pad or truncate status to terminal width
		if len(status) < width {
			status += strings.Repeat(" ", width-len(status))
		} else if len(status) > width {
			status = status[:width]
		}

		fmt.Printf("\033[7m%s\033[0m", status)

		// Read input
		buf := make([]byte, 3)
		n, _ := os.Stdin.Read(buf)
		if n == 0 {
			continue
		}

		// Handle input
		if n == 1 {
			switch buf[0] {
			case 'q', 'Q':
				// Clear screen and return
				fmt.Print("\033[2J\033[H")
				return
			case 'j': // down one line
				topLine++
			case 'k': // up one line
				topLine--
			case ' ': // page down
				topLine += viewHeight
			case 'b': // page up
				topLine -= viewHeight
			case 'h': // scroll left
				leftCol -= hScrollStep
			case 'l': // scroll right
				leftCol += hScrollStep
			case 'g': // go to start
				topLine = 0
				leftCol = 0
			case 'G': // go to end
				topLine = len(lines) - viewHeight
			case '\r', '\n': // enter = down one line
				topLine++
			}
		} else if n == 3 && buf[0] == 0x1b && buf[1] == '[' {
			// Escape sequences
			switch buf[2] {
			case 'A': // Up arrow
				topLine--
			case 'B': // Down arrow
				topLine++
			case 'C': // Right arrow
				leftCol += hScrollStep
			case 'D': // Left arrow
				leftCol -= hScrollStep
			case '5': // Page Up (need to read one more byte for ~)
				extra := make([]byte, 1)
				os.Stdin.Read(extra)
				if extra[0] == '~' {
					topLine -= viewHeight
				}
			case '6': // Page Down
				extra := make([]byte, 1)
				os.Stdin.Read(extra)
				if extra[0] == '~' {
					topLine += viewHeight
				}
			case 'H': // Home
				topLine = 0
				leftCol = 0
			case 'F': // End
				topLine = len(lines) - viewHeight
			case '1': // Home (alternate: ESC [ 1 ~)
				extra := make([]byte, 1)
				os.Stdin.Read(extra)
				if extra[0] == '~' {
					topLine = 0
					leftCol = 0
				}
			case '4': // End (alternate: ESC [ 4 ~)
				extra := make([]byte, 1)
				os.Stdin.Read(extra)
				if extra[0] == '~' {
					topLine = len(lines) - viewHeight
				}
			}
		}
	}
}

// getTerminalHeight returns the terminal height, or a default if unavailable.
func getTerminalHeight() int {
	_, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 24 // sensible default
	}
	return height
}
