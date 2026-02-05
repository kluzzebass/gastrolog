package repl

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

// pager displays output one page at a time.
// Space or Enter for next page, q to quit.
func (r *REPL) pager(output string) {
	lines := strings.Split(output, "\n")
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
	_ = width // might use later for wrapping

	// Reserve one line for the prompt
	pageSize := height - 1
	if pageSize < 1 {
		pageSize = 1
	}

	// Put terminal in raw mode to read single keys
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		// Fallback: just print everything
		fmt.Print(output)
		return
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	lineIdx := 0
	for lineIdx < len(lines) {
		// Print one page
		end := lineIdx + pageSize
		if end > len(lines) {
			end = len(lines)
		}

		for i := lineIdx; i < end; i++ {
			fmt.Println(lines[i])
		}
		lineIdx = end

		// If we've shown everything, we're done
		if lineIdx >= len(lines) {
			break
		}

		// Show prompt and wait for key
		remaining := len(lines) - lineIdx
		fmt.Printf("\033[7m -- %d more lines (space/enter: next, q: quit) -- \033[0m", remaining)

		// Read a single byte
		buf := make([]byte, 1)
		os.Stdin.Read(buf)

		// Clear the prompt line
		fmt.Print("\r\033[K")

		switch buf[0] {
		case 'q', 'Q':
			return
		case ' ', '\r', '\n':
			// Continue to next page
		default:
			// Any other key also continues
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
