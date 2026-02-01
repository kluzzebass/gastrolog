package repl

import (
	"fmt"
	"strings"
)

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
