package repl

import (
	"fmt"
	"strings"
)

func (r *REPL) cmdStores(out *strings.Builder) {
	stores := r.client.ListStores()
	if len(stores) == 0 {
		out.WriteString("No stores configured.\n")
		return
	}
	out.WriteString("Available stores:\n")
	for _, s := range stores {
		fmt.Fprintf(out, "  %s\n", s)
	}
}
