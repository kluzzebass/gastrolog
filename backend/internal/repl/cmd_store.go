package repl

import (
	"fmt"
	"strings"
)

func (r *REPL) cmdStore(out *strings.Builder, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(out, "Current store: %s\n", r.store)
		return
	}
	r.store = args[0]
	fmt.Fprintf(out, "Store set to: %s\n", r.store)
}
