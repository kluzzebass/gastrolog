package repl

import (
	"fmt"
	"strings"
)

func (r *REPL) cmdStore(out *strings.Builder, args []string) {
	if len(args) == 0 {
		if r.store == "" || r.store == "default" {
			fmt.Fprintf(out, "Current store filter: (all stores)\n")
			fmt.Fprintf(out, "Queries search across all stores. Use store=X in query to filter.\n")
		} else {
			fmt.Fprintf(out, "Current store filter: %s\n", r.store)
			fmt.Fprintf(out, "Queries are filtered to this store. Use 'store' with no args to clear.\n")
		}
		return
	}

	newStore := args[0]
	if newStore == "all" || newStore == "*" || newStore == "" {
		r.store = "default"
		fmt.Fprintf(out, "Store filter cleared. Queries now search all stores.\n")
	} else {
		r.store = newStore
		fmt.Fprintf(out, "Store filter set to: %s\n", r.store)
		fmt.Fprintf(out, "Queries will be filtered to this store.\n")
	}
}
