package repl

import "strings"

func (r *REPL) cmdHelp(out *strings.Builder) {
	out.WriteString(`Commands:
  help                     Show this help
  store [name]             Get or set target store (default: "default")
  query key=value ...      Execute a query with filters
  follow key=value ...     Continuously stream new results (press any key to stop)
  explain key=value ...    Show query execution plan (which indexes will be used)
  next [count]             Fetch next page of results
  reset                    Clear current query state
  set [key=value]          Get or set config (no args shows current settings)

Inspection:
  chunks                   List all chunks with metadata
  chunk <id>               Show details for a specific chunk
  indexes <chunk-id>       Show index status for a chunk
  analyze [chunk-id]       Analyze index health (all chunks if no ID given)
  stats                    Show overall system statistics
  status                   Show live system state

Query filters:
  start=TIME               Start time (RFC3339 or Unix timestamp)
  end=TIME                 End time (RFC3339 or Unix timestamp)
  token=WORD               Filter by token (can repeat, AND semantics)
  limit=N                  Maximum total results
  key=value                Filter by key=value in attrs OR message (can repeat, AND semantics)
  key=*                    Filter by key existence (any value)
  *=value                  Filter by value existence (any key)

Settings:
  pager=N                  Records per page (0 = no paging, show all)

Examples:
  query start=2024-01-01T00:00:00Z end=2024-01-02T00:00:00Z token=error
  query source=nginx level=error
  query status=500 method=POST
  explain token=error level=warn
  set pager=50
  chunks
  chunk 019c10bb-a3a8-7ad9-9e8e-890bf77a84d3
`)
}
