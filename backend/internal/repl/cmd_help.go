package repl

import "strings"

func (r *REPL) cmdHelp(out *strings.Builder) {
	out.WriteString(`Commands:
  help                     Show this help
  store [name|all]         Get/set store filter (default: all stores, use "all" to clear)
  query [filters...]       Execute a query with filters
  follow [filters...]      Continuously stream new results (press any key to stop)
  explain [filters...]     Show query execution plan (which indexes will be used)
  next [count]             Fetch next page of results
  reset                    Clear current query state
  set [key=value]          Get or set config (no args shows current settings)

Shortcuts:
  Enter                    With active query: fetch next page
  Escape                   Clear input and reset query state

Inspection:
  chunks                   List all chunks with metadata
  chunk <id>               Show details for a specific chunk
  indexes <chunk-id>       Show index status for a chunk
  analyze [chunk-id]       Analyze index health (all chunks if no ID given)
  stats                    Show overall system statistics
  status                   Show live system state

Query filters:
  WORD                     Bare word - filter by token (can repeat, AND semantics)
  start=TIME               Start time (RFC3339 or Unix timestamp)
  end=TIME                 End time (RFC3339 or Unix timestamp)
  limit=N                  Maximum total results
  key=value                Filter by key=value in attrs OR message (can repeat, AND semantics)
  key=*                    Filter by key existence (any value)
  *=value                  Filter by value existence (any key)
  store=NAME               Filter by store (by default, searches all stores)

Settings:
  pager=N                  Records per page (0 = no paging, show all)

Examples:
  query error                                              Search for "error" token
  query error warning                                      Search for "error" AND "warning"
  query start=2024-01-01T00:00:00Z end=2024-01-02T00:00:00Z error
  query source=nginx level=error
  query store=prod status=500                              Search only in "prod" store
  explain error level=warn
  set pager=50
  chunks
  chunk 019c10bb-a3a8-7ad9-9e8e-890bf77a84d3
`)
}
