package cli

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

// NewQueryCommand returns the top-level "query" command for searching logs.
func NewQueryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query [expression]",
		Short: "Search logs on a running gastrolog server",
		Long: `Execute a query against a running gastrolog server and stream results to stdout.

Output format is auto-detected: text for TTY, JSONL for pipes. Override with --format.

Time range, limit, and ordering are set via directives in the expression:
  last=5m    start=2026-01-01T00:00:00Z    end=2026-01-02T00:00:00Z
  limit=100  reverse=true                  order=source_ts

Examples:
  gastrolog query 'level=error last=5m'
  gastrolog query 'last=1h limit=100 reverse=true' --format json | jq .
  gastrolog query 'level=error' --count
  gastrolog query 'level=error' --explain
  gastrolog query 'level=error | stats count by host' --format table`,
		Args: cobra.MinimumNArgs(1),
		RunE: runQuery,
	}

	cmd.Flags().String("format", "", "output format: text, json, csv, raw, table (auto-detected if not set)")
	cmd.Flags().StringSlice("fields", nil, "fields to include in JSON/CSV output (default: all)")
	cmd.Flags().Bool("count", false, "print record count only, don't stream records")
	cmd.Flags().Bool("explain", false, "print query execution plan instead of results")

	return cmd
}

func runQuery(cmd *cobra.Command, args []string) error {
	client := clientFromCmd(cmd)
	expr := strings.Join(args, " ")

	// Extract limit from expression for client-side pagination control.
	limit := extractLimit(expr)

	// --explain: print the execution plan and exit.
	if explain, _ := cmd.Flags().GetBool("explain"); explain {
		return runExplain(client, expr)
	}

	// Resolve output format.
	format, _ := cmd.Flags().GetString("format")
	if format == "" {
		if term.IsTerminal(int(os.Stdout.Fd())) { //nolint:gosec // G115: Fd() fits in int on 64-bit
			format = "text"
		} else {
			format = "json"
		}
	}

	countOnly, _ := cmd.Flags().GetBool("count")
	fields, _ := cmd.Flags().GetStringSlice("fields")

	// Stream search results.
	ctx := cmd.Context()
	var totalRecords int64
	started := time.Now()

	err := streamSearch(ctx, client, expr, limit, func(resp *gastrologv1.SearchResponse) error {
		// Pipeline results (table output).
		if resp.TableResult != nil {
			if countOnly {
				return nil // count doesn't apply to pipeline results
			}
			printTableResult(resp.TableResult, format)
			return nil
		}

		for _, rec := range resp.Records {
			totalRecords++
			if countOnly {
				continue
			}
			if err := printRecord(rec, format, fields); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	if countOnly {
		fmt.Println(totalRecords)
	}

	elapsed := time.Since(started)
	if format == "text" && !countOnly {
		fmt.Fprintf(os.Stderr, "\n%d records in %s\n", totalRecords, elapsed.Truncate(time.Millisecond))
	}

	if totalRecords == 0 {
		os.Exit(1)
	}
	return nil
}

// extractLimit parses a limit=N directive from the expression string.
// Returns 0 if no limit is found.
func extractLimit(expr string) int {
	for part := range strings.FieldsSeq(expr) {
		if v, ok := strings.CutPrefix(part, "limit="); ok {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

// streamSearch paginates through the full search result set.
func streamSearch(ctx context.Context, client *server.Client, expr string, limit int, fn func(*gastrologv1.SearchResponse) error) error {
	var resumeToken []byte
	var total int

	for {
		query := &gastrologv1.Query{Expression: expr}
		if limit > 0 {
			query.Limit = int64(limit - total)
			if query.Limit <= 0 {
				return nil
			}
		}

		stream, err := client.Query.Search(ctx, connect.NewRequest(&gastrologv1.SearchRequest{
			Query:       query,
			ResumeToken: resumeToken,
		}))
		if err != nil {
			return fmt.Errorf("search: %w", err)
		}

		for stream.Receive() {
			resp := stream.Msg()
			if err := fn(resp); err != nil {
				return err
			}
			total += len(resp.Records)
			if len(resp.ResumeToken) > 0 {
				resumeToken = resp.ResumeToken
			}
			if limit > 0 && total >= limit {
				return nil
			}
		}
		if err := stream.Err(); err != nil {
			return fmt.Errorf("search stream: %w", err)
		}

		// No more pages if we didn't get a resume token.
		if len(resumeToken) == 0 {
			return nil
		}

		// Check if the last response indicated no more results.
		resumeToken = nil // will be set by next page if there are more
	}
}

func printRecord(rec *gastrologv1.Record, format string, fields []string) error {
	switch format {
	case "json":
		return printRecordJSON(rec, fields)
	case "csv":
		return printRecordCSV(rec, fields)
	case "raw":
		_, err := os.Stdout.Write(rec.Raw)
		if err == nil {
			_, err = os.Stdout.Write([]byte("\n"))
		}
		return err
	case "text":
		return printRecordText(rec)
	default:
		return fmt.Errorf("unknown format: %s", format)
	}
}

func printRecordJSON(rec *gastrologv1.Record, fields []string) error {
	obj := recordToMap(rec)
	if len(fields) > 0 {
		filtered := make(map[string]any, len(fields))
		for _, f := range fields {
			if v, ok := obj[f]; ok {
				filtered[f] = v
			}
		}
		obj = filtered
	}
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = fmt.Println(string(data))
	return err
}

var csvWriter *csv.Writer
var csvHeaderWritten bool

func printRecordCSV(rec *gastrologv1.Record, fields []string) error {
	if csvWriter == nil {
		csvWriter = csv.NewWriter(os.Stdout)
	}

	obj := recordToMap(rec)

	if len(fields) == 0 {
		fields = []string{"ingest_ts", "source_ts", "write_ts", "raw"}
		// Add all attr keys sorted.
		for k := range rec.Attrs {
			fields = append(fields, "attr."+k)
		}
	}

	if !csvHeaderWritten {
		if err := csvWriter.Write(fields); err != nil {
			return err
		}
		csvHeaderWritten = true
	}

	row := make([]string, len(fields))
	for i, f := range fields {
		if v, ok := obj[f]; ok {
			row[i] = fmt.Sprintf("%v", v)
		} else if attrKey, ok := strings.CutPrefix(f, "attr."); ok {
			row[i] = rec.Attrs[attrKey]
		}
	}
	if err := csvWriter.Write(row); err != nil {
		return err
	}
	csvWriter.Flush()
	return csvWriter.Error()
}

func printRecordText(rec *gastrologv1.Record) error {
	var ts string
	if rec.IngestTs != nil {
		ts = rec.IngestTs.AsTime().Local().Format("15:04:05.000")
	} else {
		ts = "--:--:--.---"
	}

	// Severity badge.
	level := ""
	for _, key := range []string{"level", "severity"} {
		if v, ok := rec.Attrs[key]; ok {
			level = strings.ToUpper(v)
			break
		}
	}

	raw := string(rec.Raw)
	if idx := strings.IndexByte(raw, '\n'); idx >= 0 {
		raw = raw[:idx] // single line for text mode
	}

	if level != "" {
		_, err := fmt.Fprintf(os.Stdout, "%s  %-5s  %s\n", ts, level, raw)
		return err
	}
	_, err := fmt.Fprintf(os.Stdout, "%s         %s\n", ts, raw)
	return err
}

func printTableResult(tr *gastrologv1.TableResult, format string) {
	if format == "json" {
		for _, row := range tr.Rows {
			obj := make(map[string]string, len(tr.Columns))
			for i, col := range tr.Columns {
				if i < len(row.Values) {
					obj[col] = row.Values[i]
				}
			}
			data, _ := json.Marshal(obj)
			fmt.Println(string(data))
		}
		return
	}

	// Table or text format.
	p := newPrinter("table")
	rows := make([][]string, 0, len(tr.Rows))
	for _, row := range tr.Rows {
		rows = append(rows, row.Values)
	}
	p.table(tr.Columns, rows)
}

func recordToMap(rec *gastrologv1.Record) map[string]any {
	obj := make(map[string]any)
	if rec.IngestTs != nil {
		obj["ingest_ts"] = rec.IngestTs.AsTime().Format(time.RFC3339Nano)
	}
	if rec.SourceTs != nil {
		obj["source_ts"] = rec.SourceTs.AsTime().Format(time.RFC3339Nano)
	}
	if rec.WriteTs != nil {
		obj["write_ts"] = rec.WriteTs.AsTime().Format(time.RFC3339Nano)
	}
	obj["raw"] = string(rec.Raw)
	if len(rec.Attrs) > 0 {
		obj["attrs"] = rec.Attrs
	}
	if rec.Ref != nil {
		obj["vault_id"] = rec.Ref.VaultId
		obj["chunk_id"] = rec.Ref.ChunkId
		obj["pos"] = rec.Ref.Pos
	}
	return obj
}

func runExplain(client *server.Client, expr string) error {
	query := &gastrologv1.Query{Expression: expr}
	resp, err := client.Query.Explain(context.Background(), connect.NewRequest(&gastrologv1.ExplainRequest{
		Query: query,
	}))
	if err != nil {
		return fmt.Errorf("explain: %w", err)
	}

	plan := resp.Msg
	fmt.Fprintf(os.Stderr, "Direction: %s\n", plan.Direction)
	if plan.Expression != "" {
		fmt.Fprintf(os.Stderr, "Expression: %s\n", plan.Expression)
	}
	if plan.QueryStart != nil {
		fmt.Fprintf(os.Stderr, "Start: %s\n", plan.QueryStart.AsTime().Local().Format(time.RFC3339))
	}
	if plan.QueryEnd != nil {
		fmt.Fprintf(os.Stderr, "End: %s\n", plan.QueryEnd.AsTime().Local().Format(time.RFC3339))
	}
	fmt.Fprintf(os.Stderr, "Total chunks: %d, matching: %d\n\n", plan.TotalChunks, len(plan.Chunks))

	for _, cp := range plan.Chunks {
		fmt.Fprintf(os.Stderr, "  Chunk %s  records=%d  mode=%s\n",
			cp.ChunkId, cp.RecordCount, cp.ScanMode)
		for _, step := range cp.Steps {
			fmt.Fprintf(os.Stderr, "    %s %s: %d → %d  (%s)\n",
				step.Action, step.Name, step.InputEstimate, step.OutputEstimate, step.Detail)
		}
	}

	return nil
}
