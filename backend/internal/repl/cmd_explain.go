package repl

import (
	"fmt"
	"strings"

	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

func (r *REPL) cmdExplain(out *strings.Builder, args []string) {
	q, errMsg := parseQueryArgs(args)
	if errMsg != "" {
		out.WriteString(errMsg + "\n")
		return
	}

	plan, err := r.orch.Explain(r.ctx, r.store, q)
	if err != nil {
		fmt.Fprintf(out, "Error: %v\n", err)
		return
	}

	printExplainPlan(out, plan)
}

func printExplainPlan(out *strings.Builder, plan *query.QueryPlan) {
	out.WriteString("QUERY EXECUTION PLAN\n")
	out.WriteString("====================\n\n")

	// Query summary
	out.WriteString("Query:\n")
	out.WriteString(fmt.Sprintf("  Direction: %s\n", plan.Direction))
	if !plan.Query.Start.IsZero() {
		out.WriteString(fmt.Sprintf("  Start: %s\n", plan.Query.Start.Format("2006-01-02T15:04:05")))
	}
	if !plan.Query.End.IsZero() {
		out.WriteString(fmt.Sprintf("  End: %s\n", plan.Query.End.Format("2006-01-02T15:04:05")))
	}

	// Show boolean expression if set
	if plan.Query.BoolExpr != nil {
		out.WriteString(fmt.Sprintf("  Expression: %s\n", plan.Query.BoolExpr.String()))
		dnf := querylang.ToDNF(plan.Query.BoolExpr)
		if len(dnf.Branches) > 1 {
			out.WriteString(fmt.Sprintf("  DNF Branches: %d\n", len(dnf.Branches)))
			for i, branch := range dnf.Branches {
				out.WriteString(fmt.Sprintf("    Branch %d: %s\n", i+1, branch.String()))
			}
		}
		out.WriteString("  Mode: DNF (index-accelerated branches)\n")
	} else {
		// Legacy filters
		if len(plan.Query.Tokens) > 0 {
			out.WriteString(fmt.Sprintf("  Tokens: %v\n", plan.Query.Tokens))
		}
		if len(plan.Query.KV) > 0 {
			out.WriteString(fmt.Sprintf("  KV Filters: %s\n", formatKVFilters(plan.Query.KV)))
		}
	}

	if plan.Query.Limit > 0 {
		out.WriteString(fmt.Sprintf("  Limit: %d\n", plan.Query.Limit))
	}
	out.WriteString("\n")

	// Chunk selection
	out.WriteString(fmt.Sprintf("Chunks: %d of %d selected\n\n", len(plan.ChunkPlans), plan.TotalChunks))

	if len(plan.ChunkPlans) == 0 {
		out.WriteString("No chunks match the query time range.\n")
		return
	}

	// Per-chunk plans
	for i, cp := range plan.ChunkPlans {
		sealedStr := "sealed"
		if !cp.Sealed {
			sealedStr = "active"
		}
		out.WriteString(fmt.Sprintf("Chunk %d: %s (%s)\n", i+1, cp.ChunkID.String(), sealedStr))
		out.WriteString(fmt.Sprintf("  Time Range: %s - %s [overlaps]\n",
			cp.StartTS.Format("2006-01-02T15:04:05"),
			cp.EndTS.Format("2006-01-02T15:04:05")))
		out.WriteString(fmt.Sprintf("  Records: %d\n", cp.RecordCount))

		// For skipped chunks, show skip reason and nothing else
		if cp.ScanMode == "skipped" {
			out.WriteString(fmt.Sprintf("\n  Chunk skipped: %s\n\n", cp.SkipReason))
			continue
		}

		// Branch plans (for DNF queries)
		if len(cp.BranchPlans) > 0 {
			out.WriteString("\n  DNF Branch Plans:\n")
			for j, bp := range cp.BranchPlans {
				out.WriteString(fmt.Sprintf("    Branch %d: %s\n", j+1, bp.BranchExpr))
				if bp.Skipped {
					out.WriteString(fmt.Sprintf("      Skipped: %s\n", bp.SkipReason))
				} else {
					for k, step := range bp.Pipeline {
						out.WriteString(fmt.Sprintf("      %d. %-12s %5d → %-5d [%s] reason=%s %s\n",
							k+1,
							step.Index,
							step.PositionsBefore,
							step.PositionsAfter,
							step.Action,
							step.Reason,
							step.Details,
						))
					}
					out.WriteString(fmt.Sprintf("      Estimated: ~%d records\n", bp.EstimatedScan))
				}
			}
		} else if len(cp.Pipeline) > 0 {
			// Single-branch pipeline
			out.WriteString("\n  Index Pipeline:\n")
			for j, step := range cp.Pipeline {
				out.WriteString(fmt.Sprintf("    %d. %-14s %5d → %-5d [%s] reason=%s %s\n",
					j+1,
					step.Index,
					step.PositionsBefore,
					step.PositionsAfter,
					step.Action,
					step.Reason,
					step.Details,
				))
			}
		}

		out.WriteString("\n")
		out.WriteString(fmt.Sprintf("  Scan: %s\n", cp.ScanMode))
		out.WriteString(fmt.Sprintf("  Estimated Records Scanned: ~%d\n", cp.EstimatedScan))
		out.WriteString(fmt.Sprintf("  Runtime Filter: %s\n", cp.RuntimeFilter))

		out.WriteString("\n")
	}
}

func formatKVFilters(filters []query.KeyValueFilter) string {
	var parts []string
	for _, f := range filters {
		key := f.Key
		if key == "" {
			key = "*"
		}
		value := f.Value
		if value == "" {
			value = "*"
		}
		parts = append(parts, key+"="+value)
	}
	return strings.Join(parts, ", ")
}
