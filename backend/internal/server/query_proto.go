package server

import (
	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/query"
	"gastrolog/internal/safeutf8"
)

// TableResultToBasicProto converts an internal TableResult to proto without
// pipeline-aware result type detection. Used by the search executor on remote
// nodes where the coordinating node determines the display type.
func TableResultToBasicProto(result *query.TableResult) *apiv1.TableResult {
	rows := make([]*apiv1.TableRow, len(result.Rows))
	for i, row := range result.Rows {
		rows[i] = &apiv1.TableRow{Values: safeutf8.Strings(row)}
	}
	return &apiv1.TableResult{
		Columns:    safeutf8.Strings(result.Columns),
		Rows:       rows,
		Truncated:  result.Truncated,
		ResultType: "table",
	}
}

// protoToTableResult converts a proto TableResult back to the internal type.
// Used on the coordinating node to merge results from remote nodes.
func protoToTableResult(pt *apiv1.TableResult) *query.TableResult {
	if pt == nil {
		return nil
	}
	rows := make([][]string, len(pt.Rows))
	for i, row := range pt.Rows {
		rows[i] = row.Values
	}
	return &query.TableResult{
		Columns:   pt.Columns,
		Rows:      rows,
		Truncated: pt.Truncated,
	}
}
