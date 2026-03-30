package server

import (
	"context"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// GetSyntax returns the query language keyword sets for frontend tokenization.
func (s *QueryServer) GetSyntax(
	_ context.Context,
	_ *connect.Request[apiv1.GetSyntaxRequest],
) (*connect.Response[apiv1.GetSyntaxResponse], error) {
	// Aggregation functions valid inside stats bodies.
	aggs := []string{"count", "avg", "sum", "min", "max", "bin"}
	// Combine aggs + scalar functions for the full pipeFunctions set.
	funcs := make([]string, 0, len(aggs)+len(querylang.ScalarFuncNames))
	funcs = append(funcs, aggs...)
	funcs = append(funcs, querylang.ScalarFuncNames...)

	return connect.NewResponse(&apiv1.GetSyntaxResponse{
		Directives: []string{
			"reverse", "start", "end", "last", "limit", "pos",
			"source_start", "source_end", "ingest_start", "ingest_end",
		},
		PipeKeywords:  []string{"stats", "where", "eval", "sort", "head", "tail", "slice", "rename", "fields", "timechart", "dedup", "raw", "lookup", "linechart", "barchart", "donut", "heatmap", "scatter", "map", "export"},
		PipeFunctions: funcs,
		LookupTables:  s.lookupNames,
	}), nil
}

// ValidateQuery checks whether a query expression is syntactically valid.
func (s *QueryServer) ValidateQuery(
	_ context.Context,
	req *connect.Request[apiv1.ValidateQueryRequest],
) (*connect.Response[apiv1.ValidateQueryResponse], error) {
	expr := req.Msg.Expression
	valid, msg, offset := querylang.ValidateExpression(expr)
	spans, hasPipeline := querylang.Highlight(expr, offset)

	protoSpans := make([]*apiv1.HighlightSpan, len(spans))
	for i, sp := range spans {
		protoSpans[i] = &apiv1.HighlightSpan{Text: sp.Text, Role: string(sp.Role)}
	}

	// Detect export operator in the pipeline.
	parsedPipeline := querylang.ParseExpressionPipeline(expr)
	_, hasExport := querylang.HasExportOp(parsedPipeline)

	canFollow := valid && !hasExport && (!hasPipeline || canFollowPipeline(expr))

	return connect.NewResponse(&apiv1.ValidateQueryResponse{
		Valid:        valid,
		ErrorMessage: msg,
		ErrorOffset:  int32(offset), //nolint:gosec // G115: offset fits in int32
		Spans:        protoSpans,
		Expression:   expr,
		HasPipeline:  hasPipeline,
		CanFollow:    canFollow,
		HasExport:    hasExport,
	}), nil
}

// canFollowPipeline parses the expression and checks whether its pipeline
// operators are all streamable (compatible with follow mode).
func canFollowPipeline(expr string) bool {
	pipeline := querylang.ParseExpressionPipeline(expr)
	if pipeline == nil {
		return true // no pipeline — follow is fine
	}
	return query.CanStreamPipeline(pipeline)
}

// GetPipelineFields returns available fields and completions at cursor position.
func (s *QueryServer) GetPipelineFields(
	_ context.Context,
	req *connect.Request[apiv1.GetPipelineFieldsRequest],
) (*connect.Response[apiv1.GetPipelineFieldsResponse], error) {
	fields, completions := querylang.FieldsAtCursor(
		req.Msg.Expression,
		int(req.Msg.Cursor),
		req.Msg.BaseFields,
	)
	return connect.NewResponse(&apiv1.GetPipelineFieldsResponse{
		Fields:      fields,
		Completions: completions,
	}), nil
}
