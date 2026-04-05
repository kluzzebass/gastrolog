package server

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// searchPipeline handles pipelines that require full materialization
// (stats, timechart, sort, tail, slice, raw).
func (s *QueryServer) searchPipeline(
	ctx context.Context,
	eng *query.Engine,
	q query.Query,
	pipeline *querylang.Pipeline,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	// Non-distributive cap (head/tail/slice) before aggregation: gather raw
	// records from all nodes, then run the pipeline on the coordinator.
	if query.PipelineNeedsGlobalRecords(pipeline) {
		return s.searchPipelineGlobal(ctx, eng, q, pipeline, stream)
	}

	if s.maxResultCount > 0 && (q.Limit == 0 || int64(q.Limit) > s.maxResultCount) {
		q.Limit = int(s.maxResultCount)
	}
	result, err := eng.RunPipeline(ctx, q, pipeline)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	// Compute local histogram to include alongside pipeline results.
	histogram := HistogramToProto(eng.ComputeHistogram(ctx, q, 50))

	if result.Table != nil {
		// Fan out to remote nodes and merge table results.
		remoteResults := s.collectRemotePipeline(ctx, q, pipeline)
		if len(remoteResults) > 0 {
			result.Table = mergeTableResults(result.Table, remoteResults)
		}
		return stream.Send(&apiv1.SearchResponse{
			TableResult: tableResultToProto(result.Table, pipeline),
			Histogram:   histogram,
		})
	}
	// Non-aggregating but needs full materialization (sort/tail/slice):
	// stream all records.
	batch := make([]*apiv1.Record, 0, 100)
	for _, rec := range result.Records {
		batch = append(batch, recordToProto(rec))
		if len(batch) >= 100 {
			if err := stream.Send(&apiv1.SearchResponse{Records: batch, HasMore: true}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	return stream.Send(&apiv1.SearchResponse{Records: batch, Histogram: histogram})
}

// searchPipelineGlobal handles pipelines where non-distributive cap operators
// (head, tail, slice) precede an aggregation (stats/timechart). Instead of
// fanning out the full pipeline to each remote node (which would apply the cap
// independently per-node), it gathers raw records from all remote nodes, then
// runs the entire pipeline on the coordinator.
func (s *QueryServer) searchPipelineGlobal(
	ctx context.Context,
	eng *query.Engine,
	q query.Query,
	pipeline *querylang.Pipeline,
	stream *connect.ServerStream[apiv1.SearchResponse],
) error {
	if s.maxResultCount > 0 && (q.Limit == 0 || int64(q.Limit) > s.maxResultCount) {
		q.Limit = int(s.maxResultCount)
	}

	// Collect raw records from remote nodes (no pipeline — just the base query).
	remoteIter, remoteHist, _ := s.collectRemote(ctx, q, nil)
	var extraRecords []chunk.Record
	if remoteIter != nil {
		for rec, iterErr := range remoteIter {
			if iterErr != nil {
				return connect.NewError(connect.CodeInternal, iterErr)
			}
			extraRecords = append(extraRecords, rec)
		}
	}

	result, err := eng.RunPipelineOnRecords(ctx, q, pipeline, extraRecords)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	// Compute and merge histogram.
	localHist := HistogramToProto(eng.ComputeHistogram(ctx, q, 50))
	histogram := mergeHistogramBuckets(localHist, remoteHist)

	if result.Table != nil {
		return stream.Send(&apiv1.SearchResponse{
			TableResult: tableResultToProto(result.Table, pipeline),
			Histogram:   histogram,
		})
	}

	batch := make([]*apiv1.Record, 0, 100)
	for _, rec := range result.Records {
		batch = append(batch, recordToProto(rec))
		if len(batch) >= 100 {
			if err := stream.Send(&apiv1.SearchResponse{Records: batch, HasMore: true}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	return stream.Send(&apiv1.SearchResponse{Records: batch, Histogram: histogram})
}

// buildPipelineStages converts parsed pipeline operators into proto stages
// with execution metadata and human-readable notes.
func buildPipelineStages(pipeline *querylang.Pipeline) []*apiv1.QueryPipelineStage {
	stages := make([]*apiv1.QueryPipelineStage, 0, len(pipeline.Pipes))
	for _, op := range pipeline.Pipes {
		stages = append(stages, &apiv1.QueryPipelineStage{
			Operator:      pipeOpName(op),
			Description:   op.String(),
			Materializing: isMaterializing(op),
			Note:          pipeOpNote(op),
			Execution:     pipeOpExecution(op),
		})
	}
	return stages
}

// pipeOpName returns the operator name for a PipeOp.
func pipeOpName(op querylang.PipeOp) string {
	switch op.(type) {
	case *querylang.StatsOp:
		return "stats"
	case *querylang.WhereOp:
		return "where"
	case *querylang.EvalOp:
		return "eval"
	case *querylang.SortOp:
		return "sort"
	case *querylang.HeadOp:
		return "head"
	case *querylang.TailOp:
		return "tail"
	case *querylang.SliceOp:
		return "slice"
	case *querylang.RenameOp:
		return "rename"
	case *querylang.FieldsOp:
		return "fields"
	case *querylang.TimechartOp:
		return "timechart"
	case *querylang.RawOp:
		return "raw"
	case *querylang.LookupOp:
		return "lookup"
	case *querylang.BarchartOp:
		return "barchart"
	case *querylang.DonutOp:
		return "donut"
	case *querylang.HeatmapOp:
		return "heatmap"
	case *querylang.DedupOp:
		return "dedup"
	case *querylang.MapOp:
		return "map"
	default:
		return "unknown"
	}
}

// isMaterializing returns true for pipeline operators that require full
// result materialization before producing output.
func isMaterializing(op querylang.PipeOp) bool {
	switch op.(type) {
	case *querylang.StatsOp, *querylang.TimechartOp, *querylang.SortOp,
		*querylang.TailOp, *querylang.SliceOp, *querylang.RawOp:
		return true
	default:
		return false
	}
}

// pipeOpExecution returns a short execution mode label for a pipeline operator.
func pipeOpExecution(op querylang.PipeOp) string {
	switch op.(type) {
	case *querylang.StatsOp, *querylang.TimechartOp:
		return "materializing" // runs on each node, merged on coordinator
	case *querylang.SortOp, *querylang.TailOp, *querylang.SliceOp:
		return "coordinator-only" // buffers all records on the coordinating node
	case *querylang.HeadOp:
		return "short-circuit" // stops iteration early
	case *querylang.BarchartOp, *querylang.DonutOp, *querylang.MapOp, *querylang.RawOp:
		return "render-hint" // affects presentation, not data flow
	default:
		return "streaming" // per-record, no buffering
	}
}

// pipeOpNote generates a human-readable explanation of what a pipeline operator
// does and how the engine will execute it.
func pipeOpNote(op querylang.PipeOp) string {
	switch o := op.(type) {
	case *querylang.StatsOp:
		n := fmt.Sprintf("Aggregates all matching records (%s)", aggList(o.Aggs))
		if len(o.Groups) > 0 {
			n += ", grouped by " + groupList(o.Groups)
		}
		n += ". All records must be scanned before results are produced. In a cluster, each node aggregates locally and results are merged."
		return n
	case *querylang.TimechartOp:
		n := fmt.Sprintf("Buckets records into %d time intervals", o.N)
		if o.By != "" {
			n += ", split by " + o.By
		}
		n += ". All records must be scanned. Each node runs independently, results merged on coordinator."
		return n
	case *querylang.WhereOp:
		return fmt.Sprintf("Filters records matching: %s. Applied per-record with no buffering.", o.Expr.String())
	case *querylang.EvalOp:
		fields := make([]string, len(o.Assignments))
		for i, a := range o.Assignments {
			fields[i] = a.Field
		}
		return fmt.Sprintf("Computes new fields: %s. Applied per-record.", strings.Join(fields, ", "))
	case *querylang.SortOp:
		fields := make([]string, len(o.Fields))
		for i, f := range o.Fields {
			if f.Desc {
				fields[i] = f.Name + " (desc)"
			} else {
				fields[i] = f.Name + " (asc)"
			}
		}
		return fmt.Sprintf("Sorts all results by %s. Buffers all records in memory on the coordinator.", strings.Join(fields, ", "))
	case *querylang.HeadOp:
		return fmt.Sprintf("Returns only the first %d records. Stops scanning early once the limit is reached.", o.N)
	case *querylang.TailOp:
		return fmt.Sprintf("Returns only the last %d records. All records must be scanned to find the tail.", o.N)
	case *querylang.SliceOp:
		return fmt.Sprintf("Returns records %d through %d. All records must be buffered to extract the slice.", o.Start, o.End)
	case *querylang.RenameOp:
		pairs := make([]string, len(o.Renames))
		for i, r := range o.Renames {
			pairs[i] = r.Old + " \u2192 " + r.New
		}
		return fmt.Sprintf("Renames fields: %s. Applied per-record.", strings.Join(pairs, ", "))
	case *querylang.FieldsOp:
		if o.Drop {
			return fmt.Sprintf("Drops fields: %s. Applied per-record.", strings.Join(o.Names, ", "))
		}
		return fmt.Sprintf("Keeps only fields: %s. Applied per-record.", strings.Join(o.Names, ", "))
	case *querylang.DedupOp:
		if o.Window != "" {
			return fmt.Sprintf("Removes duplicate records keyed on EventID within a %s window.", o.Window)
		}
		return "Removes duplicate records keyed on EventID within a 1s window."
	case *querylang.LookupOp:
		return fmt.Sprintf("Enriches each record by looking up %s in the %s table.", strings.Join(o.Fields, ", "), o.Table)
	case *querylang.RawOp:
		return "Forces table output format. No data transformation."
	case *querylang.BarchartOp:
		return "Renders results as a bar chart. No data transformation."
	case *querylang.DonutOp:
		return "Renders results as a donut chart. No data transformation."
	case *querylang.HeatmapOp:
		return "Renders results as a heatmap. No data transformation."
	case *querylang.MapOp:
		if o.Mode == querylang.MapChoropleth {
			return fmt.Sprintf("Renders a choropleth map by %s. No data transformation.", o.CountryField)
		}
		return fmt.Sprintf("Renders a scatter map using %s/%s coordinates. No data transformation.", o.LatField, o.LonField)
	default:
		return ""
	}
}

func aggList(aggs []querylang.AggExpr) string {
	names := make([]string, len(aggs))
	for i, a := range aggs {
		names[i] = a.DefaultAlias()
	}
	return strings.Join(names, ", ")
}

func groupList(groups []querylang.GroupExpr) string {
	names := make([]string, len(groups))
	for i, g := range groups {
		names[i] = g.String()
	}
	return strings.Join(names, ", ")
}

// PipelineStepsToProto converts internal PipelineSteps to proto.
// Exported for use by the explain executor in cluster forwarding.
func PipelineStepsToProto(steps []query.PipelineStep) []*apiv1.PipelineStep {
	out := make([]*apiv1.PipelineStep, len(steps))
	for i, step := range steps {
		out[i] = &apiv1.PipelineStep{
			Name:           step.Index,
			InputEstimate:  int64(step.PositionsBefore),
			OutputEstimate: int64(step.PositionsAfter),
			Action:         step.Action,
			Reason:         step.Reason,
			Detail:         step.Details,
			Predicate:      step.Predicate,
		}
	}
	return out
}

// tableResultToProto converts an internal TableResult to the proto type.
func tableResultToProto(result *query.TableResult, pipeline *querylang.Pipeline) *apiv1.TableResult {
	rows := make([]*apiv1.TableRow, len(result.Rows))
	for i, row := range result.Rows {
		rows[i] = &apiv1.TableRow{Values: row}
	}

	// Determine result type from pipeline: timeseries if bin() or timechart
	// is present, but raw forces plain table.
	resultType := "table"
	hasRaw := false
	var vizOp querylang.PipeOp
	for _, pipe := range pipeline.Pipes {
		if _, ok := pipe.(*querylang.RawOp); ok {
			hasRaw = true
		}
		if _, ok := pipe.(*querylang.TimechartOp); ok {
			resultType = "timechart"
		}
		if stats, ok := pipe.(*querylang.StatsOp); ok {
			for _, g := range stats.Groups {
				if g.Bin != nil {
					resultType = "timeseries"
					break
				}
			}
		}
		switch pipe.(type) {
		case *querylang.LinechartOp, *querylang.BarchartOp, *querylang.DonutOp, *querylang.HeatmapOp, *querylang.ScatterOp, *querylang.MapOp:
			vizOp = pipe
		}
	}
	if hasRaw {
		resultType = "raw"
	}

	// Explicit viz operator overrides the result type if validation passes.
	// On validation failure, falls back to whatever resultType was computed above.
	if vizOp != nil && !hasRaw {
		if vizType := query.ValidateVizOp(vizOp, result); vizType != "" {
			resultType = vizType
		}
	}

	// Auto-detect visualization when no explicit operator was given.
	if resultType == "table" && vizOp == nil {
		if vizType := query.AutoDetectVizType(result); vizType != "" {
			resultType = vizType
		}
	}

	return &apiv1.TableResult{
		Columns:    result.Columns,
		Rows:       rows,
		Truncated:  result.Truncated,
		ResultType: resultType,
	}
}
