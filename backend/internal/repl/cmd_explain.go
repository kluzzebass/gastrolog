package repl

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
	"gastrolog/internal/tokenizer"
)

// explainPlan describes how a query will be executed.
type explainPlan struct {
	Query       query.Query
	Direction   string // "forward" or "reverse"
	ChunkPlans  []chunkPlan
	TotalChunks int
}

// chunkPlan describes the execution plan for a single chunk.
type chunkPlan struct {
	ChunkID       chunk.ChunkID
	Sealed        bool
	RecordCount   int
	TimeRange     string // "StartTS - EndTS"
	TimeOverlaps  bool   // whether chunk time range overlaps query
	Pipeline      []pipelineStep
	ScanMode      string // "index-driven", "sequential", "skipped"
	SkipReason    string // reason for skipping (if ScanMode == "skipped")
	RuntimeFilter string // runtime filter description (empty if none)
	EstimatedScan int    // estimated records to scan
}

// pipelineStep describes one step in the index application pipeline.
type pipelineStep struct {
	Index           string // index name/type
	Predicate       string // what we're filtering for
	PositionsBefore int    // positions before this step (0 = all records)
	PositionsAfter  int    // positions after this step
	Action          string // "indexed", "runtime", "skipped", "seek"
	Reason          string // explicit reason for the action
	Details         string // additional details (e.g., attr_kv=N msg_kv=M)
}

func (r *REPL) cmdExplain(out *strings.Builder, args []string) {
	q, errMsg := parseQueryArgs(args)
	if errMsg != "" {
		out.WriteString(errMsg + "\n")
		return
	}

	// Build the explain plan
	plan, err := r.buildExplainPlan(q)
	if err != nil {
		fmt.Fprintf(out, "Error building explain plan: %v\n", err)
		return
	}

	// Print the plan
	r.printExplainPlan(out, plan)
}

func (r *REPL) buildExplainPlan(q query.Query) (*explainPlan, error) {
	cm := r.orch.ChunkManager(r.store)
	if cm == nil {
		return nil, fmt.Errorf("chunk manager not found for store %q", r.store)
	}

	im := r.orch.IndexManager(r.store)
	if im == nil {
		return nil, fmt.Errorf("index manager not found for store %q", r.store)
	}

	chunks, err := cm.List()
	if err != nil {
		return nil, fmt.Errorf("listing chunks: %w", err)
	}

	// Filter chunks by time range (same logic as query engine)
	lower, upper := q.TimeBounds()
	var selectedChunks []chunk.ChunkMeta
	for _, m := range chunks {
		if m.Sealed {
			if !lower.IsZero() && m.EndTS.Before(lower) {
				continue
			}
			if !upper.IsZero() && !m.StartTS.Before(upper) {
				continue
			}
		}
		selectedChunks = append(selectedChunks, m)
	}

	plan := &explainPlan{
		Query:       q,
		Direction:   "forward",
		TotalChunks: len(chunks),
	}
	if q.Reverse() {
		plan.Direction = "reverse"
	}

	// Build plan for each selected chunk
	for _, meta := range selectedChunks {
		cp := r.buildChunkPlan(q, meta, cm, im)
		plan.ChunkPlans = append(plan.ChunkPlans, cp)
	}

	return plan, nil
}

func (r *REPL) buildChunkPlan(q query.Query, meta chunk.ChunkMeta, cm chunk.ChunkManager, im index.IndexManager) chunkPlan {
	cp := chunkPlan{
		ChunkID:       meta.ID,
		Sealed:        meta.Sealed,
		RecordCount:   int(meta.RecordCount),
		TimeRange:     fmt.Sprintf("%s - %s", meta.StartTS.Format("2006-01-02T15:04:05"), meta.EndTS.Format("2006-01-02T15:04:05")),
		TimeOverlaps:  true, // if we got here, it overlaps
		RuntimeFilter: "none",
	}

	if !meta.Sealed {
		cp.ScanMode = "sequential"
		cp.EstimatedScan = cp.RecordCount
		filter := r.buildRuntimeFilterDesc(q)
		if filter != "" {
			cp.RuntimeFilter = filter
		}
		return cp
	}

	// Track current position count through the pipeline
	currentPositions := cp.RecordCount // start with all records
	var runtimeFilters []string
	var skipped bool
	var skipReason string

	// 1. Time index - used for seeking to start position
	lower, _ := q.TimeBounds()
	if !lower.IsZero() {
		step := pipelineStep{
			Index:           "time",
			Predicate:       fmt.Sprintf("start >= %s", lower.Format("15:04:05")),
			PositionsBefore: currentPositions,
		}

		timeIdx, err := im.OpenTimeIndex(meta.ID)
		if err == nil && len(timeIdx.Entries()) > 0 {
			// Find approximate skip count using time index
			reader := index.NewTimeIndexReader(meta.ID, timeIdx.Entries())
			if ref, found := reader.FindStart(lower); found {
				skippedRecords := int(ref.Pos)
				currentPositions = cp.RecordCount - skippedRecords
				step.PositionsAfter = currentPositions
				step.Action = "seek"
				step.Reason = "indexed"
				step.Details = fmt.Sprintf("skip %d via sparse index", skippedRecords)
			} else {
				step.PositionsAfter = currentPositions
				step.Action = "seek"
				step.Reason = "indexed"
				step.Details = "start before chunk"
			}
		} else {
			// Fall back to binary search
			if pos, found, err := cm.FindStartPosition(meta.ID, lower); err == nil && found {
				skippedRecords := int(pos)
				currentPositions = cp.RecordCount - skippedRecords
				step.PositionsAfter = currentPositions
				step.Action = "seek"
				step.Reason = "binary_search"
				step.Details = fmt.Sprintf("skip %d via idx.log", skippedRecords)
			} else {
				step.PositionsAfter = currentPositions
				step.Action = "seek"
				step.Reason = "binary_search"
				step.Details = "idx.log lookup"
			}
		}
		cp.Pipeline = append(cp.Pipeline, step)
	}

	// 2. Token index
	if len(q.Tokens) > 0 {
		predicate := fmt.Sprintf("token(%s)", strings.Join(q.Tokens, ", "))
		step := pipelineStep{
			Index:           "token",
			Predicate:       predicate,
			PositionsBefore: currentPositions,
		}

		tokIdx, err := im.OpenTokenIndex(meta.ID)
		if err == nil {
			reader := index.NewTokenIndexReader(meta.ID, tokIdx.Entries())
			var positions []uint64
			allFound := true
			var missingToken string
			var missingReason string

			for i, tok := range q.Tokens {
				pos, found := reader.Lookup(tok)
				if !found {
					allFound = false
					missingToken = tok
					missingReason = classifyTokenMiss(tok)
					break
				}
				if i == 0 {
					positions = pos
				} else {
					positions = intersectPositions(positions, pos)
				}
			}

			if !allFound {
				// Token not in index - fall back to runtime filter
				step.PositionsAfter = currentPositions
				step.Action = "runtime"
				step.Reason = missingReason
				step.Details = fmt.Sprintf("'%s' not indexed", missingToken)
				runtimeFilters = append(runtimeFilters, predicate)
			} else if len(positions) == 0 {
				// All tokens found but intersection is empty - no matches
				step.PositionsAfter = 0
				step.Action = "skipped"
				step.Reason = "empty_intersection"
				step.Details = "no records match all tokens"
				skipped = true
				skipReason = fmt.Sprintf("empty intersection (%s)", predicate)
			} else {
				step.PositionsAfter = len(positions)
				step.Action = "indexed"
				step.Reason = "indexed"
				step.Details = fmt.Sprintf("%d token(s) intersected", len(q.Tokens))
				currentPositions = len(positions)
			}
		} else if errors.Is(err, index.ErrIndexNotFound) {
			step.PositionsAfter = currentPositions
			step.Action = "runtime"
			step.Reason = "index_missing"
			step.Details = "no token index"
			runtimeFilters = append(runtimeFilters, predicate)
		}
		cp.Pipeline = append(cp.Pipeline, step)
	}

	// 3. Key-value indexes
	if len(q.KV) > 0 && !skipped {
		// Open all indexes once
		attrKVIdx, attrKVErr := im.OpenAttrKVIndex(meta.ID)
		attrKeyIdx, attrKeyErr := im.OpenAttrKeyIndex(meta.ID)
		attrValIdx, attrValErr := im.OpenAttrValueIndex(meta.ID)
		kvIdx, kvStatus, kvErr := im.OpenKVIndex(meta.ID)
		kvKeyIdx, kvKeyStatus, kvKeyErr := im.OpenKVKeyIndex(meta.ID)
		kvValIdx, kvValStatus, kvValErr := im.OpenKVValueIndex(meta.ID)

		for _, f := range q.KV {
			predicate := formatSingleKVFilter(f)
			step := pipelineStep{
				Index:           "kv",
				Predicate:       predicate,
				PositionsBefore: currentPositions,
			}

			result := r.lookupKVIndex(f,
				attrKVIdx, attrKVErr, attrKeyIdx, attrKeyErr, attrValIdx, attrValErr,
				kvIdx, kvStatus, kvErr, kvKeyIdx, kvKeyStatus, kvKeyErr, kvValIdx, kvValStatus, kvValErr,
				meta.ID)

			if !result.available {
				step.PositionsAfter = currentPositions
				step.Action = "runtime"
				step.Reason = result.reason
				step.Details = result.details
				runtimeFilters = append(runtimeFilters, predicate)
			} else if len(result.positions) == 0 {
				step.PositionsAfter = 0
				step.Action = "skipped"
				step.Reason = "no_match"
				step.Details = result.details
				skipped = true
				skipReason = fmt.Sprintf("no match (%s)", predicate)
			} else {
				// Intersect with current positions if we have a position list
				newCount := len(result.positions)
				if currentPositions < cp.RecordCount {
					// We already have a position list, this is an estimate
					newCount = min(currentPositions, len(result.positions))
				}
				step.PositionsAfter = newCount
				step.Action = "indexed"
				step.Reason = "indexed"
				step.Details = result.details
				currentPositions = newCount
			}
			cp.Pipeline = append(cp.Pipeline, step)

			if skipped {
				break
			}
		}
	}

	// Determine final scan mode and estimated scan
	if skipped {
		cp.ScanMode = "skipped"
		cp.SkipReason = skipReason
		cp.EstimatedScan = 0
		cp.RuntimeFilter = "" // no runtime filter for skipped chunks
	} else if currentPositions < cp.RecordCount {
		cp.ScanMode = "index-driven"
		cp.EstimatedScan = currentPositions
	} else {
		cp.ScanMode = "sequential"
		cp.EstimatedScan = currentPositions
	}

	// Build runtime filter string
	if !skipped {
		if len(runtimeFilters) > 0 {
			cp.RuntimeFilter = strings.Join(runtimeFilters, " AND ")
		}
		// Add time bounds to runtime filter if present (always applied)
		if !q.Start.IsZero() || !q.End.IsZero() {
			if cp.RuntimeFilter != "none" {
				cp.RuntimeFilter += " AND time bounds"
			} else {
				cp.RuntimeFilter = "time bounds"
			}
		}
	}

	return cp
}

// classifyTokenMiss returns the reason why a token might not be in the index.
func classifyTokenMiss(tok string) string {
	// Check for non-ASCII
	for _, r := range tok {
		if r > unicode.MaxASCII {
			return "non_ascii"
		}
	}

	// Check if it would be excluded by tokenizer rules
	tokBytes := []byte(strings.ToLower(tok))

	// Too short
	if len(tokBytes) < 2 {
		return "too_short"
	}

	// Check if it looks numeric/hex (tokenizer excludes these)
	allHex := true
	for _, b := range tokBytes {
		if !tokenizer.IsHexDigit(b) && b != '-' {
			allHex = false
			break
		}
	}
	if allHex {
		return "numeric"
	}

	// Default: token not indexed (might be low frequency or budget)
	return "not_indexed"
}

// kvLookupResult holds the result of a KV index lookup.
type kvLookupResult struct {
	positions []uint64
	available bool   // whether any index was available
	reason    string // reason for action
	details   string // breakdown of sources (e.g., "attr_kv=N msg_kv=M")
}

// lookupKVIndex looks up a single KV filter across all available indexes.
func (r *REPL) lookupKVIndex(f query.KeyValueFilter,
	attrKVIdx *index.Index[index.AttrKVIndexEntry], attrKVErr error,
	attrKeyIdx *index.Index[index.AttrKeyIndexEntry], attrKeyErr error,
	attrValIdx *index.Index[index.AttrValueIndexEntry], attrValErr error,
	kvIdx *index.Index[index.KVIndexEntry], kvStatus index.KVIndexStatus, kvErr error,
	kvKeyIdx *index.Index[index.KVKeyIndexEntry], kvKeyStatus index.KVIndexStatus, kvKeyErr error,
	kvValIdx *index.Index[index.KVValueIndexEntry], kvValStatus index.KVIndexStatus, kvValErr error,
	chunkID chunk.ChunkID) kvLookupResult {

	result := kvLookupResult{}
	var detailParts []string

	if f.Key == "" && f.Value == "" {
		return result
	} else if f.Value == "" {
		// Key only: key=* pattern
		if attrKeyErr == nil {
			result.available = true
			reader := index.NewAttrKeyIndexReader(chunkID, attrKeyIdx.Entries())
			if pos, found := reader.Lookup(strings.ToLower(f.Key)); found {
				result.positions = unionPositions(result.positions, pos)
				detailParts = append(detailParts, fmt.Sprintf("attr_key=%d", len(pos)))
			} else {
				detailParts = append(detailParts, "attr_key=0")
			}
		}
		if kvKeyErr == nil {
			if kvKeyStatus == index.KVCapped {
				detailParts = append(detailParts, "msg_key=capped")
			} else {
				result.available = true
				reader := index.NewKVKeyIndexReader(chunkID, kvKeyIdx.Entries())
				if pos, found := reader.Lookup(strings.ToLower(f.Key)); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_key=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_key=0")
				}
			}
		}
	} else if f.Key == "" {
		// Value only: *=value pattern
		if attrValErr == nil {
			result.available = true
			reader := index.NewAttrValueIndexReader(chunkID, attrValIdx.Entries())
			if pos, found := reader.Lookup(strings.ToLower(f.Value)); found {
				result.positions = unionPositions(result.positions, pos)
				detailParts = append(detailParts, fmt.Sprintf("attr_val=%d", len(pos)))
			} else {
				detailParts = append(detailParts, "attr_val=0")
			}
		}
		if kvValErr == nil {
			if kvValStatus == index.KVCapped {
				detailParts = append(detailParts, "msg_val=capped")
			} else {
				result.available = true
				reader := index.NewKVValueIndexReader(chunkID, kvValIdx.Entries())
				if pos, found := reader.Lookup(strings.ToLower(f.Value)); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_val=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_val=0")
				}
			}
		}
	} else {
		// Both key and value: exact key=value match
		if attrKVErr == nil {
			result.available = true
			reader := index.NewAttrKVIndexReader(chunkID, attrKVIdx.Entries())
			if pos, found := reader.Lookup(strings.ToLower(f.Key), strings.ToLower(f.Value)); found {
				result.positions = unionPositions(result.positions, pos)
				detailParts = append(detailParts, fmt.Sprintf("attr_kv=%d", len(pos)))
			} else {
				detailParts = append(detailParts, "attr_kv=0")
			}
		}
		if kvErr == nil {
			if kvStatus == index.KVCapped {
				detailParts = append(detailParts, "msg_kv=capped")
			} else {
				result.available = true
				reader := index.NewKVIndexReader(chunkID, kvIdx.Entries())
				if pos, found := reader.Lookup(f.Key, f.Value); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_kv=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_kv=0")
				}
			}
		}
	}

	result.details = strings.Join(detailParts, " ")

	if !result.available {
		result.reason = "index_missing"
	} else if len(result.positions) == 0 {
		// Check if any index was capped
		for _, d := range detailParts {
			if strings.Contains(d, "capped") {
				result.reason = "budget_exhausted"
				return result
			}
		}
		result.reason = "value_not_indexed"
	}

	return result
}

func (r *REPL) buildRuntimeFilterDesc(q query.Query) string {
	var parts []string
	if len(q.Tokens) > 0 {
		parts = append(parts, fmt.Sprintf("token(%s)", strings.Join(q.Tokens, ", ")))
	}
	for _, f := range q.KV {
		parts = append(parts, formatSingleKVFilter(f))
	}
	if !q.Start.IsZero() || !q.End.IsZero() {
		parts = append(parts, "time bounds")
	}
	return strings.Join(parts, " AND ")
}

func formatSingleKVFilter(f query.KeyValueFilter) string {
	key := f.Key
	if key == "" {
		key = "*"
	}
	value := f.Value
	if value == "" {
		value = "*"
	}
	return key + "=" + value
}

func formatKVFilters(filters []query.KeyValueFilter) string {
	var parts []string
	for _, f := range filters {
		parts = append(parts, formatSingleKVFilter(f))
	}
	return strings.Join(parts, ", ")
}

// intersectPositions returns positions present in both sorted slices.
func intersectPositions(a, b []uint64) []uint64 {
	var result []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// unionPositions returns all unique positions from both sorted slices, in sorted order.
func unionPositions(a, b []uint64) []uint64 {
	result := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

func (r *REPL) printExplainPlan(out *strings.Builder, plan *explainPlan) {
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
	if len(plan.Query.Tokens) > 0 {
		out.WriteString(fmt.Sprintf("  Tokens: %v\n", plan.Query.Tokens))
	}
	if len(plan.Query.KV) > 0 {
		out.WriteString(fmt.Sprintf("  KV Filters: %s\n", formatKVFilters(plan.Query.KV)))
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
		out.WriteString(fmt.Sprintf("  Time Range: %s [overlaps]\n", cp.TimeRange))
		out.WriteString(fmt.Sprintf("  Records: %d\n", cp.RecordCount))

		// For skipped chunks, show skip reason and nothing else
		if cp.ScanMode == "skipped" {
			out.WriteString(fmt.Sprintf("\n  Chunk skipped: %s\n\n", cp.SkipReason))
			continue
		}

		// Pipeline (only for non-skipped sealed chunks)
		if len(cp.Pipeline) > 0 {
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
