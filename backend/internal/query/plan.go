package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/querylang"
	"gastrolog/internal/tokenizer"

	"github.com/google/uuid"
)

// QueryPlan describes how a query will be executed across chunks.
type QueryPlan struct {
	Query       Query
	Direction   string      // "forward" or "reverse"
	ChunkPlans  []ChunkPlan // per-chunk execution plans
	TotalChunks int         // total chunks in the store
}

// ChunkPlan describes the execution plan for a single chunk.
type ChunkPlan struct {
	StoreID       uuid.UUID // store this chunk belongs to
	ChunkID       chunk.ChunkID
	Sealed        bool
	RecordCount   int
	StartTS       time.Time
	EndTS         time.Time
	Pipeline      []PipelineStep // index pipeline steps
	BranchPlans   []BranchPlan   // per-branch plans for DNF queries (len > 1)
	ScanMode      string         // "index-driven", "sequential", "skipped"
	SkipReason    string         // reason for skipping (if ScanMode == "skipped")
	RuntimeFilter string         // runtime filter description
	EstimatedScan int            // estimated records to scan
}

// BranchPlan describes the execution plan for a single DNF branch.
type BranchPlan struct {
	BranchExpr    string         // string representation of the branch
	Pipeline      []PipelineStep // index pipeline for this branch
	Skipped       bool           // whether this branch produces no matches
	SkipReason    string         // reason for skipping
	EstimatedScan int            // estimated records from this branch
}

// PipelineStep describes one step in the index application pipeline.
type PipelineStep struct {
	Index           string // index name/type: "time", "token", "kv"
	Predicate       string // what we're filtering for
	PositionsBefore int    // positions before this step (0 = all records)
	PositionsAfter  int    // positions after this step
	Action          string // "indexed", "runtime", "skipped", "seek"
	Reason          string // why this action was taken
	Details         string // additional details
}

// Explain returns the query execution plan without executing the query.
// For multi-store engines, this explains the plan across all stores.
func (e *Engine) Explain(ctx context.Context, q Query) (*QueryPlan, error) {
	// Normalize query to ensure BoolExpr is set.
	q = q.Normalize()

	// Extract store predicates and get remaining query expression.
	allStores := e.listStores()
	selectedStores, remainingExpr := ExtractStoreFilter(q.BoolExpr, allStores)
	if selectedStores == nil {
		selectedStores = allStores
	}

	// Extract chunk predicates.
	chunkIDs, remainingExpr := ExtractChunkFilter(remainingExpr)

	// Update query to use remaining expression (without store/chunk predicates).
	queryForPlan := q
	queryForPlan.BoolExpr = remainingExpr

	plan := &QueryPlan{
		Query:     q,
		Direction: "forward",
	}
	if q.Reverse() {
		plan.Direction = "reverse"
	}

	// Collect chunks from all selected stores.
	for _, storeID := range selectedStores {
		cm, im := e.getStoreManagers(storeID)
		if cm == nil {
			continue
		}

		metas, err := cm.List()
		if err != nil {
			return nil, err
		}

		plan.TotalChunks += len(metas)

		// Select chunks that overlap the query time range.
		candidates := e.selectChunks(metas, queryForPlan, chunkIDs)

		for _, meta := range candidates {
			cp := e.buildChunkPlan(ctx, queryForPlan, meta, storeID, cm, im)
			plan.ChunkPlans = append(plan.ChunkPlans, cp)
		}
	}

	return plan, nil
}

// buildChunkPlan builds the execution plan for a single chunk.
func (e *Engine) buildChunkPlan(ctx context.Context, q Query, meta chunk.ChunkMeta, storeID uuid.UUID, cm chunk.ChunkManager, im index.IndexManager) ChunkPlan {
	cp := ChunkPlan{
		StoreID:       storeID,
		ChunkID:       meta.ID,
		Sealed:        meta.Sealed,
		RecordCount:   int(meta.RecordCount),
		StartTS:       meta.StartTS,
		EndTS:         meta.EndTS,
		RuntimeFilter: "none",
	}

	// Unsealed chunks always use sequential scan with runtime filters.
	if !meta.Sealed {
		cp.ScanMode = "sequential"
		cp.EstimatedScan = cp.RecordCount
		cp.RuntimeFilter = e.buildRuntimeFilterDesc(q)
		return cp
	}

	// Track current position count through the pipeline.
	currentPositions := cp.RecordCount

	// 1. Time seek - binary search idx.log for start position.
	lower, _ := q.TimeBounds()
	if !lower.IsZero() {
		step := PipelineStep{
			Index:           "time",
			Predicate:       fmt.Sprintf("start >= %s", lower.Format("15:04:05")),
			PositionsBefore: currentPositions,
		}

		if pos, found, err := cm.FindStartPosition(meta.ID, lower); err == nil && found {
			skipped := int(pos)
			currentPositions = cp.RecordCount - skipped
			step.PositionsAfter = currentPositions
			step.Action = "seek"
			step.Reason = "binary_search"
			step.Details = fmt.Sprintf("skip %d via idx.log", skipped)
		} else {
			step.PositionsAfter = currentPositions
			step.Action = "seek"
			step.Reason = "binary_search"
			step.Details = "start before chunk"
		}
		cp.Pipeline = append(cp.Pipeline, step)
	}

	// Process boolean expression.
	if q.BoolExpr != nil {
		dnf := querylang.ToDNF(q.BoolExpr)

		if len(dnf.Branches) == 0 {
			cp.ScanMode = "skipped"
			cp.SkipReason = "empty DNF"
			cp.EstimatedScan = 0
			return cp
		}

		if len(dnf.Branches) == 1 {
			// Single branch: build pipeline directly.
			branch := &dnf.Branches[0]
			var skipped bool
			var skipReason string
			var runtimeFilters []string

			currentPositions, skipped, skipReason, runtimeFilters = e.buildBranchPipeline(
				&cp.Pipeline, branch, meta, currentPositions, im)

			if skipped {
				cp.ScanMode = "skipped"
				cp.SkipReason = skipReason
				cp.EstimatedScan = 0
				return cp
			}

			// Build runtime filter string.
			if len(runtimeFilters) > 0 {
				cp.RuntimeFilter = strings.Join(runtimeFilters, " AND ")
			}
			if len(branch.Negative) > 0 {
				var negParts []string
				for _, p := range branch.Negative {
					negParts = append(negParts, "NOT "+p.String())
				}
				negStr := strings.Join(negParts, " AND ")
				if cp.RuntimeFilter != "none" && cp.RuntimeFilter != "" {
					cp.RuntimeFilter += " AND " + negStr
				} else {
					cp.RuntimeFilter = negStr
				}
			}
		} else {
			// Multi-branch DNF: build per-branch plans.
			var totalEstimated int
			allSkipped := true

			for _, branch := range dnf.Branches {
				bp := e.buildBranchPlan(&branch, meta, cp.RecordCount, im)
				cp.BranchPlans = append(cp.BranchPlans, bp)

				if !bp.Skipped {
					allSkipped = false
					totalEstimated += bp.EstimatedScan
				}
			}

			if allSkipped {
				cp.ScanMode = "skipped"
				cp.SkipReason = "all branches empty"
				cp.EstimatedScan = 0
				return cp
			}

			currentPositions = totalEstimated
			cp.RuntimeFilter = "DNF filter"
		}
	}

	// Determine final scan mode.
	if currentPositions < cp.RecordCount {
		cp.ScanMode = "index-driven"
	} else {
		cp.ScanMode = "sequential"
	}
	cp.EstimatedScan = currentPositions

	// Add time bounds to runtime filter if present.
	if !q.Start.IsZero() || !q.End.IsZero() {
		if cp.RuntimeFilter != "none" && cp.RuntimeFilter != "" {
			cp.RuntimeFilter += " AND time bounds"
		} else {
			cp.RuntimeFilter = "time bounds"
		}
	}

	return cp
}

// buildBranchPipeline builds pipeline steps for a single DNF branch.
// Returns updated position count, whether branch is skipped, skip reason, and runtime filters.
func (e *Engine) buildBranchPipeline(pipeline *[]PipelineStep, branch *querylang.Conjunction, meta chunk.ChunkMeta, currentPositions int, im index.IndexManager) (int, bool, string, []string) {
	var runtimeFilters []string

	tokens, kv, globs, _ := ConjunctionToFilters(branch)

	// Token index.
	if len(tokens) > 0 {
		predicate := fmt.Sprintf("token(%s)", strings.Join(tokens, ", "))
		step := PipelineStep{
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
			var missingDefinitive bool

			for i, tok := range tokens {
				pos, found := reader.Lookup(tok)
				if !found {
					allFound = false
					missingToken = tok
					missingReason, missingDefinitive = classifyTokenMiss(tok)
					break
				}
				if i == 0 {
					positions = pos
				} else {
					positions = intersectPositions(positions, pos)
				}
			}

			if !allFound {
				if missingDefinitive {
					// Token is indexable but absent — no records contain it.
					step.PositionsAfter = 0
					step.Action = "skipped"
					step.Reason = "no_match"
					step.Details = fmt.Sprintf("'%s' not in chunk", missingToken)
					*pipeline = append(*pipeline, step)
					return 0, true, fmt.Sprintf("no match (%s)", predicate), nil
				}
				step.PositionsAfter = currentPositions
				step.Action = "runtime"
				step.Reason = missingReason
				step.Details = fmt.Sprintf("'%s' not indexable (%s)", missingToken, missingReason)
				runtimeFilters = append(runtimeFilters, predicate)
			} else if len(positions) == 0 {
				step.PositionsAfter = 0
				step.Action = "skipped"
				step.Reason = "empty_intersection"
				step.Details = "no records match all tokens"
				*pipeline = append(*pipeline, step)
				return 0, true, fmt.Sprintf("empty intersection (%s)", predicate), nil
			} else {
				step.PositionsAfter = len(positions)
				step.Action = "indexed"
				step.Reason = "indexed"
				step.Details = fmt.Sprintf("%d token(s) intersected", len(tokens))
				currentPositions = len(positions)
			}
		} else if errors.Is(err, index.ErrIndexNotFound) {
			step.PositionsAfter = currentPositions
			step.Action = "runtime"
			step.Reason = "index_missing"
			step.Details = "no token index"
			runtimeFilters = append(runtimeFilters, predicate)
		}
		*pipeline = append(*pipeline, step)
	}

	// Glob patterns (prefix acceleration via token index).
	if len(globs) > 0 {
		for _, g := range globs {
			predicate := fmt.Sprintf("glob(%s)", g.RawPattern)
			step := PipelineStep{
				Index:           "token",
				Predicate:       predicate,
				PositionsBefore: currentPositions,
			}

			prefix, hasPrefix := querylang.ExtractGlobPrefix(g.RawPattern)
			if !hasPrefix {
				step.PositionsAfter = currentPositions
				step.Action = "runtime"
				step.Reason = "no_prefix"
				step.Details = "glob has no literal prefix for index lookup"
				runtimeFilters = append(runtimeFilters, predicate)
			} else {
				tokIdx, err := im.OpenTokenIndex(meta.ID)
				if err == nil {
					reader := index.NewTokenIndexReader(meta.ID, tokIdx.Entries())
					positions, found := reader.LookupPrefix(prefix)
					if !found {
						step.PositionsAfter = 0
						step.Action = "skipped"
						step.Reason = "no_match"
						step.Details = fmt.Sprintf("no tokens with prefix %q", prefix)
						*pipeline = append(*pipeline, step)
						return 0, true, fmt.Sprintf("no match (%s)", predicate), nil
					}
					step.PositionsAfter = len(positions)
					step.Action = "indexed"
					step.Reason = "prefix_lookup"
					step.Details = fmt.Sprintf("prefix %q matched %d positions", prefix, len(positions))
					currentPositions = min(currentPositions, len(positions))
				} else {
					step.PositionsAfter = currentPositions
					step.Action = "runtime"
					step.Reason = "index_missing"
					step.Details = "no token index"
					runtimeFilters = append(runtimeFilters, predicate)
				}
			}
			*pipeline = append(*pipeline, step)
		}
	}

	// Regex predicates (always runtime, no index acceleration).
	for _, p := range branch.Positive {
		if p.Kind == querylang.PredRegex {
			predicate := fmt.Sprintf("regex(/%s/)", p.Value)
			step := PipelineStep{
				Index:           "runtime",
				Predicate:       predicate,
				PositionsBefore: currentPositions,
				PositionsAfter:  currentPositions,
				Action:          "runtime",
				Reason:          "no_index",
				Details:         "regex requires sequential scan",
			}
			*pipeline = append(*pipeline, step)
			runtimeFilters = append(runtimeFilters, predicate)
		}
	}

	// KV indexes.
	if len(kv) > 0 {
		for _, f := range kv {
			predicate := formatKVFilter(f)
			step := PipelineStep{
				Index:           "kv",
				Predicate:       predicate,
				PositionsBefore: currentPositions,
			}

			result := e.lookupKVIndex(f, meta.ID, im)

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
				*pipeline = append(*pipeline, step)
				return 0, true, fmt.Sprintf("no match (%s)", predicate), nil
			} else {
				newCount := len(result.positions)
				if currentPositions < int(meta.RecordCount) {
					newCount = min(currentPositions, len(result.positions))
				}
				step.PositionsAfter = newCount
				step.Action = "indexed"
				step.Reason = "indexed"
				step.Details = result.details
				currentPositions = newCount
			}
			*pipeline = append(*pipeline, step)
		}
	}

	return currentPositions, false, "", runtimeFilters
}

// buildBranchPlan builds a plan for a single DNF branch.
func (e *Engine) buildBranchPlan(branch *querylang.Conjunction, meta chunk.ChunkMeta, recordCount int, im index.IndexManager) BranchPlan {
	bp := BranchPlan{
		BranchExpr:    branch.String(),
		EstimatedScan: recordCount,
	}

	currentPositions, skipped, skipReason, _ := e.buildBranchPipeline(&bp.Pipeline, branch, meta, recordCount, im)

	if skipped {
		bp.Skipped = true
		bp.SkipReason = skipReason
		bp.EstimatedScan = 0
	} else {
		bp.EstimatedScan = currentPositions

		// Add negative predicates info.
		if len(branch.Negative) > 0 {
			var negParts []string
			for _, p := range branch.Negative {
				negParts = append(negParts, "NOT "+p.String())
			}
			step := PipelineStep{
				Index:           "runtime",
				Predicate:       strings.Join(negParts, " AND "),
				PositionsBefore: currentPositions,
				PositionsAfter:  currentPositions,
				Action:          "runtime",
				Reason:          "negative_predicate",
				Details:         "NOT cannot use index",
			}
			bp.Pipeline = append(bp.Pipeline, step)
		}
	}

	return bp
}

// kvLookupResult holds the result of a KV index lookup.
type kvLookupResult struct {
	positions []uint64
	available bool
	reason    string
	details   string
}

// lookupKVIndex looks up a single KV filter across all available indexes.
func (e *Engine) lookupKVIndex(f KeyValueFilter, chunkID chunk.ChunkID, im index.IndexManager) kvLookupResult {
	result := kvLookupResult{}
	var detailParts []string

	keyLower := strings.ToLower(f.Key)
	valLower := strings.ToLower(f.Value)

	// Open indexes.
	attrKVIdx, attrKVErr := im.OpenAttrKVIndex(chunkID)
	attrKeyIdx, attrKeyErr := im.OpenAttrKeyIndex(chunkID)
	attrValIdx, attrValErr := im.OpenAttrValueIndex(chunkID)
	kvIdx, kvStatus, kvErr := im.OpenKVIndex(chunkID)
	kvKeyIdx, kvKeyStatus, kvKeyErr := im.OpenKVKeyIndex(chunkID)
	kvValIdx, kvValStatus, kvValErr := im.OpenKVValueIndex(chunkID)

	if f.Key == "" && f.Value == "" {
		return result
	} else if f.Value == "" {
		// Key only: key=* pattern.
		if attrKeyErr == nil {
			result.available = true
			reader := index.NewAttrKeyIndexReader(chunkID, attrKeyIdx.Entries())
			if pos, found := reader.Lookup(keyLower); found {
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
				if pos, found := reader.Lookup(keyLower); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_key=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_key=0")
				}
			}
		}
	} else if f.Key == "" {
		// Value only: *=value pattern.
		if attrValErr == nil {
			result.available = true
			reader := index.NewAttrValueIndexReader(chunkID, attrValIdx.Entries())
			if pos, found := reader.Lookup(valLower); found {
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
				if pos, found := reader.Lookup(valLower); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_val=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_val=0")
				}
			}
		}
	} else if f.Op != querylang.OpEq {
		// Non-eq comparison: use key-only index, runtime value comparison.
		if attrKeyErr == nil {
			result.available = true
			reader := index.NewAttrKeyIndexReader(chunkID, attrKeyIdx.Entries())
			if pos, found := reader.Lookup(keyLower); found {
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
				if pos, found := reader.Lookup(keyLower); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_key=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_key=0")
				}
			}
		}
		detailParts = append(detailParts, "filter=runtime_compare")
	} else {
		// Both key and value: exact key=value match.
		if attrKVErr == nil {
			result.available = true
			reader := index.NewAttrKVIndexReader(chunkID, attrKVIdx.Entries())
			if pos, found := reader.Lookup(keyLower, valLower); found {
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
				if pos, found := reader.Lookup(keyLower, valLower); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_kv=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_kv=0")
				}
			}
		}
	}

	// JSON structural index (dots in key → null-byte path separators).
	jsonPathIdx, jsonPathStatus, jsonPathErr := im.OpenJSONPathIndex(chunkID)
	jsonPVIdx, jsonPVStatus, jsonPVErr := im.OpenJSONPVIndex(chunkID)
	if jsonPathErr == nil || jsonPVErr == nil {
		var pathEntries []index.JSONPathIndexEntry
		var pvEntries []index.JSONPVIndexEntry
		if jsonPathErr == nil {
			pathEntries = jsonPathIdx.Entries()
		}
		if jsonPVErr == nil {
			pvEntries = jsonPVIdx.Entries()
		}
		jsonReader := index.NewJSONIndexReader(chunkID, pathEntries, jsonPathStatus, pvEntries, jsonPVStatus)

		if f.Key != "" && f.Value == "" {
			// Key exists: path lookup.
			result.available = true
			jsonPath := dotToNull(keyLower)
			if pos, found := jsonReader.LookupPath(jsonPath); found {
				result.positions = unionPositions(result.positions, pos)
				detailParts = append(detailParts, fmt.Sprintf("msg_json=%d", len(pos)))
			} else {
				detailParts = append(detailParts, "msg_json=0")
			}
		} else if f.Key != "" && f.Value != "" && f.Op != querylang.OpEq {
			// Non-eq comparison: path-only lookup (runtime compare on value).
			result.available = true
			jsonPath := dotToNull(keyLower)
			if pos, found := jsonReader.LookupPath(jsonPath); found {
				result.positions = unionPositions(result.positions, pos)
				detailParts = append(detailParts, fmt.Sprintf("msg_json=%d", len(pos)))
			} else {
				detailParts = append(detailParts, "msg_json=0")
			}
		} else if f.Key != "" && f.Value != "" {
			// Key=value: path-value lookup.
			if jsonReader.PVStatus() == index.JSONCapped {
				detailParts = append(detailParts, "msg_json=capped")
			} else {
				result.available = true
				jsonPath := dotToNull(keyLower)
				if pos, found := jsonReader.LookupPathValue(jsonPath, valLower); found {
					result.positions = unionPositions(result.positions, pos)
					detailParts = append(detailParts, fmt.Sprintf("msg_json=%d", len(pos)))
				} else {
					detailParts = append(detailParts, "msg_json=0")
				}
			}
		}
		// No JSON index for value-only queries.
	}

	result.details = strings.Join(detailParts, " ")

	if !result.available {
		result.reason = "index_missing"
	} else if len(result.positions) == 0 {
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

// buildRuntimeFilterDesc builds a description of runtime filters for a query.
func (e *Engine) buildRuntimeFilterDesc(q Query) string {
	var parts []string

	if q.BoolExpr != nil {
		parts = append(parts, q.BoolExpr.String())
	} else {
		if len(q.Tokens) > 0 {
			parts = append(parts, fmt.Sprintf("token(%s)", strings.Join(q.Tokens, ", ")))
		}
		for _, f := range q.KV {
			parts = append(parts, formatKVFilter(f))
		}
	}

	if !q.Start.IsZero() || !q.End.IsZero() {
		parts = append(parts, "time bounds")
	}

	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " AND ")
}

// formatKVFilter formats a KeyValueFilter as a string.
func formatKVFilter(f KeyValueFilter) string {
	key := f.Key
	if key == "" {
		key = "*"
	}
	value := f.Value
	if value == "" {
		value = "*"
	}
	return key + f.Op.String() + value
}

// classifyTokenMiss returns why a token is not in the index.
// If the token is indexable (would have been indexed if present in the data),
// its absence means no records contain it. Otherwise, the tokenizer would have
// skipped it and we need a runtime filter.
func classifyTokenMiss(tok string) (reason string, definitive bool) {
	if tokenizer.IsIndexable(strings.ToLower(tok)) {
		return "no_match", true // token is valid but not in chunk data
	}

	tokLower := strings.ToLower(tok)

	// Check for non-ASCII.
	for _, r := range tokLower {
		if r > 127 {
			return "non_ascii", false
		}
	}

	// Too short.
	if len(tokLower) < 2 {
		return "too_short", false
	}

	// Numeric/hex — the tokenizer skips these.
	return "numeric", false
}
