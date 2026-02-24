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
	currentPositions = e.buildTimeSeekStep(&cp, q, meta, cm, currentPositions)

	// 2. Process boolean expression.
	if q.BoolExpr != nil {
		if done := e.buildBoolExprPlan(&cp, q, meta, im, currentPositions); done {
			return cp
		}
		// currentPositions may have been updated via cp.EstimatedScan.
		currentPositions = cp.EstimatedScan
	}

	// Determine final scan mode.
	if currentPositions < cp.RecordCount {
		cp.ScanMode = "index-driven"
	} else {
		cp.ScanMode = "sequential"
	}
	cp.EstimatedScan = currentPositions

	// Add time bounds to runtime filter if present.
	appendTimeBoundsFilter(&cp, q)

	return cp
}

// buildTimeSeekStep builds the time-seek pipeline step. Returns updated position count.
func (e *Engine) buildTimeSeekStep(cp *ChunkPlan, q Query, meta chunk.ChunkMeta, cm chunk.ChunkManager, currentPositions int) int {
	lower, _ := q.TimeBounds()
	if lower.IsZero() {
		return currentPositions
	}

	step := PipelineStep{
		Index:           "time",
		Predicate:       "start >= " + lower.Format("15:04:05"),
		PositionsBefore: currentPositions,
		Action:          "seek",
		Reason:          "binary_search",
	}

	if pos, found, err := cm.FindStartPosition(meta.ID, lower); err == nil && found {
		skipped := int(pos) //nolint:gosec // G115: pos is a record position, always fits in int on 64-bit
		currentPositions = cp.RecordCount - skipped
		step.PositionsAfter = currentPositions
		step.Details = fmt.Sprintf("skip %d via idx.log", skipped)
	} else {
		step.PositionsAfter = currentPositions
		step.Details = "start before chunk"
	}
	cp.Pipeline = append(cp.Pipeline, step)
	return currentPositions
}

// buildBoolExprPlan processes the boolean expression for a chunk plan.
// Returns true if the chunk plan is fully resolved (skipped or multi-branch).
func (e *Engine) buildBoolExprPlan(cp *ChunkPlan, q Query, meta chunk.ChunkMeta, im index.IndexManager, currentPositions int) bool {
	dnf := querylang.ToDNF(q.BoolExpr)

	if len(dnf.Branches) == 0 {
		cp.ScanMode = "skipped"
		cp.SkipReason = "empty DNF"
		cp.EstimatedScan = 0
		return true
	}

	if len(dnf.Branches) == 1 {
		return e.buildSingleBranchChunkPlan(cp, &dnf.Branches[0], meta, im, currentPositions)
	}

	return e.buildMultiBranchChunkPlan(cp, dnf, meta, im)
}

// buildSingleBranchChunkPlan processes a single-branch DNF for a chunk plan.
// Returns true if the chunk plan is fully resolved (skipped).
func (e *Engine) buildSingleBranchChunkPlan(cp *ChunkPlan, branch *querylang.Conjunction, meta chunk.ChunkMeta, im index.IndexManager, currentPositions int) bool {
	currentPositions, skipped, skipReason, runtimeFilters := e.buildBranchPipeline(
		&cp.Pipeline, branch, meta, currentPositions, im)

	if skipped {
		cp.ScanMode = "skipped"
		cp.SkipReason = skipReason
		cp.EstimatedScan = 0
		return true
	}

	// Build runtime filter string.
	if len(runtimeFilters) > 0 {
		cp.RuntimeFilter = strings.Join(runtimeFilters, " AND ")
	}
	appendNegativeFilter(cp, branch)

	cp.EstimatedScan = currentPositions
	return false
}

// buildMultiBranchChunkPlan processes a multi-branch DNF for a chunk plan.
// Returns true (always resolves the bool expr portion).
func (e *Engine) buildMultiBranchChunkPlan(cp *ChunkPlan, dnf querylang.DNF, meta chunk.ChunkMeta, im index.IndexManager) bool {
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
		return true
	}

	cp.EstimatedScan = totalEstimated
	cp.RuntimeFilter = "DNF filter"
	return false
}

// appendNegativeFilter appends negative predicate runtime filter to a chunk plan.
func appendNegativeFilter(cp *ChunkPlan, branch *querylang.Conjunction) {
	if len(branch.Negative) == 0 {
		return
	}
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

// appendTimeBoundsFilter appends time bounds to the runtime filter if present.
func appendTimeBoundsFilter(cp *ChunkPlan, q Query) {
	if q.Start.IsZero() && q.End.IsZero() {
		return
	}
	if cp.RuntimeFilter != "none" && cp.RuntimeFilter != "" {
		cp.RuntimeFilter += " AND time bounds"
	} else {
		cp.RuntimeFilter = "time bounds"
	}
}

// branchStepResult is the outcome of processing one type of index lookup within a branch pipeline.
type branchStepResult struct {
	currentPositions int
	skipped          bool
	skipReason       string
	runtimeFilters   []string
}

// buildBranchPipeline builds pipeline steps for a single DNF branch.
// Returns updated position count, whether branch is skipped, skip reason, and runtime filters.
func (e *Engine) buildBranchPipeline(pipeline *[]PipelineStep, branch *querylang.Conjunction, meta chunk.ChunkMeta, currentPositions int, im index.IndexManager) (int, bool, string, []string) {
	var runtimeFilters []string

	tokens, kv, globs, _ := ConjunctionToFilters(branch)

	// Token index.
	if len(tokens) > 0 {
		res := e.buildTokenStep(pipeline, tokens, meta, currentPositions, im)
		if res.skipped {
			return 0, true, res.skipReason, nil
		}
		currentPositions = res.currentPositions
		runtimeFilters = append(runtimeFilters, res.runtimeFilters...)
	}

	// Glob patterns (prefix acceleration via token index).
	for _, g := range globs {
		res := e.buildGlobStep(pipeline, g, meta, currentPositions, im)
		if res.skipped {
			return 0, true, res.skipReason, nil
		}
		currentPositions = res.currentPositions
		runtimeFilters = append(runtimeFilters, res.runtimeFilters...)
	}

	// Regex predicates (always runtime, no index acceleration).
	for _, p := range branch.Positive {
		if p.Kind != querylang.PredRegex {
			continue
		}
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

	// KV indexes.
	for _, f := range kv {
		res := e.buildKVStep(pipeline, f, meta, currentPositions, im)
		if res.skipped {
			return 0, true, res.skipReason, nil
		}
		currentPositions = res.currentPositions
		runtimeFilters = append(runtimeFilters, res.runtimeFilters...)
	}

	return currentPositions, false, "", runtimeFilters
}

// buildTokenStep builds the token index pipeline step.
func (e *Engine) buildTokenStep(pipeline *[]PipelineStep, tokens []string, meta chunk.ChunkMeta, currentPositions int, im index.IndexManager) branchStepResult {
	predicate := fmt.Sprintf("token(%s)", strings.Join(tokens, ", "))
	step := PipelineStep{
		Index:           "token",
		Predicate:       predicate,
		PositionsBefore: currentPositions,
	}

	tokIdx, err := im.OpenTokenIndex(meta.ID)
	if errors.Is(err, index.ErrIndexNotFound) {
		step.PositionsAfter = currentPositions
		step.Action = "runtime"
		step.Reason = "index_missing"
		step.Details = "no token index"
		*pipeline = append(*pipeline, step)
		return branchStepResult{
			currentPositions: currentPositions,
			runtimeFilters:   []string{predicate},
		}
	}
	if err != nil {
		*pipeline = append(*pipeline, step)
		return branchStepResult{currentPositions: currentPositions}
	}

	result := e.lookupTokenPositions(tokens, meta, tokIdx)

	switch {
	case !result.allFound && result.missingDefinitive:
		step.PositionsAfter = 0
		step.Action = "skipped"
		step.Reason = "no_match"
		step.Details = fmt.Sprintf("'%s' not in chunk", result.missingToken)
		*pipeline = append(*pipeline, step)
		return branchStepResult{skipped: true, skipReason: fmt.Sprintf("no match (%s)", predicate)}

	case !result.allFound:
		step.PositionsAfter = currentPositions
		step.Action = "runtime"
		step.Reason = result.missingReason
		step.Details = fmt.Sprintf("'%s' not indexable (%s)", result.missingToken, result.missingReason)
		*pipeline = append(*pipeline, step)
		return branchStepResult{
			currentPositions: currentPositions,
			runtimeFilters:   []string{predicate},
		}

	case len(result.positions) == 0:
		step.PositionsAfter = 0
		step.Action = "skipped"
		step.Reason = "empty_intersection"
		step.Details = "no records match all tokens"
		*pipeline = append(*pipeline, step)
		return branchStepResult{skipped: true, skipReason: fmt.Sprintf("empty intersection (%s)", predicate)}

	default:
		step.PositionsAfter = len(result.positions)
		step.Action = "indexed"
		step.Reason = "indexed"
		step.Details = fmt.Sprintf("%d token(s) intersected", len(tokens))
		currentPositions = len(result.positions)
		*pipeline = append(*pipeline, step)
		return branchStepResult{currentPositions: currentPositions}
	}
}

// tokenLookupResult holds the result of looking up tokens in the token index.
type tokenLookupResult struct {
	positions        []uint64
	allFound         bool
	missingToken     string
	missingReason    string
	missingDefinitive bool
}

// lookupTokenPositions looks up all tokens in the token index and intersects results.
func (e *Engine) lookupTokenPositions(tokens []string, meta chunk.ChunkMeta, tokIdx *index.Index[index.TokenIndexEntry]) tokenLookupResult {
	reader := index.NewTokenIndexReader(meta.ID, tokIdx.Entries())
	var positions []uint64

	for i, tok := range tokens {
		pos, found := reader.Lookup(tok)
		if !found {
			reason, definitive := classifyTokenMiss(tok)
			return tokenLookupResult{
				missingToken:     tok,
				missingReason:    reason,
				missingDefinitive: definitive,
			}
		}
		if i == 0 {
			positions = pos
		} else {
			positions = intersectPositions(positions, pos)
		}
	}

	return tokenLookupResult{positions: positions, allFound: true}
}

// buildGlobStep builds a glob pattern pipeline step.
func (e *Engine) buildGlobStep(pipeline *[]PipelineStep, g GlobFilter, meta chunk.ChunkMeta, currentPositions int, im index.IndexManager) branchStepResult {
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
		*pipeline = append(*pipeline, step)
		return branchStepResult{
			currentPositions: currentPositions,
			runtimeFilters:   []string{predicate},
		}
	}

	tokIdx, err := im.OpenTokenIndex(meta.ID)
	if err != nil {
		step.PositionsAfter = currentPositions
		step.Action = "runtime"
		step.Reason = "index_missing"
		step.Details = "no token index"
		*pipeline = append(*pipeline, step)
		return branchStepResult{
			currentPositions: currentPositions,
			runtimeFilters:   []string{predicate},
		}
	}

	reader := index.NewTokenIndexReader(meta.ID, tokIdx.Entries())
	positions, found := reader.LookupPrefix(prefix)
	if !found {
		step.PositionsAfter = 0
		step.Action = "skipped"
		step.Reason = "no_match"
		step.Details = fmt.Sprintf("no tokens with prefix %q", prefix)
		*pipeline = append(*pipeline, step)
		return branchStepResult{skipped: true, skipReason: fmt.Sprintf("no match (%s)", predicate)}
	}

	step.PositionsAfter = len(positions)
	step.Action = "indexed"
	step.Reason = "prefix_lookup"
	step.Details = fmt.Sprintf("prefix %q matched %d positions", prefix, len(positions))
	currentPositions = min(currentPositions, len(positions))
	*pipeline = append(*pipeline, step)
	return branchStepResult{currentPositions: currentPositions}
}

// buildKVStep builds a KV index pipeline step.
func (e *Engine) buildKVStep(pipeline *[]PipelineStep, f KeyValueFilter, meta chunk.ChunkMeta, currentPositions int, im index.IndexManager) branchStepResult {
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
		*pipeline = append(*pipeline, step)
		return branchStepResult{
			currentPositions: currentPositions,
			runtimeFilters:   []string{predicate},
		}
	}

	if len(result.positions) == 0 {
		step.PositionsAfter = 0
		step.Action = "skipped"
		step.Reason = "no_match"
		step.Details = result.details
		*pipeline = append(*pipeline, step)
		return branchStepResult{skipped: true, skipReason: fmt.Sprintf("no match (%s)", predicate)}
	}

	newCount := len(result.positions)
	if currentPositions < int(meta.RecordCount) {
		newCount = min(currentPositions, len(result.positions))
	}
	step.PositionsAfter = newCount
	step.Action = "indexed"
	step.Reason = "indexed"
	step.Details = result.details
	*pipeline = append(*pipeline, step)
	return branchStepResult{currentPositions: newCount}
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

// kvIndexes holds all opened KV-related indexes for a chunk.
type kvIndexes struct {
	attrKV     *index.Index[index.AttrKVIndexEntry]
	attrKVErr  error
	attrKey    *index.Index[index.AttrKeyIndexEntry]
	attrKeyErr error
	attrVal    *index.Index[index.AttrValueIndexEntry]
	attrValErr error
	kv          *index.Index[index.KVIndexEntry]
	kvStatus    index.KVIndexStatus
	kvErr       error
	kvKey       *index.Index[index.KVKeyIndexEntry]
	kvKeyStatus index.KVIndexStatus
	kvKeyErr    error
	kvVal       *index.Index[index.KVValueIndexEntry]
	kvValStatus index.KVIndexStatus
	kvValErr    error
}

// openKVIndexes opens all KV-related indexes for a chunk.
func openKVIndexes(chunkID chunk.ChunkID, im index.IndexManager) kvIndexes {
	var idx kvIndexes
	idx.attrKV, idx.attrKVErr = im.OpenAttrKVIndex(chunkID)
	idx.attrKey, idx.attrKeyErr = im.OpenAttrKeyIndex(chunkID)
	idx.attrVal, idx.attrValErr = im.OpenAttrValueIndex(chunkID)
	idx.kv, idx.kvStatus, idx.kvErr = im.OpenKVIndex(chunkID)
	idx.kvKey, idx.kvKeyStatus, idx.kvKeyErr = im.OpenKVKeyIndex(chunkID)
	idx.kvVal, idx.kvValStatus, idx.kvValErr = im.OpenKVValueIndex(chunkID)
	return idx
}

// lookupKVIndex looks up a single KV filter across all available indexes.
func (e *Engine) lookupKVIndex(f KeyValueFilter, chunkID chunk.ChunkID, im index.IndexManager) kvLookupResult {
	if f.Key == "" && f.Value == "" {
		return kvLookupResult{}
	}

	idx := openKVIndexes(chunkID, im)
	keyLower := strings.ToLower(f.Key)
	valLower := strings.ToLower(f.Value)

	result, detailParts := e.lookupKVStandard(f, chunkID, keyLower, valLower, &idx)
	detailParts = e.lookupKVJSON(f, chunkID, keyLower, valLower, im, &result, detailParts)

	result.details = strings.Join(detailParts, " ")
	classifyKVResult(&result, detailParts)
	return result
}

// lookupKVStandard performs the standard (non-JSON) KV index lookup.
func (e *Engine) lookupKVStandard(f KeyValueFilter, chunkID chunk.ChunkID, keyLower, valLower string, idx *kvIndexes) (kvLookupResult, []string) {
	var result kvLookupResult
	var detailParts []string

	switch {
	case f.Value == "":
		result, detailParts = lookupKeyOnly(chunkID, keyLower, idx)
	case f.Key == "":
		result, detailParts = lookupValueOnly(chunkID, valLower, idx)
	case f.Op != querylang.OpEq:
		result, detailParts = lookupKeyNonEq(chunkID, keyLower, idx)
	default:
		result, detailParts = lookupKeyValueExact(chunkID, keyLower, valLower, idx)
	}

	return result, detailParts
}

// lookupKeyOnly handles key=* pattern KV lookups.
func lookupKeyOnly(chunkID chunk.ChunkID, keyLower string, idx *kvIndexes) (kvLookupResult, []string) {
	var result kvLookupResult
	var detailParts []string

	if idx.attrKeyErr == nil {
		result.available = true
		reader := index.NewAttrKeyIndexReader(chunkID, idx.attrKey.Entries())
		if pos, found := reader.Lookup(keyLower); found {
			result.positions = unionPositions(result.positions, pos)
			detailParts = append(detailParts, fmt.Sprintf("attr_key=%d", len(pos)))
		}
	}

	if idx.kvKeyErr != nil {
		return result, detailParts
	}
	if idx.kvKeyStatus == index.KVCapped {
		detailParts = append(detailParts, "msg_key=capped")
		return result, detailParts
	}
	result.available = true
	reader := index.NewKVKeyIndexReader(chunkID, idx.kvKey.Entries())
	if pos, found := reader.Lookup(keyLower); found {
		result.positions = unionPositions(result.positions, pos)
		detailParts = append(detailParts, fmt.Sprintf("msg_key=%d", len(pos)))
	}

	return result, detailParts
}

// lookupValueOnly handles *=value pattern KV lookups.
func lookupValueOnly(chunkID chunk.ChunkID, valLower string, idx *kvIndexes) (kvLookupResult, []string) {
	var result kvLookupResult
	var detailParts []string

	if idx.attrValErr == nil {
		result.available = true
		reader := index.NewAttrValueIndexReader(chunkID, idx.attrVal.Entries())
		if pos, found := reader.Lookup(valLower); found {
			result.positions = unionPositions(result.positions, pos)
			detailParts = append(detailParts, fmt.Sprintf("attr_val=%d", len(pos)))
		}
	}

	if idx.kvValErr != nil {
		return result, detailParts
	}
	if idx.kvValStatus == index.KVCapped {
		detailParts = append(detailParts, "msg_val=capped")
		return result, detailParts
	}
	result.available = true
	reader := index.NewKVValueIndexReader(chunkID, idx.kvVal.Entries())
	if pos, found := reader.Lookup(valLower); found {
		result.positions = unionPositions(result.positions, pos)
		detailParts = append(detailParts, fmt.Sprintf("msg_val=%d", len(pos)))
	}

	return result, detailParts
}

// lookupKeyNonEq handles non-eq comparison KV lookups (key-only index, runtime value comparison).
func lookupKeyNonEq(chunkID chunk.ChunkID, keyLower string, idx *kvIndexes) (kvLookupResult, []string) {
	var result kvLookupResult
	var detailParts []string

	if idx.attrKeyErr == nil {
		result.available = true
		reader := index.NewAttrKeyIndexReader(chunkID, idx.attrKey.Entries())
		if pos, found := reader.Lookup(keyLower); found {
			result.positions = unionPositions(result.positions, pos)
			detailParts = append(detailParts, fmt.Sprintf("attr_key=%d", len(pos)))
		}
	}

	if idx.kvKeyErr == nil {
		if idx.kvKeyStatus == index.KVCapped {
			detailParts = append(detailParts, "msg_key=capped")
		} else {
			result.available = true
			reader := index.NewKVKeyIndexReader(chunkID, idx.kvKey.Entries())
			if pos, found := reader.Lookup(keyLower); found {
				result.positions = unionPositions(result.positions, pos)
				detailParts = append(detailParts, fmt.Sprintf("msg_key=%d", len(pos)))
			}
		}
	}

	detailParts = append(detailParts, "filter=runtime_compare")
	return result, detailParts
}

// lookupKeyValueExact handles exact key=value KV lookups.
func lookupKeyValueExact(chunkID chunk.ChunkID, keyLower, valLower string, idx *kvIndexes) (kvLookupResult, []string) {
	var result kvLookupResult
	var detailParts []string

	if idx.attrKVErr == nil {
		result.available = true
		reader := index.NewAttrKVIndexReader(chunkID, idx.attrKV.Entries())
		if pos, found := reader.Lookup(keyLower, valLower); found {
			result.positions = unionPositions(result.positions, pos)
			detailParts = append(detailParts, fmt.Sprintf("attr_kv=%d", len(pos)))
		}
	}

	if idx.kvErr == nil {
		if idx.kvStatus == index.KVCapped {
			detailParts = append(detailParts, "msg_kv=capped")
		} else {
			result.available = true
			reader := index.NewKVIndexReader(chunkID, idx.kv.Entries())
			if pos, found := reader.Lookup(keyLower, valLower); found {
				result.positions = unionPositions(result.positions, pos)
				detailParts = append(detailParts, fmt.Sprintf("msg_kv=%d", len(pos)))
			}
		}
	}

	return result, detailParts
}

// lookupKVJSON performs JSON structural index lookups.
func (e *Engine) lookupKVJSON(f KeyValueFilter, chunkID chunk.ChunkID, keyLower, valLower string, im index.IndexManager, result *kvLookupResult, detailParts []string) []string {
	jsonPathIdx, jsonPathStatus, jsonPathErr := im.OpenJSONPathIndex(chunkID)
	jsonPVIdx, jsonPVStatus, jsonPVErr := im.OpenJSONPVIndex(chunkID)

	if jsonPathErr != nil && jsonPVErr != nil {
		return detailParts
	}

	var pathEntries []index.JSONPathIndexEntry
	var pvEntries []index.JSONPVIndexEntry
	if jsonPathErr == nil {
		pathEntries = jsonPathIdx.Entries()
	}
	if jsonPVErr == nil {
		pvEntries = jsonPVIdx.Entries()
	}
	jsonReader := index.NewJSONIndexReader(chunkID, pathEntries, jsonPathStatus, pvEntries, jsonPVStatus)

	return lookupKVJSONByFilter(f, keyLower, valLower, jsonReader, result, detailParts)
}

// lookupKVJSONByFilter dispatches the JSON lookup based on the filter pattern.
func lookupKVJSONByFilter(f KeyValueFilter, keyLower, valLower string, jsonReader *index.JSONIndexReader, result *kvLookupResult, detailParts []string) []string {
	// No JSON index for value-only queries.
	if f.Key == "" {
		return detailParts
	}

	// Key exists (any value) or non-eq comparison: path-only lookup.
	if f.Value == "" || f.Op != querylang.OpEq {
		result.available = true
		jsonPath := dotToNull(keyLower)
		if pos, found := jsonReader.LookupPath(jsonPath); found {
			result.positions = unionPositions(result.positions, pos)
			detailParts = append(detailParts, fmt.Sprintf("msg_json=%d", len(pos)))
		}
		return detailParts
	}

	// Key=value exact: path-value lookup.
	if jsonReader.PVStatus() == index.JSONCapped {
		detailParts = append(detailParts, "msg_json=capped")
		return detailParts
	}
	result.available = true
	jsonPath := dotToNull(keyLower)
	if pos, found := jsonReader.LookupPathValue(jsonPath, valLower); found {
		result.positions = unionPositions(result.positions, pos)
		detailParts = append(detailParts, fmt.Sprintf("msg_json=%d", len(pos)))
	} else {
		detailParts = append(detailParts, "msg_json=0")
	}
	return detailParts
}

// classifyKVResult sets the reason field on a kvLookupResult based on availability and positions.
func classifyKVResult(result *kvLookupResult, detailParts []string) {
	if !result.available {
		result.reason = "index_missing"
		return
	}
	if len(result.positions) > 0 {
		return
	}
	for _, d := range detailParts {
		if strings.Contains(d, "capped") {
			result.reason = "budget_exhausted"
			return
		}
	}
	result.reason = "value_not_indexed"
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
