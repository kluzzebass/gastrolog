package repl

import (
	"errors"
	"fmt"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
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
	ChunkID      chunk.ChunkID
	Sealed       bool
	RecordCount  int
	TimeRange    string // "StartTS - EndTS"
	ScanStrategy string // "sequential" or "index-driven"
	IndexesUsed  []indexUsage
	Filters      []string // runtime filters applied
}

// indexUsage describes how an index is used.
type indexUsage struct {
	Name        string
	Status      string // "available", "unavailable", "not applicable"
	Purpose     string // what it's used for
	Positions   int    // number of positions from this index (0 if N/A)
	Description string // human-readable description
}

func (r *REPL) cmdExplain(out *strings.Builder, args []string) {
	// Parse query args (same as cmdQuery)
	q := query.Query{}
	var tokens []string
	var kvFilters []query.KeyValueFilter

	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			fmt.Fprintf(out, "Invalid filter: %s (expected key=value)\n", arg)
			return
		}

		switch k {
		case "start":
			t, err := parseTime(v)
			if err != nil {
				fmt.Fprintf(out, "Invalid start time: %v\n", err)
				return
			}
			q.Start = t
		case "end":
			t, err := parseTime(v)
			if err != nil {
				fmt.Fprintf(out, "Invalid end time: %v\n", err)
				return
			}
			q.End = t
		case "token":
			tokens = append(tokens, v)
		case "limit":
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
				fmt.Fprintf(out, "Invalid limit: %v\n", err)
				return
			}
			q.Limit = n
		default:
			// Treat as key=value filter
			key := k
			value := v
			if k == "*" {
				key = ""
			}
			if v == "*" {
				value = ""
			}
			kvFilters = append(kvFilters, query.KeyValueFilter{Key: key, Value: value})
		}
	}

	if len(tokens) > 0 {
		q.Tokens = tokens
	}
	if len(kvFilters) > 0 {
		q.KV = kvFilters
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
		ChunkID:     meta.ID,
		Sealed:      meta.Sealed,
		RecordCount: int(meta.RecordCount),
		TimeRange:   fmt.Sprintf("%s - %s", meta.StartTS.Format("2006-01-02T15:04:05"), meta.EndTS.Format("2006-01-02T15:04:05")),
	}

	if !meta.Sealed {
		cp.ScanStrategy = "sequential (unsealed)"
		cp.Filters = r.getRuntimeFilters(q)
		return cp
	}

	// Check which indexes are available and how they'd be used
	var indexesUsed []indexUsage
	var hasPositionSource bool

	// 1. Time index - used for seeking, not position filtering
	lower, _ := q.TimeBounds()
	if !lower.IsZero() {
		timeIdx, err := im.OpenTimeIndex(meta.ID)
		if err == nil && len(timeIdx.Entries()) > 0 {
			indexesUsed = append(indexesUsed, indexUsage{
				Name:        "time",
				Status:      "available",
				Purpose:     "seek to start position",
				Positions:   len(timeIdx.Entries()),
				Description: fmt.Sprintf("sparse index with %d entries, used for initial seek", len(timeIdx.Entries())),
			})
		} else {
			// Fall back to binary search on idx.log
			indexesUsed = append(indexesUsed, indexUsage{
				Name:        "time",
				Status:      "unavailable",
				Purpose:     "seek to start position",
				Description: "falling back to binary search on idx.log",
			})
		}
	}

	// 2. Token index
	if len(q.Tokens) > 0 {
		tokIdx, err := im.OpenTokenIndex(meta.ID)
		if err == nil {
			reader := index.NewTokenIndexReader(meta.ID, tokIdx.Entries())
			var totalPositions int
			allFound := true
			for _, tok := range q.Tokens {
				positions, found := reader.Lookup(tok)
				if !found {
					allFound = false
					break
				}
				if totalPositions == 0 {
					totalPositions = len(positions)
				} else {
					// Intersection reduces positions
					totalPositions = min(totalPositions, len(positions))
				}
			}
			if allFound {
				indexesUsed = append(indexesUsed, indexUsage{
					Name:        "token",
					Status:      "available",
					Purpose:     "position filtering",
					Positions:   totalPositions,
					Description: fmt.Sprintf("filtering by %d token(s), ~%d positions", len(q.Tokens), totalPositions),
				})
				hasPositionSource = true
			} else {
				indexesUsed = append(indexesUsed, indexUsage{
					Name:        "token",
					Status:      "available (no match)",
					Purpose:     "position filtering",
					Description: "token not found in index - chunk will be skipped",
				})
			}
		} else if errors.Is(err, index.ErrIndexNotFound) {
			indexesUsed = append(indexesUsed, indexUsage{
				Name:        "token",
				Status:      "unavailable",
				Purpose:     "position filtering",
				Description: "runtime filter will tokenize each record",
			})
			cp.Filters = append(cp.Filters, fmt.Sprintf("token filter: %v", q.Tokens))
		}
	}

	// 3. Key-value indexes (attr + kv)
	var chunkSkipped bool
	if len(q.KV) > 0 {
		kvUsage := r.analyzeKVIndexes(q.KV, meta.ID, im)
		indexesUsed = append(indexesUsed, kvUsage...)
		for _, u := range kvUsage {
			if u.Status == "available" && u.Positions > 0 {
				hasPositionSource = true
			} else if u.Status == "no match" {
				// Index available but no match - chunk will be skipped
				chunkSkipped = true
			} else if u.Status == "unavailable" {
				// Index not available - need runtime filter
				cp.Filters = append(cp.Filters, fmt.Sprintf("kv filter: %s", u.Name))
			}
		}
	}

	cp.IndexesUsed = indexesUsed

	if chunkSkipped {
		cp.ScanStrategy = "skipped (no index match)"
	} else if hasPositionSource {
		cp.ScanStrategy = "index-driven (seek to positions)"
	} else {
		cp.ScanStrategy = "sequential"
	}

	// Add time bounds as runtime filter if present
	if !q.Start.IsZero() || !q.End.IsZero() {
		cp.Filters = append(cp.Filters, "time bounds check")
	}

	return cp
}

func (r *REPL) analyzeKVIndexes(filters []query.KeyValueFilter, chunkID chunk.ChunkID, im index.IndexManager) []indexUsage {
	var usages []indexUsage

	// Check attr indexes - track availability separately from match
	attrKVIdx, attrKVErr := im.OpenAttrKVIndex(chunkID)
	attrKeyIdx, attrKeyErr := im.OpenAttrKeyIndex(chunkID)
	attrValIdx, attrValErr := im.OpenAttrValueIndex(chunkID)

	// Check kv indexes (message body)
	kvIdx, kvStatus, kvErr := im.OpenKVIndex(chunkID)
	kvKeyIdx, kvKeyStatus, kvKeyErr := im.OpenKVKeyIndex(chunkID)
	kvValIdx, kvValStatus, kvValErr := im.OpenKVValueIndex(chunkID)

	for _, f := range filters {
		var filterUsage indexUsage
		filterUsage.Purpose = "position filtering"

		if f.Key == "" && f.Value == "" {
			continue // matches everything
		} else if f.Value == "" {
			// Key only: key=* pattern
			filterUsage.Name = fmt.Sprintf("key index (%s=*)", f.Key)
			var positions int
			var anyIndexAvailable bool
			var details []string

			if attrKeyErr == nil {
				anyIndexAvailable = true
				reader := index.NewAttrKeyIndexReader(chunkID, attrKeyIdx.Entries())
				if pos, found := reader.Lookup(strings.ToLower(f.Key)); found {
					positions += len(pos)
					details = append(details, fmt.Sprintf("attr_key: %d", len(pos)))
				} else {
					details = append(details, "attr_key: no match")
				}
			}
			if kvKeyErr == nil && kvKeyStatus != index.KVCapped {
				anyIndexAvailable = true
				reader := index.NewKVKeyIndexReader(chunkID, kvKeyIdx.Entries())
				if pos, found := reader.Lookup(strings.ToLower(f.Key)); found {
					positions += len(pos)
					details = append(details, fmt.Sprintf("msg_key: %d", len(pos)))
				} else {
					details = append(details, "msg_key: no match")
				}
			}

			if !anyIndexAvailable {
				filterUsage.Status = "unavailable"
				filterUsage.Description = "no key index available, using runtime filter"
			} else if positions > 0 {
				filterUsage.Status = "available"
				filterUsage.Positions = positions
				filterUsage.Description = fmt.Sprintf("%d positions (%s)", positions, strings.Join(details, ", "))
			} else {
				filterUsage.Status = "no match"
				filterUsage.Description = fmt.Sprintf("key %q not in index (%s) - chunk skipped", f.Key, strings.Join(details, ", "))
			}
		} else if f.Key == "" {
			// Value only: *=value pattern
			filterUsage.Name = fmt.Sprintf("value index (*=%s)", f.Value)
			var positions int
			var anyIndexAvailable bool
			var details []string

			if attrValErr == nil {
				anyIndexAvailable = true
				reader := index.NewAttrValueIndexReader(chunkID, attrValIdx.Entries())
				if pos, found := reader.Lookup(strings.ToLower(f.Value)); found {
					positions += len(pos)
					details = append(details, fmt.Sprintf("attr_val: %d", len(pos)))
				} else {
					details = append(details, "attr_val: no match")
				}
			}
			if kvValErr == nil && kvValStatus != index.KVCapped {
				anyIndexAvailable = true
				reader := index.NewKVValueIndexReader(chunkID, kvValIdx.Entries())
				if pos, found := reader.Lookup(strings.ToLower(f.Value)); found {
					positions += len(pos)
					details = append(details, fmt.Sprintf("msg_val: %d", len(pos)))
				} else {
					details = append(details, "msg_val: no match")
				}
			}

			if !anyIndexAvailable {
				filterUsage.Status = "unavailable"
				filterUsage.Description = "no value index available, using runtime filter"
			} else if positions > 0 {
				filterUsage.Status = "available"
				filterUsage.Positions = positions
				filterUsage.Description = fmt.Sprintf("%d positions (%s)", positions, strings.Join(details, ", "))
			} else {
				filterUsage.Status = "no match"
				filterUsage.Description = fmt.Sprintf("value %q not in index (%s) - chunk skipped", f.Value, strings.Join(details, ", "))
			}
		} else {
			// Both key and value: exact key=value match
			filterUsage.Name = fmt.Sprintf("kv index (%s=%s)", f.Key, f.Value)
			var positions int
			var anyIndexAvailable bool
			var details []string

			if attrKVErr == nil {
				anyIndexAvailable = true
				reader := index.NewAttrKVIndexReader(chunkID, attrKVIdx.Entries())
				if pos, found := reader.Lookup(strings.ToLower(f.Key), strings.ToLower(f.Value)); found {
					positions += len(pos)
					details = append(details, fmt.Sprintf("attr_kv: %d", len(pos)))
				} else {
					details = append(details, "attr_kv: no match")
				}
			}
			if kvErr == nil && kvStatus != index.KVCapped {
				anyIndexAvailable = true
				reader := index.NewKVIndexReader(chunkID, kvIdx.Entries())
				if pos, found := reader.Lookup(f.Key, f.Value); found {
					positions += len(pos)
					details = append(details, fmt.Sprintf("msg_kv: %d", len(pos)))
				} else {
					details = append(details, "msg_kv: no match")
				}
			}

			if !anyIndexAvailable {
				filterUsage.Status = "unavailable"
				filterUsage.Description = "no kv index available, using runtime filter"
			} else if positions > 0 {
				filterUsage.Status = "available"
				filterUsage.Positions = positions
				filterUsage.Description = fmt.Sprintf("%d positions (%s)", positions, strings.Join(details, ", "))
			} else {
				filterUsage.Status = "no match"
				filterUsage.Description = fmt.Sprintf("%s=%s not in index (%s) - chunk skipped", f.Key, f.Value, strings.Join(details, ", "))
			}
		}

		usages = append(usages, filterUsage)
	}

	return usages
}

func (r *REPL) getRuntimeFilters(q query.Query) []string {
	var filters []string
	if len(q.Tokens) > 0 {
		filters = append(filters, fmt.Sprintf("token filter: %v", q.Tokens))
	}
	if len(q.KV) > 0 {
		filters = append(filters, fmt.Sprintf("kv filter: %v", formatKVFilters(q.KV)))
	}
	if !q.Start.IsZero() || !q.End.IsZero() {
		filters = append(filters, "time bounds check")
	}
	return filters
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
			sealedStr = "unsealed"
		}
		out.WriteString(fmt.Sprintf("Chunk %d: %s (%s)\n", i+1, cp.ChunkID.String(), sealedStr))
		out.WriteString(fmt.Sprintf("  Time Range: %s\n", cp.TimeRange))
		out.WriteString(fmt.Sprintf("  Records: %d\n", cp.RecordCount))
		out.WriteString(fmt.Sprintf("  Strategy: %s\n", cp.ScanStrategy))

		if len(cp.IndexesUsed) > 0 {
			out.WriteString("  Indexes:\n")
			for _, idx := range cp.IndexesUsed {
				out.WriteString(fmt.Sprintf("    - %s [%s]: %s\n", idx.Name, idx.Status, idx.Description))
			}
		}

		if len(cp.Filters) > 0 {
			out.WriteString("  Runtime Filters:\n")
			for _, f := range cp.Filters {
				out.WriteString(fmt.Sprintf("    - %s\n", f))
			}
		}

		out.WriteString("\n")
	}
}
