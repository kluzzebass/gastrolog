package query

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/lookup"
	"gastrolog/internal/querylang"
)

// applyRecordOps applies the given pipeline operators in order to the records
// yielded by the iterator.
//
// Several bounded-memory paths avoid materializing the full search result:
//   - a trailing tail/slice with no preceding sort streams through inline
//     filters/transforms into a ring buffer or positional collector;
//   - a sort whose output is capped (head/tail/slice, or the caller's
//     implicit limit) keeps only a bounded top-N working set;
//   - a sortless, capless pipeline with an implicit limit stops collecting
//     once the limit is reached.
//
// Only pipelines that genuinely need the full record set (e.g. an uncapped
// sort, or a filter after a sort) fall back to full materialization.
func applyRecordOps(ctx context.Context, it iter.Seq2[chunk.Record, error], ops []querylang.PipeOp, resolve lookup.Resolver) ([]chunk.Record, error) {
	return applyRecordOpsLimit(ctx, it, ops, resolve, 0)
}

// applyRecordOpsLimit is applyRecordOps with an implicit output limit.
// implicitLimit (0 = none) is the caller's post-pipeline truncation: the
// caller keeps only the first implicitLimit records of the final result.
// Knowing it up front lets sortless pipelines stop collecting early and
// capless sorts bound their working set.
func applyRecordOpsLimit(ctx context.Context, it iter.Seq2[chunk.Record, error], ops []querylang.PipeOp, resolve lookup.Resolver, implicitLimit int) ([]chunk.Record, error) {
	// Streaming tail/slice when no sort precedes the cap.
	if capIdx, ok := findStreamableCap(ops); ok {
		return applyStreamingCap(ctx, it, ops, capIdx, resolve)
	}

	// Bounded sort: a sort whose output is capped by a following
	// head/tail/slice (or by the implicit limit) keeps a top-N working set.
	if plan, ok := planBoundedSort(ops, implicitLimit); ok {
		return applyBoundedSort(ctx, it, ops, plan, resolve)
	}

	// Sortless, capless pipeline with an implicit output limit: apply
	// operators per-record and stop once the limit is reached.
	if implicitLimit > 0 && opsStreamable(ops) {
		return applyStreamingLimit(ctx, it, ops, implicitLimit, resolve)
	}

	// Full materialization fallback.
	var records []chunk.Record
	for rec, err := range it {
		if err != nil {
			return nil, err
		}
		records = append(records, rec.Copy())
	}
	return applyBatchOps(ctx, records, ops, resolve)
}

// findStreamableCap returns the index of the last TailOp or SliceOp in ops
// that no SortOp precedes. Operators before the cap run per-record; operators
// after it (including sorts) run batch-wise on the capped, bounded set.
// Returns (index, true) if the streaming optimization can be applied.
func findStreamableCap(ops []querylang.PipeOp) (int, bool) {
	capIdx := -1
	for i, op := range ops {
		switch op.(type) {
		case *querylang.SortOp:
			// Caps after a sort cannot stream; caps before it still can.
			return capIdx, capIdx >= 0
		case *querylang.TailOp, *querylang.SliceOp:
			capIdx = i
		}
	}
	return capIdx, capIdx >= 0
}

// opsStreamable reports whether every operator can run per-record via
// streamFilter (no sort, no tail, no slice).
func opsStreamable(ops []querylang.PipeOp) bool {
	for _, op := range ops {
		switch op.(type) {
		case *querylang.WhereOp, *querylang.DedupOp, *querylang.EvalOp,
			*querylang.RenameOp, *querylang.FieldsOp, *querylang.LookupOp,
			*querylang.HeadOp:
		default:
			return false
		}
	}
	return true
}

// applyStreamingLimit applies streamable operators per-record and stops as
// soon as implicitLimit records survive the pipeline. Because every operator
// is order-preserving and per-record, the first implicitLimit survivors equal
// the first implicitLimit records of the fully materialized result.
func applyStreamingLimit(ctx context.Context, it iter.Seq2[chunk.Record, error], ops []querylang.PipeOp, limit int, resolve lookup.Resolver) ([]chunk.Record, error) {
	sf := newStreamFilter(ctx, ops, resolve)
	out := make([]chunk.Record, 0, min(limit, 1024))
	for rec, err := range it {
		if err != nil {
			return nil, err
		}
		rec = rec.Copy()
		materializeRecord(&rec)

		keep, evalErr := sf.apply(&rec)
		if evalErr != nil {
			return nil, evalErr
		}
		if keep {
			out = append(out, rec)
			if len(out) >= limit {
				break
			}
		}
		if sf.exhausted {
			break
		}
	}
	return out, nil
}

// boundedSortPlan describes a sort whose output is capped, allowing a
// bounded top-N working set instead of full materialization.
type boundedSortPlan struct {
	sortIdx int  // index of the SortOp in ops
	keep    int  // working-set bound (head N, tail N, slice End, or implicit limit)
	largest bool // keep the largest records (tail) instead of the smallest (head/slice)
}

// planBoundedSort reports whether ops contain a sort whose output is capped
// closely enough that only `keep` records need to be retained:
//   - all operators before the sort are streamable (per-record), and
//   - the operators between the sort and the first head/tail/slice preserve
//     record count and order (eval/rename/fields/lookup), and
//   - a head/tail/slice follows, or implicitLimit > 0 and every remaining
//     operator preserves count and order (the caller's truncation acts as a
//     trailing head).
func planBoundedSort(ops []querylang.PipeOp, implicitLimit int) (boundedSortPlan, bool) {
	sortIdx := -1
	for i, op := range ops {
		if _, isSort := op.(*querylang.SortOp); isSort {
			sortIdx = i
			break
		}
		switch op.(type) {
		case *querylang.WhereOp, *querylang.DedupOp, *querylang.EvalOp,
			*querylang.RenameOp, *querylang.FieldsOp, *querylang.LookupOp,
			*querylang.HeadOp:
		default:
			return boundedSortPlan{}, false
		}
	}
	if sortIdx < 0 {
		return boundedSortPlan{}, false
	}

	plan := boundedSortPlan{sortIdx: sortIdx}
	for _, op := range ops[sortIdx+1:] {
		switch o := op.(type) {
		case *querylang.EvalOp, *querylang.RenameOp, *querylang.FieldsOp, *querylang.LookupOp:
			// Count- and order-preserving: commutes with the cap.
		case *querylang.HeadOp:
			plan.keep = o.N
			return plan, plan.keep > 0
		case *querylang.TailOp:
			plan.keep = o.N
			plan.largest = true
			return plan, plan.keep > 0
		case *querylang.SliceOp:
			plan.keep = o.End
			return plan, plan.keep > 0
		default:
			// where/dedup/sort after the sort — the cap no longer bounds
			// the sort's required input.
			return boundedSortPlan{}, false
		}
	}
	// No explicit cap after the sort; the caller's implicit limit acts as a
	// trailing head because all remaining operators preserve count and order.
	plan.keep = implicitLimit
	return plan, plan.keep > 0
}

// testBoundedSortObserver, when non-nil, receives the bounded-sort working-set
// high-water mark after collection. Test-only instrumentation.
var testBoundedSortObserver func(maxItems int)

// applyBoundedSort streams pre-sort operators per-record into a bounded top-N
// collector, then applies the post-sort operators batch-wise on the small
// surviving set. Output is identical to full materialization + stable sort.
func applyBoundedSort(ctx context.Context, it iter.Seq2[chunk.Record, error], ops []querylang.PipeOp, plan boundedSortPlan, resolve lookup.Resolver) ([]chunk.Record, error) {
	sortOp := ops[plan.sortIdx].(*querylang.SortOp)
	sf := newStreamFilter(ctx, ops[:plan.sortIdx], resolve)
	col := newTopNCollector(sortOp.Fields, plan.keep, plan.largest)

	for rec, err := range it {
		if err != nil {
			return nil, err
		}
		rec = rec.Copy()
		materializeRecord(&rec)

		keep, evalErr := sf.apply(&rec)
		if evalErr != nil {
			return nil, evalErr
		}
		if keep {
			col.add(rec)
		}
		if sf.exhausted {
			break
		}
	}

	if testBoundedSortObserver != nil {
		testBoundedSortObserver(col.maxItems)
	}

	// The collector's output equals the capped prefix/suffix of the full
	// stable sort; the remaining operators (including the cap itself) run
	// batch-wise on this bounded set with unchanged semantics.
	return applyBatchOps(ctx, col.result(), ops[plan.sortIdx+1:], resolve)
}

// topNCollector retains the keep smallest (or largest) records under a sort
// order, bounding memory at 2×keep items. Ties are broken by arrival order
// (ascending), which reproduces the stable-sort semantics of the batch path.
type topNCollector struct {
	fields   []querylang.SortField
	keep     int
	largest  bool
	items    []sortItem
	seq      int
	maxItems int // high-water mark, for test instrumentation
}

// sortItem carries a record with its precomputed sort keys and arrival order.
type sortItem struct {
	rec  chunk.Record
	keys []string
	seq  int
}

func newTopNCollector(fields []querylang.SortField, keep int, largest bool) *topNCollector {
	return &topNCollector{
		fields:  fields,
		keep:    keep,
		largest: largest,
		items:   make([]sortItem, 0, min(2*keep, 1024)),
	}
}

// add inserts a record, compacting the working set when it reaches 2×keep.
func (c *topNCollector) add(rec chunk.Record) {
	row := RecordToRow(rec)
	keys := make([]string, len(c.fields))
	for i, f := range c.fields {
		keys[i] = row[f.Name]
	}
	c.items = append(c.items, sortItem{rec: rec, keys: keys, seq: c.seq})
	c.seq++
	c.maxItems = max(c.maxItems, len(c.items))
	if len(c.items) >= 2*c.keep {
		c.compact()
	}
}

// cmp is a strict total order: sort fields first, arrival order as tiebreak.
// It matches the batch path's stable sort exactly.
func (c *topNCollector) cmp(a, b sortItem) int {
	for i, f := range c.fields {
		cmp := compareSortValues(a.keys[i], b.keys[i])
		if f.Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return a.seq - b.seq
}

// compact sorts the working set and trims it to the keep smallest (or
// largest) items. Any item outside the current top keep can never appear in
// the final result, so trimming preserves correctness.
func (c *topNCollector) compact() {
	slices.SortFunc(c.items, c.cmp)
	if len(c.items) <= c.keep {
		return
	}
	if c.largest {
		copy(c.items, c.items[len(c.items)-c.keep:])
	}
	clear(c.items[c.keep:]) // release dropped records for GC
	c.items = c.items[:c.keep]
}

// result returns the retained records in sorted order: for the smallest mode
// this is the sorted prefix, for the largest mode the sorted suffix, of the
// full stable sort.
func (c *topNCollector) result() []chunk.Record {
	c.compact()
	out := make([]chunk.Record, len(c.items))
	for i, item := range c.items {
		out[i] = item.rec
	}
	return out
}

// applyStreamingCap processes the iterator with bounded memory by applying
// pre-cap operators inline per-record and using a ring buffer (tail) or
// positional collector (slice) instead of materializing all records.
func applyStreamingCap(ctx context.Context, it iter.Seq2[chunk.Record, error], ops []querylang.PipeOp, capIdx int, resolve lookup.Resolver) ([]chunk.Record, error) {
	preOps := ops[:capIdx]
	capOp := ops[capIdx]
	postOps := ops[capIdx+1:]

	sf := newStreamFilter(ctx, preOps, resolve)

	// Initialize collector based on cap type.
	var ring []chunk.Record
	var ringPos, ringN int
	var sliceStart, sliceEnd int // 1-indexed inclusive
	var sliceCount int           // 0-indexed count of survivors seen so far
	var collected []chunk.Record

	switch op := capOp.(type) {
	case *querylang.TailOp:
		ringN = op.N
		ring = make([]chunk.Record, ringN)
	case *querylang.SliceOp:
		sliceStart = op.Start // 1-indexed
		sliceEnd = op.End     // 1-indexed inclusive
		collected = make([]chunk.Record, 0, sliceEnd-sliceStart+1)
	}

	done := false
	for rec, err := range it {
		if err != nil {
			return nil, err
		}
		rec = rec.Copy()
		materializeRecord(&rec)

		keep, evalErr := sf.apply(&rec)
		if evalErr != nil {
			return nil, evalErr
		}
		if keep {
			// Feed to collector.
			switch capOp.(type) {
			case *querylang.TailOp:
				ring[ringPos%ringN] = rec
				ringPos++
			case *querylang.SliceOp:
				sliceCount++
				if sliceCount >= sliceStart && sliceCount <= sliceEnd {
					collected = append(collected, rec)
				}
				if sliceCount >= sliceEnd {
					done = true
				}
			}
		}
		if done || sf.exhausted {
			break
		}
	}

	records := linearizeCollector(capOp, ring, ringPos, ringN, collected)

	// Apply post-cap ops on the small result set.
	if len(postOps) > 0 {
		return applyBatchOps(ctx, records, postOps, resolve)
	}
	return records, nil
}

// streamFilter applies pre-cap pipeline operators inline per-record.
type streamFilter struct {
	ctx     context.Context
	ops     []querylang.PipeOp
	filters map[int]func(chunk.Record) bool // compiled where filters by op index
	dedups  map[int]*dedupTracker           // dedup state by op index
	heads   map[int]*headState              // head pass counts by op index
	eval    *querylang.Evaluator
	resolve lookup.Resolver
	// exhausted is set once a head operator has passed its N records:
	// no future record can survive the pipeline, so callers may stop the scan.
	exhausted bool
}

type dedupTracker struct {
	seen   map[chunk.EventID]time.Time
	window time.Duration
}

// headState tracks how many records have passed a head operator at its
// position in the pipeline (matching the batch path, where head truncates
// the record set at that point).
type headState struct {
	n      int
	passed int
}

func newStreamFilter(ctx context.Context, ops []querylang.PipeOp, resolve lookup.Resolver) *streamFilter {
	sf := &streamFilter{
		ctx:     ctx,
		ops:     ops,
		filters: make(map[int]func(chunk.Record) bool),
		dedups:  make(map[int]*dedupTracker),
		heads:   make(map[int]*headState),
		eval:    querylang.NewEvaluator(),
		resolve: resolve,
	}
	for i, op := range ops {
		switch o := op.(type) {
		case *querylang.WhereOp:
			sf.filters[i] = CompileFilter(o.Expr)
		case *querylang.DedupOp:
			sf.dedups[i] = &dedupTracker{seen: make(map[chunk.EventID]time.Time), window: parseDedupWindow(o.Window)}
		case *querylang.HeadOp:
			sf.heads[i] = &headState{n: o.N}
		}
	}
	return sf
}

// apply runs all pre-cap operators on a single record. Returns (keep, error).
// When keep is false, the record should be skipped.
func (sf *streamFilter) apply(rec *chunk.Record) (bool, error) {
	for i, op := range sf.ops {
		switch o := op.(type) {
		case *querylang.WhereOp:
			if !sf.filters[i](*rec) {
				return false, nil
			}
		case *querylang.DedupOp:
			dt := sf.dedups[i]
			if firstTS, exists := dt.seen[rec.EventID]; exists && rec.WriteTS.Sub(firstTS) <= dt.window {
				return false, nil
			}
			dt.seen[rec.EventID] = rec.WriteTS
		case *querylang.EvalOp:
			if err := applyInlineEval(rec, o, sf.eval); err != nil {
				return false, err
			}
		case *querylang.RenameOp:
			applyInlineRename(rec, o)
		case *querylang.FieldsOp:
			applyInlineFields(rec, o)
		case *querylang.LookupOp:
			applyRecordLookup(sf.ctx, []chunk.Record{*rec}, o, sf.resolve)
		case *querylang.HeadOp:
			hs := sf.heads[i]
			if hs.passed >= hs.n {
				sf.exhausted = true
				return false, nil
			}
			hs.passed++
			if hs.passed >= hs.n {
				// The current record passes; later records cannot.
				sf.exhausted = true
			}
		}
	}
	return true, nil
}

func applyInlineEval(rec *chunk.Record, op *querylang.EvalOp, eval *querylang.Evaluator) error {
	row := RecordToRow(*rec)
	for _, a := range op.Assignments {
		val, err := eval.Eval(a.Expr, row)
		if err != nil {
			return fmt.Errorf("eval %s: %w", a.Field, err)
		}
		if !val.Missing {
			if rec.Attrs == nil {
				rec.Attrs = make(chunk.Attributes)
			}
			rec.Attrs[a.Field] = val.Str
			row[a.Field] = val.Str
		}
	}
	return nil
}

func applyInlineRename(rec *chunk.Record, op *querylang.RenameOp) {
	for _, m := range op.Renames {
		if v, ok := rec.Attrs[m.Old]; ok {
			if rec.Attrs == nil {
				rec.Attrs = make(chunk.Attributes)
			}
			rec.Attrs[m.New] = v
			delete(rec.Attrs, m.Old)
		}
	}
}

func applyInlineFields(rec *chunk.Record, op *querylang.FieldsOp) {
	if rec.Attrs == nil {
		return
	}
	nameSet := make(map[string]bool, len(op.Names))
	for _, n := range op.Names {
		nameSet[n] = true
	}
	for k := range rec.Attrs {
		drop := op.Drop && nameSet[k] || !op.Drop && !nameSet[k]
		if drop {
			delete(rec.Attrs, k)
		}
	}
}

// linearizeCollector extracts the final record slice from the ring buffer or slice collector.
func linearizeCollector(capOp querylang.PipeOp, ring []chunk.Record, ringPos, ringN int, collected []chunk.Record) []chunk.Record {
	switch capOp.(type) {
	case *querylang.TailOp:
		total := ringPos
		if total < ringN {
			out := make([]chunk.Record, total)
			copy(out, ring[:total])
			return out
		}
		out := make([]chunk.Record, ringN)
		start := ringPos % ringN
		copy(out, ring[start:])
		copy(out[ringN-start:], ring[:start])
		return out
	case *querylang.SliceOp:
		return collected
	}
	return nil
}

// applyBatchOps applies operators to an already-materialized record slice.
// It is both the full-materialization fallback for applyRecordOps and the
// post-cap/post-sort finisher for the bounded paths.
func applyBatchOps(ctx context.Context, records []chunk.Record, ops []querylang.PipeOp, resolve lookup.Resolver) ([]chunk.Record, error) {
	materializeFields(records)
	eval := querylang.NewEvaluator()
	for _, op := range ops {
		var err error
		switch o := op.(type) {
		case *querylang.WhereOp:
			records = applyRecordWhere(records, o)
		case *querylang.DedupOp:
			records = applyRecordDedup(records, parseDedupWindow(o.Window))
		case *querylang.EvalOp:
			records, err = applyRecordEval(records, o, eval)
		case *querylang.SortOp:
			applyRecordSort(records, o)
		case *querylang.HeadOp:
			records = applyRecordHead(records, o)
		case *querylang.TailOp:
			records = applyRecordTail(records, o)
		case *querylang.SliceOp:
			records = applyRecordSlice(records, o)
		case *querylang.RenameOp:
			applyRecordRename(records, o)
		case *querylang.FieldsOp:
			applyRecordFields(records, o)
		case *querylang.LookupOp:
			applyRecordLookup(ctx, records, o, resolve)
		default:
			return nil, fmt.Errorf("unsupported record operator: %T", op)
		}
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

// materializeRecord extracts KV/JSON/logfmt fields from a single record into Attrs.
func materializeRecord(rec *chunk.Record) {
	row := RecordToRow(*rec)
	if rec.Attrs == nil {
		rec.Attrs = make(chunk.Attributes, len(row))
	}
	for k, v := range row {
		if k == "raw" {
			continue
		}
		if _, exists := rec.Attrs[k]; !exists {
			rec.Attrs[k] = v
		}
	}
}

const defaultDedupWindow = time.Second

// parseDedupWindow parses the window string from DedupOp, falling back to the default.
func parseDedupWindow(raw string) time.Duration {
	if raw == "" {
		return defaultDedupWindow
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return defaultDedupWindow
	}
	return d
}

// applyRecordDedup removes duplicate records keyed on EventID within a time window.
// Records routed to multiple vaults share the same EventID but have different
// WriteTS values. The window parameter controls how far apart WriteTSes can be
// and still be considered duplicates.
func applyRecordDedup(records []chunk.Record, window time.Duration) []chunk.Record {
	if len(records) == 0 {
		return records
	}
	seen := make(map[chunk.EventID]time.Time, len(records))
	return slices.DeleteFunc(records, func(r chunk.Record) bool {
		eid := r.EventID
		if firstWriteTS, exists := seen[eid]; exists {
			if r.WriteTS.Sub(firstWriteTS) <= window {
				return true // duplicate within window
			}
		}
		seen[eid] = r.WriteTS
		return false
	})
}

// applyRecordWhere filters records using a compiled boolean expression.
func applyRecordWhere(records []chunk.Record, op *querylang.WhereOp) []chunk.Record {
	filter := CompileFilter(op.Expr)
	return slices.DeleteFunc(records, func(r chunk.Record) bool {
		return !filter(r)
	})
}

// applyRecordEval evaluates expressions and writes results to record Attrs.
func applyRecordEval(records []chunk.Record, op *querylang.EvalOp, eval *querylang.Evaluator) ([]chunk.Record, error) {
	for i := range records {
		row := RecordToRow(records[i])
		for _, a := range op.Assignments {
			val, err := eval.Eval(a.Expr, row)
			if err != nil {
				return nil, fmt.Errorf("eval %s: %w", a.Field, err)
			}
			if !val.Missing {
				if records[i].Attrs == nil {
					records[i].Attrs = make(chunk.Attributes)
				}
				records[i].Attrs[a.Field] = val.Str
				row[a.Field] = val.Str // visible to subsequent assignments
			}
		}
	}
	return records, nil
}

// applyRecordSort sorts records by the given fields using stable sort.
func applyRecordSort(records []chunk.Record, op *querylang.SortOp) {
	slices.SortStableFunc(records, func(a, b chunk.Record) int {
		rowA := RecordToRow(a)
		rowB := RecordToRow(b)
		for _, f := range op.Fields {
			va := rowA[f.Name]
			vb := rowB[f.Name]
			cmp := compareSortValues(va, vb)
			if f.Desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	})
}

// applyRecordHead truncates the record slice to at most N records.
func applyRecordHead(records []chunk.Record, op *querylang.HeadOp) []chunk.Record {
	if len(records) > op.N {
		return records[:op.N]
	}
	return records
}

// applyRecordTail keeps only the last N records.
func applyRecordTail(records []chunk.Record, op *querylang.TailOp) []chunk.Record {
	if len(records) > op.N {
		return records[len(records)-op.N:]
	}
	return records
}

// applyRecordSlice keeps records from Start to End (1-indexed, inclusive).
func applyRecordSlice(records []chunk.Record, op *querylang.SliceOp) []chunk.Record {
	start := op.Start - 1 // convert to 0-indexed
	end := op.End         // exclusive upper bound (End is inclusive, so +1-1=0)
	if start >= len(records) {
		return nil
	}
	if end > len(records) {
		end = len(records)
	}
	return records[start:end]
}

// applyRecordRename renames keys in record Attrs.
func applyRecordRename(records []chunk.Record, op *querylang.RenameOp) {
	for i := range records {
		for _, m := range op.Renames {
			if v, ok := records[i].Attrs[m.Old]; ok { //nolint:gosec // G602: i is a range index over records, always in bounds
				if records[i].Attrs == nil { //nolint:gosec // G602: i is a range index over records, always in bounds
					records[i].Attrs = make(chunk.Attributes) //nolint:gosec // G602: i is a range index over records, always in bounds
				}
				records[i].Attrs[m.New] = v     //nolint:gosec // G602: i is a range index over records, always in bounds
				delete(records[i].Attrs, m.Old) //nolint:gosec // G602: i is a range index over records, always in bounds
			}
		}
	}
}

// applyRecordFields filters record Attrs to keep or drop the given fields.
func applyRecordFields(records []chunk.Record, op *querylang.FieldsOp) {
	nameSet := make(map[string]bool, len(op.Names))
	for _, n := range op.Names {
		nameSet[n] = true
	}
	for i := range records {
		if records[i].Attrs == nil {
			continue
		}
		if op.Drop {
			for k := range records[i].Attrs {
				if nameSet[k] {
					delete(records[i].Attrs, k) //nolint:gosec // G602: i is a range index over records, always in bounds
				}
			}
		} else {
			for k := range records[i].Attrs {
				if !nameSet[k] {
					delete(records[i].Attrs, k) //nolint:gosec // G602: i is a range index over records, always in bounds
				}
			}
		}
	}
}

// --- Table operators (post-stats) ---

// applyTableOps applies pipeline operators to a table result (post-stats).
func applyTableOps(ctx context.Context, table *TableResult, ops []querylang.PipeOp, resolve lookup.Resolver) (*TableResult, error) {
	eval := querylang.NewEvaluator()

	for _, op := range ops {
		var err error
		switch o := op.(type) {
		case *querylang.WhereOp:
			table, err = applyTableWhere(table, o)
		case *querylang.EvalOp:
			table, err = applyTableEval(table, o, eval)
		case *querylang.SortOp:
			applyTableSort(table, o)
		case *querylang.HeadOp:
			applyTableHead(table, o)
		case *querylang.TailOp:
			applyTableTail(table, o)
		case *querylang.SliceOp:
			applyTableSlice(table, o)
		case *querylang.RenameOp:
			applyTableRename(table, o)
		case *querylang.FieldsOp:
			applyTableFields(table, o)
		case *querylang.LookupOp:
			table = applyTableLookup(ctx, table, o, resolve)
		default:
			return nil, fmt.Errorf("unsupported post-stats operator: %T", op)
		}
		if err != nil {
			return nil, err
		}
	}
	return table, nil
}

// applyTableWhere filters table rows using a compiled boolean expression.
func applyTableWhere(table *TableResult, op *querylang.WhereOp) (*TableResult, error) {
	filter := CompileFilter(op.Expr)
	var kept [][]string
	for _, row := range table.Rows {
		// Convert table row to a pseudo-record for the filter.
		rec := tableRowToRecord(table.Columns, row)
		if filter(rec) {
			kept = append(kept, row)
		}
	}
	table.Rows = kept
	return table, nil
}

// applyTableEval evaluates expressions and adds/replaces columns.
func applyTableEval(table *TableResult, op *querylang.EvalOp, eval *querylang.Evaluator) (*TableResult, error) {
	for _, a := range op.Assignments {
		colIdx := -1
		for i, c := range table.Columns {
			if c == a.Field {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			// Add new column.
			table.Columns = append(table.Columns, a.Field)
			colIdx = len(table.Columns) - 1
			for i := range table.Rows {
				table.Rows[i] = append(table.Rows[i], "")
			}
		}

		for i, row := range table.Rows {
			qrow := tableRowToQueryRow(table.Columns, row)
			val, err := eval.Eval(a.Expr, qrow)
			if err != nil {
				return nil, fmt.Errorf("eval %s: %w", a.Field, err)
			}
			if !val.Missing {
				table.Rows[i][colIdx] = val.Str
			}
		}
	}
	return table, nil
}

// applyTableSort sorts table rows by the given fields.
func applyTableSort(table *TableResult, op *querylang.SortOp) {
	colMap := make(map[string]int, len(table.Columns))
	for i, c := range table.Columns {
		colMap[c] = i
	}
	slices.SortStableFunc(table.Rows, func(a, b []string) int {
		for _, f := range op.Fields {
			idx, ok := colMap[f.Name]
			if !ok {
				continue
			}
			cmp := compareSortValues(a[idx], b[idx])
			if f.Desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	})
}

// applyTableHead truncates the table to at most N rows.
func applyTableHead(table *TableResult, op *querylang.HeadOp) {
	if len(table.Rows) > op.N {
		table.Rows = table.Rows[:op.N]
	}
}

// applyTableTail keeps only the last N rows.
func applyTableTail(table *TableResult, op *querylang.TailOp) {
	if len(table.Rows) > op.N {
		table.Rows = table.Rows[len(table.Rows)-op.N:]
	}
}

// applyTableSlice keeps rows from Start to End (1-indexed, inclusive).
func applyTableSlice(table *TableResult, op *querylang.SliceOp) {
	start := op.Start - 1 // convert to 0-indexed
	end := op.End         // exclusive upper bound
	if start >= len(table.Rows) {
		table.Rows = nil
		return
	}
	if end > len(table.Rows) {
		end = len(table.Rows)
	}
	table.Rows = table.Rows[start:end]
}

// applyTableRename renames columns.
func applyTableRename(table *TableResult, op *querylang.RenameOp) {
	for _, m := range op.Renames {
		for i, c := range table.Columns {
			if c == m.Old {
				table.Columns[i] = m.New
				break
			}
		}
	}
}

// applyTableFields keeps or drops columns.
func applyTableFields(table *TableResult, op *querylang.FieldsOp) {
	nameSet := make(map[string]bool, len(op.Names))
	for _, n := range op.Names {
		nameSet[n] = true
	}

	// Determine which column indices to keep.
	var keepIdx []int
	var keepCols []string
	for i, c := range table.Columns {
		keep := nameSet[c]
		if op.Drop {
			keep = !keep
		}
		if keep {
			keepIdx = append(keepIdx, i)
			keepCols = append(keepCols, c)
		}
	}
	table.Columns = keepCols
	for i, row := range table.Rows {
		newRow := make([]string, len(keepIdx))
		for j, idx := range keepIdx {
			newRow[j] = row[idx]
		}
		table.Rows[i] = newRow
	}
}

// --- lookup operators ---

// applyRecordLookup enriches records by looking up field values in a table.
func applyRecordLookup(ctx context.Context, records []chunk.Record, op *querylang.LookupOp, resolve lookup.Resolver) {
	if resolve == nil {
		return
	}
	table := resolve(op.Table)
	if table == nil {
		return
	}

	if len(table.Parameters()) > 1 {
		applyRecordLookupParameterized(ctx, records, op, table)
		return
	}
	applyRecordLookupSingle(ctx, records, op, table)
}

// applyRecordLookupParameterized collects all field values per record, makes one call, prefixes with table name.
func applyRecordLookupParameterized(ctx context.Context, records []chunk.Record, op *querylang.LookupOp, table lookup.LookupTable) {
	params := table.Parameters()
	for i := range records {
		values := collectParamValues(params, op.Fields, records[i].Attrs)
		result := table.LookupValues(ctx, values)
		if result == nil {
			continue
		}
		if records[i].Attrs == nil {
			records[i].Attrs = make(chunk.Attributes)
		}
		for suffix, v := range result {
			records[i].Attrs[op.Table+"_"+suffix] = v
		}
	}
}

// applyRecordLookupSingle performs per-field independent lookups.
func applyRecordLookupSingle(ctx context.Context, records []chunk.Record, op *querylang.LookupOp, table lookup.LookupTable) {
	param := table.Parameters()[0]
	for i := range records {
		for _, field := range op.Fields {
			val, ok := records[i].Attrs[field]
			if !ok || val == "" {
				continue
			}
			result := table.LookupValues(ctx, map[string]string{param: val})
			if result == nil {
				continue
			}
			if records[i].Attrs == nil {
				records[i].Attrs = make(chunk.Attributes)
			}
			for suffix, v := range result {
				records[i].Attrs[field+"_"+suffix] = v
			}
		}
	}
}

// collectParamValues maps query fields positionally to parameter names and collects values from attrs.
func collectParamValues(params, fields []string, attrs chunk.Attributes) map[string]string {
	values := make(map[string]string, len(params))
	for j, param := range params {
		if j < len(fields) {
			if v, ok := attrs[fields[j]]; ok {
				values[param] = v
			}
		}
	}
	return values
}

// applyTableLookup enriches table rows by looking up column values in a table.
func applyTableLookup(ctx context.Context, table *TableResult, op *querylang.LookupOp, resolve lookup.Resolver) *TableResult {
	if resolve == nil {
		return table
	}
	lt := resolve(op.Table)
	if lt == nil {
		return table
	}

	if len(lt.Parameters()) > 1 {
		return applyTableLookupParameterized(ctx, table, op, lt)
	}
	return applyTableLookupSingle(ctx, table, op, lt)
}

// applyTableLookupParameterized collects all field values per row, makes one call, prefixes with table name.
func applyTableLookupParameterized(ctx context.Context, table *TableResult, op *querylang.LookupOp, lt lookup.LookupTable) *TableResult {
	params := lt.Parameters()
	srcIdxs := findColumnIndices(table.Columns, op.Fields)

	suffixes := lt.Suffixes()
	baseCol := len(table.Columns)
	for _, suffix := range suffixes {
		table.Columns = append(table.Columns, op.Table+"_"+suffix)
	}

	for i, row := range table.Rows {
		for range suffixes {
			table.Rows[i] = append(table.Rows[i], "")
		}
		values := make(map[string]string, len(params))
		for j, param := range params {
			if j < len(srcIdxs) && srcIdxs[j] >= 0 && srcIdxs[j] < len(row) {
				values[param] = row[srcIdxs[j]]
			}
		}
		result := lt.LookupValues(ctx, values)
		if result == nil {
			continue
		}
		for j, suffix := range suffixes {
			if v, ok := result[suffix]; ok {
				table.Rows[i][baseCol+j] = v
			}
		}
	}
	return table
}

// applyTableLookupSingle performs per-field independent lookups.
func applyTableLookupSingle(ctx context.Context, table *TableResult, op *querylang.LookupOp, lt lookup.LookupTable) *TableResult {
	suffixes := lt.Suffixes()
	for _, field := range op.Fields {
		srcIdx := -1
		for i, c := range table.Columns {
			if c == field {
				srcIdx = i
				break
			}
		}
		if srcIdx < 0 {
			continue
		}
		table = applyTableLookupField(ctx, table, lt, field, srcIdx, suffixes)
	}
	return table
}

// applyTableLookupField enriches table rows for a single source field.
func applyTableLookupField(ctx context.Context, table *TableResult, lt lookup.LookupTable, field string, srcIdx int, suffixes []string) *TableResult {
	baseCol := len(table.Columns)
	for _, suffix := range suffixes {
		table.Columns = append(table.Columns, field+"_"+suffix)
	}
	for i, row := range table.Rows {
		for range suffixes {
			table.Rows[i] = append(table.Rows[i], "")
		}
		if srcIdx >= len(row) || row[srcIdx] == "" {
			continue
		}
		result := lt.LookupValues(ctx, map[string]string{lt.Parameters()[0]: row[srcIdx]})
		if result == nil {
			continue
		}
		for j, suffix := range suffixes {
			if v, ok := result[suffix]; ok {
				table.Rows[i][baseCol+j] = v
			}
		}
	}
	return table
}

// findColumnIndices returns the column index for each field name, or -1 if not found.
func findColumnIndices(columns []string, fields []string) []int {
	idxs := make([]int, len(fields))
	for j, field := range fields {
		idxs[j] = -1
		for i, c := range columns {
			if c == field {
				idxs[j] = i
				break
			}
		}
	}
	return idxs
}

// --- helpers ---

// materializeFields extracts KV, JSON, and logfmt fields from each record's
// Raw message and merges them into Attrs. Existing Attrs take precedence.
// This ensures all pipeline operators see extracted fields, not just explicit attributes.
func materializeFields(records []chunk.Record) {
	for i := range records {
		row := RecordToRow(records[i])
		if records[i].Attrs == nil {
			records[i].Attrs = make(chunk.Attributes, len(row))
		}
		for k, v := range row {
			if k == "raw" {
				continue
			}
			if _, exists := records[i].Attrs[k]; !exists { //nolint:gosec // G602: i is a range index over records, always in bounds
				records[i].Attrs[k] = v //nolint:gosec // G602: i is a range index over records, always in bounds
			}
		}
	}
}

// compareSortValues compares two string values, trying numeric comparison first.
func compareSortValues(a, b string) int {
	na, errA := strconv.ParseFloat(a, 64)
	nb, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		switch {
		case na < nb:
			return -1
		case na > nb:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(a, b)
}

// tableRowToRecord creates a pseudo-record from table column/row data
// so that the existing filter infrastructure (which works on Records) can
// be reused for post-stats where clauses.
func tableRowToRecord(columns []string, row []string) chunk.Record {
	attrs := make(chunk.Attributes, len(columns))
	for i, c := range columns {
		if i < len(row) {
			attrs[c] = row[i]
		}
	}
	return chunk.Record{Attrs: attrs}
}

// tableRowToQueryRow creates a querylang.Row from table column/row data
// for expression evaluation.
func tableRowToQueryRow(columns []string, row []string) querylang.Row {
	qrow := make(querylang.Row, len(columns))
	for i, c := range columns {
		if i < len(row) {
			qrow[c] = row[i]
		}
	}
	return qrow
}

// --- Streaming pipeline support ---

// CanStreamPipeline reports whether a pipeline's operators can be applied
// per-record in a streaming fashion (with resume-token pagination).
// Pipelines that require full materialization (sort, tail, slice, stats,
// timechart, raw) return false.
func CanStreamPipeline(pipeline *querylang.Pipeline) bool {
	for _, op := range pipeline.Pipes {
		switch op.(type) {
		case *querylang.WhereOp, *querylang.EvalOp, *querylang.RenameOp,
			*querylang.FieldsOp, *querylang.HeadOp, *querylang.LookupOp,
			*querylang.RawOp:
			// streamable
		default:
			return false
		}
	}
	return true
}

// RecordTransform applies a sequence of streamable pipeline operators to
// individual records. It pre-compiles where filters and caches lookup
// tables so the per-record cost is minimal.
type RecordTransform struct {
	steps   []transformStep
	eval    *querylang.Evaluator
	headN   int // 0 = no head limit
	emitted int
}

// transformStep is a single pipeline operator compiled for per-record use.
type transformStep struct {
	kind    stepKind
	filter  func(chunk.Record) bool
	evalOp  *querylang.EvalOp
	rename  *querylang.RenameOp
	fields  *querylang.FieldsOp
	lookupT lookup.LookupTable
	lookupF []string // lookup field names
	lookupN string   // lookup table name (for parameterized prefix)
}

type stepKind int

const (
	stepWhere stepKind = iota
	stepEval
	stepRename
	stepFields
	stepLookup
)

// NewRecordTransform compiles a sequence of pipeline operators into a
// per-record transform. All ops must be streamable (see CanStreamPipeline).
func NewRecordTransform(ops []querylang.PipeOp, resolve lookup.Resolver) *RecordTransform {
	rt := &RecordTransform{eval: querylang.NewEvaluator()}
	for _, op := range ops {
		switch o := op.(type) {
		case *querylang.WhereOp:
			rt.steps = append(rt.steps, transformStep{
				kind:   stepWhere,
				filter: CompileFilter(o.Expr),
			})
		case *querylang.EvalOp:
			rt.steps = append(rt.steps, transformStep{kind: stepEval, evalOp: o})
		case *querylang.RenameOp:
			rt.steps = append(rt.steps, transformStep{kind: stepRename, rename: o})
		case *querylang.FieldsOp:
			rt.steps = append(rt.steps, transformStep{kind: stepFields, fields: o})
		case *querylang.LookupOp:
			var lt lookup.LookupTable
			if resolve != nil {
				lt = resolve(o.Table)
			}
			rt.steps = append(rt.steps, transformStep{
				kind:    stepLookup,
				lookupT: lt,
				lookupF: o.Fields,
				lookupN: o.Table,
			})
		case *querylang.DedupOp:
			// Dedup is not streamable — handled by batch path only.
		case *querylang.HeadOp:
			rt.headN = o.N
		}
	}
	return rt
}

// Apply transforms a single record through the pipeline operators.
// Returns (record, keep). When keep is false the record was filtered out
// or the head limit was reached; the caller should skip it.
// The record is copied and fields are materialized before transforms run.
func (rt *RecordTransform) Apply(ctx context.Context, rec chunk.Record) (chunk.Record, bool) {
	if rt.headN > 0 && rt.emitted >= rt.headN {
		return rec, false
	}

	rec = rec.Copy()
	materializeFields([]chunk.Record{rec})

	for i := range rt.steps {
		s := &rt.steps[i]
		switch s.kind {
		case stepWhere:
			if !s.filter(rec) {
				return rec, false
			}
		case stepEval:
			rec = s.applyEval(rec, rt.eval)
		case stepRename:
			s.applyRename(&rec)
		case stepFields:
			s.applyFields(&rec)
		case stepLookup:
			s.applyLookup(ctx, &rec)
		}
	}

	rt.emitted++
	return rec, true
}

func (s *transformStep) applyEval(rec chunk.Record, eval *querylang.Evaluator) chunk.Record {
	row := RecordToRow(rec)
	for _, a := range s.evalOp.Assignments {
		val, err := eval.Eval(a.Expr, row)
		if err == nil && !val.Missing {
			if rec.Attrs == nil {
				rec.Attrs = make(chunk.Attributes)
			}
			rec.Attrs[a.Field] = val.Str
			row[a.Field] = val.Str
		}
	}
	return rec
}

func (s *transformStep) applyRename(rec *chunk.Record) {
	for _, m := range s.rename.Renames {
		if v, ok := rec.Attrs[m.Old]; ok {
			if rec.Attrs == nil {
				rec.Attrs = make(chunk.Attributes)
			}
			rec.Attrs[m.New] = v
			delete(rec.Attrs, m.Old)
		}
	}
}

func (s *transformStep) applyFields(rec *chunk.Record) {
	if rec.Attrs == nil {
		return
	}
	nameSet := make(map[string]bool, len(s.fields.Names))
	for _, n := range s.fields.Names {
		nameSet[n] = true
	}
	for k := range rec.Attrs {
		if s.fields.Drop == nameSet[k] {
			delete(rec.Attrs, k)
		}
	}
}

func (s *transformStep) applyLookup(ctx context.Context, rec *chunk.Record) {
	if s.lookupT == nil {
		return
	}

	if len(s.lookupT.Parameters()) > 1 {
		s.applyLookupParameterized(ctx, rec, s.lookupT)
		return
	}

	param := s.lookupT.Parameters()[0]
	for _, field := range s.lookupF {
		val, ok := rec.Attrs[field]
		if !ok || val == "" {
			continue
		}
		result := s.lookupT.LookupValues(ctx, map[string]string{param: val})
		if result == nil {
			continue
		}
		if rec.Attrs == nil {
			rec.Attrs = make(chunk.Attributes)
		}
		for suffix, v := range result {
			rec.Attrs[field+"_"+suffix] = v
		}
	}
}

func (s *transformStep) applyLookupParameterized(ctx context.Context, rec *chunk.Record, table lookup.LookupTable) {
	values := collectParamValues(table.Parameters(), s.lookupF, rec.Attrs)
	result := table.LookupValues(ctx, values)
	if result == nil {
		return
	}
	if rec.Attrs == nil {
		rec.Attrs = make(chunk.Attributes)
	}
	for suffix, v := range result {
		rec.Attrs[s.lookupN+"_"+suffix] = v
	}
}

// Done reports whether the head limit has been reached.
func (rt *RecordTransform) Done() bool { return rt.headN > 0 && rt.emitted >= rt.headN }
