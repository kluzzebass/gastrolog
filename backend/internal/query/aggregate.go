package query

import (
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"


	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
	"gastrolog/internal/tokenizer"
)

// MaxGroupCardinality limits the number of distinct groups to prevent memory exhaustion.
const MaxGroupCardinality = 10_000

// TableResult holds the output of a stats aggregation.
type TableResult struct {
	Columns   []string   // column names in order (groups first, then aggregates)
	Rows      [][]string // row values (same order as Columns)
	Truncated bool       // true if cardinality cap was hit
}

// extractors is the default set of KV extractors used by RecordToRow.
var extractors = tokenizer.DefaultExtractors()

// RecordToRow converts a chunk.Record to a querylang.Row for expression evaluation.
// It extracts fields from the raw message using both KV and JSON extractors,
// then overlays record attributes (which take precedence), and adds _raw.
func RecordToRow(rec chunk.Record) querylang.Row {
	// Extract fields from raw text (KV, logfmt, access log + JSON).
	kvs := tokenizer.CombinedExtract(rec.Raw, extractors)
	jsonKVs := tokenizer.ExtractJSON(rec.Raw)

	size := 1 + len(kvs) + len(jsonKVs)
	if rec.Attrs != nil {
		size += len(rec.Attrs)
	}
	row := make(querylang.Row, size)
	for _, kv := range kvs {
		row[kv.Key] = kv.Value
	}
	for _, kv := range jsonKVs {
		row[kv.Key] = kv.Value
	}
	maps.Copy(row, rec.Attrs) // attrs take precedence over extracted fields
	row["_raw"] = string(rec.Raw)
	return row
}

// ParseBinDuration parses a duration string like "5m", "1h", "30s", "1d".
func ParseBinDuration(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid duration: %q", s)
	}
	numStr := s[:len(s)-1]
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration number in %q: %w", s, err)
	}
	if num <= 0 {
		return 0, fmt.Errorf("duration must be positive: %q", s)
	}
	unit := s[len(s)-1]
	switch unit {
	case 's':
		return time.Duration(num * float64(time.Second)), nil
	case 'm':
		return time.Duration(num * float64(time.Minute)), nil
	case 'h':
		return time.Duration(num * float64(time.Hour)), nil
	case 'd':
		return time.Duration(num * 24 * float64(time.Hour)), nil
	default:
		return 0, fmt.Errorf("unknown duration unit %q in %q", string(unit), s)
	}
}

// Aggregator accumulates records into aggregate groups for a stats operation.
type Aggregator struct {
	aggs   []querylang.AggExpr
	groups []querylang.GroupExpr
	eval   *querylang.Evaluator

	binWidth time.Duration // 0 if no bin() in groups
	binField string        // timestamp field for bin(); "" means WriteTS
	binIdx   int           // index within groups where bin() appears (-1 if none)

	state    map[string]*groupState
	keyOrder []string // insertion order for deterministic output
	truncated bool
}

type groupState struct {
	groupValues []string      // display value for each group column
	accs        []accumulator // one per aggregate expression
}

// NewAggregator creates an Aggregator from a parsed StatsOp.
func NewAggregator(stats *querylang.StatsOp) (*Aggregator, error) {
	a := &Aggregator{
		aggs:   stats.Aggs,
		groups: stats.Groups,
		eval:   querylang.NewEvaluator(),
		binIdx: -1,
		state:  make(map[string]*groupState),
	}

	// Find and parse bin() group if present.
	for i, g := range stats.Groups {
		if g.Bin != nil {
			if a.binIdx >= 0 {
				return nil, fmt.Errorf("only one bin() group is allowed")
			}
			dur, err := ParseBinDuration(g.Bin.Duration)
			if err != nil {
				return nil, fmt.Errorf("invalid bin duration: %w", err)
			}
			a.binWidth = dur
			a.binIdx = i
			if g.Bin.Field != nil {
				a.binField = g.Bin.Field.Name
			}
		}
	}

	// Validate aggregate functions.
	for _, agg := range stats.Aggs {
		switch strings.ToLower(agg.Func) {
		case "count", "sum", "avg", "min", "max",
			"dcount", "median", "first", "last", "values":
			// OK
		default:
			return nil, fmt.Errorf("unknown aggregate function: %s", agg.Func)
		}
	}

	return a, nil
}

// Add processes a record, updating aggregate state.
func (a *Aggregator) Add(rec chunk.Record) error {
	if a.truncated {
		return nil // already hit cardinality cap, skip silently
	}

	row := RecordToRow(rec)

	// Compute group values.
	groupValues := make([]string, len(a.groups))
	for i, g := range a.groups {
		if g.Bin != nil {
			ts, ok := a.getTimestamp(rec)
			if !ok {
				return nil // skip records with missing timestamp
			}
			binTS := ts.Truncate(a.binWidth)
			groupValues[i] = binTS.UTC().Format(time.RFC3339)
		} else {
			v, ok := row[g.Field.Name]
			if !ok {
				groupValues[i] = "" // missing field → empty group value
			} else {
				groupValues[i] = v
			}
		}
	}

	key := makeGroupKey(groupValues)
	gs, exists := a.state[key]
	if !exists {
		if len(a.state) >= MaxGroupCardinality {
			a.truncated = true
			return nil
		}
		accs, err := a.makeAccumulators()
		if err != nil {
			return err
		}
		gs = &groupState{
			groupValues: groupValues,
			accs:        accs,
		}
		a.state[key] = gs
		a.keyOrder = append(a.keyOrder, key)
	}

	// Evaluate and accumulate each aggregate.
	for i, agg := range a.aggs {
		var val querylang.Value
		if agg.Arg == nil {
			// Bare count — always non-missing.
			val = querylang.NumValue(1)
		} else {
			v, err := a.eval.Eval(agg.Arg, row)
			if err != nil {
				// Expression evaluation errors → missing (skip silently).
				val = querylang.MissingValue()
			} else {
				val = v
			}
		}
		gs.accs[i].Add(val)
	}

	return nil
}

// Result produces the final TableResult.
// start and end are used for gap-filling when bin() is present.
// Pass zero times to skip gap-filling or to gap-fill using the data range.
func (a *Aggregator) Result(start, end time.Time) *TableResult {
	// Gap-fill if bin() is present.
	if a.binIdx >= 0 && a.binWidth > 0 {
		a.gapFill(start, end)
	}

	// Build column names.
	columns := make([]string, 0, len(a.groups)+len(a.aggs))
	for _, g := range a.groups {
		if g.Bin != nil {
			columns = append(columns, "_time")
		} else {
			columns = append(columns, g.Field.Name)
		}
	}
	for _, agg := range a.aggs {
		columns = append(columns, agg.DefaultAlias())
	}

	// Build rows.
	var rows [][]string

	if len(a.groups) == 0 && len(a.state) == 0 {
		// No group-by, no records → single row with default values (like SQL).
		accs, _ := a.makeAccumulators()
		row := make([]string, len(a.aggs))
		for i, acc := range accs {
			v := acc.Result()
			if v.Missing {
				row[i] = ""
			} else {
				row[i] = v.Str
			}
		}
		rows = append(rows, row)
	} else {
		for _, key := range a.keyOrder {
			gs := a.state[key]
			row := make([]string, 0, len(columns))
			row = append(row, gs.groupValues...)
			for _, acc := range gs.accs {
				v := acc.Result()
				if v.Missing {
					row = append(row, "")
				} else {
					row = append(row, v.Str)
				}
			}
			rows = append(rows, row)
		}
	}

	// Sort rows by group values.
	a.sortRows(rows)

	return &TableResult{
		Columns:   columns,
		Rows:      rows,
		Truncated: a.truncated,
	}
}

// RunStats is a convenience function that runs a stats aggregation over a record stream.
func RunStats(records iter.Seq2[chunk.Record, error], stats *querylang.StatsOp, start, end time.Time) (*TableResult, error) {
	agg, err := NewAggregator(stats)
	if err != nil {
		return nil, err
	}

	for rec, err := range records {
		if err != nil {
			return nil, err
		}
		if err := agg.Add(rec); err != nil {
			return nil, err
		}
	}

	return agg.Result(start, end), nil
}

// makeGroupKey creates a hashable string from group values using null byte separator.
func makeGroupKey(values []string) string {
	return strings.Join(values, "\x00")
}

func (a *Aggregator) makeAccumulators() ([]accumulator, error) {
	accs := make([]accumulator, len(a.aggs))
	for i, agg := range a.aggs {
		acc, err := newAccumulator(agg.Func)
		if err != nil {
			return nil, err
		}
		accs[i] = acc
	}
	return accs, nil
}

func (a *Aggregator) getTimestamp(rec chunk.Record) (time.Time, bool) {
	switch a.binField {
	case "", "_write_ts":
		return rec.WriteTS, true
	case "_ingest_ts":
		return rec.IngestTS, true
	case "_source_ts":
		if rec.SourceTS.IsZero() {
			return time.Time{}, false
		}
		return rec.SourceTS, true
	default:
		// Try parsing from record attributes.
		v, ok := rec.Attrs[a.binField]
		if !ok {
			return time.Time{}, false
		}
		// Try Unix epoch seconds.
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			return time.Unix(sec, nsec), true
		}
		// Try RFC3339.
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, true
		}
		return time.Time{}, false
	}
}

// gapFill inserts zero-valued entries for time bins that have no records.
// For multi-dimensional groups (field + bin), it fills all bins for each
// unique combination of non-bin group values.
func (a *Aggregator) gapFill(start, end time.Time) {
	if len(a.state) == 0 {
		return
	}

	// Determine time range.
	minTS, maxTS := a.dataTimeRange()
	if !start.IsZero() {
		minTS = start.Truncate(a.binWidth)
	}
	if !end.IsZero() {
		// Truncate end to bin boundary (inclusive).
		maxTS = end.Truncate(a.binWidth)
	}
	if minTS.IsZero() || maxTS.IsZero() || !minTS.Before(maxTS) {
		return
	}

	// Find all unique non-bin group value combinations.
	nonBinGroups := make(map[string][]string) // nbKey → nbValues
	for _, key := range a.keyOrder {
		gs := a.state[key]
		nbValues := make([]string, 0, len(gs.groupValues))
		for i, v := range gs.groupValues {
			if i != a.binIdx {
				nbValues = append(nbValues, v)
			}
		}
		nbKey := makeGroupKey(nbValues)
		if _, exists := nonBinGroups[nbKey]; !exists {
			nonBinGroups[nbKey] = nbValues
		}
	}

	// For each non-bin group combination, fill all bins from minTS to maxTS.
	for _, nbValues := range nonBinGroups {
		for t := minTS; !t.After(maxTS); t = t.Add(a.binWidth) {
			binStr := t.UTC().Format(time.RFC3339)

			// Build full group values.
			fullValues := make([]string, len(a.groups))
			nbIdx := 0
			for i := range a.groups {
				if i == a.binIdx {
					fullValues[i] = binStr
				} else {
					fullValues[i] = nbValues[nbIdx]
					nbIdx++
				}
			}

			key := makeGroupKey(fullValues)
			if _, exists := a.state[key]; !exists {
				accs, err := a.makeAccumulators()
				if err != nil {
					continue
				}
				a.state[key] = &groupState{
					groupValues: fullValues,
					accs:        accs,
				}
				a.keyOrder = append(a.keyOrder, key)
			}
		}
	}
}

// dataTimeRange returns the min and max bin timestamps from the current data.
func (a *Aggregator) dataTimeRange() (time.Time, time.Time) {
	var minTS, maxTS time.Time
	for _, key := range a.keyOrder {
		gs := a.state[key]
		if a.binIdx >= len(gs.groupValues) {
			continue
		}
		binStr := gs.groupValues[a.binIdx]
		t, err := time.Parse(time.RFC3339, binStr)
		if err != nil {
			continue
		}
		if minTS.IsZero() || t.Before(minTS) {
			minTS = t
		}
		if maxTS.IsZero() || t.After(maxTS) {
			maxTS = t
		}
	}
	return minTS, maxTS
}

// sortRows sorts result rows by group values, with proper timestamp ordering
// for bin() columns.
func (a *Aggregator) sortRows(rows [][]string) {
	if len(rows) <= 1 || len(a.groups) == 0 {
		return
	}

	slices.SortStableFunc(rows, func(ri, rj []string) int {
		for k := 0; k < len(a.groups); k++ {
			if ri[k] == rj[k] {
				continue
			}
			if k == a.binIdx {
				// Parse timestamps for chronological ordering.
				ti, erri := time.Parse(time.RFC3339, ri[k])
				tj, errj := time.Parse(time.RFC3339, rj[k])
				if erri == nil && errj == nil {
					return ti.Compare(tj)
				}
			}
			return strings.Compare(ri[k], rj[k])
		}
		return 0
	})
}

// accumulator is the interface for aggregate function state.
type accumulator interface {
	Add(v querylang.Value)
	Result() querylang.Value
}

// countAcc counts non-missing values. For bare count (no argument),
// the caller passes a non-missing value for every record.
type countAcc struct{ n int64 }

func (a *countAcc) Add(v querylang.Value) {
	if !v.Missing {
		a.n++
	}
}

func (a *countAcc) Result() querylang.Value {
	return querylang.NumValue(float64(a.n))
}

type sumAcc struct {
	sum float64
	any bool
}

func (a *sumAcc) Add(v querylang.Value) {
	if n, ok := v.ToNum(); ok {
		a.sum += n
		a.any = true
	}
}

func (a *sumAcc) Result() querylang.Value {
	if !a.any {
		return querylang.MissingValue()
	}
	return querylang.NumValue(a.sum)
}

type avgAcc struct {
	sum   float64
	count int64
}

func (a *avgAcc) Add(v querylang.Value) {
	if n, ok := v.ToNum(); ok {
		a.sum += n
		a.count++
	}
}

func (a *avgAcc) Result() querylang.Value {
	if a.count == 0 {
		return querylang.MissingValue()
	}
	return querylang.NumValue(a.sum / float64(a.count))
}

type minAcc struct {
	min float64
	any bool
}

func (a *minAcc) Add(v querylang.Value) {
	if n, ok := v.ToNum(); ok {
		if !a.any || n < a.min {
			a.min = n
			a.any = true
		}
	}
}

func (a *minAcc) Result() querylang.Value {
	if !a.any {
		return querylang.MissingValue()
	}
	return querylang.NumValue(a.min)
}

type maxAcc struct {
	max float64
	any bool
}

func (a *maxAcc) Add(v querylang.Value) {
	if n, ok := v.ToNum(); ok {
		if !a.any || n > a.max {
			a.max = n
			a.any = true
		}
	}
}

func (a *maxAcc) Result() querylang.Value {
	if !a.any {
		return querylang.MissingValue()
	}
	return querylang.NumValue(a.max)
}

// dcountAcc counts distinct non-missing string values.
type dcountAcc struct {
	seen map[string]bool
}

func (a *dcountAcc) Add(v querylang.Value) {
	if v.Missing {
		return
	}
	if a.seen == nil {
		a.seen = make(map[string]bool)
	}
	a.seen[v.Str] = true
}

func (a *dcountAcc) Result() querylang.Value {
	return querylang.NumValue(float64(len(a.seen)))
}

// medianAcc collects numeric values and returns the median.
type medianAcc struct {
	vals []float64
}

func (a *medianAcc) Add(v querylang.Value) {
	if n, ok := v.ToNum(); ok {
		a.vals = append(a.vals, n)
	}
}

func (a *medianAcc) Result() querylang.Value {
	if len(a.vals) == 0 {
		return querylang.MissingValue()
	}
	slices.Sort(a.vals)
	n := len(a.vals)
	if n%2 == 1 {
		return querylang.NumValue(a.vals[n/2])
	}
	return querylang.NumValue((a.vals[n/2-1] + a.vals[n/2]) / 2)
}

// firstAcc tracks the first non-missing value seen.
type firstAcc struct {
	val querylang.Value
	set bool
}

func (a *firstAcc) Add(v querylang.Value) {
	if !a.set && !v.Missing {
		a.val = v
		a.set = true
	}
}

func (a *firstAcc) Result() querylang.Value {
	if !a.set {
		return querylang.MissingValue()
	}
	return a.val
}

// lastAcc tracks the last non-missing value seen.
type lastAcc struct {
	val querylang.Value
	set bool
}

func (a *lastAcc) Add(v querylang.Value) {
	if !v.Missing {
		a.val = v
		a.set = true
	}
}

func (a *lastAcc) Result() querylang.Value {
	if !a.set {
		return querylang.MissingValue()
	}
	return a.val
}

// valuesAcc collects distinct values and returns them comma-separated.
type valuesAcc struct {
	seen  map[string]bool
	order []string
}

func (a *valuesAcc) Add(v querylang.Value) {
	if v.Missing {
		return
	}
	if a.seen == nil {
		a.seen = make(map[string]bool)
	}
	if !a.seen[v.Str] {
		a.seen[v.Str] = true
		a.order = append(a.order, v.Str)
	}
}

func (a *valuesAcc) Result() querylang.Value {
	if len(a.order) == 0 {
		return querylang.MissingValue()
	}
	return querylang.StrValue(strings.Join(a.order, ", "))
}

func newAccumulator(funcName string) (accumulator, error) {
	switch strings.ToLower(funcName) {
	case "count":
		return &countAcc{}, nil
	case "sum":
		return &sumAcc{}, nil
	case "avg":
		return &avgAcc{}, nil
	case "min":
		return &minAcc{}, nil
	case "max":
		return &maxAcc{}, nil
	case "dcount":
		return &dcountAcc{}, nil
	case "median":
		return &medianAcc{}, nil
	case "first":
		return &firstAcc{}, nil
	case "last":
		return &lastAcc{}, nil
	case "values":
		return &valuesAcc{}, nil
	default:
		return nil, fmt.Errorf("unknown aggregate function: %s", funcName)
	}
}
