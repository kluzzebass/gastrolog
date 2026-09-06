package query

import (
	"context"
	"iter"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
)

// timechartBins is the per-bucket state a timechart accumulates from records.
type timechartBins struct {
	start, end  time.Time
	bucketWidth time.Duration
	numBuckets  int
	groupField  string
	counts      []int64
	groupCounts []map[string]int64
}

func newTimechartBins(start, end time.Time, numBuckets int, groupField string) *timechartBins {
	bucketWidth := end.Sub(start) / time.Duration(numBuckets)
	if bucketWidth <= 0 {
		bucketWidth = time.Second
	}
	b := &timechartBins{
		start:       start,
		end:         end,
		bucketWidth: bucketWidth,
		numBuckets:  numBuckets,
		groupField:  groupField,
		counts:      make([]int64, numBuckets),
		groupCounts: make([]map[string]int64, numBuckets),
	}
	for i := range b.groupCounts {
		b.groupCounts[i] = make(map[string]int64)
	}
	return b
}

func (b *timechartBins) add(rec chunk.Record, orderBy OrderBy, budget *Budget) error {
	return timechartBinRecord(orderBy.RecordTS(rec), rec.Attrs, b.start, b.end, b.bucketWidth, b.numBuckets,
		b.groupField, b.groupField != "", b.counts, b.groupCounts, budget)
}

// table renders the bins. Every count here came from a scanned record, so no
// bucket carries a cloud-derived estimate.
func (b *timechartBins) table() *TableResult {
	return timechartToTable(b.groupField, b.start, b.bucketWidth, b.numBuckets, b.counts, b.groupCounts,
		make([]bool, b.numBuckets), make([]int64, b.numBuckets))
}

// runTimechartOverStream bins a timechart from this node's records merged in
// query order with a stream from the rest of the cluster. It is the path a
// coordinator takes when an operator ahead of the timechart — a cap, a sort —
// has to see every cluster record, so binning from local chunk metadata would
// answer for one node while claiming to answer for all.
//
// Without an explicit time range the buckets span the records the pre-ops
// kept. No node's chunk metadata is consulted, so the answer does not depend
// on which node coordinates.
func (e *Engine) runTimechartOverStream(ctx context.Context, q Query, tc *querylang.TimechartOp, preOps []querylang.PipeOp, remote iter.Seq2[chunk.Record, error], budget *Budget) (*TableResult, error) {
	numBuckets := clampBuckets(tc.N)
	groupField := tc.By

	q.Limit = 0
	local, _ := e.Search(ctx, q, nil)
	it := mergeOrdered(local, remote, q.OrderBy, q.Reverse())

	start, end := q.Start, q.End
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	rangeKnown := !start.IsZero() && !end.IsZero() && start.Before(end)

	if rangeKnown && opsStreamable(preOps) {
		bins := newTimechartBins(start, end, numBuckets, groupField)
		err := e.forEachSurvivor(ctx, it, preOps, budget, func(rec chunk.Record) error {
			return bins.add(rec, q.OrderBy, budget)
		})
		if err != nil {
			return nil, err
		}
		return bins.table(), nil
	}

	records, err := e.collectSurvivors(ctx, it, preOps, budget)
	if err != nil {
		return nil, err
	}
	if !rangeKnown {
		start, end = recordSpan(records, q.OrderBy)
		if start.IsZero() {
			return &TableResult{Columns: timechartColumns(groupField)}, nil
		}
	}
	bins := newTimechartBins(start, end, numBuckets, groupField)
	for _, rec := range records {
		if err := bins.add(rec, q.OrderBy, budget); err != nil {
			return nil, err
		}
	}
	return bins.table(), nil
}

// collectSurvivors materializes the records that pass preOps. Streamable
// operators run per record so a head stops the scan; anything else goes
// through the bounded batch paths.
func (e *Engine) collectSurvivors(ctx context.Context, it iter.Seq2[chunk.Record, error], preOps []querylang.PipeOp, budget *Budget) ([]chunk.Record, error) {
	if !opsStreamable(preOps) {
		return applyRecordOps(ctx, it, preOps, e.lookupResolver, budget)
	}
	var records []chunk.Record
	err := e.forEachSurvivor(ctx, it, preOps, budget, func(rec chunk.Record) error {
		if err := budget.ChargeRecord(consumerRecordBuffer, rec); err != nil {
			return err
		}
		records = append(records, rec)
		return nil
	})
	return records, err
}

// forEachSurvivor runs streamable preOps over it record by record and hands
// each record they keep to fn. It stops as soon as a head is satisfied.
func (e *Engine) forEachSurvivor(ctx context.Context, it iter.Seq2[chunk.Record, error], preOps []querylang.PipeOp, budget *Budget, fn func(chunk.Record) error) error {
	sf := newStreamFilter(ctx, preOps, e.lookupResolver, budget)
	for rec, err := range it {
		if err != nil {
			return err
		}
		rec = rec.Copy()
		materializeRecord(&rec)
		keep, err := sf.apply(&rec)
		if err != nil {
			return err
		}
		if keep {
			if err := fn(rec); err != nil {
				return err
			}
		}
		if sf.exhausted {
			return nil
		}
	}
	return nil
}

// recordSpan returns the half-open range [oldest, newest+1ns] covering every
// record's ordering timestamp, so the newest record bins like the rest.
// Returns zero times for an empty set.
func recordSpan(records []chunk.Record, orderBy OrderBy) (time.Time, time.Time) {
	var lo, hi time.Time
	for _, rec := range records {
		ts := orderBy.RecordTS(rec)
		if lo.IsZero() || ts.Before(lo) {
			lo = ts
		}
		if hi.IsZero() || ts.After(hi) {
			hi = ts
		}
	}
	if lo.IsZero() {
		return lo, hi
	}
	return lo, hi.Add(time.Nanosecond)
}
