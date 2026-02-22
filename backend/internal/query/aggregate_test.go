package query

import (
	"math"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
)

func makeRec(writeTS time.Time, attrs chunk.Attributes, raw string) chunk.Record {
	return chunk.Record{
		WriteTS:  writeTS,
		IngestTS: writeTS,
		Attrs:    attrs,
		Raw:      []byte(raw),
	}
}

var baseTime = time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

func TestParseBinDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
		err   bool
	}{
		{"30s", 30 * time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"1h", time.Hour, false},
		{"2h", 2 * time.Hour, false},
		{"1d", 24 * time.Hour, false},
		{"0.5h", 30 * time.Minute, false},
		{"", 0, true},
		{"5", 0, true},
		{"m", 0, true},
		{"-1m", 0, true},
		{"5x", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseBinDuration(tt.input)
			if (err != nil) != tt.err {
				t.Fatalf("err = %v, wantErr = %v", err, tt.err)
			}
			if !tt.err && got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordToRow(t *testing.T) {
	rec := chunk.Record{
		WriteTS: baseTime,
		Attrs:   chunk.Attributes{"status": "200", "method": "GET"},
		Raw:     []byte("GET /api/test 200"),
	}

	row := RecordToRow(rec)

	if row["status"] != "200" {
		t.Errorf("status = %q, want 200", row["status"])
	}
	if row["method"] != "GET" {
		t.Errorf("method = %q, want GET", row["method"])
	}
	if row["_raw"] != "GET /api/test 200" {
		t.Errorf("_raw = %q, want 'GET /api/test 200'", row["_raw"])
	}
}

func TestRecordToRowNilAttrs(t *testing.T) {
	rec := chunk.Record{Raw: []byte("hello")}
	row := RecordToRow(rec)
	if row["_raw"] != "hello" {
		t.Errorf("_raw = %q, want hello", row["_raw"])
	}
}

func TestAggregatorBareCount(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	for i := range 5 {
		agg.Add(makeRec(baseTime.Add(time.Duration(i)*time.Second), nil, "line"))
	}

	result := agg.Result(time.Time{}, time.Time{})
	if len(result.Columns) != 1 || result.Columns[0] != "count" {
		t.Errorf("columns = %v, want [count]", result.Columns)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if result.Rows[0][0] != "5" {
		t.Errorf("count = %q, want 5", result.Rows[0][0])
	}
}

func TestAggregatorCountExpression(t *testing.T) {
	// count(status) counts records where status is non-missing.
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{
			Func: "count",
			Arg:  &querylang.FieldRef{Name: "status"},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"status": "200"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"status": "500"}, ""))
	agg.Add(makeRec(baseTime, nil, "")) // missing status

	result := agg.Result(time.Time{}, time.Time{})
	if result.Rows[0][0] != "2" {
		t.Errorf("count(status) = %q, want 2", result.Rows[0][0])
	}
}

func TestAggregatorGroupByField(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs:   []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{Field: &querylang.FieldRef{Name: "method"}}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"method": "GET"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"method": "GET"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"method": "POST"}, ""))

	result := agg.Result(time.Time{}, time.Time{})
	if len(result.Columns) != 2 {
		t.Fatalf("columns = %v, want [method, count]", result.Columns)
	}
	if result.Columns[0] != "method" || result.Columns[1] != "count" {
		t.Errorf("columns = %v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}

	// Rows sorted by group value (GET < POST).
	if result.Rows[0][0] != "GET" || result.Rows[0][1] != "2" {
		t.Errorf("row 0 = %v, want [GET 2]", result.Rows[0])
	}
	if result.Rows[1][0] != "POST" || result.Rows[1][1] != "1" {
		t.Errorf("row 1 = %v, want [POST 1]", result.Rows[1])
	}
}

func TestAggregatorMultipleAggs(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{
			{Func: "count"},
			{Func: "avg", Arg: &querylang.FieldRef{Name: "duration"}},
			{Func: "sum", Arg: &querylang.FieldRef{Name: "duration"}},
		},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"duration": "100"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"duration": "200"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"duration": "300"}, ""))

	result := agg.Result(time.Time{}, time.Time{})
	if len(result.Columns) != 3 {
		t.Fatalf("columns = %v", result.Columns)
	}
	if result.Columns[0] != "count" || result.Columns[1] != "avg_duration" || result.Columns[2] != "sum_duration" {
		t.Errorf("columns = %v", result.Columns)
	}

	row := result.Rows[0]
	if row[0] != "3" {
		t.Errorf("count = %q, want 3", row[0])
	}
	if row[1] != "200" {
		t.Errorf("avg = %q, want 200", row[1])
	}
	if row[2] != "600" {
		t.Errorf("sum = %q, want 600", row[2])
	}
}

func TestAggregatorMinMax(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{
			{Func: "min", Arg: &querylang.FieldRef{Name: "val"}},
			{Func: "max", Arg: &querylang.FieldRef{Name: "val"}},
		},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range []string{"50", "10", "90", "30"} {
		agg.Add(makeRec(baseTime, chunk.Attributes{"val": v}, ""))
	}

	result := agg.Result(time.Time{}, time.Time{})
	if result.Rows[0][0] != "10" {
		t.Errorf("min = %q, want 10", result.Rows[0][0])
	}
	if result.Rows[0][1] != "90" {
		t.Errorf("max = %q, want 90", result.Rows[0][1])
	}
}

func TestAggregatorBinTime(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{
			Bin: &querylang.BinExpr{Duration: "5m"},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	// Records at 10:01, 10:03, 10:06, 10:11.
	agg.Add(makeRec(baseTime.Add(1*time.Minute), nil, ""))
	agg.Add(makeRec(baseTime.Add(3*time.Minute), nil, ""))
	agg.Add(makeRec(baseTime.Add(6*time.Minute), nil, ""))
	agg.Add(makeRec(baseTime.Add(11*time.Minute), nil, ""))

	result := agg.Result(time.Time{}, time.Time{})
	if result.Columns[0] != "_time" || result.Columns[1] != "count" {
		t.Errorf("columns = %v", result.Columns)
	}

	// Expected bins: 10:00 (2 records), 10:05 (1 record), 10:10 (1 record).
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(result.Rows))
	}

	// Rows sorted chronologically.
	want := []struct {
		time  string
		count string
	}{
		{"2025-06-15T10:00:00Z", "2"},
		{"2025-06-15T10:05:00Z", "1"},
		{"2025-06-15T10:10:00Z", "1"},
	}
	for i, w := range want {
		if result.Rows[i][0] != w.time || result.Rows[i][1] != w.count {
			t.Errorf("row %d = %v, want [%s %s]", i, result.Rows[i], w.time, w.count)
		}
	}
}

func TestAggregatorBinWithCustomField(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{
			Bin: &querylang.BinExpr{
				Duration: "1h",
				Field:    &querylang.FieldRef{Name: "_ingest_ts"},
			},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	rec := chunk.Record{
		WriteTS:  baseTime,
		IngestTS: baseTime.Add(30 * time.Minute), // ingest at 10:30
		Raw:      []byte("test"),
	}
	agg.Add(rec)

	result := agg.Result(time.Time{}, time.Time{})
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	// bin(1h) of 10:30 → 10:00
	if result.Rows[0][0] != "2025-06-15T10:00:00Z" {
		t.Errorf("bin = %q, want 2025-06-15T10:00:00Z", result.Rows[0][0])
	}
}

func TestAggregatorGapFill(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{
			Bin: &querylang.BinExpr{Duration: "5m"},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	// Records at 10:01 and 10:14 — gap at 10:05 and 10:10.
	agg.Add(makeRec(baseTime.Add(1*time.Minute), nil, ""))
	agg.Add(makeRec(baseTime.Add(14*time.Minute), nil, ""))

	result := agg.Result(time.Time{}, time.Time{})

	// Expected: 10:00 (1), 10:05 (0), 10:10 (0 — gap-filled from data range).
	if len(result.Rows) != 3 {
		t.Fatalf("rows = %d, want 3 (with gap-fill)", len(result.Rows))
	}

	// Check gap-filled row has count=0.
	if result.Rows[1][0] != "2025-06-15T10:05:00Z" || result.Rows[1][1] != "0" {
		t.Errorf("gap-filled row = %v, want [2025-06-15T10:05:00Z 0]", result.Rows[1])
	}
}

func TestAggregatorGapFillWithTimeBounds(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{
			Bin: &querylang.BinExpr{Duration: "5m"},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	// Only one record at 10:03.
	agg.Add(makeRec(baseTime.Add(3*time.Minute), nil, ""))

	// Gap-fill from 10:00 to 10:15 (query time bounds).
	start := baseTime
	end := baseTime.Add(15 * time.Minute)
	result := agg.Result(start, end)

	// Expected: 10:00 (1), 10:05 (0), 10:10 (0), 10:15 (0).
	if len(result.Rows) != 4 {
		t.Fatalf("rows = %d, want 4 (with gap-fill from bounds)", len(result.Rows))
	}
}

func TestAggregatorMultiDimensionalGrouping(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{
			{Field: &querylang.FieldRef{Name: "level"}},
			{Bin: &querylang.BinExpr{Duration: "5m"}},
		},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	// error at 10:01, info at 10:01, info at 10:06.
	agg.Add(makeRec(baseTime.Add(1*time.Minute), chunk.Attributes{"level": "error"}, ""))
	agg.Add(makeRec(baseTime.Add(1*time.Minute), chunk.Attributes{"level": "info"}, ""))
	agg.Add(makeRec(baseTime.Add(6*time.Minute), chunk.Attributes{"level": "info"}, ""))

	result := agg.Result(time.Time{}, time.Time{})

	// Expected: gap-fill within each level.
	// error: 10:00 (1), 10:05 (0)
	// info:  10:00 (1), 10:05 (1)
	if len(result.Rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(result.Rows))
	}

	// Sorted by level, then time.
	expected := [][]string{
		{"error", "2025-06-15T10:00:00Z", "1"},
		{"error", "2025-06-15T10:05:00Z", "0"},
		{"info", "2025-06-15T10:00:00Z", "1"},
		{"info", "2025-06-15T10:05:00Z", "1"},
	}
	for i, want := range expected {
		if i >= len(result.Rows) {
			break
		}
		for j := range want {
			if result.Rows[i][j] != want[j] {
				t.Errorf("row %d col %d = %q, want %q", i, j, result.Rows[i][j], want[j])
			}
		}
	}
}

func TestAggregatorMissingFields(t *testing.T) {
	// avg(duration) where some records don't have duration.
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{
			Func: "avg",
			Arg:  &querylang.FieldRef{Name: "duration"},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"duration": "100"}, ""))
	agg.Add(makeRec(baseTime, nil, ""))                                  // missing
	agg.Add(makeRec(baseTime, chunk.Attributes{"duration": "200"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"other": "foo"}, ""))     // missing

	result := agg.Result(time.Time{}, time.Time{})
	// avg should only count records with duration: (100+200)/2 = 150
	if result.Rows[0][0] != "150" {
		t.Errorf("avg = %q, want 150", result.Rows[0][0])
	}
}

func TestAggregatorNonNumericSkipped(t *testing.T) {
	// sum(val) where some values are non-numeric.
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{
			Func: "sum",
			Arg:  &querylang.FieldRef{Name: "val"},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"val": "10"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"val": "hello"}, "")) // non-numeric
	agg.Add(makeRec(baseTime, chunk.Attributes{"val": "20"}, ""))

	result := agg.Result(time.Time{}, time.Time{})
	if result.Rows[0][0] != "30" {
		t.Errorf("sum = %q, want 30", result.Rows[0][0])
	}
}

func TestAggregatorAllNonNumeric(t *testing.T) {
	// avg of all non-numeric → missing.
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{
			Func: "avg",
			Arg:  &querylang.FieldRef{Name: "val"},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"val": "foo"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"val": "bar"}, ""))

	result := agg.Result(time.Time{}, time.Time{})
	if result.Rows[0][0] != "" {
		t.Errorf("avg of non-numeric = %q, want empty (missing)", result.Rows[0][0])
	}
}

func TestAggregatorNoRecords(t *testing.T) {
	// No records, no group-by → single row with count=0.
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{
			{Func: "count"},
			{Func: "sum", Arg: &querylang.FieldRef{Name: "val"}},
		},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	result := agg.Result(time.Time{}, time.Time{})
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if result.Rows[0][0] != "0" {
		t.Errorf("count = %q, want 0", result.Rows[0][0])
	}
	if result.Rows[0][1] != "" {
		t.Errorf("sum = %q, want empty (missing)", result.Rows[0][1])
	}
}

func TestAggregatorNoRecordsWithGroupBy(t *testing.T) {
	// No records with group-by → zero rows.
	stats := &querylang.StatsOp{
		Aggs:   []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{Field: &querylang.FieldRef{Name: "method"}}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	result := agg.Result(time.Time{}, time.Time{})
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(result.Rows))
	}
}

func TestAggregatorCardinalityCap(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs:   []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{Field: &querylang.FieldRef{Name: "id"}}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	// Exceed cardinality cap.
	for i := range MaxGroupCardinality + 100 {
		id := "id-" + time.Duration(i).String()
		agg.Add(makeRec(baseTime, chunk.Attributes{"id": id}, ""))
	}

	result := agg.Result(time.Time{}, time.Time{})
	if !result.Truncated {
		t.Error("expected Truncated=true")
	}
	if len(result.Rows) != MaxGroupCardinality {
		t.Errorf("rows = %d, want %d", len(result.Rows), MaxGroupCardinality)
	}
}

func TestAggregatorExpressionArg(t *testing.T) {
	// avg(toNumber(response_time) / 1000)
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{
			Func: "avg",
			Arg: &querylang.ArithExpr{
				Left:  &querylang.FuncCall{Name: "toNumber", Args: []querylang.PipeExpr{&querylang.FieldRef{Name: "response_time"}}},
				Op:    querylang.ArithDiv,
				Right: &querylang.NumberLit{Value: "1000"},
			},
			Alias: "avg_sec",
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"response_time": "1000"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"response_time": "2000"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"response_time": "3000"}, ""))

	result := agg.Result(time.Time{}, time.Time{})
	if result.Columns[0] != "avg_sec" {
		t.Errorf("column = %q, want avg_sec", result.Columns[0])
	}
	if result.Rows[0][0] != "2" {
		t.Errorf("avg_sec = %q, want 2", result.Rows[0][0])
	}
}

func TestAggregatorParsedPipeline(t *testing.T) {
	// Integration test: parse a pipeline, run the stats operator.
	pipeline, err := querylang.ParsePipeline("error | stats count, avg(duration) as avg_dur by method")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	stats := pipeline.Pipes[0].(*querylang.StatsOp)
	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	agg.Add(makeRec(baseTime, chunk.Attributes{"method": "GET", "duration": "100"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"method": "GET", "duration": "300"}, ""))
	agg.Add(makeRec(baseTime, chunk.Attributes{"method": "POST", "duration": "200"}, ""))

	result := agg.Result(time.Time{}, time.Time{})

	if len(result.Columns) != 3 {
		t.Fatalf("columns = %v", result.Columns)
	}
	if result.Columns[0] != "method" || result.Columns[1] != "count" || result.Columns[2] != "avg_dur" {
		t.Errorf("columns = %v", result.Columns)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}

	// GET: count=2, avg=200
	if result.Rows[0][0] != "GET" || result.Rows[0][1] != "2" || result.Rows[0][2] != "200" {
		t.Errorf("GET row = %v, want [GET 2 200]", result.Rows[0])
	}
	// POST: count=1, avg=200
	if result.Rows[1][0] != "POST" || result.Rows[1][1] != "1" || result.Rows[1][2] != "200" {
		t.Errorf("POST row = %v, want [POST 1 200]", result.Rows[1])
	}
}

func TestAggregatorBinOnlyGrouping(t *testing.T) {
	// bin() as the only group expression — use direct AST construction.
	stats := &querylang.StatsOp{
		Aggs:   []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{Bin: &querylang.BinExpr{Duration: "1h"}}},
	}
	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	// 3 records: 10:15, 10:45, 11:30
	agg.Add(makeRec(baseTime.Add(15*time.Minute), nil, ""))
	agg.Add(makeRec(baseTime.Add(45*time.Minute), nil, ""))
	agg.Add(makeRec(baseTime.Add(90*time.Minute), nil, ""))

	result := agg.Result(time.Time{}, time.Time{})

	// Expected: 10:00 (2), 11:00 (1)
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
	if result.Rows[0][0] != "2025-06-15T10:00:00Z" || result.Rows[0][1] != "2" {
		t.Errorf("row 0 = %v, want [2025-06-15T10:00:00Z 2]", result.Rows[0])
	}
	if result.Rows[1][0] != "2025-06-15T11:00:00Z" || result.Rows[1][1] != "1" {
		t.Errorf("row 1 = %v, want [2025-06-15T11:00:00Z 1]", result.Rows[1])
	}
}

func TestAggregatorSourceTSBin(t *testing.T) {
	// bin(5m, _source_ts)
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{
			Bin: &querylang.BinExpr{
				Duration: "5m",
				Field:    &querylang.FieldRef{Name: "_source_ts"},
			},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	rec := chunk.Record{
		WriteTS:  baseTime,
		IngestTS: baseTime,
		SourceTS: baseTime.Add(2 * time.Minute), // 10:02
		Raw:      []byte("test"),
	}
	agg.Add(rec)

	result := agg.Result(time.Time{}, time.Time{})
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(result.Rows))
	}
	if result.Rows[0][0] != "2025-06-15T10:00:00Z" {
		t.Errorf("bin = %q, want 2025-06-15T10:00:00Z", result.Rows[0][0])
	}
}

func TestAggregatorMissingSourceTS(t *testing.T) {
	// bin(5m, _source_ts) with zero SourceTS → record skipped.
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{
			Bin: &querylang.BinExpr{
				Duration: "5m",
				Field:    &querylang.FieldRef{Name: "_source_ts"},
			},
		}},
	}

	agg, err := NewAggregator(stats)
	if err != nil {
		t.Fatal(err)
	}

	rec := chunk.Record{
		WriteTS:  baseTime,
		IngestTS: baseTime,
		// SourceTS is zero
		Raw: []byte("test"),
	}
	agg.Add(rec)

	result := agg.Result(time.Time{}, time.Time{})
	// No rows because the record was skipped (missing source timestamp).
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 (source_ts missing)", len(result.Rows))
	}
}

func TestAggregatorUnknownFunction(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "median"}},
	}

	_, err := NewAggregator(stats)
	if err == nil {
		t.Fatal("expected error for unknown function")
	}
}

func TestAggregatorMultipleBinGroups(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{
			{Bin: &querylang.BinExpr{Duration: "5m"}},
			{Bin: &querylang.BinExpr{Duration: "1h"}},
		},
	}

	_, err := NewAggregator(stats)
	if err == nil {
		t.Fatal("expected error for multiple bin() groups")
	}
}

func TestRunStats(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs:   []querylang.AggExpr{{Func: "count"}},
		Groups: []querylang.GroupExpr{{Field: &querylang.FieldRef{Name: "level"}}},
	}

	records := func(yield func(chunk.Record, error) bool) {
		if !yield(makeRec(baseTime, chunk.Attributes{"level": "error"}, ""), nil) {
			return
		}
		if !yield(makeRec(baseTime, chunk.Attributes{"level": "error"}, ""), nil) {
			return
		}
		if !yield(makeRec(baseTime, chunk.Attributes{"level": "info"}, ""), nil) {
			return
		}
	}

	result, err := RunStats(records, stats, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(result.Rows))
	}
	if result.Rows[0][0] != "error" || result.Rows[0][1] != "2" {
		t.Errorf("error row = %v", result.Rows[0])
	}
	if result.Rows[1][0] != "info" || result.Rows[1][1] != "1" {
		t.Errorf("info row = %v", result.Rows[1])
	}
}

func TestRunStatsError(t *testing.T) {
	stats := &querylang.StatsOp{
		Aggs: []querylang.AggExpr{{Func: "count"}},
	}

	testErr := errForTest("test error")
	records := func(yield func(chunk.Record, error) bool) {
		yield(chunk.Record{}, testErr)
	}

	_, err := RunStats(records, stats, time.Time{}, time.Time{})
	if err == nil {
		t.Fatal("expected error")
	}
}

type errForTest string

func (e errForTest) Error() string { return string(e) }

func TestAccumulatorEdgeCases(t *testing.T) {
	t.Run("count never missing", func(t *testing.T) {
		acc := &countAcc{}
		v := acc.Result()
		if v.Missing || v.Num != 0 {
			t.Errorf("empty count = %v, want 0", v)
		}
	})

	t.Run("sum missing when empty", func(t *testing.T) {
		acc := &sumAcc{}
		v := acc.Result()
		if !v.Missing {
			t.Error("expected missing for empty sum")
		}
	})

	t.Run("avg missing when empty", func(t *testing.T) {
		acc := &avgAcc{}
		v := acc.Result()
		if !v.Missing {
			t.Error("expected missing for empty avg")
		}
	})

	t.Run("min missing when empty", func(t *testing.T) {
		acc := &minAcc{}
		v := acc.Result()
		if !v.Missing {
			t.Error("expected missing for empty min")
		}
	})

	t.Run("max missing when empty", func(t *testing.T) {
		acc := &maxAcc{}
		v := acc.Result()
		if !v.Missing {
			t.Error("expected missing for empty max")
		}
	})

	t.Run("count skips missing", func(t *testing.T) {
		acc := &countAcc{}
		acc.Add(querylang.NumValue(1))
		acc.Add(querylang.MissingValue())
		acc.Add(querylang.NumValue(2))
		if acc.Result().Num != 2 {
			t.Errorf("count = %v, want 2", acc.Result().Num)
		}
	})

	t.Run("avg precision", func(t *testing.T) {
		acc := &avgAcc{}
		acc.Add(querylang.NumValue(1))
		acc.Add(querylang.NumValue(2))
		acc.Add(querylang.NumValue(3))
		v := acc.Result()
		if math.Abs(v.Num-2.0) > 1e-9 {
			t.Errorf("avg = %v, want 2", v.Num)
		}
	})

	t.Run("min negative", func(t *testing.T) {
		acc := &minAcc{}
		acc.Add(querylang.NumValue(5))
		acc.Add(querylang.NumValue(-3))
		acc.Add(querylang.NumValue(1))
		if acc.Result().Num != -3 {
			t.Errorf("min = %v, want -3", acc.Result().Num)
		}
	})

	t.Run("max negative", func(t *testing.T) {
		acc := &maxAcc{}
		acc.Add(querylang.NumValue(-5))
		acc.Add(querylang.NumValue(-3))
		acc.Add(querylang.NumValue(-1))
		if acc.Result().Num != -1 {
			t.Errorf("max = %v, want -1", acc.Result().Num)
		}
	})
}
