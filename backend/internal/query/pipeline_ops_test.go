package query

import (
	"gastrolog/internal/glid"
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/lookup"
	"gastrolog/internal/querylang"

)

func TestApplyRecordEval(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"duration": "2000"}, "request took 2000ms"),
		makeRec(baseTime, chunk.Attributes{"duration": "500"}, "request took 500ms"),
	}

	op := &querylang.EvalOp{
		Assignments: []querylang.EvalAssignment{
			{Field: "duration_s", Expr: &querylang.ArithExpr{
				Left: &querylang.FieldRef{Name: "duration"},
				Op:   querylang.ArithDiv,
				Right: &querylang.NumberLit{Value: "1000"},
			}},
		},
	}

	eval := querylang.NewEvaluator()
	result, err := applyRecordEval(records, op, eval)
	if err != nil {
		t.Fatalf("applyRecordEval: %v", err)
	}
	if result[0].Attrs["duration_s"] != "2" {
		t.Errorf("record 0 duration_s = %q, want '2'", result[0].Attrs["duration_s"])
	}
	if result[1].Attrs["duration_s"] != "0.5" {
		t.Errorf("record 1 duration_s = %q, want '0.5'", result[1].Attrs["duration_s"])
	}
}

func TestApplyRecordSort(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"count": "5"}, "five"),
		makeRec(baseTime, chunk.Attributes{"count": "20"}, "twenty"),
		makeRec(baseTime, chunk.Attributes{"count": "3"}, "three"),
	}

	op := &querylang.SortOp{Fields: []querylang.SortField{{Name: "count"}}}
	applyRecordSort(records, op)

	// Ascending numeric sort: 3, 5, 20.
	if records[0].Attrs["count"] != "3" {
		t.Errorf("record 0 count = %q, want '3'", records[0].Attrs["count"])
	}
	if records[1].Attrs["count"] != "5" {
		t.Errorf("record 1 count = %q, want '5'", records[1].Attrs["count"])
	}
	if records[2].Attrs["count"] != "20" {
		t.Errorf("record 2 count = %q, want '20'", records[2].Attrs["count"])
	}
}

func TestApplyRecordSortDesc(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"count": "5"}, "five"),
		makeRec(baseTime, chunk.Attributes{"count": "20"}, "twenty"),
		makeRec(baseTime, chunk.Attributes{"count": "3"}, "three"),
	}

	op := &querylang.SortOp{Fields: []querylang.SortField{{Name: "count", Desc: true}}}
	applyRecordSort(records, op)

	// Descending numeric sort: 20, 5, 3.
	if records[0].Attrs["count"] != "20" {
		t.Errorf("record 0 count = %q, want '20'", records[0].Attrs["count"])
	}
	if records[1].Attrs["count"] != "5" {
		t.Errorf("record 1 count = %q, want '5'", records[1].Attrs["count"])
	}
	if records[2].Attrs["count"] != "3" {
		t.Errorf("record 2 count = %q, want '3'", records[2].Attrs["count"])
	}
}

func TestApplyRecordHead(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, nil, "a"),
		makeRec(baseTime, nil, "b"),
		makeRec(baseTime, nil, "c"),
		makeRec(baseTime, nil, "d"),
	}

	result := applyRecordHead(records, &querylang.HeadOp{N: 2})
	if len(result) != 2 {
		t.Fatalf("expected 2 records, got %d", len(result))
	}
	if string(result[0].Raw) != "a" || string(result[1].Raw) != "b" {
		t.Errorf("unexpected records: %s, %s", result[0].Raw, result[1].Raw)
	}
}

func TestApplyRecordRename(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"src": "192.168.1.1", "dst": "10.0.0.1"}, ""),
	}

	op := &querylang.RenameOp{Renames: []querylang.RenameMapping{
		{Old: "src", New: "source"},
		{Old: "dst", New: "destination"},
	}}
	applyRecordRename(records, op)

	if records[0].Attrs["source"] != "192.168.1.1" {
		t.Errorf("source = %q, want '192.168.1.1'", records[0].Attrs["source"])
	}
	if records[0].Attrs["destination"] != "10.0.0.1" {
		t.Errorf("destination = %q, want '10.0.0.1'", records[0].Attrs["destination"])
	}
	if _, ok := records[0].Attrs["src"]; ok {
		t.Error("old key 'src' should be removed")
	}
}

func TestApplyRecordFieldsKeep(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"host": "a", "level": "info", "pid": "123"}, ""),
	}

	op := &querylang.FieldsOp{Names: []string{"host", "level"}, Drop: false}
	applyRecordFields(records, op)

	if _, ok := records[0].Attrs["host"]; !ok {
		t.Error("'host' should be kept")
	}
	if _, ok := records[0].Attrs["level"]; !ok {
		t.Error("'level' should be kept")
	}
	if _, ok := records[0].Attrs["pid"]; ok {
		t.Error("'pid' should be removed")
	}
}

func TestApplyRecordFieldsDrop(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"host": "a", "level": "info", "pid": "123"}, ""),
	}

	op := &querylang.FieldsOp{Names: []string{"pid"}, Drop: true}
	applyRecordFields(records, op)

	if _, ok := records[0].Attrs["host"]; !ok {
		t.Error("'host' should be kept")
	}
	if _, ok := records[0].Attrs["pid"]; ok {
		t.Error("'pid' should be removed")
	}
}

// --- Table operator tests ---

func TestApplyTableSort(t *testing.T) {
	table := &TableResult{
		Columns: []string{"status", "count"},
		Rows: [][]string{
			{"200", "50"},
			{"500", "5"},
			{"404", "20"},
		},
	}

	op := &querylang.SortOp{Fields: []querylang.SortField{{Name: "count", Desc: true}}}
	applyTableSort(table, op)

	if table.Rows[0][1] != "50" {
		t.Errorf("row 0 count = %q, want '50'", table.Rows[0][1])
	}
	if table.Rows[1][1] != "20" {
		t.Errorf("row 1 count = %q, want '20'", table.Rows[1][1])
	}
	if table.Rows[2][1] != "5" {
		t.Errorf("row 2 count = %q, want '5'", table.Rows[2][1])
	}
}

func TestApplyTableHead(t *testing.T) {
	table := &TableResult{
		Columns: []string{"status"},
		Rows:    [][]string{{"200"}, {"404"}, {"500"}},
	}

	applyTableHead(table, &querylang.HeadOp{N: 2})

	if len(table.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(table.Rows))
	}
}

func TestApplyTableRename(t *testing.T) {
	table := &TableResult{
		Columns: []string{"cnt", "status"},
		Rows:    [][]string{{"10", "200"}},
	}

	op := &querylang.RenameOp{Renames: []querylang.RenameMapping{
		{Old: "cnt", New: "count"},
	}}
	applyTableRename(table, op)

	if table.Columns[0] != "count" {
		t.Errorf("column 0 = %q, want 'count'", table.Columns[0])
	}
}

func TestApplyTableFieldsDrop(t *testing.T) {
	table := &TableResult{
		Columns: []string{"status", "count", "avg"},
		Rows:    [][]string{{"200", "50", "1.5"}, {"404", "20", "2.3"}},
	}

	op := &querylang.FieldsOp{Names: []string{"avg"}, Drop: true}
	applyTableFields(table, op)

	if len(table.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(table.Columns))
	}
	if table.Columns[0] != "status" || table.Columns[1] != "count" {
		t.Errorf("unexpected columns: %v", table.Columns)
	}
	if len(table.Rows[0]) != 2 {
		t.Fatalf("expected 2 values per row, got %d", len(table.Rows[0]))
	}
}

func TestApplyTableEval(t *testing.T) {
	table := &TableResult{
		Columns: []string{"count", "total"},
		Rows: [][]string{
			{"10", "100"},
			{"20", "400"},
		},
	}

	op := &querylang.EvalOp{
		Assignments: []querylang.EvalAssignment{
			{Field: "avg", Expr: &querylang.ArithExpr{
				Left:  &querylang.FieldRef{Name: "total"},
				Op:    querylang.ArithDiv,
				Right: &querylang.FieldRef{Name: "count"},
			}},
		},
	}

	eval := querylang.NewEvaluator()
	result, err := applyTableEval(table, op, eval)
	if err != nil {
		t.Fatalf("applyTableEval: %v", err)
	}
	if len(result.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(result.Columns))
	}
	if result.Columns[2] != "avg" {
		t.Errorf("column 2 = %q, want 'avg'", result.Columns[2])
	}
	if result.Rows[0][2] != "10" {
		t.Errorf("row 0 avg = %q, want '10'", result.Rows[0][2])
	}
	if result.Rows[1][2] != "20" {
		t.Errorf("row 1 avg = %q, want '20'", result.Rows[1][2])
	}
}

func TestMaterializeFieldsFromRaw(t *testing.T) {
	// Records with KV pairs in Raw but not in Attrs should become visible after materialization.
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"level": "info"}, `duration=250 method=GET status=200`),
		makeRec(baseTime, nil, `host=web-01 bytes=4096`),
	}

	materializeFields(records)

	// First record: extracted KV pairs should be in Attrs, existing Attrs preserved.
	if records[0].Attrs["level"] != "info" {
		t.Errorf("existing attr 'level' should be preserved, got %q", records[0].Attrs["level"])
	}
	if records[0].Attrs["duration"] != "250" {
		t.Errorf("extracted 'duration' = %q, want '250'", records[0].Attrs["duration"])
	}
	if records[0].Attrs["method"] == "" {
		t.Error("extracted 'method' should be present")
	}
	if records[0].Attrs["status"] != "200" {
		t.Errorf("extracted 'status' = %q, want '200'", records[0].Attrs["status"])
	}

	// Second record: nil Attrs should be created with extracted fields.
	if records[1].Attrs["host"] != "web-01" {
		t.Errorf("extracted 'host' = %q, want 'web-01'", records[1].Attrs["host"])
	}
	if records[1].Attrs["bytes"] != "4096" {
		t.Errorf("extracted 'bytes' = %q, want '4096'", records[1].Attrs["bytes"])
	}
}

func TestMaterializeFieldsAttrPrecedence(t *testing.T) {
	// Attrs should take precedence over extracted fields with the same name.
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"status": "override"}, `status=200`),
	}

	materializeFields(records)

	if records[0].Attrs["status"] != "override" {
		t.Errorf("attr should win over extracted: got %q, want 'override'", records[0].Attrs["status"])
	}
}

func TestMaterializeFieldsJSON(t *testing.T) {
	// JSON fields should be extracted from the raw message.
	records := []chunk.Record{
		makeRec(baseTime, nil, `{"method":"POST","status":201,"path":"/api/users"}`),
	}

	materializeFields(records)

	if records[0].Attrs["method"] == "" {
		t.Error("extracted JSON 'method' should be present")
	}
	if records[0].Attrs["status"] != "201" {
		t.Errorf("extracted JSON 'status' = %q, want '201'", records[0].Attrs["status"])
	}
}

func TestRecordsToTableWithExtractedFields(t *testing.T) {
	// recordsToTable should include extracted fields as columns, not just Attrs.
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"level": "info"}, `duration=250 method=GET`),
	}

	table := recordsToTable(records)

	// Check that extracted fields appear as columns.
	colSet := make(map[string]bool)
	for _, c := range table.Columns {
		colSet[c] = true
	}
	if !colSet["duration"] {
		t.Error("extracted field 'duration' should be a column")
	}
	if !colSet["method"] {
		t.Error("extracted field 'method' should be a column")
	}
	if !colSet["level"] {
		t.Error("attr 'level' should be a column")
	}
}

// --- Lookup operator tests ---

// staticTable is a test LookupTable with fixed responses.
type staticTable struct {
	data     map[string]map[string]string
	suffixes []string
}

func (s *staticTable) Parameters() []string { return []string{"value"} }

func (s *staticTable) LookupValues(_ context.Context, values map[string]string) map[string]string {
	return s.data[values["value"]]
}

func (s *staticTable) Suffixes() []string {
	return s.suffixes
}

func TestApplyRecordLookup(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"src_ip": "8.8.8.8"}, ""),
		makeRec(baseTime, chunk.Attributes{"src_ip": "1.2.3.4"}, ""),
		makeRec(baseTime, chunk.Attributes{"other": "field"}, ""), // no src_ip
	}

	table := &staticTable{
		data: map[string]map[string]string{
			"8.8.8.8": {"hostname": "dns.google"},
		},
		suffixes: []string{"hostname"},
	}

	resolve := func(name string) lookup.LookupTable {
		if name == "rdns" {
			return table
		}
		return nil
	}

	op := &querylang.LookupOp{Table: "rdns", Fields: []string{"src_ip"}}
	applyRecordLookup(context.Background(), records, op, resolve)

	if records[0].Attrs["src_ip_hostname"] != "dns.google" {
		t.Errorf("record 0 src_ip_hostname = %q, want 'dns.google'", records[0].Attrs["src_ip_hostname"])
	}
	if _, ok := records[1].Attrs["src_ip_hostname"]; ok {
		t.Errorf("record 1 should not have src_ip_hostname (lookup miss)")
	}
	if _, ok := records[2].Attrs["src_ip_hostname"]; ok {
		t.Errorf("record 2 should not have src_ip_hostname (no src_ip field)")
	}
}

func TestApplyRecordLookupNilResolver(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"src_ip": "8.8.8.8"}, ""),
	}

	op := &querylang.LookupOp{Table: "rdns", Fields: []string{"src_ip"}}
	applyRecordLookup(context.Background(), records, op, nil) // should not panic

	if _, ok := records[0].Attrs["src_ip_hostname"]; ok {
		t.Error("should not enrich with nil resolver")
	}
}

func TestApplyRecordLookupUnknownTable(t *testing.T) {
	records := []chunk.Record{
		makeRec(baseTime, chunk.Attributes{"src_ip": "8.8.8.8"}, ""),
	}

	resolve := func(name string) lookup.LookupTable { return nil }

	op := &querylang.LookupOp{Table: "nonexistent", Fields: []string{"src_ip"}}
	applyRecordLookup(context.Background(), records, op, resolve)

	if _, ok := records[0].Attrs["src_ip_hostname"]; ok {
		t.Error("should not enrich with unknown table")
	}
}

func TestApplyTableLookup(t *testing.T) {
	table := &TableResult{
		Columns: []string{"src_ip", "count"},
		Rows: [][]string{
			{"8.8.8.8", "100"},
			{"1.2.3.4", "50"},
			{"", "10"},
		},
	}

	lt := &staticTable{
		data: map[string]map[string]string{
			"8.8.8.8": {"hostname": "dns.google"},
		},
		suffixes: []string{"hostname"},
	}

	resolve := func(name string) lookup.LookupTable {
		if name == "rdns" {
			return lt
		}
		return nil
	}

	op := &querylang.LookupOp{Table: "rdns", Fields: []string{"src_ip"}}
	result := applyTableLookup(context.Background(), table, op, resolve)

	if len(result.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d: %v", len(result.Columns), result.Columns)
	}
	if result.Columns[2] != "src_ip_hostname" {
		t.Errorf("column 2 = %q, want 'src_ip_hostname'", result.Columns[2])
	}
	if result.Rows[0][2] != "dns.google" {
		t.Errorf("row 0 hostname = %q, want 'dns.google'", result.Rows[0][2])
	}
	if result.Rows[1][2] != "" {
		t.Errorf("row 1 hostname = %q, want '' (miss)", result.Rows[1][2])
	}
	if result.Rows[2][2] != "" {
		t.Errorf("row 2 hostname = %q, want '' (empty src_ip)", result.Rows[2][2])
	}
}

func TestApplyTableLookupMissingColumn(t *testing.T) {
	table := &TableResult{
		Columns: []string{"other_field", "count"},
		Rows:    [][]string{{"value", "10"}},
	}

	lt := &staticTable{
		data:     map[string]map[string]string{},
		suffixes: []string{"hostname"},
	}
	resolve := func(name string) lookup.LookupTable { return lt }

	op := &querylang.LookupOp{Table: "rdns", Fields: []string{"src_ip"}}
	result := applyTableLookup(context.Background(), table, op, resolve)

	// Should be unchanged — src_ip column doesn't exist.
	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns (unchanged), got %d", len(result.Columns))
	}
}

func TestApplyRecordDedup(t *testing.T) {
	idA := glid.MustParse("00000000-0000-0000-0000-00000000000a")
	idB := glid.MustParse("00000000-0000-0000-0000-00000000000b")
	t0 := baseTime
	t1 := baseTime.Add(time.Second)
	t2 := baseTime.Add(2 * time.Second)

	records := []chunk.Record{
		{IngestTS: t0, WriteTS: t0, EventID: chunk.EventID{IngesterID: idA, IngestTS: t0, IngestSeq: 0}, Raw: []byte("first")},
		{IngestTS: t0, WriteTS: t0.Add(100 * time.Millisecond), EventID: chunk.EventID{IngesterID: idA, IngestTS: t0, IngestSeq: 0}, Raw: []byte("dup")},
		{IngestTS: t1, WriteTS: t1, EventID: chunk.EventID{IngesterID: idB, IngestTS: t1, IngestSeq: 0}, Raw: []byte("second")},
		{IngestTS: t2, WriteTS: t2, EventID: chunk.EventID{IngesterID: idA, IngestTS: t2, IngestSeq: 1}, Raw: []byte("third")},
		{IngestTS: t2, WriteTS: t2.Add(50 * time.Millisecond), EventID: chunk.EventID{IngesterID: idA, IngestTS: t2, IngestSeq: 1}, Raw: []byte("dup2")},
	}

	result := applyRecordDedup(records, defaultDedupWindow)
	if len(result) != 3 {
		t.Fatalf("expected 3 records after dedup, got %d", len(result))
	}
	if string(result[0].Raw) != "first" {
		t.Errorf("record 0 = %q, want 'first'", result[0].Raw)
	}
	if string(result[1].Raw) != "second" {
		t.Errorf("record 1 = %q, want 'second'", result[1].Raw)
	}
	if string(result[2].Raw) != "third" {
		t.Errorf("record 2 = %q, want 'third'", result[2].Raw)
	}
}

func TestApplyRecordDedupNoDups(t *testing.T) {
	idA := glid.MustParse("00000000-0000-0000-0000-00000000000a")
	idB := glid.MustParse("00000000-0000-0000-0000-00000000000b")
	records := []chunk.Record{
		{IngestTS: baseTime, WriteTS: baseTime, EventID: chunk.EventID{IngesterID: idA, IngestTS: baseTime, IngestSeq: 0}, Raw: []byte("one")},
		{IngestTS: baseTime.Add(time.Second), WriteTS: baseTime.Add(time.Second), EventID: chunk.EventID{IngesterID: idB, IngestTS: baseTime.Add(time.Second), IngestSeq: 0}, Raw: []byte("two")},
	}

	result := applyRecordDedup(records, defaultDedupWindow)
	if len(result) != 2 {
		t.Fatalf("expected 2 records (no dups), got %d", len(result))
	}
}

func TestApplyRecordDedupEmpty(t *testing.T) {
	result := applyRecordDedup(nil, defaultDedupWindow)
	if len(result) != 0 {
		t.Fatalf("expected 0 records for nil input, got %d", len(result))
	}
}

func TestApplyRecordDedupSameTimeDiffIngester(t *testing.T) {
	idA := glid.MustParse("00000000-0000-0000-0000-00000000000a")
	idB := glid.MustParse("00000000-0000-0000-0000-00000000000b")
	records := []chunk.Record{
		{IngestTS: baseTime, WriteTS: baseTime, EventID: chunk.EventID{IngesterID: idA, IngestTS: baseTime, IngestSeq: 0}, Raw: []byte("one")},
		{IngestTS: baseTime, WriteTS: baseTime, EventID: chunk.EventID{IngesterID: idB, IngestTS: baseTime, IngestSeq: 0}, Raw: []byte("two")},
	}

	result := applyRecordDedup(records, defaultDedupWindow)
	if len(result) != 2 {
		t.Fatalf("expected 2 records (different ingester_id), got %d", len(result))
	}
}

func TestApplyRecordDedupWindowExpiry(t *testing.T) {
	idA := glid.MustParse("00000000-0000-0000-0000-00000000000a")
	t0 := baseTime
	// Same EventID, but WriteTS is more than 1s apart — should NOT be deduped.
	records := []chunk.Record{
		{IngestTS: t0, WriteTS: t0, EventID: chunk.EventID{IngesterID: idA, IngestTS: t0, IngestSeq: 0}, Raw: []byte("first")},
		{IngestTS: t0, WriteTS: t0.Add(2 * time.Second), EventID: chunk.EventID{IngesterID: idA, IngestTS: t0, IngestSeq: 0}, Raw: []byte("late-dup")},
	}

	result := applyRecordDedup(records, defaultDedupWindow)
	if len(result) != 2 {
		t.Fatalf("expected 2 records (window expired), got %d", len(result))
	}
}

func TestCompareSortValues(t *testing.T) {
	// Numeric comparison.
	if compareSortValues("5", "20") >= 0 {
		t.Error("5 should be less than 20")
	}
	if compareSortValues("20", "5") <= 0 {
		t.Error("20 should be greater than 5")
	}
	if compareSortValues("10", "10") != 0 {
		t.Error("10 should equal 10")
	}
	// String comparison.
	if compareSortValues("abc", "xyz") >= 0 {
		t.Error("abc should be less than xyz")
	}
}

// recordIter creates an iter.Seq2 from a slice of records (for testing applyRecordOps).
func recordIter(records []chunk.Record) func(func(chunk.Record, error) bool) {
	return func(yield func(chunk.Record, error) bool) {
		for _, r := range records {
			if !yield(r, nil) {
				return
			}
		}
	}
}

// makeTestRecords creates N records with "msg-0", "msg-1", etc. and optional attrs.
func makeTestRecords(n int, attrs map[string]string) []chunk.Record {
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	records := make([]chunk.Record, n)
	for i := range n {
		a := make(chunk.Attributes, len(attrs))
		for k, v := range attrs {
			a[k] = v
		}
		records[i] = chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			WriteTS:  t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "msg-%d", i),
			Attrs:    a,
		}
	}
	return records
}

func TestStreamingTailBasic(t *testing.T) {
	records := makeTestRecords(1000, nil)
	ops := []querylang.PipeOp{&querylang.TailOp{N: 10}}

	result, err := applyRecordOps(context.Background(), recordIter(records), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	if len(result) != 10 {
		t.Fatalf("expected 10 records, got %d", len(result))
	}
	// Should be the last 10 records.
	for i, r := range result {
		expected := string(records[990+i].Raw)
		if string(r.Raw) != expected {
			t.Errorf("record %d: got %q, want %q", i, string(r.Raw), expected)
		}
	}
}

func TestStreamingTailFewerRecordsThanN(t *testing.T) {
	records := makeTestRecords(5, nil)
	ops := []querylang.PipeOp{&querylang.TailOp{N: 100}}

	result, err := applyRecordOps(context.Background(), recordIter(records), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	if len(result) != 5 {
		t.Errorf("expected 5 records, got %d", len(result))
	}
}

func TestStreamingTailWithWhere(t *testing.T) {
	records := makeTestRecords(100, nil)
	// Set level=error on every 10th record.
	for i := range records {
		if i%10 == 0 {
			records[i].Attrs["level"] = "error"
		} else {
			records[i].Attrs["level"] = "info"
		}
	}
	ops := []querylang.PipeOp{
		&querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "level", Value: "error"}},
		&querylang.TailOp{N: 3},
	}

	result, err := applyRecordOps(context.Background(), recordIter(records), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	// 10 records match (0,10,20,...,90), tail 3 gives last 3: indices 70,80,90.
	if len(result) != 3 {
		t.Fatalf("expected 3 records, got %d", len(result))
	}
	if string(result[0].Raw) != string(records[70].Raw) {
		t.Errorf("first record: got %q, want %q", string(result[0].Raw), string(records[70].Raw))
	}
}

func TestStreamingSliceBasic(t *testing.T) {
	records := makeTestRecords(100, nil)
	ops := []querylang.PipeOp{&querylang.SliceOp{Start: 5, End: 10}}

	result, err := applyRecordOps(context.Background(), recordIter(records), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	if len(result) != 6 { // positions 5-10 inclusive
		t.Fatalf("expected 6 records, got %d", len(result))
	}
	// Record at position 5 (1-indexed) = index 4.
	if string(result[0].Raw) != string(records[4].Raw) {
		t.Errorf("first record: got %q, want %q", string(result[0].Raw), string(records[4].Raw))
	}
}

func TestStreamingSliceWithWhere(t *testing.T) {
	records := makeTestRecords(100, nil)
	for i := range records {
		if i%2 == 0 {
			records[i].Attrs["parity"] = "even"
		} else {
			records[i].Attrs["parity"] = "odd"
		}
	}
	ops := []querylang.PipeOp{
		&querylang.WhereOp{Expr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "parity", Value: "even"}},
		&querylang.SliceOp{Start: 2, End: 4},
	}

	result, err := applyRecordOps(context.Background(), recordIter(records), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	// 50 even records (0,2,4,...,98). Slice 2-4: indices 2,4,6 (survivors 2nd,3rd,4th).
	if len(result) != 3 {
		t.Fatalf("expected 3 records, got %d", len(result))
	}
	if string(result[0].Raw) != string(records[2].Raw) {
		t.Errorf("first record: got %q, want %q", string(result[0].Raw), string(records[2].Raw))
	}
}

func TestStreamingSliceEarlyExit(t *testing.T) {
	// Verify that slice stops iterating after collecting enough records.
	// Use a large dataset but slice only the first 3.
	records := makeTestRecords(10000, nil)
	iterCount := 0
	countingIter := func(yield func(chunk.Record, error) bool) {
		for _, r := range records {
			iterCount++
			if !yield(r, nil) {
				return
			}
		}
	}
	ops := []querylang.PipeOp{&querylang.SliceOp{Start: 1, End: 3}}

	result, err := applyRecordOps(context.Background(), countingIter, ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 records, got %d", len(result))
	}
	// The iterator should have stopped well before 10000.
	// It will iterate exactly 3 (sliceEnd) + 1 (the break check happens after incrementing).
	// Actually with range-over-func, break stops at the current iteration.
	if iterCount > 10 {
		t.Errorf("expected early exit, but iterated %d times", iterCount)
	}
}

func TestSortBeforeTailFallsBackToMaterialization(t *testing.T) {
	records := makeTestRecords(100, nil)
	ops := []querylang.PipeOp{
		&querylang.SortOp{Fields: []querylang.SortField{{Name: "raw"}}},
		&querylang.TailOp{N: 5},
	}

	// Should not panic or error — falls back to batch path.
	result, err := applyRecordOps(context.Background(), recordIter(records), ops, nil)
	if err != nil {
		t.Fatalf("applyRecordOps: %v", err)
	}
	if len(result) != 5 {
		t.Errorf("expected 5 records, got %d", len(result))
	}
}
