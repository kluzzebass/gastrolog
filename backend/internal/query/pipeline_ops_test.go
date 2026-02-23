package query

import (
	"testing"

	"gastrolog/internal/chunk"
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
