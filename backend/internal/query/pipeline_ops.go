package query

import (
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
)

// applyRecordOps collects all records from the iterator and applies the given
// pipeline operators in order. Each operator transforms the record slice.
func applyRecordOps(it iter.Seq2[chunk.Record, error], ops []querylang.PipeOp) ([]chunk.Record, error) {
	var records []chunk.Record
	for rec, err := range it {
		if err != nil {
			return nil, err
		}
		records = append(records, rec.Copy())
	}

	// Materialize extracted fields (KV, JSON, logfmt) into Attrs so all
	// operators see the full field set, not just explicit attributes.
	materializeFields(records)

	eval := querylang.NewEvaluator()

	for _, op := range ops {
		var err error
		switch o := op.(type) {
		case *querylang.WhereOp:
			records = applyRecordWhere(records, o)
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
		default:
			return nil, fmt.Errorf("unsupported pre-stats operator: %T", op)
		}
		if err != nil {
			return nil, err
		}
	}
	return records, nil
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
			if v, ok := records[i].Attrs[m.Old]; ok {
				if records[i].Attrs == nil {
					records[i].Attrs = make(chunk.Attributes)
				}
				records[i].Attrs[m.New] = v
				delete(records[i].Attrs, m.Old)
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
					delete(records[i].Attrs, k)
				}
			}
		} else {
			for k := range records[i].Attrs {
				if !nameSet[k] {
					delete(records[i].Attrs, k)
				}
			}
		}
	}
}

// --- Table operators (post-stats) ---

// applyTableOps applies pipeline operators to a table result (post-stats).
func applyTableOps(table *TableResult, ops []querylang.PipeOp) (*TableResult, error) {
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
			if k == "_raw" {
				continue
			}
			if _, exists := records[i].Attrs[k]; !exists {
				records[i].Attrs[k] = v
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
