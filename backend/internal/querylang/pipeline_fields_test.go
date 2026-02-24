package querylang

import (
	"slices"
	"testing"
)

func TestFieldsAtCursor_FilterSegment(t *testing.T) {
	base := []string{"level", "message", "host"}
	fields, completions := FieldsAtCursor("error timeout", 5, base)
	if len(completions) != 0 {
		t.Errorf("expected no completions in filter, got %v", completions)
	}
	if !slices.Contains(fields, "level") || !slices.Contains(fields, "message") || !slices.Contains(fields, "host") {
		t.Errorf("expected base fields, got %v", fields)
	}
}

func TestFieldsAtCursor_AfterPipe(t *testing.T) {
	// Cursor after "| " — in the keyword position.
	base := []string{"level", "message"}
	fields, _ := FieldsAtCursor("error | ", 8, base)
	if !slices.Contains(fields, "level") {
		t.Errorf("expected base fields after pipe, got %v", fields)
	}
}

func TestFieldsAtCursor_EvalAddsFields(t *testing.T) {
	// "error | eval x=1 | "
	//                      ^ cursor at end
	base := []string{"level", "message"}
	fields, _ := FieldsAtCursor("error | eval x=1 | ", 19, base)
	if !slices.Contains(fields, "x") {
		t.Errorf("expected 'x' from eval, got %v", fields)
	}
	if !slices.Contains(fields, "level") {
		t.Errorf("expected base fields preserved, got %v", fields)
	}
}

func TestFieldsAtCursor_RenameTransforms(t *testing.T) {
	// "error | rename level as severity | "
	base := []string{"level", "message"}
	expr := "error | rename level as severity | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if slices.Contains(fields, "level") {
		t.Errorf("'level' should be renamed away, got %v", fields)
	}
	if !slices.Contains(fields, "severity") {
		t.Errorf("expected 'severity' after rename, got %v", fields)
	}
	if !slices.Contains(fields, "message") {
		t.Errorf("expected 'message' preserved, got %v", fields)
	}
}

func TestFieldsAtCursor_FieldsKeep(t *testing.T) {
	// "error | fields level, message | "
	base := []string{"level", "message", "host"}
	expr := "error | fields level, message | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if slices.Contains(fields, "host") {
		t.Errorf("'host' should be dropped by fields keep, got %v", fields)
	}
	if !slices.Contains(fields, "level") {
		t.Errorf("expected 'level', got %v", fields)
	}
}

func TestFieldsAtCursor_FieldsDrop(t *testing.T) {
	// "error | fields - host | "
	base := []string{"level", "message", "host"}
	expr := "error | fields - host | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if slices.Contains(fields, "host") {
		t.Errorf("'host' should be dropped, got %v", fields)
	}
	if !slices.Contains(fields, "level") || !slices.Contains(fields, "message") {
		t.Errorf("expected remaining fields preserved, got %v", fields)
	}
}

func TestFieldsAtCursor_StatsReplacesSchema(t *testing.T) {
	// "error | stats count by level | "
	base := []string{"level", "message", "host"}
	expr := "error | stats count by level | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if slices.Contains(fields, "message") || slices.Contains(fields, "host") {
		t.Errorf("stats should replace schema, got %v", fields)
	}
	if !slices.Contains(fields, "count") {
		t.Errorf("expected 'count' from stats, got %v", fields)
	}
	if !slices.Contains(fields, "level") {
		t.Errorf("expected 'level' from stats group, got %v", fields)
	}
}

func TestFieldsAtCursor_StatsWithAlias(t *testing.T) {
	// "error | stats count as n, avg(latency) as avg_lat by level | "
	base := []string{"level", "latency", "host"}
	expr := "error | stats count as n, avg(latency) as avg_lat by level | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if !slices.Contains(fields, "n") {
		t.Errorf("expected 'n' alias, got %v", fields)
	}
	if !slices.Contains(fields, "avg_lat") {
		t.Errorf("expected 'avg_lat' alias, got %v", fields)
	}
	if !slices.Contains(fields, "level") {
		t.Errorf("expected 'level' group field, got %v", fields)
	}
}

func TestFieldsAtCursor_ChainedOperators(t *testing.T) {
	// "error | eval x=1 | stats count by x | "
	base := []string{"level"}
	expr := "error | eval x=1 | stats count by x | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if !slices.Contains(fields, "count") {
		t.Errorf("expected 'count', got %v", fields)
	}
	if !slices.Contains(fields, "x") {
		t.Errorf("expected 'x' from group, got %v", fields)
	}
	if slices.Contains(fields, "level") {
		t.Errorf("'level' should be replaced by stats, got %v", fields)
	}
}

func TestFieldsAtCursor_StatsCompletions(t *testing.T) {
	// Cursor inside stats body.
	base := []string{"level"}
	expr := "error | stats count "
	_, completions := FieldsAtCursor(expr, len(expr), base)
	if !slices.Contains(completions, "by") {
		t.Errorf("expected 'by' completion for stats, got %v", completions)
	}
	if !slices.Contains(completions, "as") {
		t.Errorf("expected 'as' completion for stats, got %v", completions)
	}
}

func TestFieldsAtCursor_RenameCompletions(t *testing.T) {
	base := []string{"level"}
	expr := "error | rename level "
	_, completions := FieldsAtCursor(expr, len(expr), base)
	if !slices.Contains(completions, "as") {
		t.Errorf("expected 'as' completion for rename, got %v", completions)
	}
}

func TestFieldsAtCursor_PassthroughOps(t *testing.T) {
	// where, sort, head, tail, slice, raw don't change fields.
	base := []string{"level", "message"}
	for _, op := range []string{"where level=error", "sort level", "head 10", "tail 5", "slice 1 10", "raw"} {
		expr := "error | " + op + " | "
		fields, _ := FieldsAtCursor(expr, len(expr), base)
		if !slices.Contains(fields, "level") || !slices.Contains(fields, "message") {
			t.Errorf("passthrough op %q should preserve fields, got %v", op, fields)
		}
	}
}

func TestFieldsAtCursor_WithDirectives(t *testing.T) {
	// Directives should be stripped before processing.
	base := []string{"level", "message"}
	expr := "last=5m error | eval x=1 | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if !slices.Contains(fields, "x") {
		t.Errorf("expected 'x' from eval with directive, got %v", fields)
	}
}

func TestFieldsAtCursor_CursorInsideOperator(t *testing.T) {
	// Cursor is inside the stats body — should return base fields (not stats output).
	base := []string{"level", "message", "host"}
	expr := "error | stats count by "
	// Cursor at the end — in the stats body, we return fields before stats.
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	// When cursor is inside the first pipe operator, we return base fields.
	if !slices.Contains(fields, "level") {
		t.Errorf("expected base fields when inside stats, got %v", fields)
	}
}

func TestFieldsAtCursor_EmptyExpression(t *testing.T) {
	base := []string{"level"}
	fields, completions := FieldsAtCursor("", 0, base)
	if !slices.Contains(fields, "level") {
		t.Errorf("expected base fields for empty expr, got %v", fields)
	}
	if len(completions) != 0 {
		t.Errorf("expected no completions, got %v", completions)
	}
}

func TestFieldsAtCursor_TimechartOutput(t *testing.T) {
	base := []string{"level", "message"}
	expr := "error | timechart 50 by level | "
	fields, _ := FieldsAtCursor(expr, len(expr), base)
	if !slices.Contains(fields, "_time") {
		t.Errorf("expected '_time' from timechart, got %v", fields)
	}
	if !slices.Contains(fields, "count") {
		t.Errorf("expected 'count' from timechart, got %v", fields)
	}
}
