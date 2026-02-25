package querylang

import (
	"strings"
	"testing"
)

func spanTexts(spans []Span) string {
	var sb strings.Builder
	for _, s := range spans {
		sb.WriteString(s.Text)
	}
	return sb.String()
}

func spanRoles(spans []Span) []string {
	roles := make([]string, len(spans))
	for i, s := range spans {
		roles[i] = string(s.Role)
	}
	return roles
}

func TestHighlight_Roundtrip(t *testing.T) {
	inputs := []string{
		"",
		"error",
		"level=error",
		"last=5m",
		`"hello world"`,
		"error | stats count by level",
		"(error OR warn) AND NOT debug",
		"level>=400 level!=info",
		"/error.*timeout/",
		"error*",
		"*timeout",
		"hello # comment",
		"hello # comment\nworld",
		"start=2024-01-01 level=error | where level!=debug | stats count by level",
		"level=",
	}

	for _, input := range inputs {
		spans, _ := Highlight(input, -1)
		got := spanTexts(spans)
		if got != input {
			t.Errorf("roundtrip failed for %q:\n  got:  %q\n  want: %q", input, got, input)
		}
	}
}

func TestHighlight_LevelError(t *testing.T) {
	spans, hasPipeline := Highlight("level=error", -1)
	if hasPipeline {
		t.Error("expected hasPipeline=false")
	}
	roles := spanRoles(spans)
	expected := []string{"key", "eq", "value"}
	if !slicesEqual(roles, expected) {
		t.Errorf("level=error roles: got %v, want %v", roles, expected)
	}
}

func TestHighlight_DirectiveKey(t *testing.T) {
	spans, _ := Highlight("last=5m", -1)
	roles := spanRoles(spans)
	expected := []string{"directive-key", "eq", "value"}
	if !slicesEqual(roles, expected) {
		t.Errorf("last=5m roles: got %v, want %v", roles, expected)
	}
}

func TestHighlight_Pipeline(t *testing.T) {
	spans, hasPipeline := Highlight("error | stats count by level", -1)
	if !hasPipeline {
		t.Error("expected hasPipeline=true")
	}

	// Expected: token ws pipe ws pipe-keyword ws function ws pipe-keyword ws token
	roles := spanRoles(spans)
	expected := []string{
		"token", "whitespace", "pipe", "whitespace",
		"pipe-keyword", "whitespace", "function", "whitespace",
		"pipe-keyword", "whitespace", "token",
	}
	if !slicesEqual(roles, expected) {
		t.Errorf("pipeline roles:\n  got:  %v\n  want: %v", roles, expected)
	}
}

func TestHighlight_QuotedString(t *testing.T) {
	spans, _ := Highlight(`"hello world"`, -1)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d: %v", len(spans), spans)
	}
	if spans[0].Role != RoleQuoted {
		t.Errorf("expected quoted role, got %s", spans[0].Role)
	}
	if spans[0].Text != `"hello world"` {
		t.Errorf("expected text with quotes, got %q", spans[0].Text)
	}
}

func TestHighlight_Comment(t *testing.T) {
	spans, _ := Highlight("hello # comment", -1)
	roles := spanRoles(spans)
	expected := []string{"token", "whitespace", "comment"}
	if !slicesEqual(roles, expected) {
		t.Errorf("comment roles: got %v, want %v", roles, expected)
	}
}

func TestHighlight_ErrorMarking(t *testing.T) {
	spans, _ := Highlight("level=", 6)
	// "level" is at 0..5, "=" is at 5..6, error offset is 6 (past the =).
	// There are only 2 spans: "level" and "=".
	// Since errorOffset=6 equals total length, nothing gets marked as error.
	// Actually let's check what we get.
	got := spanTexts(spans)
	if got != "level=" {
		t.Errorf("error roundtrip: got %q, want %q", got, "level=")
	}
}

func TestHighlight_ErrorMarkingMidExpression(t *testing.T) {
	input := "level=error AND"
	// Simulate error at offset 12 (at "AND").
	spans, _ := Highlight(input, 12)
	got := spanTexts(spans)
	if got != input {
		t.Errorf("error roundtrip: got %q, want %q", got, input)
	}

	// The "AND" token should be marked as error.
	for _, sp := range spans {
		if sp.Text == "AND" {
			if sp.Role != RoleError {
				t.Errorf("expected AND to be error role, got %s", sp.Role)
			}
		}
	}
}

func TestHighlight_Operators(t *testing.T) {
	spans, _ := Highlight("error OR warn AND NOT debug", -1)
	roles := spanRoles(spans)
	expected := []string{
		"token", "whitespace", "operator", "whitespace",
		"token", "whitespace", "operator", "whitespace",
		"operator", "whitespace", "token",
	}
	if !slicesEqual(roles, expected) {
		t.Errorf("operator roles: got %v, want %v", roles, expected)
	}
}

func TestHighlight_CompareOps(t *testing.T) {
	spans, _ := Highlight("level!=info", -1)
	roles := spanRoles(spans)
	expected := []string{"key", "compare-op", "value"}
	if !slicesEqual(roles, expected) {
		t.Errorf("compare roles: got %v, want %v", roles, expected)
	}
}

func TestHighlight_Regex(t *testing.T) {
	spans, _ := Highlight("/error.*/", -1)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Role != RoleRegex {
		t.Errorf("expected regex role, got %s", spans[0].Role)
	}
}

func TestHighlight_Glob(t *testing.T) {
	spans, _ := Highlight("error*", -1)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Role != RoleGlob {
		t.Errorf("expected glob role, got %s", spans[0].Role)
	}
}

func TestHighlight_ScalarFunctionInFilter(t *testing.T) {
	spans, _ := Highlight("len(message)>100", -1)
	roles := spanRoles(spans)
	// len ( message ) > 100
	expected := []string{"function", "paren", "token", "paren", "compare-op", "token"}
	if !slicesEqual(roles, expected) {
		t.Errorf("scalar func roles: got %v, want %v", roles, expected)
	}
}

func TestHighlight_WhereBody(t *testing.T) {
	spans, _ := Highlight("error | where level=error", -1)
	roles := spanRoles(spans)
	// error ws | ws where ws level = error
	expected := []string{
		"token", "whitespace", "pipe", "whitespace",
		"pipe-keyword", "whitespace", "key", "eq", "value",
	}
	if !slicesEqual(roles, expected) {
		t.Errorf("where body roles:\n  got:  %v\n  want: %v", roles, expected)
	}
}

func TestHighlight_Parens(t *testing.T) {
	spans, _ := Highlight("(error)", -1)
	roles := spanRoles(spans)
	expected := []string{"paren", "token", "paren"}
	if !slicesEqual(roles, expected) {
		t.Errorf("paren roles: got %v, want %v", roles, expected)
	}
}

func TestHighlight_Star(t *testing.T) {
	spans, _ := Highlight("*", -1)
	if len(spans) != 1 || spans[0].Role != RoleStar {
		t.Errorf("star: got %v", spans)
	}
}

func TestHighlight_Empty(t *testing.T) {
	spans, hasPipeline := Highlight("", -1)
	if len(spans) != 0 {
		t.Errorf("expected no spans for empty input, got %d", len(spans))
	}
	if hasPipeline {
		t.Error("expected hasPipeline=false for empty input")
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
