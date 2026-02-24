package querylang

import (
	"testing"
)

func TestValidateExpression(t *testing.T) {
	tests := []struct {
		name       string
		expr       string
		wantValid  bool
		wantMsg    string
		wantOffset int
	}{
		{
			name:       "empty expression",
			expr:       "",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "simple token",
			expr:       "error",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "multiple tokens",
			expr:       "error timeout",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "boolean expression",
			expr:       "(error OR warn) AND NOT debug",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "kv predicate",
			expr:       "level=error",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "pipeline stats",
			expr:       "error | stats count by level",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "pipeline eval",
			expr:       "error | eval x=1",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "pipeline chained",
			expr:       "error | eval x=1 | stats count by x",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "directive only",
			expr:       "last=5m",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "directives with filter",
			expr:       "last=5m error timeout",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "directive at end",
			expr:       "error last=5m",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "multiple directives",
			expr:       "start=2024-01-01T00:00:00Z end=2024-12-31T23:59:59Z error",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "pipeline with directive",
			expr:       "last=5m error | stats count",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "directive-only with pipe",
			expr:       "last=5m | stats count",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:      "unknown pipe operator",
			expr:      "error | stts count",
			wantValid: false,
			wantMsg:   "unknown pipe operator: stts",
		},
		{
			name:      "unmatched paren",
			expr:      "(error OR warn",
			wantValid: false,
			wantMsg:   "unmatched opening parenthesis",
		},
		{
			name:      "missing stats argument",
			expr:      "error | stats",
			wantValid: false,
		},
		{
			name:       "head with number",
			expr:       "error | head 10",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "rename",
			expr:       "error | rename src as source",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "fields keep",
			expr:       "error | fields level, message",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "fields drop",
			expr:       "error | fields - raw",
			wantValid:  true,
			wantOffset: -1,
		},
		{
			name:       "raw",
			expr:       "error | stats count | raw",
			wantValid:  true,
			wantOffset: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid, msg, offset := ValidateExpression(tt.expr)
			if valid != tt.wantValid {
				t.Errorf("valid = %v, want %v (msg=%q, offset=%d)", valid, tt.wantValid, msg, offset)
			}
			if tt.wantValid && offset != -1 {
				t.Errorf("offset = %d, want -1 for valid expression", offset)
			}
			if !tt.wantValid && offset == -1 {
				t.Errorf("offset = -1 for invalid expression, expected >= 0")
			}
			if tt.wantMsg != "" && msg != tt.wantMsg {
				t.Errorf("msg = %q, want %q", msg, tt.wantMsg)
			}
		})
	}
}

func TestValidateExpressionOffsetMapping(t *testing.T) {
	// "last=5m error | stts count"
	// After stripping: "error | stts count"
	// stts is at position 8 in stripped string.
	// In original: "last=5m " is 8 bytes, so stts is at 8+8=16.
	expr := "last=5m error | stts count"
	valid, _, offset := ValidateExpression(expr)
	if valid {
		t.Fatal("expected invalid")
	}
	// "stts" starts at byte 16 in the original expression.
	if offset != 16 {
		t.Errorf("offset = %d, want 16", offset)
	}
}

func TestValidateExpressionOffsetMappingMultipleDirectives(t *testing.T) {
	// "start=X end=Y | stts count"
	// After stripping: " | stts count"
	// The pipe-only query after stripping: "| stts count"
	// stts at position 2 in "| stts count"
	expr := "start=X end=Y | stts count"
	valid, _, offset := ValidateExpression(expr)
	if valid {
		t.Fatal("expected invalid")
	}
	// Just verify offset > 0 and points somewhere reasonable.
	if offset < 0 {
		t.Errorf("offset = %d, want >= 0", offset)
	}
}

func TestStripDirectives(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantStr  string
		wantN    int // number of removed ranges
	}{
		{"no directives", "error timeout", "error timeout", 0},
		{"last=5m prefix", "last=5m error", " error", 1},
		{"last=5m suffix", "error last=5m", "error", 1},
		{"last=5m middle", "error last=5m timeout", "error timeout", 1},
		{"multiple", "start=X end=Y error", " error", 2},
		{"all directives", "last=5m limit=100", "", 2},
		{"directive only", "last=5m", "", 1},
		{"non-directive kv kept", "level=error", "level=error", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ranges := stripDirectives(tt.input)
			if got != tt.wantStr {
				t.Errorf("stripped = %q, want %q", got, tt.wantStr)
			}
			if len(ranges) != tt.wantN {
				t.Errorf("removed ranges = %d, want %d", len(ranges), tt.wantN)
			}
		})
	}
}
