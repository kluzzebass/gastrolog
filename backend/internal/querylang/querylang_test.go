package querylang

import (
	"errors"
	"regexp"
	"testing"
)

func TestParseSingleToken(t *testing.T) {
	tests := []struct {
		input string
		want  *PredicateExpr
	}{
		{"error", &PredicateExpr{Kind: PredToken, Value: "error"}},
		{"warn", &PredicateExpr{Kind: PredToken, Value: "warn"}},
		{"ERROR", &PredicateExpr{Kind: PredToken, Value: "ERROR"}}, // case preserved
		{"foo123", &PredicateExpr{Kind: PredToken, Value: "foo123"}},
		{"my-token", &PredicateExpr{Kind: PredToken, Value: "my-token"}},
		{"my_token", &PredicateExpr{Kind: PredToken, Value: "my_token"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			pred, ok := expr.(*PredicateExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *PredicateExpr", tt.input, expr)
			}
			if pred.Kind != tt.want.Kind || pred.Value != tt.want.Value {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, pred, tt.want)
			}
		})
	}
}

func TestParseKeyValue(t *testing.T) {
	tests := []struct {
		input string
		want  *PredicateExpr
	}{
		{"level=error", &PredicateExpr{Kind: PredKV, Key: "level", Value: "error"}},
		{"host=server1", &PredicateExpr{Kind: PredKV, Key: "host", Value: "server1"}},
		{"level=*", &PredicateExpr{Kind: PredKeyExists, Key: "level"}},
		{"*=error", &PredicateExpr{Kind: PredValueExists, Value: "error"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			pred, ok := expr.(*PredicateExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *PredicateExpr", tt.input, expr)
			}
			if pred.Kind != tt.want.Kind || pred.Key != tt.want.Key || pred.Value != tt.want.Value {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, pred, tt.want)
			}
		})
	}
}

func TestParseQuotedStrings(t *testing.T) {
	tests := []struct {
		input string
		want  *PredicateExpr
	}{
		{`"disk full"`, &PredicateExpr{Kind: PredToken, Value: "disk full"}},
		{`'stack trace'`, &PredicateExpr{Kind: PredToken, Value: "stack trace"}},
		{`"hello world"`, &PredicateExpr{Kind: PredToken, Value: "hello world"}},
		{`""`, &PredicateExpr{Kind: PredToken, Value: ""}}, // empty string
		{`''`, &PredicateExpr{Kind: PredToken, Value: ""}}, // empty string
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			pred, ok := expr.(*PredicateExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *PredicateExpr", tt.input, expr)
			}
			if pred.Kind != tt.want.Kind || pred.Value != tt.want.Value {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, pred, tt.want)
			}
		})
	}
}

func TestParseQuotedKeyValue(t *testing.T) {
	tests := []struct {
		input string
		want  *PredicateExpr
	}{
		{`message="disk full"`, &PredicateExpr{Kind: PredKV, Key: "message", Value: "disk full"}},
		{`key='value with spaces'`, &PredicateExpr{Kind: PredKV, Key: "key", Value: "value with spaces"}},
		{`"key with space"=value`, &PredicateExpr{Kind: PredKV, Key: "key with space", Value: "value"}},
		{`"key"="value"`, &PredicateExpr{Kind: PredKV, Key: "key", Value: "value"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			pred, ok := expr.(*PredicateExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *PredicateExpr", tt.input, expr)
			}
			if pred.Kind != tt.want.Kind || pred.Key != tt.want.Key || pred.Value != tt.want.Value {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, pred, tt.want)
			}
		})
	}
}

func TestParseEscapeSequences(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"hello\"world"`, `hello"world`},
		{`'it\'s fine'`, "it's fine"},
		{`"line1\nline2"`, "line1\nline2"},
		{`"tab\there"`, "tab\there"},
		{`"back\\slash"`, `back\slash`},
		{`"carriage\rreturn"`, "carriage\rreturn"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			pred, ok := expr.(*PredicateExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *PredicateExpr", tt.input, expr)
			}
			if pred.Value != tt.want {
				t.Errorf("Parse(%q).Value = %q, want %q", tt.input, pred.Value, tt.want)
			}
		})
	}
}

func TestParseAnd(t *testing.T) {
	tests := []struct {
		name  string
		input string
		terms int
	}{
		{"explicit AND", "error AND warn", 2},
		{"implicit AND", "error warn", 2},
		{"mixed AND", "error AND warn debug", 3},
		{"three terms", "a b c", 3},
		{"explicit three", "a AND b AND c", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			and, ok := expr.(*AndExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *AndExpr", tt.input, expr)
			}
			if len(and.Terms) != tt.terms {
				t.Errorf("Parse(%q) has %d terms, want %d", tt.input, len(and.Terms), tt.terms)
			}
		})
	}
}

func TestParseOr(t *testing.T) {
	tests := []struct {
		name  string
		input string
		terms int
	}{
		{"two terms", "error OR warn", 2},
		{"three terms", "a OR b OR c", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			or, ok := expr.(*OrExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *OrExpr", tt.input, expr)
			}
			if len(or.Terms) != tt.terms {
				t.Errorf("Parse(%q) has %d terms, want %d", tt.input, len(or.Terms), tt.terms)
			}
		})
	}
}

func TestParseNot(t *testing.T) {
	input := "NOT error"
	expr, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", input, err)
	}
	not, ok := expr.(*NotExpr)
	if !ok {
		t.Fatalf("Parse(%q) = %T, want *NotExpr", input, expr)
	}
	pred, ok := not.Term.(*PredicateExpr)
	if !ok {
		t.Fatalf("Parse(%q).Term = %T, want *PredicateExpr", input, not.Term)
	}
	if pred.Value != "error" {
		t.Errorf("Parse(%q).Term.Value = %q, want %q", input, pred.Value, "error")
	}
}

func TestParsePrecedence(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		checkFn  func(Expr) bool
		describe string
	}{
		{
			name:  "AND binds tighter than OR",
			input: "a OR b AND c",
			checkFn: func(e Expr) bool {
				// Should be OR(a, AND(b, c))
				or, ok := e.(*OrExpr)
				if !ok || len(or.Terms) != 2 {
					return false
				}
				_, firstIsPred := or.Terms[0].(*PredicateExpr)
				_, secondIsAnd := or.Terms[1].(*AndExpr)
				return firstIsPred && secondIsAnd
			},
			describe: "OR(a, AND(b, c))",
		},
		{
			name:  "AND binds tighter than OR (reversed)",
			input: "a AND b OR c",
			checkFn: func(e Expr) bool {
				// Should be OR(AND(a, b), c)
				or, ok := e.(*OrExpr)
				if !ok || len(or.Terms) != 2 {
					return false
				}
				_, firstIsAnd := or.Terms[0].(*AndExpr)
				_, secondIsPred := or.Terms[1].(*PredicateExpr)
				return firstIsAnd && secondIsPred
			},
			describe: "OR(AND(a, b), c)",
		},
		{
			name:  "NOT binds tighter than AND",
			input: "NOT a AND b",
			checkFn: func(e Expr) bool {
				// Should be AND(NOT(a), b)
				and, ok := e.(*AndExpr)
				if !ok || len(and.Terms) != 2 {
					return false
				}
				_, firstIsNot := and.Terms[0].(*NotExpr)
				_, secondIsPred := and.Terms[1].(*PredicateExpr)
				return firstIsNot && secondIsPred
			},
			describe: "AND(NOT(a), b)",
		},
		{
			name:  "parentheses override precedence",
			input: "a AND (b OR c)",
			checkFn: func(e Expr) bool {
				// Should be AND(a, OR(b, c))
				and, ok := e.(*AndExpr)
				if !ok || len(and.Terms) != 2 {
					return false
				}
				_, firstIsPred := and.Terms[0].(*PredicateExpr)
				_, secondIsOr := and.Terms[1].(*OrExpr)
				return firstIsPred && secondIsOr
			},
			describe: "AND(a, OR(b, c))",
		},
		{
			name:  "nested NOT",
			input: "NOT NOT a",
			checkFn: func(e Expr) bool {
				// Should be NOT(NOT(a))
				not1, ok := e.(*NotExpr)
				if !ok {
					return false
				}
				not2, ok := not1.Term.(*NotExpr)
				if !ok {
					return false
				}
				_, isPred := not2.Term.(*PredicateExpr)
				return isPred
			},
			describe: "NOT(NOT(a))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			if !tt.checkFn(expr) {
				t.Errorf("Parse(%q) structure incorrect, want %s, got %s", tt.input, tt.describe, expr.String())
			}
		})
	}
}

func TestParseComplexExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"quoted with AND", `"quoted" AND bareword`},
		{"complex OR AND", `(error OR warn) AND NOT debug`},
		{"nested parens", `((a OR b) AND (c OR d))`},
		{"kv with boolean", `level=error OR level=warn`},
		{"mixed predicates", `error AND level=warn OR host=*`},
		{"quoted in kv with boolean", `message="disk full" OR message="out of memory"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			if expr == nil {
				t.Fatalf("Parse(%q) returned nil", tt.input)
			}
			// Just verify it parses without error
			t.Logf("Parse(%q) = %s", tt.input, expr.String())
		})
	}
}

func TestParseCaseInsensitiveKeywords(t *testing.T) {
	tests := []string{
		"a or b",
		"a OR b",
		"a Or b",
		"a oR b",
		"a and b",
		"a AND b",
		"a And b",
		"not a",
		"NOT a",
		"Not a",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", input, err)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"empty query", "", ErrEmptyQuery},
		{"unmatched open paren", "(error", ErrUnmatchedParen},
		{"unmatched close paren", "error)", ErrUnexpectedToken},
		{"OR at start", "OR error", ErrUnexpectedToken},
		{"AND at start", "AND error", ErrUnexpectedToken},
		{"trailing OR", "error OR", ErrUnexpectedEOF},
		{"trailing AND", "error AND", ErrUnexpectedEOF},
		{"trailing NOT", "NOT", ErrUnexpectedEOF},
		{"double OR", "error OR OR warn", ErrUnexpectedToken},
		{"empty parens", "()", ErrEmptyQuery},
		{"star alone", "*", ErrUnexpectedToken},
		{"star star", "*=*", ErrUnexpectedToken},
		{"equals alone", "=value", ErrUnexpectedToken},
		{"unterminated string", `"unterminated`, ErrUnterminatedString},
		{"unterminated single", `'also unterminated`, ErrUnterminatedString},
		{"invalid escape", `"foo\x"`, ErrInvalidEscape},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err == nil {
				t.Fatalf("Parse(%q) expected error, got nil", tt.input)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Parse(%q) error = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestParseWhitespace(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"  error  "},
		{"\terror\t"},
		{"\nerror\n"},
		{"error  AND  warn"},
		{"  (  error  OR  warn  )  "},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
		})
	}
}

func TestLexer(t *testing.T) {
	input := `error OR (level="disk full" AND NOT debug)`
	lex := NewLexer(input)

	expected := []struct {
		kind TokenKind
		lit  string
	}{
		{TokWord, "error"},
		{TokOr, "OR"},
		{TokLParen, "("},
		{TokWord, "level"},
		{TokEq, "="},
		{TokWord, "disk full"},
		{TokAnd, "AND"},
		{TokNot, "NOT"},
		{TokWord, "debug"},
		{TokRParen, ")"},
		{TokEOF, ""},
	}

	for i, want := range expected {
		tok, err := lex.Next()
		if err != nil {
			t.Fatalf("token %d: unexpected error: %v", i, err)
		}
		if tok.Kind != want.kind {
			t.Errorf("token %d: Kind = %v, want %v", i, tok.Kind, want.kind)
		}
		if tok.Kind != TokEOF && tok.Lit != want.lit {
			t.Errorf("token %d: Lit = %q, want %q", i, tok.Lit, want.lit)
		}
	}
}

func TestExprString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"error", "token(error)"},
		{"level=error", "level=error"},
		{"level=*", "level=*"},
		{"*=error", "*=error"},
		{"NOT error", "NOT token(error)"},
		{"a AND b", "(token(a) AND token(b))"},
		{"a OR b", "(token(a) OR token(b))"},
		{`/error\d+/`, `regex(/error\d+/)`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			got := expr.String()
			if got != tt.want {
				t.Errorf("Parse(%q).String() = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseRegex(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		pattern string
	}{
		{"basic regex", `/error\d+/`, `error\d+`},
		{"regex with dot-star", `/failed.*connection/`, `failed.*connection`},
		{"regex IP pattern", `/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/`, `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}`},
		{"regex with escaped slash", `/path\/to\/file/`, `path/to/file`},
		{"empty regex", `//`, ``},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			pred, ok := expr.(*PredicateExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *PredicateExpr", tt.input, expr)
			}
			if pred.Kind != PredRegex {
				t.Errorf("Parse(%q).Kind = %v, want PredRegex", tt.input, pred.Kind)
			}
			if pred.Value != tt.pattern {
				t.Errorf("Parse(%q).Value = %q, want %q", tt.input, pred.Value, tt.pattern)
			}
			if pred.Pattern == nil {
				t.Fatalf("Parse(%q).Pattern is nil", tt.input)
			}
			// Verify case-insensitive flag is applied.
			wantRe := regexp.MustCompile("(?i)" + tt.pattern)
			if pred.Pattern.String() != wantRe.String() {
				t.Errorf("Parse(%q).Pattern = %q, want %q", tt.input, pred.Pattern.String(), wantRe.String())
			}
		})
	}
}

func TestParseRegexInBooleanExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"regex AND token", `/timeout/ AND level=error`},
		{"token AND regex", `level=error AND /timeout/`},
		{"NOT regex", `NOT /debug/`},
		{"regex OR regex", `/error/ OR /warn/`},
		{"implicit AND with regex", `error /timeout/`},
		{"regex in parens", `(/error/ OR /warn/) AND level=error`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			if expr == nil {
				t.Fatalf("Parse(%q) returned nil", tt.input)
			}
			t.Logf("Parse(%q) = %s", tt.input, expr.String())
		})
	}
}

func TestParseRegexErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"unterminated regex", `/pattern`, ErrUnterminatedRegex},
		{"invalid regex pattern", `/[invalid/`, ErrInvalidRegex},
		{"invalid regex unclosed group", `/(unclosed/`, ErrInvalidRegex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err == nil {
				t.Fatalf("Parse(%q) expected error, got nil", tt.input)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Parse(%q) error = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestLexerRegex(t *testing.T) {
	input := `/error\d+/ AND level=warn`
	lex := NewLexer(input)

	expected := []struct {
		kind TokenKind
		lit  string
	}{
		{TokRegex, `error\d+`},
		{TokAnd, "AND"},
		{TokWord, "level"},
		{TokEq, "="},
		{TokWord, "warn"},
		{TokEOF, ""},
	}

	for i, want := range expected {
		tok, err := lex.Next()
		if err != nil {
			t.Fatalf("token %d: unexpected error: %v", i, err)
		}
		if tok.Kind != want.kind {
			t.Errorf("token %d: Kind = %v, want %v", i, tok.Kind, want.kind)
		}
		if tok.Kind != TokEOF && tok.Lit != want.lit {
			t.Errorf("token %d: Lit = %q, want %q", i, tok.Lit, want.lit)
		}
	}
}

func TestSlashNoLongerInBareword(t *testing.T) {
	// Verify that '/' terminates barewords.
	// "path/to" should NOT parse as a single token.
	lex := NewLexer("path/to/file")
	tok, err := lex.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.Kind != TokWord || tok.Lit != "path" {
		t.Errorf("first token = (%v, %q), want (WORD, \"path\")", tok.Kind, tok.Lit)
	}
	// Next should be a regex token /to/
	tok, err = lex.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.Kind != TokRegex || tok.Lit != "to" {
		t.Errorf("second token = (%v, %q), want (REGEX, \"to\")", tok.Kind, tok.Lit)
	}
}
