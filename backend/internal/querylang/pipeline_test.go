package querylang

import (
	"errors"
	"testing"
)

// --- Lexer tests for new tokens ---

func TestLexerPipeToken(t *testing.T) {
	lex := NewLexer("error | stats count")
	expected := []struct {
		kind TokenKind
		lit  string
	}{
		{TokWord, "error"},
		{TokPipe, "|"},
		{TokWord, "stats"},
		{TokWord, "count"},
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

func TestLexerCommaToken(t *testing.T) {
	lex := NewLexer("count, avg(duration)")
	// In default mode, lexer should produce comma tokens since ',' is not a bareword char.
	expected := []struct {
		kind TokenKind
		lit  string
	}{
		{TokWord, "count"},
		{TokComma, ","},
		{TokWord, "avg"},
		{TokLParen, "("},
		{TokWord, "duration"},
		{TokRParen, ")"},
		{TokEOF, ""},
	}

	for i, want := range expected {
		tok, err := lex.Next()
		if err != nil {
			t.Fatalf("token %d: unexpected error: %v", i, err)
		}
		if tok.Kind != want.kind {
			t.Errorf("token %d: Kind = %v, want %v (lit=%q)", i, tok.Kind, want.kind, tok.Lit)
		}
		if tok.Kind != TokEOF && tok.Lit != want.lit {
			t.Errorf("token %d: Lit = %q, want %q", i, tok.Lit, want.lit)
		}
	}
}

func TestLexerPipeMode(t *testing.T) {
	// In pipe mode, '/' should be TokSlash and '-' should be TokMinus.
	lex := NewLexer("a / b - c")
	lex.SetPipeMode(true)

	expected := []struct {
		kind TokenKind
		lit  string
	}{
		{TokWord, "a"},
		{TokSlash, "/"},
		{TokWord, "b"},
		{TokMinus, "-"},
		{TokWord, "c"},
		{TokEOF, ""},
	}

	for i, want := range expected {
		tok, err := lex.Next()
		if err != nil {
			t.Fatalf("token %d: unexpected error: %v", i, err)
		}
		if tok.Kind != want.kind {
			t.Errorf("token %d: Kind = %v, want %v (lit=%q)", i, tok.Kind, want.kind, tok.Lit)
		}
		if tok.Kind != TokEOF && tok.Lit != want.lit {
			t.Errorf("token %d: Lit = %q, want %q", i, tok.Lit, want.lit)
		}
	}
}

func TestLexerArithmeticTokens(t *testing.T) {
	lex := NewLexer("a + b * c")
	lex.SetPipeMode(true)

	expected := []struct {
		kind TokenKind
		lit  string
	}{
		{TokWord, "a"},
		{TokPlus, "+"},
		{TokWord, "b"},
		{TokStar, "*"},
		{TokWord, "c"},
		{TokEOF, ""},
	}

	for i, want := range expected {
		tok, err := lex.Next()
		if err != nil {
			t.Fatalf("token %d: unexpected error: %v", i, err)
		}
		if tok.Kind != want.kind {
			t.Errorf("token %d: Kind = %v, want %v (lit=%q)", i, tok.Kind, want.kind, tok.Lit)
		}
		if tok.Kind != TokEOF && tok.Lit != want.lit {
			t.Errorf("token %d: Lit = %q, want %q", i, tok.Lit, want.lit)
		}
	}
}

func TestLexerHyphenInBarewordPreserved(t *testing.T) {
	// In non-pipe mode, hyphenated words should still be single barewords.
	lex := NewLexer("my-token")
	tok, err := lex.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.Kind != TokWord || tok.Lit != "my-token" {
		t.Errorf("got (%v, %q), want (WORD, \"my-token\")", tok.Kind, tok.Lit)
	}
}

// --- ParsePipeline tests ---

func TestParsePipelineFilterOnly(t *testing.T) {
	// No pipe: should work the same as Parse.
	p, err := ParsePipeline("error AND level=warn")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if p.Filter == nil {
		t.Fatal("expected filter expression, got nil")
	}
	if len(p.Pipes) != 0 {
		t.Errorf("expected no pipes, got %d", len(p.Pipes))
	}
}

func TestParsePipelineStatsCount(t *testing.T) {
	p, err := ParsePipeline("error | stats count")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if p.Filter == nil {
		t.Fatal("expected filter expression")
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	stats, ok := p.Pipes[0].(*StatsOp)
	if !ok {
		t.Fatalf("expected StatsOp, got %T", p.Pipes[0])
	}
	if len(stats.Aggs) != 1 || stats.Aggs[0].Func != "count" {
		t.Errorf("expected count agg, got %v", stats.Aggs)
	}
	if stats.Aggs[0].Arg != nil {
		t.Errorf("expected nil arg for bare count, got %v", stats.Aggs[0].Arg)
	}
	if len(stats.Groups) != 0 {
		t.Errorf("expected no groups, got %d", len(stats.Groups))
	}
}

func TestParsePipelineStatsCountParens(t *testing.T) {
	// count() should work the same as bare count.
	p, err := ParsePipeline("error | stats count()")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	if stats.Aggs[0].Func != "count" {
		t.Errorf("expected count, got %s", stats.Aggs[0].Func)
	}
}

func TestParsePipelineStatsWithBy(t *testing.T) {
	p, err := ParsePipeline("error | stats count by status")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	if len(stats.Aggs) != 1 {
		t.Fatalf("expected 1 agg, got %d", len(stats.Aggs))
	}
	if len(stats.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(stats.Groups))
	}
	if stats.Groups[0].Field == nil || stats.Groups[0].Field.Name != "status" {
		t.Errorf("expected group by 'status', got %v", stats.Groups[0])
	}
}

func TestParsePipelineStatsMultipleAggs(t *testing.T) {
	p, err := ParsePipeline("error | stats count, avg(duration), max(bytes) by status, method")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	if len(stats.Aggs) != 3 {
		t.Fatalf("expected 3 aggs, got %d", len(stats.Aggs))
	}
	if stats.Aggs[0].Func != "count" {
		t.Errorf("agg 0: expected count, got %s", stats.Aggs[0].Func)
	}
	if stats.Aggs[1].Func != "avg" {
		t.Errorf("agg 1: expected avg, got %s", stats.Aggs[1].Func)
	}
	if stats.Aggs[1].Arg == nil {
		t.Fatal("agg 1: expected arg, got nil")
	}
	if ref, ok := stats.Aggs[1].Arg.(*FieldRef); !ok || ref.Name != "duration" {
		t.Errorf("agg 1: expected FieldRef(duration), got %v", stats.Aggs[1].Arg)
	}
	if stats.Aggs[2].Func != "max" {
		t.Errorf("agg 2: expected max, got %s", stats.Aggs[2].Func)
	}
	if len(stats.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(stats.Groups))
	}
	if stats.Groups[0].Field.Name != "status" {
		t.Errorf("group 0: expected status, got %s", stats.Groups[0].Field.Name)
	}
	if stats.Groups[1].Field.Name != "method" {
		t.Errorf("group 1: expected method, got %s", stats.Groups[1].Field.Name)
	}
}

func TestParsePipelineStatsBin(t *testing.T) {
	p, err := ParsePipeline("error | stats count by bin(5m)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	if len(stats.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(stats.Groups))
	}
	if stats.Groups[0].Bin == nil {
		t.Fatal("expected bin group, got field")
	}
	if stats.Groups[0].Bin.Duration != "5m" {
		t.Errorf("expected duration 5m, got %s", stats.Groups[0].Bin.Duration)
	}
	if stats.Groups[0].Bin.Field != nil {
		t.Errorf("expected no field, got %v", stats.Groups[0].Bin.Field)
	}
}

func TestParsePipelineStatsBinWithField(t *testing.T) {
	p, err := ParsePipeline("error | stats count by bin(1h, source_ts)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	bin := stats.Groups[0].Bin
	if bin.Duration != "1h" {
		t.Errorf("expected duration 1h, got %s", bin.Duration)
	}
	if bin.Field == nil || bin.Field.Name != "source_ts" {
		t.Errorf("expected field source_ts, got %v", bin.Field)
	}
}

func TestParsePipelineStatsBinAndFields(t *testing.T) {
	p, err := ParsePipeline("error | stats count by bin(5m), status")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	if len(stats.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(stats.Groups))
	}
	if stats.Groups[0].Bin == nil {
		t.Error("expected bin group at index 0")
	}
	if stats.Groups[1].Field == nil || stats.Groups[1].Field.Name != "status" {
		t.Error("expected field group 'status' at index 1")
	}
}

func TestParsePipelineStatsAliasing(t *testing.T) {
	p, err := ParsePipeline("error | stats count as total, avg(duration) as avg_ms by status")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	if stats.Aggs[0].Alias != "total" {
		t.Errorf("agg 0: expected alias 'total', got %q", stats.Aggs[0].Alias)
	}
	if stats.Aggs[1].Alias != "avg_ms" {
		t.Errorf("agg 1: expected alias 'avg_ms', got %q", stats.Aggs[1].Alias)
	}
}

func TestParsePipelineStatsDefaultAliases(t *testing.T) {
	p, err := ParsePipeline("error | stats count, avg(duration)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	if stats.Aggs[0].DefaultAlias() != "count" {
		t.Errorf("agg 0: expected default alias 'count', got %q", stats.Aggs[0].DefaultAlias())
	}
	if stats.Aggs[1].DefaultAlias() != "avg_duration" {
		t.Errorf("agg 1: expected default alias 'avg_duration', got %q", stats.Aggs[1].DefaultAlias())
	}
}

func TestParsePipelineWhere(t *testing.T) {
	p, err := ParsePipeline("error | stats count by status | where count>10")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 2 {
		t.Fatalf("expected 2 pipes, got %d", len(p.Pipes))
	}
	_, ok := p.Pipes[0].(*StatsOp)
	if !ok {
		t.Fatalf("pipe 0: expected StatsOp, got %T", p.Pipes[0])
	}
	where, ok := p.Pipes[1].(*WhereOp)
	if !ok {
		t.Fatalf("pipe 1: expected WhereOp, got %T", p.Pipes[1])
	}
	// The where expression should be a KV predicate: count > 10.
	pred, ok := where.Expr.(*PredicateExpr)
	if !ok {
		t.Fatalf("where expr: expected PredicateExpr, got %T", where.Expr)
	}
	if pred.Key != "count" || pred.Op != OpGt || pred.Value != "10" {
		t.Errorf("where expr: expected count>10, got %s%s%s", pred.Key, pred.Op, pred.Value)
	}
}

func TestParsePipelineWhereWithBoolean(t *testing.T) {
	p, err := ParsePipeline("error | stats count by status | where count>10 AND status>=500")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	where := p.Pipes[1].(*WhereOp)
	and, ok := where.Expr.(*AndExpr)
	if !ok {
		t.Fatalf("expected AndExpr, got %T", where.Expr)
	}
	if len(and.Terms) != 2 {
		t.Errorf("expected 2 terms, got %d", len(and.Terms))
	}
}

// --- Expression parser tests ---

func TestParsePipelineExprArithmetic(t *testing.T) {
	p, err := ParsePipeline("error | stats avg(duration / 1000) as avg_sec")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	agg := stats.Aggs[0]
	if agg.Func != "avg" {
		t.Errorf("expected avg, got %s", agg.Func)
	}
	arith, ok := agg.Arg.(*ArithExpr)
	if !ok {
		t.Fatalf("expected ArithExpr, got %T", agg.Arg)
	}
	if arith.Op != ArithDiv {
		t.Errorf("expected ArithDiv, got %v", arith.Op)
	}
	left, ok := arith.Left.(*FieldRef)
	if !ok || left.Name != "duration" {
		t.Errorf("expected FieldRef(duration), got %v", arith.Left)
	}
	right, ok := arith.Right.(*NumberLit)
	if !ok || right.Value != "1000" {
		t.Errorf("expected NumberLit(1000), got %v", arith.Right)
	}
}

func TestParsePipelineExprPrecedence(t *testing.T) {
	// a + b * c should parse as a + (b * c).
	p, err := ParsePipeline("error | stats avg(a + b * c)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	add, ok := stats.Aggs[0].Arg.(*ArithExpr)
	if !ok || add.Op != ArithAdd {
		t.Fatalf("expected top-level ArithAdd, got %T %v", stats.Aggs[0].Arg, stats.Aggs[0].Arg)
	}
	_, leftIsField := add.Left.(*FieldRef)
	if !leftIsField {
		t.Errorf("expected left to be FieldRef, got %T", add.Left)
	}
	mul, ok := add.Right.(*ArithExpr)
	if !ok || mul.Op != ArithMul {
		t.Errorf("expected right to be ArithMul, got %T", add.Right)
	}
}

func TestParsePipelineExprParens(t *testing.T) {
	// (a + b) * c should parse as (a + b) * c.
	p, err := ParsePipeline("error | stats avg((a + b) * c)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	mul, ok := stats.Aggs[0].Arg.(*ArithExpr)
	if !ok || mul.Op != ArithMul {
		t.Fatalf("expected top-level ArithMul, got %T", stats.Aggs[0].Arg)
	}
	add, ok := mul.Left.(*ArithExpr)
	if !ok || add.Op != ArithAdd {
		t.Errorf("expected left to be ArithAdd, got %T", mul.Left)
	}
}

func TestParsePipelineExprNestedFuncCall(t *testing.T) {
	// avg(toNumber(response_time))
	p, err := ParsePipeline("error | stats avg(toNumber(response_time))")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	agg := stats.Aggs[0]
	fc, ok := agg.Arg.(*FuncCall)
	if !ok {
		t.Fatalf("expected FuncCall, got %T", agg.Arg)
	}
	if fc.Name != "toNumber" {
		t.Errorf("expected toNumber, got %s", fc.Name)
	}
	if len(fc.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(fc.Args))
	}
	ref, ok := fc.Args[0].(*FieldRef)
	if !ok || ref.Name != "response_time" {
		t.Errorf("expected FieldRef(response_time), got %v", fc.Args[0])
	}
}

func TestParsePipelineExprComplexNested(t *testing.T) {
	// avg(toNumber(response_time) / 1000)
	p, err := ParsePipeline("error | stats avg(toNumber(response_time) / 1000)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	arith, ok := stats.Aggs[0].Arg.(*ArithExpr)
	if !ok {
		t.Fatalf("expected ArithExpr, got %T", stats.Aggs[0].Arg)
	}
	if arith.Op != ArithDiv {
		t.Errorf("expected ArithDiv, got %v", arith.Op)
	}
	fc, ok := arith.Left.(*FuncCall)
	if !ok || fc.Name != "toNumber" {
		t.Errorf("expected FuncCall(toNumber), got %v", arith.Left)
	}
}

func TestParsePipelineExprModulo(t *testing.T) {
	p, err := ParsePipeline("error | stats avg(a % 3)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	arith, ok := stats.Aggs[0].Arg.(*ArithExpr)
	if !ok || arith.Op != ArithMod {
		t.Fatalf("expected ArithMod, got %T %v", stats.Aggs[0].Arg, stats.Aggs[0].Arg)
	}
}

func TestParsePipelineExprModuloPrecedence(t *testing.T) {
	// a + b % c should parse as a + (b % c) — % has same precedence as *.
	p, err := ParsePipeline("error | stats avg(a + b % c)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	add, ok := stats.Aggs[0].Arg.(*ArithExpr)
	if !ok || add.Op != ArithAdd {
		t.Fatalf("expected top-level ArithAdd, got %T", stats.Aggs[0].Arg)
	}
	mod, ok := add.Right.(*ArithExpr)
	if !ok || mod.Op != ArithMod {
		t.Errorf("expected right to be ArithMod, got %T", add.Right)
	}
}

func TestParsePipelineExprUnaryNegation(t *testing.T) {
	p, err := ParsePipeline("error | stats avg(-a)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	unary, ok := stats.Aggs[0].Arg.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", stats.Aggs[0].Arg)
	}
	if unary.Op != ArithSub {
		t.Errorf("expected ArithSub, got %v", unary.Op)
	}
	ref, ok := unary.Expr.(*FieldRef)
	if !ok || ref.Name != "a" {
		t.Errorf("expected FieldRef(a), got %v", unary.Expr)
	}
}

func TestParsePipelineExprDoubleNegation(t *testing.T) {
	p, err := ParsePipeline("error | stats avg(--a)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	stats := p.Pipes[0].(*StatsOp)
	outer, ok := stats.Aggs[0].Arg.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", stats.Aggs[0].Arg)
	}
	inner, ok := outer.Expr.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected nested UnaryExpr, got %T", outer.Expr)
	}
	_, ok = inner.Expr.(*FieldRef)
	if !ok {
		t.Errorf("expected FieldRef, got %T", inner.Expr)
	}
}

func TestLexerPercentToken(t *testing.T) {
	lex := NewLexer("a % b")
	lex.SetPipeMode(true)

	expected := []struct {
		kind TokenKind
		lit  string
	}{
		{TokWord, "a"},
		{TokPercent, "%"},
		{TokWord, "b"},
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

// --- String() tests ---

func TestPipelineString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			"error | stats count",
			"token(error) | stats count",
		},
		{
			"error | stats count by status",
			"token(error) | stats count by status",
		},
		{
			"error | stats count, avg(duration) by bin(5m), status",
			"token(error) | stats count, avg(duration) by bin(5m), status",
		},
		{
			"error | stats count as n by status | where n>10",
			"token(error) | stats count as n by status | where n>10",
		},
		{
			"error | stats avg(duration / 1000)",
			"token(error) | stats avg((duration / 1000))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			p, err := ParsePipeline(tt.input)
			if err != nil {
				t.Fatalf("ParsePipeline(%q) error: %v", tt.input, err)
			}
			got := p.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Error tests ---

func TestParsePipelineErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"pipe at start", "| stats count", ErrUnexpectedToken},
		{"empty after pipe", "error |", ErrUnexpectedEOF},
		{"unknown pipe operator", "error | foo count", ErrUnexpectedToken},
		{"stats without agg", "error | stats by status", ErrUnexpectedToken},
		{"stats agg missing paren", "error | stats avg duration", ErrUnexpectedToken},
		{"stats agg unclosed paren", "error | stats avg(duration", ErrUnexpectedToken},
		{"bin missing paren", "error | stats count by bin 5m", ErrUnexpectedToken},
		{"bin unclosed paren", "error | stats count by bin(5m", ErrUnexpectedToken},
		{"empty where", "error | where", ErrUnexpectedToken},
		{"where pipe", "error | where |", ErrUnexpectedToken},
		{"by without group", "error | stats count by", ErrUnexpectedToken},
		{"duplicate alias", "error | stats count, count", ErrUnexpectedToken},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePipeline(tt.input)
			if err == nil {
				t.Fatalf("ParsePipeline(%q) expected error, got nil", tt.input)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("ParsePipeline(%q) error = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// --- Backwards compatibility ---

func TestParsePipelineBackwardsCompat(t *testing.T) {
	// Existing filter-only queries should work through ParsePipeline.
	tests := []string{
		"error",
		"error AND warn",
		"error OR warn",
		"NOT debug",
		"level=error",
		"level=error AND host=server1",
		`(error OR warn) AND NOT debug`,
		`/timeout/`,
		`error*`,
		`status>=500`,
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			p, err := ParsePipeline(input)
			if err != nil {
				t.Fatalf("ParsePipeline(%q) error: %v", input, err)
			}
			if p.Filter == nil {
				t.Error("expected filter, got nil")
			}
			if len(p.Pipes) != 0 {
				t.Errorf("expected no pipes, got %d", len(p.Pipes))
			}
		})
	}
}

// --- Real-world query examples ---

func TestParsePipelineRealWorld(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			"basic aggregation",
			"service=api status>=500 | stats count by status",
		},
		{
			"multi-agg with time bucketing",
			"service=api | stats count, avg(duration) by bin(5m), method",
		},
		{
			"filter aggregate results",
			"service=api | stats count by status | where count>10",
		},
		{
			"aliased aggregations",
			"service=api | stats count as total, avg(duration) as avg_ms by bin(1h, source_ts)",
		},
		{
			"complex filter with simple stats",
			"(level=error OR level=warn) AND NOT service=debug | stats count by bin(5m)",
		},
		{
			"nested function in agg",
			"service=api | stats avg(toNumber(response_time) / 1000) as avg_sec by method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParsePipeline(tt.input)
			if err != nil {
				t.Fatalf("ParsePipeline(%q) error: %v", tt.input, err)
			}
			// Verify it round-trips to String without panic.
			_ = p.String()
			t.Logf("ParsePipeline(%q) = %s", tt.input, p.String())
		})
	}
}

func TestParsePipelineWhereRegex(t *testing.T) {
	// where clause should support regex (since it reuses filter parser in filter mode).
	p, err := ParsePipeline("error | stats count by status | where /5\\d\\d/")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	where := p.Pipes[1].(*WhereOp)
	pred, ok := where.Expr.(*PredicateExpr)
	if !ok {
		t.Fatalf("expected PredicateExpr, got %T", where.Expr)
	}
	if pred.Kind != PredRegex {
		t.Errorf("expected PredRegex, got %v", pred.Kind)
	}
}
