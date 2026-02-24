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

// --- New operator tests ---

func TestParsePipelineEval(t *testing.T) {
	p, err := ParsePipeline("error | eval duration_ms = duration / 1000")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	ev, ok := p.Pipes[0].(*EvalOp)
	if !ok {
		t.Fatalf("expected EvalOp, got %T", p.Pipes[0])
	}
	if len(ev.Assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(ev.Assignments))
	}
	if ev.Assignments[0].Field != "duration_ms" {
		t.Errorf("expected field 'duration_ms', got %q", ev.Assignments[0].Field)
	}
	arith, ok := ev.Assignments[0].Expr.(*ArithExpr)
	if !ok || arith.Op != ArithDiv {
		t.Errorf("expected ArithDiv expression, got %T", ev.Assignments[0].Expr)
	}
}

func TestParsePipelineEvalMultiple(t *testing.T) {
	p, err := ParsePipeline("error | eval a = x + 1, b = y * 2")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	ev := p.Pipes[0].(*EvalOp)
	if len(ev.Assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(ev.Assignments))
	}
	if ev.Assignments[0].Field != "a" || ev.Assignments[1].Field != "b" {
		t.Errorf("unexpected fields: %s, %s", ev.Assignments[0].Field, ev.Assignments[1].Field)
	}
}

func TestParsePipelineSort(t *testing.T) {
	p, err := ParsePipeline("error | sort status")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	s, ok := p.Pipes[0].(*SortOp)
	if !ok {
		t.Fatalf("expected SortOp, got %T", p.Pipes[0])
	}
	if len(s.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(s.Fields))
	}
	if s.Fields[0].Name != "status" || s.Fields[0].Desc {
		t.Errorf("expected ascending 'status', got %+v", s.Fields[0])
	}
}

func TestParsePipelineSortDesc(t *testing.T) {
	p, err := ParsePipeline("error | sort -count, status")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	s := p.Pipes[0].(*SortOp)
	if len(s.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(s.Fields))
	}
	if s.Fields[0].Name != "count" || !s.Fields[0].Desc {
		t.Errorf("expected descending 'count', got %+v", s.Fields[0])
	}
	if s.Fields[1].Name != "status" || s.Fields[1].Desc {
		t.Errorf("expected ascending 'status', got %+v", s.Fields[1])
	}
}

func TestParsePipelineHead(t *testing.T) {
	p, err := ParsePipeline("error | head 10")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	h, ok := p.Pipes[0].(*HeadOp)
	if !ok {
		t.Fatalf("expected HeadOp, got %T", p.Pipes[0])
	}
	if h.N != 10 {
		t.Errorf("expected N=10, got %d", h.N)
	}
}

func TestParsePipelineTail(t *testing.T) {
	p, err := ParsePipeline("error | tail 5")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	ta, ok := p.Pipes[0].(*TailOp)
	if !ok {
		t.Fatalf("expected TailOp, got %T", p.Pipes[0])
	}
	if ta.N != 5 {
		t.Errorf("expected N=5, got %d", ta.N)
	}
}

func TestParsePipelineSlice(t *testing.T) {
	p, err := ParsePipeline("error | slice 12 54")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	sl, ok := p.Pipes[0].(*SliceOp)
	if !ok {
		t.Fatalf("expected SliceOp, got %T", p.Pipes[0])
	}
	if sl.Start != 12 {
		t.Errorf("expected Start=12, got %d", sl.Start)
	}
	if sl.End != 54 {
		t.Errorf("expected End=54, got %d", sl.End)
	}
}

func TestParsePipelineRename(t *testing.T) {
	p, err := ParsePipeline("error | rename src as source, dst as destination")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	r, ok := p.Pipes[0].(*RenameOp)
	if !ok {
		t.Fatalf("expected RenameOp, got %T", p.Pipes[0])
	}
	if len(r.Renames) != 2 {
		t.Fatalf("expected 2 renames, got %d", len(r.Renames))
	}
	if r.Renames[0].Old != "src" || r.Renames[0].New != "source" {
		t.Errorf("rename 0: expected src → source, got %+v", r.Renames[0])
	}
	if r.Renames[1].Old != "dst" || r.Renames[1].New != "destination" {
		t.Errorf("rename 1: expected dst → destination, got %+v", r.Renames[1])
	}
}

func TestParsePipelineFieldsKeep(t *testing.T) {
	p, err := ParsePipeline("error | fields host, level, message")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	f, ok := p.Pipes[0].(*FieldsOp)
	if !ok {
		t.Fatalf("expected FieldsOp, got %T", p.Pipes[0])
	}
	if f.Drop {
		t.Error("expected keep mode, got drop")
	}
	if len(f.Names) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(f.Names))
	}
}

func TestParsePipelineFieldsDrop(t *testing.T) {
	p, err := ParsePipeline("error | fields - debug, trace")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	f := p.Pipes[0].(*FieldsOp)
	if !f.Drop {
		t.Error("expected drop mode, got keep")
	}
	if len(f.Names) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(f.Names))
	}
}

func TestParsePipelineChained(t *testing.T) {
	// Test multiple new operators chained together.
	p, err := ParsePipeline("error | eval ms = duration / 1000 | sort -ms | head 5")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 3 {
		t.Fatalf("expected 3 pipes, got %d", len(p.Pipes))
	}
	if _, ok := p.Pipes[0].(*EvalOp); !ok {
		t.Errorf("pipe 0: expected EvalOp, got %T", p.Pipes[0])
	}
	if _, ok := p.Pipes[1].(*SortOp); !ok {
		t.Errorf("pipe 1: expected SortOp, got %T", p.Pipes[1])
	}
	if _, ok := p.Pipes[2].(*HeadOp); !ok {
		t.Errorf("pipe 2: expected HeadOp, got %T", p.Pipes[2])
	}
}

func TestParsePipelinePostStats(t *testing.T) {
	// Operators after stats work on table data.
	p, err := ParsePipeline("error | stats count by status | sort -count | head 10")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 3 {
		t.Fatalf("expected 3 pipes, got %d", len(p.Pipes))
	}
	if _, ok := p.Pipes[0].(*StatsOp); !ok {
		t.Errorf("pipe 0: expected StatsOp, got %T", p.Pipes[0])
	}
	if _, ok := p.Pipes[1].(*SortOp); !ok {
		t.Errorf("pipe 1: expected SortOp, got %T", p.Pipes[1])
	}
	if _, ok := p.Pipes[2].(*HeadOp); !ok {
		t.Errorf("pipe 2: expected HeadOp, got %T", p.Pipes[2])
	}
}

func TestParsePipelineNewOpsString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			"error | eval ms = duration / 1000",
			"token(error) | eval ms = (duration / 1000)",
		},
		{
			"error | sort -count, status",
			"token(error) | sort -count, status",
		},
		{
			"error | head 10",
			"token(error) | head 10",
		},
		{
			"error | rename src as source",
			"token(error) | rename src as source",
		},
		{
			"error | fields host, level",
			"token(error) | fields host, level",
		},
		{
			"error | fields - debug",
			"token(error) | fields - debug",
		},
		{
			"error | tail 5",
			"token(error) | tail 5",
		},
		{
			"error | slice 12 54",
			"token(error) | slice 12 54",
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

func TestParsePipelineNewOpsErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"eval no assignment", "error | eval"},
		{"eval no equals", "error | eval x"},
		{"eval no expr", "error | eval x ="},
		{"sort no field", "error | sort"},
		{"head no number", "error | head"},
		{"head zero", "error | head 0"},
		{"head negative", "error | head -1"},
		{"head not number", "error | head abc"},
		{"tail no number", "error | tail"},
		{"tail zero", "error | tail 0"},
		{"tail negative", "error | tail -1"},
		{"tail not number", "error | tail abc"},
		{"slice no args", "error | slice"},
		{"slice one arg", "error | slice 5"},
		{"slice zero start", "error | slice 0 10"},
		{"slice end before start", "error | slice 10 5"},
		{"slice not number start", "error | slice abc 10"},
		{"slice not number end", "error | slice 5 abc"},
		{"rename no as", "error | rename src"},
		{"rename no new", "error | rename src as"},
		{"fields no field", "error | fields"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePipeline(tt.input)
			if err == nil {
				t.Fatalf("ParsePipeline(%q) expected error, got nil", tt.input)
			}
		})
	}
}

func TestParsePipelineTimechart(t *testing.T) {
	p, err := ParsePipeline("error | timechart 50")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	tc, ok := p.Pipes[0].(*TimechartOp)
	if !ok {
		t.Fatalf("expected TimechartOp, got %T", p.Pipes[0])
	}
	if tc.N != 50 {
		t.Errorf("expected N=50, got %d", tc.N)
	}
}

func TestParsePipelineTimechartBarePipe(t *testing.T) {
	p, err := ParsePipeline("| timechart 100")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if p.Filter != nil {
		t.Errorf("expected nil filter (match-all), got %v", p.Filter)
	}
	tc := p.Pipes[0].(*TimechartOp)
	if tc.N != 100 {
		t.Errorf("expected N=100, got %d", tc.N)
	}
}

func TestParsePipelineTimechartString(t *testing.T) {
	p, err := ParsePipeline("error | timechart 50")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	got := p.String()
	want := "token(error) | timechart 50"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestParsePipelineTimechartErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"timechart no number", "error | timechart"},
		{"timechart zero", "error | timechart 0"},
		{"timechart negative", "error | timechart -1"},
		{"timechart not number", "error | timechart abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePipeline(tt.input)
			if err == nil {
				t.Fatalf("ParsePipeline(%q) expected error, got nil", tt.input)
			}
		})
	}
}

func TestParsePipelineTimechartBy(t *testing.T) {
	p, err := ParsePipeline("error | timechart 50 by status")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	tc, ok := p.Pipes[0].(*TimechartOp)
	if !ok {
		t.Fatalf("expected TimechartOp, got %T", p.Pipes[0])
	}
	if tc.N != 50 {
		t.Errorf("expected N=50, got %d", tc.N)
	}
	if tc.By != "status" {
		t.Errorf("expected By=status, got %q", tc.By)
	}
	got := p.String()
	want := "token(error) | timechart 50 by status"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestParsePipelineTimechartByMissingField(t *testing.T) {
	_, err := ParsePipeline("error | timechart 50 by")
	if err == nil {
		t.Fatal("expected error for 'timechart 50 by' with no field")
	}
}

func TestParsePipelineRaw(t *testing.T) {
	p, err := ParsePipeline("error | raw")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	if _, ok := p.Pipes[0].(*RawOp); !ok {
		t.Fatalf("expected RawOp, got %T", p.Pipes[0])
	}
}

func TestParsePipelineBarePipe(t *testing.T) {
	// A query starting with "|" (no filter) should be valid — implies match-all.
	p, err := ParsePipeline("| raw")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if p.Filter != nil {
		t.Errorf("expected nil filter (match-all), got %v", p.Filter)
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	if _, ok := p.Pipes[0].(*RawOp); !ok {
		t.Fatalf("expected RawOp, got %T", p.Pipes[0])
	}
}

func TestParsePipelineBarePipeStats(t *testing.T) {
	p, err := ParsePipeline("| stats count by bin(5m)")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if p.Filter != nil {
		t.Errorf("expected nil filter (match-all), got %v", p.Filter)
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	if _, ok := p.Pipes[0].(*StatsOp); !ok {
		t.Fatalf("expected StatsOp, got %T", p.Pipes[0])
	}
}

func TestParsePipelineRawAfterStats(t *testing.T) {
	p, err := ParsePipeline("error | stats count by bin(5m) | raw")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 2 {
		t.Fatalf("expected 2 pipes, got %d", len(p.Pipes))
	}
	if _, ok := p.Pipes[0].(*StatsOp); !ok {
		t.Errorf("pipe 0: expected StatsOp, got %T", p.Pipes[0])
	}
	if _, ok := p.Pipes[1].(*RawOp); !ok {
		t.Errorf("pipe 1: expected RawOp, got %T", p.Pipes[1])
	}
}

func TestParsePipelineLookup(t *testing.T) {
	p, err := ParsePipeline("error | lookup rdns src_ip")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 1 {
		t.Fatalf("expected 1 pipe, got %d", len(p.Pipes))
	}
	lu, ok := p.Pipes[0].(*LookupOp)
	if !ok {
		t.Fatalf("expected LookupOp, got %T", p.Pipes[0])
	}
	if lu.Table != "rdns" {
		t.Errorf("Table = %q, want 'rdns'", lu.Table)
	}
	if lu.Field != "src_ip" {
		t.Errorf("Field = %q, want 'src_ip'", lu.Field)
	}
}

func TestParsePipelineLookupString(t *testing.T) {
	p, err := ParsePipeline("error | lookup rdns src_ip")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	got := p.String()
	want := "token(error) | lookup rdns src_ip"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestParsePipelineLookupAfterStats(t *testing.T) {
	p, err := ParsePipeline("error | stats count by src_ip | lookup rdns src_ip")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}
	if len(p.Pipes) != 2 {
		t.Fatalf("expected 2 pipes, got %d", len(p.Pipes))
	}
	if _, ok := p.Pipes[0].(*StatsOp); !ok {
		t.Errorf("pipe 0: expected StatsOp, got %T", p.Pipes[0])
	}
	if _, ok := p.Pipes[1].(*LookupOp); !ok {
		t.Errorf("pipe 1: expected LookupOp, got %T", p.Pipes[1])
	}
}

func TestParsePipelineLookupErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"lookup no args", "error | lookup"},
		{"lookup one arg", "error | lookup rdns"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePipeline(tt.input)
			if err == nil {
				t.Fatalf("ParsePipeline(%q) expected error, got nil", tt.input)
			}
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
