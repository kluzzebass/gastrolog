package querylang

import (
	"fmt"
	"math"
	"testing"
)

func TestEvalFieldRef(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"status": "200", "duration": "1500", "method": "GET"}

	tests := []struct {
		name    string
		expr    PipeExpr
		wantStr string
		wantNum float64
		isNum   bool
		missing bool
	}{
		{"numeric field", &FieldRef{Name: "status"}, "200", 200, true, false},
		{"string field", &FieldRef{Name: "method"}, "GET", 0, false, false},
		{"missing field", &FieldRef{Name: "nonexistent"}, "", 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := eval.Eval(tt.expr, row)
			if err != nil {
				t.Fatalf("Eval error: %v", err)
			}
			if v.Missing != tt.missing {
				t.Errorf("Missing = %v, want %v", v.Missing, tt.missing)
			}
			if !tt.missing {
				if v.Str != tt.wantStr {
					t.Errorf("Str = %q, want %q", v.Str, tt.wantStr)
				}
				if v.IsNum != tt.isNum {
					t.Errorf("IsNum = %v, want %v", v.IsNum, tt.isNum)
				}
				if tt.isNum && v.Num != tt.wantNum {
					t.Errorf("Num = %v, want %v", v.Num, tt.wantNum)
				}
			}
		})
	}
}

func TestEvalNumberLit(t *testing.T) {
	eval := NewEvaluator()
	row := Row{}

	v, err := eval.Eval(&NumberLit{Value: "42"}, row)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.IsNum || v.Num != 42 {
		t.Errorf("expected 42, got %v", v)
	}

	v, err = eval.Eval(&NumberLit{Value: "3.14"}, row)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.IsNum || v.Num != 3.14 {
		t.Errorf("expected 3.14, got %v", v)
	}
}

func TestEvalStringLit(t *testing.T) {
	eval := NewEvaluator()
	row := Row{}

	v, err := eval.Eval(&StringLit{Value: "hello"}, row)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if v.Str != "hello" || v.IsNum {
		t.Errorf("expected string 'hello', got %v", v)
	}
}

func TestEvalArithmetic(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"a": "10", "b": "3"}

	tests := []struct {
		name string
		expr PipeExpr
		want float64
	}{
		{
			"addition",
			&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithAdd, Right: &FieldRef{Name: "b"}},
			13,
		},
		{
			"subtraction",
			&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithSub, Right: &FieldRef{Name: "b"}},
			7,
		},
		{
			"multiplication",
			&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithMul, Right: &FieldRef{Name: "b"}},
			30,
		},
		{
			"division",
			&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithDiv, Right: &FieldRef{Name: "b"}},
			10.0 / 3.0,
		},
		{
			"field and literal",
			&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithMul, Right: &NumberLit{Value: "1000"}},
			10000,
		},
		{
			"nested: (a + b) * 2",
			&ArithExpr{
				Left: &ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithAdd, Right: &FieldRef{Name: "b"}},
				Op:    ArithMul,
				Right: &NumberLit{Value: "2"},
			},
			26,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := eval.Eval(tt.expr, row)
			if err != nil {
				t.Fatalf("Eval error: %v", err)
			}
			if !v.IsNum {
				t.Fatalf("expected numeric result, got %v", v)
			}
			if math.Abs(v.Num-tt.want) > 1e-9 {
				t.Errorf("got %v, want %v", v.Num, tt.want)
			}
		})
	}
}

func TestEvalDivisionByZero(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"a": "10"}

	v, err := eval.Eval(
		&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithDiv, Right: &NumberLit{Value: "0"}},
		row,
	)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !math.IsNaN(v.Num) {
		t.Errorf("expected NaN, got %v", v.Num)
	}
}

func TestEvalArithMissingField(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"a": "10"}

	v, err := eval.Eval(
		&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithAdd, Right: &FieldRef{Name: "missing"}},
		row,
	)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.Missing {
		t.Error("expected missing result when operand is missing")
	}
}

func TestEvalArithNonNumeric(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"a": "10", "b": "hello"}

	_, err := eval.Eval(
		&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithAdd, Right: &FieldRef{Name: "b"}},
		row,
	)
	if err == nil {
		t.Fatal("expected error for non-numeric operand")
	}
}

func TestEvalToNumber(t *testing.T) {
	eval := NewEvaluator()

	tests := []struct {
		name    string
		row     Row
		want    float64
		missing bool
	}{
		{"numeric string", Row{"val": "42.5"}, 42.5, false},
		{"already numeric", Row{"val": "100"}, 100, false},
		{"non-numeric", Row{"val": "hello"}, 0, true},
		{"missing field", Row{}, 0, true},
	}

	expr := &FuncCall{Name: "toNumber", Args: []PipeExpr{&FieldRef{Name: "val"}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := eval.Eval(expr, tt.row)
			if err != nil {
				t.Fatalf("Eval error: %v", err)
			}
			if v.Missing != tt.missing {
				t.Errorf("Missing = %v, want %v", v.Missing, tt.missing)
			}
			if !tt.missing && v.Num != tt.want {
				t.Errorf("Num = %v, want %v", v.Num, tt.want)
			}
		})
	}
}

func TestEvalToNumberArgError(t *testing.T) {
	eval := NewEvaluator()

	// toNumber with wrong number of args.
	_, err := eval.Eval(&FuncCall{Name: "toNumber", Args: []PipeExpr{}}, Row{})
	if err == nil {
		t.Fatal("expected error for toNumber with 0 args")
	}

	_, err = eval.Eval(&FuncCall{Name: "toNumber", Args: []PipeExpr{
		&FieldRef{Name: "a"}, &FieldRef{Name: "b"},
	}}, Row{"a": "1", "b": "2"})
	if err == nil {
		t.Fatal("expected error for toNumber with 2 args")
	}
}

func TestEvalUnknownFunction(t *testing.T) {
	eval := NewEvaluator()
	_, err := eval.Eval(&FuncCall{Name: "bogus", Args: []PipeExpr{}}, Row{})
	if err == nil {
		t.Fatal("expected error for unknown function")
	}
}

func TestEvalNestedFuncAndArith(t *testing.T) {
	// avg(toNumber(response_time) / 1000) — just the inner expression part.
	eval := NewEvaluator()
	row := Row{"response_time": "1500"}

	expr := &ArithExpr{
		Left:  &FuncCall{Name: "toNumber", Args: []PipeExpr{&FieldRef{Name: "response_time"}}},
		Op:    ArithDiv,
		Right: &NumberLit{Value: "1000"},
	}

	v, err := eval.Eval(expr, row)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.IsNum || v.Num != 1.5 {
		t.Errorf("expected 1.5, got %v", v)
	}
}

func TestEvalCustomFunc(t *testing.T) {
	eval := NewEvaluator()
	eval.RegisterFunc("double", func(args []Value) (Value, error) {
		if len(args) != 1 {
			return Value{}, fmt.Errorf("double requires 1 arg")
		}
		n, ok := args[0].ToNum()
		if !ok {
			return MissingValue(), nil
		}
		return NumValue(n * 2), nil
	})

	v, err := eval.Eval(&FuncCall{Name: "double", Args: []PipeExpr{&NumberLit{Value: "21"}}}, Row{})
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.IsNum || v.Num != 42 {
		t.Errorf("expected 42, got %v", v)
	}
}

func TestEvalValueToNum(t *testing.T) {
	tests := []struct {
		name string
		val  Value
		want float64
		ok   bool
	}{
		{"numeric", NumValue(42), 42, true},
		{"string numeric", StrValue("3.14"), 3.14, true},
		{"string non-numeric", StrValue("hello"), 0, false},
		{"missing", MissingValue(), 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.val.ToNum()
			if ok != tt.ok {
				t.Errorf("ok = %v, want %v", ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvalModulo(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"a": "10", "b": "3"}

	v, err := eval.Eval(
		&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithMod, Right: &FieldRef{Name: "b"}},
		row,
	)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.IsNum || v.Num != 1 {
		t.Errorf("expected 1, got %v", v)
	}
}

func TestEvalModuloByZero(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"a": "10"}

	v, err := eval.Eval(
		&ArithExpr{Left: &FieldRef{Name: "a"}, Op: ArithMod, Right: &NumberLit{Value: "0"}},
		row,
	)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !math.IsNaN(v.Num) {
		t.Errorf("expected NaN, got %v", v.Num)
	}
}

func TestEvalUnaryNegation(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"x": "42"}

	v, err := eval.Eval(&UnaryExpr{Op: ArithSub, Expr: &FieldRef{Name: "x"}}, row)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.IsNum || v.Num != -42 {
		t.Errorf("expected -42, got %v", v)
	}
}

func TestEvalUnaryNegationMissing(t *testing.T) {
	eval := NewEvaluator()
	v, err := eval.Eval(&UnaryExpr{Op: ArithSub, Expr: &FieldRef{Name: "missing"}}, Row{})
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.Missing {
		t.Error("expected missing result for missing operand")
	}
}

func TestEvalScalarFunctions(t *testing.T) {
	eval := NewEvaluator()

	tests := []struct {
		name    string
		expr    PipeExpr
		row     Row
		wantStr string
		wantNum float64
		isNum   bool
		missing bool
	}{
		// Math
		{"abs positive", &FuncCall{Name: "abs", Args: []PipeExpr{&NumberLit{Value: "-5"}}}, Row{}, "", 5, true, false},
		{"abs negative", &FuncCall{Name: "abs", Args: []PipeExpr{&NumberLit{Value: "5"}}}, Row{}, "", 5, true, false},
		{"ceil", &FuncCall{Name: "ceil", Args: []PipeExpr{&NumberLit{Value: "2.3"}}}, Row{}, "", 3, true, false},
		{"floor", &FuncCall{Name: "floor", Args: []PipeExpr{&NumberLit{Value: "2.7"}}}, Row{}, "", 2, true, false},
		{"round default", &FuncCall{Name: "round", Args: []PipeExpr{&NumberLit{Value: "2.7"}}}, Row{}, "", 3, true, false},
		{"round 2 decimals", &FuncCall{Name: "round", Args: []PipeExpr{&NumberLit{Value: "3.14159"}, &NumberLit{Value: "2"}}}, Row{}, "", 3.14, true, false},
		{"sqrt", &FuncCall{Name: "sqrt", Args: []PipeExpr{&NumberLit{Value: "9"}}}, Row{}, "", 3, true, false},
		{"pow", &FuncCall{Name: "pow", Args: []PipeExpr{&NumberLit{Value: "2"}, &NumberLit{Value: "10"}}}, Row{}, "", 1024, true, false},
		{"log", &FuncCall{Name: "log", Args: []PipeExpr{&NumberLit{Value: "1"}}}, Row{}, "", 0, true, false},
		{"exp", &FuncCall{Name: "exp", Args: []PipeExpr{&NumberLit{Value: "0"}}}, Row{}, "", 1, true, false},

		// String
		{"len", &FuncCall{Name: "len", Args: []PipeExpr{&StringLit{Value: "hello"}}}, Row{}, "", 5, true, false},
		{"lower", &FuncCall{Name: "lower", Args: []PipeExpr{&StringLit{Value: "HELLO"}}}, Row{}, "hello", 0, false, false},
		{"upper", &FuncCall{Name: "upper", Args: []PipeExpr{&StringLit{Value: "hello"}}}, Row{}, "HELLO", 0, false, false},
		{"trim", &FuncCall{Name: "trim", Args: []PipeExpr{&StringLit{Value: "  hi  "}}}, Row{}, "hi", 0, false, false},
		{"substr", &FuncCall{Name: "substr", Args: []PipeExpr{&StringLit{Value: "hello"}, &NumberLit{Value: "1"}, &NumberLit{Value: "3"}}}, Row{}, "ell", 0, false, false},
		{"replace", &FuncCall{Name: "replace", Args: []PipeExpr{&StringLit{Value: "foo bar foo"}, &StringLit{Value: "foo"}, &StringLit{Value: "baz"}}}, Row{}, "baz bar baz", 0, false, false},
		{"concat", &FuncCall{Name: "concat", Args: []PipeExpr{&StringLit{Value: "hello"}, &StringLit{Value: " "}, &StringLit{Value: "world"}}}, Row{}, "hello world", 0, false, false},

		// Type/control
		{"toString", &FuncCall{Name: "toString", Args: []PipeExpr{&NumberLit{Value: "42"}}}, Row{}, "42", 0, false, false},
		{"isnull missing", &FuncCall{Name: "isnull", Args: []PipeExpr{&FieldRef{Name: "missing"}}}, Row{}, "", 1, true, false},
		{"isnull present", &FuncCall{Name: "isnull", Args: []PipeExpr{&FieldRef{Name: "x"}}}, Row{"x": "1"}, "", 0, true, false},
		{"typeof number", &FuncCall{Name: "typeof", Args: []PipeExpr{&NumberLit{Value: "42"}}}, Row{}, "number", 0, false, false},
		{"typeof string", &FuncCall{Name: "typeof", Args: []PipeExpr{&StringLit{Value: "hi"}}}, Row{}, "string", 0, false, false},
		{"typeof missing", &FuncCall{Name: "typeof", Args: []PipeExpr{&FieldRef{Name: "missing"}}}, Row{}, "missing", 0, false, false},
		{"coalesce", &FuncCall{Name: "coalesce", Args: []PipeExpr{&FieldRef{Name: "missing"}, &NumberLit{Value: "7"}}}, Row{}, "", 7, true, false},

		// Bitwise
		{"bitor", &FuncCall{Name: "bitor", Args: []PipeExpr{&NumberLit{Value: "5"}, &NumberLit{Value: "3"}}}, Row{}, "", 7, true, false},
		{"bitand", &FuncCall{Name: "bitand", Args: []PipeExpr{&NumberLit{Value: "7"}, &NumberLit{Value: "3"}}}, Row{}, "", 3, true, false},
		{"bitxor", &FuncCall{Name: "bitxor", Args: []PipeExpr{&NumberLit{Value: "5"}, &NumberLit{Value: "3"}}}, Row{}, "", 6, true, false},
		{"bitnot", &FuncCall{Name: "bitnot", Args: []PipeExpr{&NumberLit{Value: "0"}}}, Row{}, "", -1, true, false},
		{"bitshl", &FuncCall{Name: "bitshl", Args: []PipeExpr{&NumberLit{Value: "1"}, &NumberLit{Value: "3"}}}, Row{}, "", 8, true, false},
		{"bitshr", &FuncCall{Name: "bitshr", Args: []PipeExpr{&NumberLit{Value: "8"}, &NumberLit{Value: "2"}}}, Row{}, "", 2, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := eval.Eval(tt.expr, tt.row)
			if err != nil {
				t.Fatalf("Eval error: %v", err)
			}
			if v.Missing != tt.missing {
				t.Fatalf("Missing = %v, want %v", v.Missing, tt.missing)
			}
			if tt.missing {
				return
			}
			if v.IsNum != tt.isNum {
				t.Fatalf("IsNum = %v, want %v", v.IsNum, tt.isNum)
			}
			if tt.isNum {
				if math.Abs(v.Num-tt.wantNum) > 1e-9 {
					t.Errorf("Num = %v, want %v", v.Num, tt.wantNum)
				}
			} else if v.Str != tt.wantStr {
				t.Errorf("Str = %q, want %q", v.Str, tt.wantStr)
			}
		})
	}
}

func TestEvalMissingPropagation(t *testing.T) {
	eval := NewEvaluator()

	// All 1-arg functions should propagate missing.
	funcs := []string{"abs", "ceil", "floor", "sqrt", "log", "log10", "log2", "exp", "len", "lower", "upper", "trim", "isnull", "typeof", "bitnot", "tostring"}
	for _, fn := range funcs {
		if fn == "isnull" || fn == "typeof" {
			continue // these return a value even for missing
		}
		t.Run(fn, func(t *testing.T) {
			v, err := eval.Eval(&FuncCall{Name: fn, Args: []PipeExpr{&FieldRef{Name: "missing"}}}, Row{})
			if err != nil {
				t.Fatalf("Eval error: %v", err)
			}
			if !v.Missing {
				t.Errorf("%s(missing) should be missing, got %v", fn, v)
			}
		})
	}
}

// Ensure the evaluator works with parsed expressions from the pipeline parser.
func TestEvalWithParsedExpressions(t *testing.T) {
	eval := NewEvaluator()
	row := Row{"duration": "1500", "status": "200"}

	// Parse "avg(toNumber(duration) / 1000)" — extract the inner expression.
	pipeline, err := ParsePipeline("error | stats avg(toNumber(duration) / 1000) as avg_sec")
	if err != nil {
		t.Fatalf("ParsePipeline error: %v", err)
	}

	stats := pipeline.Pipes[0].(*StatsOp)
	innerExpr := stats.Aggs[0].Arg

	v, err := eval.Eval(innerExpr, row)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	if !v.IsNum || v.Num != 1.5 {
		t.Errorf("expected 1.5, got %v", v)
	}
}
