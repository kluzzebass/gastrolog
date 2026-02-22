package querylang

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Value represents the result of evaluating a pipe expression.
// It can be a string or a numeric value.
type Value struct {
	Str     string  // string representation (always set)
	Num     float64 // numeric value (only valid if IsNum is true)
	IsNum   bool    // true if the value is numeric
	Missing bool    // true if the field was not found
}

// NumValue creates a numeric Value.
func NumValue(n float64) Value {
	return Value{Str: strconv.FormatFloat(n, 'f', -1, 64), Num: n, IsNum: true}
}

// StrValue creates a string Value.
func StrValue(s string) Value {
	return Value{Str: s}
}

// MissingValue creates a Value representing a missing field.
func MissingValue() Value {
	return Value{Missing: true}
}

// ToNum attempts to interpret the value as a number.
// If already numeric, returns as-is. Otherwise tries to parse the string.
func (v Value) ToNum() (float64, bool) {
	if v.Missing {
		return 0, false
	}
	if v.IsNum {
		return v.Num, true
	}
	f, err := strconv.ParseFloat(v.Str, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// Row provides field values for expression evaluation.
// It's a simple string map — the caller populates it from record attributes,
// extracted KV pairs, or upstream pipe operator output.
type Row map[string]string

// ScalarFunc is a scalar function that operates on evaluated arguments.
type ScalarFunc func(args []Value) (Value, error)

// Evaluator evaluates pipe expressions against a row of field values.
type Evaluator struct {
	funcs map[string]ScalarFunc
}

// NewEvaluator creates an Evaluator with the built-in scalar functions.
func NewEvaluator() *Evaluator {
	e := &Evaluator{
		funcs: make(map[string]ScalarFunc),
	}
	e.registerBuiltins()
	return e
}

// RegisterFunc adds a scalar function. Overwrites any existing function with the same name.
func (e *Evaluator) RegisterFunc(name string, fn ScalarFunc) {
	e.funcs[strings.ToLower(name)] = fn
}

// Eval evaluates a pipe expression against a row.
func (e *Evaluator) Eval(expr PipeExpr, row Row) (Value, error) {
	switch ex := expr.(type) {
	case *FieldRef:
		return e.evalFieldRef(ex, row), nil

	case *NumberLit:
		f, err := strconv.ParseFloat(ex.Value, 64)
		if err != nil {
			return Value{}, fmt.Errorf("invalid number literal %q: %w", ex.Value, err)
		}
		return NumValue(f), nil

	case *StringLit:
		return StrValue(ex.Value), nil

	case *FuncCall:
		return e.evalFuncCall(ex, row)

	case *ArithExpr:
		return e.evalArith(ex, row)

	default:
		return Value{}, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Evaluator) evalFieldRef(ref *FieldRef, row Row) Value {
	v, ok := row[ref.Name]
	if !ok {
		return MissingValue()
	}
	// Try to detect numeric values.
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return Value{Str: v, Num: f, IsNum: true}
	}
	return StrValue(v)
}

func (e *Evaluator) evalFuncCall(fc *FuncCall, row Row) (Value, error) {
	fn, ok := e.funcs[strings.ToLower(fc.Name)]
	if !ok {
		return Value{}, fmt.Errorf("unknown function: %s", fc.Name)
	}

	args := make([]Value, len(fc.Args))
	for i, argExpr := range fc.Args {
		val, err := e.Eval(argExpr, row)
		if err != nil {
			return Value{}, fmt.Errorf("evaluating argument %d of %s: %w", i, fc.Name, err)
		}
		args[i] = val
	}

	return fn(args)
}

func (e *Evaluator) evalArith(expr *ArithExpr, row Row) (Value, error) {
	left, err := e.Eval(expr.Left, row)
	if err != nil {
		return Value{}, err
	}
	right, err := e.Eval(expr.Right, row)
	if err != nil {
		return Value{}, err
	}

	lf, lok := left.ToNum()
	rf, rok := right.ToNum()
	if !lok || !rok {
		// If either side is missing, propagate missing.
		if left.Missing || right.Missing {
			return MissingValue(), nil
		}
		return Value{}, fmt.Errorf("arithmetic requires numeric operands: %q %s %q", left.Str, expr.Op, right.Str)
	}

	var result float64
	switch expr.Op {
	case ArithAdd:
		result = lf + rf
	case ArithSub:
		result = lf - rf
	case ArithMul:
		result = lf * rf
	case ArithDiv:
		if rf == 0 {
			return NumValue(math.NaN()), nil
		}
		result = lf / rf
	default:
		return Value{}, fmt.Errorf("unknown arithmetic operator: %v", expr.Op)
	}

	return NumValue(result), nil
}

// registerBuiltins adds the built-in scalar functions.
func (e *Evaluator) registerBuiltins() {
	e.funcs["tonumber"] = builtinToNumber
}

// builtinToNumber converts a value to a number.
// If already numeric, returns as-is. Otherwise parses the string.
func builtinToNumber(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("toNumber requires exactly 1 argument, got %d", len(args))
	}
	v := args[0]
	if v.Missing {
		return MissingValue(), nil
	}
	if v.IsNum {
		return v, nil
	}
	f, err := strconv.ParseFloat(v.Str, 64)
	if err != nil {
		return MissingValue(), nil // unparseable → missing (skip silently per design)
	}
	return NumValue(f), nil
}
