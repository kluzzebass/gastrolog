package querylang

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ScalarFuncNames is the canonical list of scalar function names.
// These are the functions available in pipe expressions and filter expression
// predicates (PredExpr). Aggregation-only functions (count, avg, sum, etc.)
// are NOT included — they only make sense inside stats operators.
//
// This list is the single source of truth. It is used by:
//   - registerBuiltins() to register implementations
//   - knownScalarFuncs (parser) to detect expression predicates
//   - The frontend tokenizer mirrors this list in SCALAR_FUNCTIONS
var ScalarFuncNames = []string{
	// Type coercion.
	"tonumber", "tostring",
	// Math (1-arg).
	"abs", "ceil", "floor", "sqrt", "log", "log10", "log2", "exp",
	// Math (2-arg).
	"pow",
	// Math (1 or 2 arg).
	"round",
	// String.
	"len", "lower", "upper", "substr", "replace", "trim", "concat",
	// Control flow.
	"coalesce", "isnull", "typeof",
	// Bitwise.
	"bitor", "bitand", "bitxor", "bitnot", "bitshl", "bitshr",
}

// scalarFuncSet is the set form of ScalarFuncNames for O(1) lookup.
var scalarFuncSet = func() map[string]bool {
	m := make(map[string]bool, len(ScalarFuncNames))
	for _, n := range ScalarFuncNames {
		m[n] = true
	}
	return m
}()

// IsScalarFunc reports whether name (lowercase) is a known scalar function.
func IsScalarFunc(name string) bool {
	return scalarFuncSet[name]
}

// registerBuiltins adds the built-in scalar function implementations.
func (e *Evaluator) registerBuiltins() {
	// Type coercion.
	e.funcs["tonumber"] = builtinToNumber
	e.funcs["tostring"] = builtinToString

	// Math (1-arg).
	e.funcs["abs"] = mathFunc1("abs", math.Abs)
	e.funcs["ceil"] = mathFunc1("ceil", math.Ceil)
	e.funcs["floor"] = mathFunc1("floor", math.Floor)
	e.funcs["sqrt"] = mathFunc1("sqrt", math.Sqrt)
	e.funcs["log"] = mathFunc1("log", math.Log)
	e.funcs["log10"] = mathFunc1("log10", math.Log10)
	e.funcs["log2"] = mathFunc1("log2", math.Log2)
	e.funcs["exp"] = mathFunc1("exp", math.Exp)

	// Math (2-arg).
	e.funcs["pow"] = mathFunc2("pow", math.Pow)

	// Math (1 or 2 arg).
	e.funcs["round"] = builtinRound

	// String.
	e.funcs["len"] = builtinLen
	e.funcs["lower"] = builtinLower
	e.funcs["upper"] = builtinUpper
	e.funcs["substr"] = builtinSubstr
	e.funcs["replace"] = builtinReplace
	e.funcs["trim"] = builtinTrim
	e.funcs["concat"] = builtinConcat

	// Control flow.
	e.funcs["coalesce"] = builtinCoalesce
	e.funcs["isnull"] = builtinIsNull
	e.funcs["typeof"] = builtinTypeof

	// Bitwise.
	e.funcs["bitor"] = bitwiseFunc2("bitor", func(a, b int64) int64 { return a | b })
	e.funcs["bitand"] = bitwiseFunc2("bitand", func(a, b int64) int64 { return a & b })
	e.funcs["bitxor"] = bitwiseFunc2("bitxor", func(a, b int64) int64 { return a ^ b })
	e.funcs["bitnot"] = bitwiseFunc1("bitnot", func(a int64) int64 { return ^a })
	e.funcs["bitshl"] = bitwiseFunc2("bitshl", func(a, b int64) int64 { return a << uint(b) })
	e.funcs["bitshr"] = bitwiseFunc2("bitshr", func(a, b int64) int64 { return a >> uint(b) })
}

// --- Type coercion ---

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
		return MissingValue(), nil
	}
	return NumValue(f), nil
}

func builtinToString(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("toString requires exactly 1 argument, got %d", len(args))
	}
	v := args[0]
	if v.Missing {
		return MissingValue(), nil
	}
	return StrValue(v.Str), nil
}

// --- Math helpers ---

func mathFunc1(name string, fn func(float64) float64) ScalarFunc {
	return func(args []Value) (Value, error) {
		if len(args) != 1 {
			return Value{}, fmt.Errorf("%s requires exactly 1 argument, got %d", name, len(args))
		}
		v := args[0]
		if v.Missing {
			return MissingValue(), nil
		}
		n, ok := v.ToNum()
		if !ok {
			return MissingValue(), nil
		}
		return NumValue(fn(n)), nil
	}
}

func mathFunc2(name string, fn func(float64, float64) float64) ScalarFunc {
	return func(args []Value) (Value, error) {
		if len(args) != 2 {
			return Value{}, fmt.Errorf("%s requires exactly 2 arguments, got %d", name, len(args))
		}
		if args[0].Missing || args[1].Missing {
			return MissingValue(), nil
		}
		a, aOK := args[0].ToNum()
		b, bOK := args[1].ToNum()
		if !aOK || !bOK {
			return MissingValue(), nil
		}
		return NumValue(fn(a, b)), nil
	}
}

func builtinRound(args []Value) (Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return Value{}, fmt.Errorf("round requires 1 or 2 arguments, got %d", len(args))
	}
	if args[0].Missing {
		return MissingValue(), nil
	}
	n, ok := args[0].ToNum()
	if !ok {
		return MissingValue(), nil
	}
	decimals := 0
	if len(args) == 2 {
		if args[1].Missing {
			return MissingValue(), nil
		}
		d, dOK := args[1].ToNum()
		if !dOK {
			return MissingValue(), nil
		}
		decimals = int(d)
	}
	shift := math.Pow(10, float64(decimals))
	return NumValue(math.Round(n*shift) / shift), nil
}

// --- String functions ---

func builtinLen(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("len requires exactly 1 argument, got %d", len(args))
	}
	if args[0].Missing {
		return MissingValue(), nil
	}
	return NumValue(float64(len(args[0].Str))), nil
}

func builtinLower(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("lower requires exactly 1 argument, got %d", len(args))
	}
	if args[0].Missing {
		return MissingValue(), nil
	}
	return StrValue(strings.ToLower(args[0].Str)), nil
}

func builtinUpper(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("upper requires exactly 1 argument, got %d", len(args))
	}
	if args[0].Missing {
		return MissingValue(), nil
	}
	return StrValue(strings.ToUpper(args[0].Str)), nil
}

func builtinSubstr(args []Value) (Value, error) {
	if len(args) != 3 {
		return Value{}, fmt.Errorf("substr requires exactly 3 arguments, got %d", len(args))
	}
	if args[0].Missing {
		return MissingValue(), nil
	}
	s := args[0].Str
	start, ok1 := args[1].ToNum()
	length, ok2 := args[2].ToNum()
	if !ok1 || !ok2 {
		return MissingValue(), nil
	}
	si := int(start)
	li := int(length)
	if si < 0 {
		si = 0
	}
	if si >= len(s) {
		return StrValue(""), nil
	}
	end := si + li
	if end > len(s) {
		end = len(s)
	}
	return StrValue(s[si:end]), nil
}

func builtinReplace(args []Value) (Value, error) {
	if len(args) != 3 {
		return Value{}, fmt.Errorf("replace requires exactly 3 arguments, got %d", len(args))
	}
	if args[0].Missing {
		return MissingValue(), nil
	}
	return StrValue(strings.ReplaceAll(args[0].Str, args[1].Str, args[2].Str)), nil
}

func builtinTrim(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("trim requires exactly 1 argument, got %d", len(args))
	}
	if args[0].Missing {
		return MissingValue(), nil
	}
	return StrValue(strings.TrimSpace(args[0].Str)), nil
}

func builtinConcat(args []Value) (Value, error) {
	if len(args) == 0 {
		return Value{}, fmt.Errorf("concat requires at least 1 argument")
	}
	var sb strings.Builder
	for _, a := range args {
		if a.Missing {
			continue
		}
		sb.WriteString(a.Str)
	}
	return StrValue(sb.String()), nil
}

// --- Control flow ---

func builtinCoalesce(args []Value) (Value, error) {
	if len(args) == 0 {
		return Value{}, fmt.Errorf("coalesce requires at least 1 argument")
	}
	for _, a := range args {
		if !a.Missing {
			return a, nil
		}
	}
	return MissingValue(), nil
}

func builtinIsNull(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("isnull requires exactly 1 argument, got %d", len(args))
	}
	if args[0].Missing {
		return NumValue(1), nil
	}
	return NumValue(0), nil
}

func builtinTypeof(args []Value) (Value, error) {
	if len(args) != 1 {
		return Value{}, fmt.Errorf("typeof requires exactly 1 argument, got %d", len(args))
	}
	v := args[0]
	if v.Missing {
		return StrValue("missing"), nil
	}
	if v.IsNum {
		return StrValue("number"), nil
	}
	return StrValue("string"), nil
}

// --- Bitwise helpers ---

func bitwiseFunc1(name string, fn func(int64) int64) ScalarFunc {
	return func(args []Value) (Value, error) {
		if len(args) != 1 {
			return Value{}, fmt.Errorf("%s requires exactly 1 argument, got %d", name, len(args))
		}
		if args[0].Missing {
			return MissingValue(), nil
		}
		n, ok := args[0].ToNum()
		if !ok {
			return MissingValue(), nil
		}
		return NumValue(float64(fn(int64(n)))), nil
	}
}

func bitwiseFunc2(name string, fn func(int64, int64) int64) ScalarFunc {
	return func(args []Value) (Value, error) {
		if len(args) != 2 {
			return Value{}, fmt.Errorf("%s requires exactly 2 arguments, got %d", name, len(args))
		}
		if args[0].Missing || args[1].Missing {
			return MissingValue(), nil
		}
		a, aOK := args[0].ToNum()
		b, bOK := args[1].ToNum()
		if !aOK || !bOK {
			return MissingValue(), nil
		}
		return NumValue(float64(fn(int64(a), int64(b)))), nil
	}
}
