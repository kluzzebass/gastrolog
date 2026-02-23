# Scalar Functions

Scalar functions transform individual values within expressions. They work in [pipeline](help:pipeline) operators (`stats` arguments, `where` conditions) and directly in [filter expressions](help:query-language) as expression predicates — for example, `len(message) > 100` works as a standalone filter without needing a pipe.

Expressions also support arithmetic: `+`, `-`, `*`, `/`, `%` (modulo), with standard precedence. Unary `-` for negation. Parentheses for grouping.

## Math

| Function | Description |
|----------|-------------|
| `abs(x)` | Absolute value |
| `ceil(x)` | Round up to nearest integer |
| `floor(x)` | Round down to nearest integer |
| `round(x)` or `round(x, n)` | Round to n decimal places (default 0) |
| `sqrt(x)` | Square root |
| `pow(base, exp)` | Exponentiation |
| `log(x)` | Natural logarithm |
| `log10(x)` | Base-10 logarithm |
| `log2(x)` | Base-2 logarithm |
| `exp(x)` | e raised to the power x |

## String

| Function | Description |
|----------|-------------|
| `len(s)` | String length |
| `lower(s)` | Convert to lowercase |
| `upper(s)` | Convert to uppercase |
| `substr(s, start, length)` | Substring (0-indexed) |
| `replace(s, old, new)` | Replace all occurrences |
| `trim(s)` | Remove leading/trailing whitespace |
| `concat(a, b, ...)` | Concatenate values |

## Type & Control

| Function | Description |
|----------|-------------|
| `toNumber(x)` | Convert to number |
| `toString(x)` | Convert to string |
| `coalesce(a, b, ...)` | First non-missing value |
| `isnull(x)` | 1 if missing, 0 otherwise |
| `typeof(x)` | Returns "number", "string", or "missing" |

## Bitwise

| Function | Description |
|----------|-------------|
| `bitor(a, b)` | Bitwise OR |
| `bitand(a, b)` | Bitwise AND |
| `bitxor(a, b)` | Bitwise XOR |
| `bitnot(a)` | Bitwise NOT |
| `bitshl(a, n)` | Left shift |
| `bitshr(a, n)` | Right shift |
