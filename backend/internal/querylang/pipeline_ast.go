package querylang

import (
	"fmt"
	"strings"
)

// Pipeline represents a parsed query with optional pipe operators.
// If Pipes is empty, the query is a filter-only query (existing behavior).
type Pipeline struct {
	Filter Expr   // the filter expression (left of first |); nil for pipe-only queries
	Pipes  []PipeOp // pipe operators in order (right of |)
}

// String returns a human-readable representation of the pipeline.
func (p *Pipeline) String() string {
	var parts []string
	if p.Filter != nil {
		parts = append(parts, p.Filter.String())
	}
	for _, op := range p.Pipes {
		parts = append(parts, op.String())
	}
	return strings.Join(parts, " | ")
}

// PipeOp is the interface for pipe operators (stats, where, etc.).
type PipeOp interface {
	pipeOp()
	String() string
}

// StatsOp represents: stats agg_list (by group_list)?
type StatsOp struct {
	Aggs   []AggExpr
	Groups []GroupExpr
}

func (StatsOp) pipeOp() {}

func (s *StatsOp) String() string {
	var parts []string
	parts = append(parts, "stats")

	aggStrs := make([]string, len(s.Aggs))
	for i, a := range s.Aggs {
		aggStrs[i] = a.String()
	}
	parts = append(parts, strings.Join(aggStrs, ", "))

	if len(s.Groups) > 0 {
		groupStrs := make([]string, len(s.Groups))
		for i, g := range s.Groups {
			groupStrs[i] = g.String()
		}
		parts = append(parts, "by")
		parts = append(parts, strings.Join(groupStrs, ", "))
	}

	return strings.Join(parts, " ")
}

// WhereOp represents: where filter_expr
type WhereOp struct {
	Expr Expr // reuses the filter expression AST
}

func (WhereOp) pipeOp() {}

func (w *WhereOp) String() string {
	return "where " + w.Expr.String()
}

// AggExpr represents an aggregation expression: count or func(expr) [as alias].
type AggExpr struct {
	Func  string     // aggregate function name: "count", "avg", "sum", "min", "max"
	Arg   PipeExpr   // argument expression; nil for bare "count"
	Alias string     // optional alias from "as"; empty if not specified
}

// String returns a human-readable representation.
func (a *AggExpr) String() string {
	var s string
	if a.Arg == nil {
		s = a.Func
	} else {
		s = fmt.Sprintf("%s(%s)", a.Func, a.Arg.String())
	}
	if a.Alias != "" {
		s += " as " + a.Alias
	}
	return s
}

// DefaultAlias returns the alias for this aggregation, using the default naming
// convention if no explicit alias was given.
func (a *AggExpr) DefaultAlias() string {
	if a.Alias != "" {
		return a.Alias
	}
	if a.Arg == nil {
		return a.Func // "count"
	}
	// For func(field), use "func_field" if arg is a simple field reference.
	if ref, ok := a.Arg.(*FieldRef); ok {
		return a.Func + "_" + ref.Name
	}
	// Complex expressions: just use the function name.
	return a.Func
}

// GroupExpr represents a group-by expression: a field name or bin(duration[, field]).
type GroupExpr struct {
	Field *FieldRef // simple field reference; nil if Bin is set
	Bin   *BinExpr  // time bucketing; nil if Field is set
}

// String returns a human-readable representation.
func (g *GroupExpr) String() string {
	if g.Bin != nil {
		return g.Bin.String()
	}
	return g.Field.Name
}

// BinExpr represents: bin(duration[, field]).
type BinExpr struct {
	Duration string    // raw duration string, e.g. "5m", "1h", "30s"
	Field    *FieldRef // optional time field; nil means default (WriteTS)
}

// String returns a human-readable representation.
func (b *BinExpr) String() string {
	if b.Field != nil {
		return fmt.Sprintf("bin(%s, %s)", b.Duration, b.Field.Name)
	}
	return fmt.Sprintf("bin(%s)", b.Duration)
}

// EvalOp represents: eval field = expr (, field = expr)*
type EvalOp struct {
	Assignments []EvalAssignment
}

// EvalAssignment is a single field = expression assignment.
type EvalAssignment struct {
	Field string
	Expr  PipeExpr
}

func (EvalOp) pipeOp() {}

func (e *EvalOp) String() string {
	parts := make([]string, len(e.Assignments))
	for i, a := range e.Assignments {
		parts[i] = a.Field + " = " + a.Expr.String()
	}
	return "eval " + strings.Join(parts, ", ")
}

// SortOp represents: sort [-]field (, [-]field)*
type SortOp struct {
	Fields []SortField
}

// SortField is a single sort field with optional descending flag.
type SortField struct {
	Name string
	Desc bool
}

func (SortOp) pipeOp() {}

func (s *SortOp) String() string {
	parts := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		if f.Desc {
			parts[i] = "-" + f.Name
		} else {
			parts[i] = f.Name
		}
	}
	return "sort " + strings.Join(parts, ", ")
}

// HeadOp represents: head N
type HeadOp struct {
	N int
}

func (HeadOp) pipeOp() {}

func (h *HeadOp) String() string {
	return fmt.Sprintf("head %d", h.N)
}

// TailOp represents: tail N
type TailOp struct {
	N int
}

func (TailOp) pipeOp() {}

func (t *TailOp) String() string {
	return fmt.Sprintf("tail %d", t.N)
}

// SliceOp represents: slice START END (1-indexed, inclusive)
type SliceOp struct {
	Start int // first row to include (1-indexed)
	End   int // last row to include (1-indexed, inclusive)
}

func (SliceOp) pipeOp() {}

func (s *SliceOp) String() string {
	return fmt.Sprintf("slice %d %d", s.Start, s.End)
}

// RenameOp represents: rename old as new (, old as new)*
type RenameOp struct {
	Renames []RenameMapping
}

// RenameMapping is a single old → new rename.
type RenameMapping struct {
	Old string
	New string
}

func (RenameOp) pipeOp() {}

func (r *RenameOp) String() string {
	parts := make([]string, len(r.Renames))
	for i, m := range r.Renames {
		parts[i] = m.Old + " as " + m.New
	}
	return "rename " + strings.Join(parts, ", ")
}

// FieldsOp represents: fields [-] field (, field)*
// If Drop is true, the listed fields are removed; otherwise only they are kept.
type FieldsOp struct {
	Names []string
	Drop  bool
}

func (FieldsOp) pipeOp() {}

func (f *FieldsOp) String() string {
	prefix := "fields "
	if f.Drop {
		prefix = "fields - "
	}
	return prefix + strings.Join(f.Names, ", ")
}

// RawOp represents: raw
// Forces the pipeline result into a flat table — no charts, no single-value display.
// For non-aggregating pipelines, converts records to a table.
// For post-stats pipelines, forces resultType to "table".
type RawOp struct{}

func (RawOp) pipeOp() {}

func (RawOp) String() string { return "raw" }

// PipeExpr is the interface for expressions used in pipe operators.
// These are distinct from filter Expr — they represent computed values,
// not boolean search predicates.
type PipeExpr interface {
	pipeExpr()
	String() string
}

// FieldRef references a field by name.
type FieldRef struct {
	Name string
}

func (FieldRef) pipeExpr() {}

func (f *FieldRef) String() string {
	return f.Name
}

// NumberLit is a numeric literal.
type NumberLit struct {
	Value string // raw string representation (preserves precision)
}

func (NumberLit) pipeExpr() {}

func (n *NumberLit) String() string {
	return n.Value
}

// StringLit is a string literal (from quoted strings).
type StringLit struct {
	Value string
}

func (StringLit) pipeExpr() {}

func (s *StringLit) String() string {
	return fmt.Sprintf("%q", s.Value)
}

// FuncCall represents a function call: name(args...).
type FuncCall struct {
	Name string
	Args []PipeExpr
}

func (FuncCall) pipeExpr() {}

func (f *FuncCall) String() string {
	argStrs := make([]string, len(f.Args))
	for i, a := range f.Args {
		argStrs[i] = a.String()
	}
	return fmt.Sprintf("%s(%s)", f.Name, strings.Join(argStrs, ", "))
}

// ArithOp identifies an arithmetic operator.
type ArithOp int

const (
	ArithAdd ArithOp = iota // +
	ArithSub                // -
	ArithMul                // *
	ArithDiv                // /
	ArithMod                // %
)

func (op ArithOp) String() string {
	switch op {
	case ArithAdd:
		return "+"
	case ArithSub:
		return "-"
	case ArithMul:
		return "*"
	case ArithDiv:
		return "/"
	case ArithMod:
		return "%"
	default:
		return "?"
	}
}

// ArithExpr represents a binary arithmetic expression: left op right.
type ArithExpr struct {
	Left  PipeExpr
	Op    ArithOp
	Right PipeExpr
}

func (ArithExpr) pipeExpr() {}

func (a *ArithExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", a.Left.String(), a.Op, a.Right.String())
}

// UnaryExpr represents a unary expression: -expr.
type UnaryExpr struct {
	Op   ArithOp
	Expr PipeExpr
}

func (UnaryExpr) pipeExpr() {}

func (u *UnaryExpr) String() string {
	return fmt.Sprintf("(%s%s)", u.Op, u.Expr.String())
}
