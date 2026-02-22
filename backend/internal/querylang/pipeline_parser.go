package querylang

import "strings"

// Pipeline parser for pipe-based query transformation.
//
// Grammar (extending the existing filter grammar):
//
//	pipeline      = filter_expr ( "|" pipe_op )*
//	pipe_op       = stats_op | where_op
//	stats_op      = "stats" agg_list ( "by" group_list )?
//	agg_list      = agg_expr ( "," agg_expr )*
//	agg_expr      = "count" ( "as" IDENT )?
//	              | IDENT "(" expr ")" ( "as" IDENT )?
//	group_list    = group_expr ( "," group_expr )*
//	group_expr    = "bin" "(" DURATION [ "," IDENT ] ")" | IDENT
//	where_op      = "where" filter_expr
//
//	expr          = add_expr
//	add_expr      = mul_expr ( ( "+" | "-" ) mul_expr )*
//	mul_expr      = unary_expr ( ( "*" | "/" ) unary_expr )*
//	unary_expr    = primary_expr
//	primary_expr  = IDENT "(" expr_list ")"    -- function call
//	              | IDENT                       -- field reference
//	              | NUMBER                      -- numeric literal
//	              | STRING                      -- string literal
//	              | "(" expr ")"               -- grouping

// ParsePipeline parses a query string into a Pipeline.
// If the query has no pipe operators, Pipeline.Pipes will be empty.
func ParsePipeline(input string) (*Pipeline, error) {
	p := &parser{lex: NewLexer(input)}

	if err := p.advance(); err != nil {
		return nil, err
	}

	// Check for empty query.
	if p.cur.Kind == TokEOF {
		return nil, newParseError(0, ErrEmptyQuery, "empty query")
	}

	// Check if the query starts with a pipe (no filter expression).
	if p.cur.Kind == TokPipe {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "query must start with a filter expression before '|'")
	}

	// Parse the filter expression.
	filter, err := p.parseOrExpr()
	if err != nil {
		return nil, err
	}

	pipeline := &Pipeline{Filter: filter}

	// Parse pipe operators.
	for p.cur.Kind == TokPipe {
		if err := p.advance(); err != nil {
			return nil, err
		}

		// Switch lexer to pipe mode (/ becomes division, - becomes minus).
		p.lex.SetPipeMode(true)

		op, err := p.parsePipeOp()
		if err != nil {
			return nil, err
		}
		pipeline.Pipes = append(pipeline.Pipes, op)

		// Switch back to filter mode before checking for next pipe.
		// This way, if there's another pipe, the filter parser in where
		// has already finished and we're back to normal.
		p.lex.SetPipeMode(false)
	}

	if p.cur.Kind != TokEOF {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "unexpected token: %s", p.cur.Lit)
	}

	return pipeline, nil
}

// parsePipeOp parses a single pipe operator.
// The lexer is already in pipe mode and the leading '|' has been consumed.
func (p *parser) parsePipeOp() (PipeOp, error) {
	if p.cur.Kind == TokEOF {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedEOF, "expected pipe operator after '|'")
	}

	if p.cur.Kind != TokWord {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected pipe operator name, got %s", p.cur.Kind)
	}

	switch strings.ToLower(p.cur.Lit) {
	case "stats":
		return p.parseStatsOp()
	case "where":
		return p.parseWhereOp()
	default:
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "unknown pipe operator: %s", p.cur.Lit)
	}
}

// parseStatsOp parses: "stats" agg_list ( "by" group_list )?
func (p *parser) parseStatsOp() (*StatsOp, error) {
	if err := p.advance(); err != nil { // consume "stats"
		return nil, err
	}

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}

	if len(aggs) == 0 {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "stats requires at least one aggregation")
	}

	// Check for duplicate default aliases.
	if err := checkDuplicateAliases(aggs); err != nil {
		return nil, err
	}

	op := &StatsOp{Aggs: aggs}

	// Check for "by" clause.
	if p.cur.Kind == TokWord && strings.ToLower(p.cur.Lit) == "by" {
		if err := p.advance(); err != nil { // consume "by"
			return nil, err
		}
		groups, err := p.parseGroupList()
		if err != nil {
			return nil, err
		}
		if len(groups) == 0 {
			return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "'by' requires at least one group expression")
		}
		op.Groups = groups
	}

	return op, nil
}

// parseAggList parses: agg_expr ( "," agg_expr )*
func (p *parser) parseAggList() ([]AggExpr, error) {
	var aggs []AggExpr

	agg, err := p.parseAggExpr()
	if err != nil {
		return nil, err
	}
	aggs = append(aggs, *agg)

	for p.cur.Kind == TokComma {
		if err := p.advance(); err != nil { // consume ","
			return nil, err
		}
		agg, err := p.parseAggExpr()
		if err != nil {
			return nil, err
		}
		aggs = append(aggs, *agg)
	}

	return aggs, nil
}

// parseAggExpr parses: "count" ("as" IDENT)? | IDENT "(" expr ")" ("as" IDENT)?
func (p *parser) parseAggExpr() (*AggExpr, error) {
	if p.cur.Kind != TokWord {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected aggregation function, got %s", p.cur.Kind)
	}

	funcName := strings.ToLower(p.cur.Lit)
	pos := p.cur.Pos

	if err := p.advance(); err != nil {
		return nil, err
	}

	// "count" can appear without parentheses.
	if funcName == "count" && p.cur.Kind != TokLParen {
		agg := &AggExpr{Func: "count"}
		alias, err := p.parseOptionalAlias()
		if err != nil {
			return nil, err
		}
		agg.Alias = alias
		return agg, nil
	}

	// func(expr) or func()
	if p.cur.Kind != TokLParen {
		return nil, newParseError(pos, ErrUnexpectedToken, "expected '(' after aggregation function '%s'", funcName)
	}
	if err := p.advance(); err != nil { // consume "("
		return nil, err
	}

	// Handle empty argument list: func() — treated same as bare func name.
	agg := &AggExpr{Func: funcName}
	if p.cur.Kind != TokRParen {
		arg, err := p.parsePipeExpr()
		if err != nil {
			return nil, err
		}
		agg.Arg = arg
	}

	if p.cur.Kind != TokRParen {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected ')' after aggregation argument, got %s", p.cur.Kind)
	}
	if err := p.advance(); err != nil { // consume ")"
		return nil, err
	}
	alias, err := p.parseOptionalAlias()
	if err != nil {
		return nil, err
	}
	agg.Alias = alias

	return agg, nil
}

// parseOptionalAlias parses: ( "as" IDENT )?
func (p *parser) parseOptionalAlias() (string, error) {
	if p.cur.Kind != TokWord || strings.ToLower(p.cur.Lit) != "as" {
		return "", nil
	}
	if err := p.advance(); err != nil { // consume "as"
		return "", err
	}
	if p.cur.Kind != TokWord {
		return "", newParseError(p.cur.Pos, ErrUnexpectedToken, "expected alias name after 'as', got %s", p.cur.Kind)
	}
	alias := p.cur.Lit
	if err := p.advance(); err != nil {
		return "", err
	}
	return alias, nil
}

// parseGroupList parses: group_expr ( "," group_expr )*
func (p *parser) parseGroupList() ([]GroupExpr, error) {
	var groups []GroupExpr

	group, err := p.parseGroupExpr()
	if err != nil {
		return nil, err
	}
	groups = append(groups, *group)

	for p.cur.Kind == TokComma {
		if err := p.advance(); err != nil { // consume ","
			return nil, err
		}
		group, err := p.parseGroupExpr()
		if err != nil {
			return nil, err
		}
		groups = append(groups, *group)
	}

	return groups, nil
}

// parseGroupExpr parses: "bin" "(" DURATION [ "," IDENT ] ")" | IDENT
func (p *parser) parseGroupExpr() (*GroupExpr, error) {
	if p.cur.Kind != TokWord {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected field name or 'bin', got %s", p.cur.Kind)
	}

	if strings.ToLower(p.cur.Lit) == "bin" {
		return p.parseBinGroupExpr()
	}

	// Simple field reference.
	name := p.cur.Lit
	if err := p.advance(); err != nil {
		return nil, err
	}
	return &GroupExpr{Field: &FieldRef{Name: name}}, nil
}

// parseBinGroupExpr parses: "bin" "(" DURATION [ "," IDENT ] ")"
func (p *parser) parseBinGroupExpr() (*GroupExpr, error) {
	if err := p.advance(); err != nil { // consume "bin"
		return nil, err
	}

	if p.cur.Kind != TokLParen {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected '(' after 'bin', got %s", p.cur.Kind)
	}
	if err := p.advance(); err != nil { // consume "("
		return nil, err
	}

	// Duration argument (parsed as a bareword, e.g. "5m", "1h").
	if p.cur.Kind != TokWord {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected duration in bin(), got %s", p.cur.Kind)
	}

	duration := p.cur.Lit
	if err := p.advance(); err != nil {
		return nil, err
	}

	bin := &BinExpr{Duration: duration}

	// Optional field argument.
	if p.cur.Kind == TokComma {
		if err := p.advance(); err != nil { // consume ","
			return nil, err
		}
		if p.cur.Kind != TokWord {
			return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected field name in bin(), got %s", p.cur.Kind)
		}
		bin.Field = &FieldRef{Name: p.cur.Lit}
		if err := p.advance(); err != nil {
			return nil, err
		}
	}

	if p.cur.Kind != TokRParen {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected ')' to close bin(), got %s", p.cur.Kind)
	}
	if err := p.advance(); err != nil { // consume ")"
		return nil, err
	}

	return &GroupExpr{Bin: bin}, nil
}

// parseWhereOp parses: "where" filter_expr
// Switches the lexer back to filter mode so the filter expression parser
// handles regex literals and bareword hyphens correctly.
func (p *parser) parseWhereOp() (*WhereOp, error) {
	// Switch back to filter mode BEFORE consuming "where" so the next
	// token is lexed in filter mode (e.g. '/' scans as regex, not division).
	p.lex.SetPipeMode(false)

	if err := p.advance(); err != nil { // consume "where"
		return nil, err
	}

	if p.cur.Kind == TokEOF || p.cur.Kind == TokPipe {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected expression after 'where'")
	}

	expr, err := p.parseOrExpr()
	if err != nil {
		return nil, err
	}

	// Restore pipe mode for subsequent pipe operators.
	p.lex.SetPipeMode(true)

	return &WhereOp{Expr: expr}, nil
}

// --- Pipe expression parser ---
//
// Recursive descent with operator precedence:
//   expr         = add_expr
//   add_expr     = mul_expr ( ("+" | "-") mul_expr )*
//   mul_expr     = unary_expr ( ("*" | "/") unary_expr )*
//   unary_expr   = primary_expr
//   primary_expr = IDENT "(" expr_list ")" | IDENT | NUMBER | STRING | "(" expr ")"

// parsePipeExpr parses a pipe expression.
func (p *parser) parsePipeExpr() (PipeExpr, error) {
	return p.parseAddExpr()
}

// parseAddExpr parses: mul_expr ( ("+" | "-") mul_expr )*
func (p *parser) parseAddExpr() (PipeExpr, error) {
	left, err := p.parseMulExpr()
	if err != nil {
		return nil, err
	}

	for p.cur.Kind == TokPlus || p.cur.Kind == TokMinus {
		op := ArithAdd
		if p.cur.Kind == TokMinus {
			op = ArithSub
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseMulExpr()
		if err != nil {
			return nil, err
		}
		left = &ArithExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

// parseMulExpr parses: unary_expr ( ("*" | "/") unary_expr )*
func (p *parser) parseMulExpr() (PipeExpr, error) {
	left, err := p.parseUnaryPipeExpr()
	if err != nil {
		return nil, err
	}

	for p.cur.Kind == TokStar || p.cur.Kind == TokSlash {
		op := ArithMul
		if p.cur.Kind == TokSlash {
			op = ArithDiv
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseUnaryPipeExpr()
		if err != nil {
			return nil, err
		}
		left = &ArithExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

// parseUnaryPipeExpr parses: primary_expr (no unary operators in v1).
func (p *parser) parseUnaryPipeExpr() (PipeExpr, error) {
	return p.parsePrimaryPipeExpr()
}

// parsePrimaryPipeExpr parses: IDENT "(" expr_list ")" | IDENT | NUMBER | STRING | "(" expr ")"
func (p *parser) parsePrimaryPipeExpr() (PipeExpr, error) {
	switch p.cur.Kind {
	case TokWord:
		name := p.cur.Lit

		// Check if this is a number (digits, possibly with decimal point).
		if isNumericLiteral(name) {
			if err := p.advance(); err != nil {
				return nil, err
			}
			return &NumberLit{Value: name}, nil
		}

		if err := p.advance(); err != nil {
			return nil, err
		}

		// Check for function call: IDENT "("
		if p.cur.Kind == TokLParen {
			return p.parseFuncCall(name)
		}

		// Simple field reference.
		return &FieldRef{Name: name}, nil

	case TokLParen:
		// Grouped expression: "(" expr ")"
		if err := p.advance(); err != nil { // consume "("
			return nil, err
		}
		expr, err := p.parsePipeExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Kind != TokRParen {
			return nil, newParseError(p.cur.Pos, ErrUnmatchedParen, "expected ')' in expression")
		}
		if err := p.advance(); err != nil { // consume ")"
			return nil, err
		}
		return expr, nil

	default:
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected expression, got %s", p.cur.Kind)
	}
}

// parseFuncCall parses the argument list after IDENT "(": expr_list ")".
// The opening "(" has NOT been consumed yet.
func (p *parser) parseFuncCall(name string) (PipeExpr, error) {
	if err := p.advance(); err != nil { // consume "("
		return nil, err
	}

	// Handle empty argument list: func().
	if p.cur.Kind == TokRParen {
		if err := p.advance(); err != nil { // consume ")"
			return nil, err
		}
		return &FuncCall{Name: name}, nil
	}

	var args []PipeExpr
	arg, err := p.parsePipeExpr()
	if err != nil {
		return nil, err
	}
	args = append(args, arg)

	for p.cur.Kind == TokComma {
		if err := p.advance(); err != nil { // consume ","
			return nil, err
		}
		arg, err := p.parsePipeExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	if p.cur.Kind != TokRParen {
		return nil, newParseError(p.cur.Pos, ErrUnmatchedParen, "expected ')' after function arguments")
	}
	if err := p.advance(); err != nil { // consume ")"
		return nil, err
	}

	return &FuncCall{Name: name, Args: args}, nil
}

// isNumericLiteral returns true if the string looks like a numeric literal.
// Handles integers and decimals (e.g. "42", "3.14", "0.5").
func isNumericLiteral(s string) bool {
	if len(s) == 0 {
		return false
	}
	hasDot := false
	for i, ch := range s {
		if ch == '.' {
			if hasDot {
				return false // multiple dots
			}
			hasDot = true
			continue
		}
		if ch < '0' || ch > '9' {
			// Allow leading minus for negative numbers.
			if ch == '-' && i == 0 && len(s) > 1 {
				continue
			}
			return false
		}
	}
	return true
}

// checkDuplicateAliases validates that no two aggregations produce the same
// output column name (considering both explicit aliases and default names).
func checkDuplicateAliases(aggs []AggExpr) error {
	seen := make(map[string]bool)
	for _, agg := range aggs {
		name := agg.DefaultAlias()
		if seen[name] {
			return newParseError(0, ErrUnexpectedToken,
				"duplicate aggregation output name %q; use 'as' to provide distinct aliases", name)
		}
		seen[name] = true
	}
	return nil
}
