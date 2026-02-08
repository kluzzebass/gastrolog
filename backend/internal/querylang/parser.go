package querylang

// Parser parses a query string into an AST.
// This is the source of truth for query syntax. The frontend has a mirrored
// parser in frontend/src/queryTokenizer.ts (validate function) for syntax
// highlighting and error detection. Changes here must be reflected there.
//
// Grammar (EBNF):
//
//	query     = or_expr EOF
//	or_expr   = and_expr ( "OR" and_expr )*
//	and_expr  = unary_expr ( [ "AND" ] unary_expr )*
//	unary_expr = "NOT" unary_expr | primary
//	primary   = "(" or_expr ")" | predicate
//	predicate = kv_pred | token_pred
//	kv_pred   = ( WORD | "*" ) "=" ( WORD | "*" )
//	token_pred = WORD
//
// Precedence (highest to lowest):
//  1. Parentheses
//  2. NOT (prefix, right-associative)
//  3. AND (implicit or explicit)
//  4. OR
type parser struct {
	lex *Lexer
	cur Token
	err error // first error encountered
}

// Parse parses a query string into an AST.
func Parse(input string) (Expr, error) {
	p := &parser{lex: NewLexer(input)}

	// Prime the parser with the first token.
	if err := p.advance(); err != nil {
		return nil, err
	}

	// Check for empty query.
	if p.cur.Kind == TokEOF {
		return nil, newParseError(0, ErrEmptyQuery, "empty query")
	}

	expr, err := p.parseOrExpr()
	if err != nil {
		return nil, err
	}

	// Ensure we consumed all input.
	if p.cur.Kind != TokEOF {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "unexpected token: %s", p.cur.Lit)
	}

	return expr, nil
}

// advance moves to the next token.
func (p *parser) advance() error {
	tok, err := p.lex.Next()
	if err != nil {
		return err
	}
	p.cur = tok
	return nil
}

// parseOrExpr parses: or_expr = and_expr ( "OR" and_expr )*
func (p *parser) parseOrExpr() (Expr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.cur.Kind == TokOr {
		if err := p.advance(); err != nil {
			return nil, err
		}

		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}

		left = flattenOr(left, right)
	}

	return left, nil
}

// parseAndExpr parses: and_expr = unary_expr ( [ "AND" ] unary_expr )*
func (p *parser) parseAndExpr() (Expr, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	for p.isAndStart() {
		// Consume optional AND keyword.
		if p.cur.Kind == TokAnd {
			if err := p.advance(); err != nil {
				return nil, err
			}
		}

		right, err := p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}

		left = flattenAnd(left, right)
	}

	return left, nil
}

// isAndStart returns true if the current token could start another unary_expr
// in an implicit AND sequence. This does NOT include TokAnd itself, which is
// handled explicitly in parseAndExpr.
func (p *parser) isAndStart() bool {
	switch p.cur.Kind {
	case TokAnd:
		// Explicit AND
		return true
	case TokNot, TokLParen, TokWord, TokStar:
		// Could start a unary_expr (implicit AND)
		return true
	default:
		return false
	}
}

// parseUnaryExpr parses: unary_expr = "NOT" unary_expr | primary
func (p *parser) parseUnaryExpr() (Expr, error) {
	if p.cur.Kind == TokNot {
		pos := p.cur.Pos
		if err := p.advance(); err != nil {
			return nil, err
		}

		// Check for something after NOT.
		if p.cur.Kind == TokEOF {
			return nil, newParseError(pos, ErrUnexpectedEOF, "expected expression after NOT")
		}
		if p.cur.Kind == TokOr || p.cur.Kind == TokAnd || p.cur.Kind == TokRParen {
			return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected expression after NOT, got %s", p.cur.Kind)
		}

		term, err := p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}

		return &NotExpr{Term: term}, nil
	}

	return p.parsePrimary()
}

// parsePrimary parses: primary = "(" or_expr ")" | predicate
func (p *parser) parsePrimary() (Expr, error) {
	if p.cur.Kind == TokLParen {
		openPos := p.cur.Pos
		if err := p.advance(); err != nil {
			return nil, err
		}

		// Check for empty parens.
		if p.cur.Kind == TokRParen {
			return nil, newParseError(openPos, ErrEmptyQuery, "empty parentheses")
		}

		expr, err := p.parseOrExpr()
		if err != nil {
			return nil, err
		}

		if p.cur.Kind != TokRParen {
			return nil, newParseError(openPos, ErrUnmatchedParen, "unmatched opening parenthesis")
		}
		if err := p.advance(); err != nil {
			return nil, err
		}

		return expr, nil
	}

	return p.parsePredicate()
}

// parsePredicate parses: predicate = kv_pred | token_pred
// kv_pred = ( WORD | "*" ) "=" ( WORD | "*" )
// token_pred = WORD
func (p *parser) parsePredicate() (Expr, error) {
	// Handle unexpected tokens.
	switch p.cur.Kind {
	case TokEOF:
		return nil, newParseError(p.cur.Pos, ErrUnexpectedEOF, "unexpected end of query")
	case TokOr, TokAnd:
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "unexpected keyword %s", p.cur.Lit)
	case TokRParen:
		return nil, newParseError(p.cur.Pos, ErrUnmatchedParen, "unmatched closing parenthesis")
	case TokEq:
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "unexpected '='")
	}

	// First part: WORD or "*"
	if p.cur.Kind != TokWord && p.cur.Kind != TokStar {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected word or '*', got %s", p.cur.Kind)
	}

	first := p.cur
	if err := p.advance(); err != nil {
		return nil, err
	}

	// Check for "=" to distinguish kv_pred from token_pred.
	if p.cur.Kind != TokEq {
		// token_pred: just a word (not allowed to be "*" alone)
		if first.Kind == TokStar {
			return nil, newParseError(first.Pos, ErrUnexpectedToken, "'*' must be followed by '='")
		}
		return &PredicateExpr{Kind: PredToken, Value: first.Lit}, nil
	}

	// kv_pred: consume "="
	if err := p.advance(); err != nil {
		return nil, err
	}

	// Second part: WORD or "*"
	if p.cur.Kind != TokWord && p.cur.Kind != TokStar {
		return nil, newParseError(p.cur.Pos, ErrUnexpectedToken, "expected word or '*' after '=', got %s", p.cur.Kind)
	}

	second := p.cur
	if err := p.advance(); err != nil {
		return nil, err
	}

	// Determine predicate kind based on wildcards.
	switch {
	case first.Kind == TokStar && second.Kind == TokStar:
		// *=* is nonsensical but could be allowed; for now, treat as error.
		return nil, newParseError(first.Pos, ErrUnexpectedToken, "'*=*' is not a valid predicate")
	case first.Kind == TokStar:
		// *=value
		return &PredicateExpr{Kind: PredValueExists, Value: second.Lit}, nil
	case second.Kind == TokStar:
		// key=*
		return &PredicateExpr{Kind: PredKeyExists, Key: first.Lit}, nil
	default:
		// key=value
		return &PredicateExpr{Kind: PredKV, Key: first.Lit, Value: second.Lit}, nil
	}
}
