package querylang

import "strings"

// SpanRole identifies a syntax highlighting role.
type SpanRole string

const (
	RoleOperator     SpanRole = "operator"
	RoleDirectiveKey SpanRole = "directive-key"
	RoleKey          SpanRole = "key"
	RoleEq           SpanRole = "eq"
	RoleCompareOp    SpanRole = "compare-op"
	RoleValue        SpanRole = "value"
	RoleToken        SpanRole = "token"
	RoleQuoted       SpanRole = "quoted"
	RoleGlob         SpanRole = "glob"
	RoleRegex        SpanRole = "regex"
	RoleParen        SpanRole = "paren"
	RoleStar         SpanRole = "star"
	RolePipe         SpanRole = "pipe"
	RolePipeKeyword  SpanRole = "pipe-keyword"
	RoleFunction     SpanRole = "function"
	RoleComma        SpanRole = "comma"
	RoleComment      SpanRole = "comment"
	RoleWhitespace   SpanRole = "whitespace"
	RoleError        SpanRole = "error"
)

// Span is a highlighted text span.
type Span struct {
	Text string
	Role SpanRole
}

// rawToken is an intermediate token from the lexer with raw source text preserved.
type rawToken struct {
	text string
	kind string // "whitespace", "comment", or lexer token kind string
	tok  Token  // original lexer token (zero value for whitespace/comment)
}

// pipeKeywordSet contains all recognized pipe operator keywords.
var pipeKeywordSet = map[string]bool{
	"stats": true, "where": true, "eval": true, "sort": true,
	"head": true, "tail": true, "slice": true, "rename": true,
	"fields": true, "timechart": true, "raw": true, "lookup": true,
	"barchart": true, "donut": true, "map": true,
}

// aggFuncSet contains aggregation function names.
var aggFuncSet = map[string]bool{
	"count": true, "avg": true, "sum": true, "min": true, "max": true, "bin": true,
}

// Highlight produces syntax highlighting spans and a pipeline flag for the input.
// errorOffset < 0 means valid; >= 0 marks that position onward as error.
func Highlight(input string, errorOffset int) ([]Span, bool) {
	if len(input) == 0 {
		return nil, false
	}

	raw := lexHighlight(input)
	spans, hasPipeline := classify(raw, input)

	if errorOffset >= 0 {
		spans = applyError(spans, errorOffset)
	}

	return spans, hasPipeline
}

// consumeGaps scans whitespace and comment runs starting at pos,
// appending them to tokens. Returns the new position.
func consumeGaps(input string, pos int, tokens *[]rawToken) int {
	for pos < len(input) {
		ch := input[pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			wsStart := pos
			for pos < len(input) {
				c := input[pos]
				if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
					break
				}
				pos++
			}
			*tokens = append(*tokens, rawToken{text: input[wsStart:pos], kind: "whitespace"})
			continue
		}
		if ch == '#' {
			commentStart := pos
			for pos < len(input) && input[pos] != '\n' {
				pos++
			}
			*tokens = append(*tokens, rawToken{text: input[commentStart:pos], kind: "comment"})
			continue
		}
		break
	}
	return pos
}

// lexHighlight lexes the input, capturing whitespace and comments that the
// normal lexer skips, so that concatenating all raw token texts reproduces
// the original input.
func lexHighlight(input string) []rawToken {
	l := NewLexer(input)
	var tokens []rawToken
	lastEnd := 0

	for {
		beforePos := l.Pos()
		lastEnd = consumeGaps(input, lastEnd, &tokens)

		// Sync the lexer past whitespace/comments we already consumed.
		if lastEnd != beforePos {
			l.Restore(LexerState{pos: lastEnd, pipeMode: l.Save().pipeMode})
		}

		tok, err := l.Next()
		if err != nil {
			// Lexer error (unterminated string/regex): emit rest as error.
			if lastEnd < len(input) {
				tokens = append(tokens, rawToken{
					text: input[lastEnd:],
					kind: "error",
				})
			}
			break
		}

		if tok.Kind == TokEOF {
			break
		}

		// Toggle pipe mode after seeing a pipe, same as the parser does.
		if tok.Kind == TokPipe {
			l.SetPipeMode(true)
		}

		// Raw text is input[tok.Pos : l.Pos()] — includes quotes/delimiters.
		rawText := input[tok.Pos:l.Pos()]

		tokens = append(tokens, rawToken{
			text: rawText,
			kind: tok.Kind.String(),
			tok:  tok,
		})
		lastEnd = l.Pos()
	}

	return tokens
}

// classify assigns highlight roles to raw tokens based on context.
func classify(tokens []rawToken, _ string) ([]Span, bool) {
	// Split tokens into segments by pipe.
	type segment struct {
		tokens []rawToken
		isPipe bool // true for the pipe token itself
	}

	var segments []segment
	var current []rawToken
	hasPipeline := false

	for _, t := range tokens {
		if t.kind == "|" {
			hasPipeline = true
			if len(current) > 0 {
				segments = append(segments, segment{tokens: current})
				current = nil
			}
			segments = append(segments, segment{tokens: []rawToken{t}, isPipe: true})
		} else {
			current = append(current, t)
		}
	}
	if len(current) > 0 {
		segments = append(segments, segment{tokens: current})
	}

	var spans []Span
	seenPipe := false

	for _, seg := range segments {
		if seg.isPipe {
			spans = append(spans, Span{Text: seg.tokens[0].text, Role: RolePipe})
			seenPipe = true
			continue
		}

		if !seenPipe {
			spans = append(spans, classifyFilter(seg.tokens)...)
		} else {
			spans = append(spans, classifyPipeSegment(seg.tokens)...)
		}
	}

	return spans, hasPipeline
}

// classifyFilter assigns roles to tokens in the filter portion (before any pipe).
func classifyFilter(tokens []rawToken) []Span {
	spans := make([]Span, len(tokens))

	for i, t := range tokens {
		spans[i] = Span{Text: t.text, Role: defaultRole(t)}
	}

	// Post-pass: detect key op value triples.
	// We skip whitespace tokens when looking for patterns but assign roles to them.
	nonWS := nonWhitespaceIndices(tokens)

	for j := 0; j+2 < len(nonWS); j++ {
		ki := nonWS[j]
		oi := nonWS[j+1]
		vi := nonWS[j+2]

		keyTok := tokens[ki]
		opTok := tokens[oi]
		valTok := tokens[vi]

		// Check for key = value or key compare-op value.
		isEq := opTok.kind == "="
		isCompare := opTok.kind == "!=" || opTok.kind == ">" || opTok.kind == ">=" || opTok.kind == "<" || opTok.kind == "<="

		if (isEq || isCompare) && (keyTok.kind == "WORD" || keyTok.kind == "GLOB") {
			keyName := strings.ToLower(keyTok.tok.Lit)

			// Key role.
			if directiveKeys[keyName] {
				spans[ki].Role = RoleDirectiveKey
			} else {
				spans[ki].Role = RoleKey
			}

			// Operator role.
			if isEq {
				spans[oi].Role = RoleEq
			} else {
				spans[oi].Role = RoleCompareOp
			}

			// Value role.
			spans[vi].Role = valueRole(valTok)

			j += 2 // skip past the triple
		}
	}

	// Detect scalar function calls: word( → function + paren.
	for j := 0; j+1 < len(nonWS); j++ {
		wi := nonWS[j]
		pi := nonWS[j+1]
		if tokens[wi].kind == "WORD" && tokens[pi].kind == "(" {
			name := strings.ToLower(tokens[wi].tok.Lit)
			if IsScalarFunc(name) {
				spans[wi].Role = RoleFunction
			}
		}
	}

	return spans
}

// classifyPipeSegment assigns roles to tokens in a pipe segment (after a |).
func classifyPipeSegment(tokens []rawToken) []Span {
	spans := make([]Span, len(tokens))
	for i, t := range tokens {
		spans[i] = Span{Text: t.text, Role: defaultRole(t)}
	}

	// Find the first non-whitespace token — it's the pipe keyword.
	nonWS := nonWhitespaceIndices(tokens)
	if len(nonWS) == 0 {
		return spans
	}

	kwIdx := nonWS[0]
	kwTok := tokens[kwIdx]
	kwName := strings.ToLower(kwTok.tok.Lit)

	if pipeKeywordSet[kwName] {
		spans[kwIdx].Role = RolePipeKeyword
	}

	// Remaining tokens after the keyword.
	restNonWS := nonWS[1:]

	switch kwName {
	case "where":
		// "where" body is a filter expression.
		restTokens := tokens[kwIdx+1:]
		restSpans := classifyFilter(restTokens)
		copy(spans[kwIdx+1:], restSpans)

	case "eval":
		// Detect field = expr patterns and function calls.
		classifyEvalBody(tokens, spans, restNonWS)

	case "stats", "timechart", "barchart", "donut", "map":
		// Detect function calls, "by"/"as" keywords, field references.
		classifyStatsBody(tokens, spans, restNonWS)

	default:
		// For sort, head, tail, slice, rename, fields, raw, lookup:
		// detect "by"/"as" keywords and leave rest as tokens.
		classifyGenericPipeBody(tokens, spans, restNonWS)
	}

	return spans
}

// classifyEvalBody handles eval segments: field = expr [, field = expr]*.
func classifyEvalBody(tokens []rawToken, spans []Span, nonWS []int) {
	for j := 0; j+2 < len(nonWS); j++ {
		fi := nonWS[j]
		ei := nonWS[j+1]
		if tokens[ei].kind == "=" && tokens[fi].kind == "WORD" {
			spans[fi].Role = RoleKey
			spans[ei].Role = RoleEq
			j++ // skip the = sign, value will be classified next iteration
		}
	}
	// Detect function calls.
	for j := 0; j+1 < len(nonWS); j++ {
		wi := nonWS[j]
		pi := nonWS[j+1]
		if tokens[wi].kind == "WORD" && tokens[pi].kind == "(" {
			name := strings.ToLower(tokens[wi].tok.Lit)
			if IsScalarFunc(name) || aggFuncSet[name] {
				spans[wi].Role = RoleFunction
			}
		}
	}
}

// classifyStatsBody handles stats/timechart/barchart/donut/map segments.
func classifyStatsBody(tokens []rawToken, spans []Span, nonWS []int) {
	for j := range nonWS {
		idx := nonWS[j]
		t := tokens[idx]

		if t.kind == "WORD" {
			word := strings.ToLower(t.tok.Lit)
			switch {
			case word == "by" || word == "as":
				spans[idx].Role = RolePipeKeyword
			case aggFuncSet[word] || IsScalarFunc(word):
				// Check if followed by ( — function call.
				if j+1 < len(nonWS) && tokens[nonWS[j+1]].kind == "(" {
					spans[idx].Role = RoleFunction
				} else if word == "count" {
					// Bare "count" without parens is still a function.
					spans[idx].Role = RoleFunction
				}
			}
		}
	}
}

// classifyGenericPipeBody handles pipe operators with simple structure.
func classifyGenericPipeBody(tokens []rawToken, spans []Span, nonWS []int) {
	for _, idx := range nonWS {
		t := tokens[idx]
		if t.kind == "WORD" {
			word := strings.ToLower(t.tok.Lit)
			if word == "by" || word == "as" {
				spans[idx].Role = RolePipeKeyword
			}
		}
	}
}

// defaultRole maps a raw token to its default highlight role.
func defaultRole(t rawToken) SpanRole {
	switch t.kind {
	case "whitespace":
		return RoleWhitespace
	case "comment":
		return RoleComment
	case "error":
		return RoleError
	case "OR", "AND", "NOT":
		return RoleOperator
	case "(", ")":
		return RoleParen
	case "=":
		return RoleEq
	case "!=", ">", ">=", "<", "<=":
		return RoleCompareOp
	case "*":
		return RoleStar
	case "REGEX":
		return RoleRegex
	case "GLOB":
		return RoleGlob
	case "|":
		return RolePipe
	case ",":
		return RoleComma
	case "+", "-", "/", "%":
		return RoleToken
	case "WORD":
		if t.tok.Quoted {
			return RoleQuoted
		}
		return RoleToken
	default:
		return RoleToken
	}
}

// valueRole returns the highlight role for a value token.
func valueRole(t rawToken) SpanRole {
	switch {
	case t.tok.Quoted:
		return RoleQuoted
	case t.kind == "GLOB":
		return RoleGlob
	case t.kind == "*":
		return RoleStar
	case t.kind == "REGEX":
		return RoleRegex
	default:
		return RoleValue
	}
}

// applyError marks all spans at or after errorOffset as error
// (except whitespace and comment).
func applyError(spans []Span, errorOffset int) []Span {
	bytePos := 0
	foundError := false
	result := make([]Span, len(spans))

	for i, sp := range spans {
		if !foundError && bytePos+len(sp.Text) > errorOffset {
			foundError = true
		}
		if foundError && sp.Role != RoleWhitespace && sp.Role != RoleComment {
			result[i] = Span{Text: sp.Text, Role: RoleError}
		} else {
			result[i] = sp
		}
		bytePos += len(sp.Text)
	}

	return result
}

// nonWhitespaceIndices returns the indices of non-whitespace, non-comment tokens.
func nonWhitespaceIndices(tokens []rawToken) []int {
	var indices []int
	for i, t := range tokens {
		if t.kind != "whitespace" && t.kind != "comment" {
			indices = append(indices, i)
		}
	}
	return indices
}
