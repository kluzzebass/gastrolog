package querylang

import (
	"strings"
)

// Regex delimiters used by the lexer.
const regexDelimiter = '/'

// TokenKind identifies the type of lexical token.
type TokenKind int

const (
	TokEOF    TokenKind = iota
	TokWord             // bareword or quoted string (quotes stripped, escapes processed)
	TokOr               // OR (case-insensitive)
	TokAnd              // AND (case-insensitive)
	TokNot              // NOT (case-insensitive)
	TokLParen           // (
	TokRParen           // )
	TokEq               // =
	TokNe               // !=
	TokGt               // >
	TokGte              // >=
	TokLt               // <
	TokLte              // <=
	TokStar             // *
	TokRegex            // /pattern/ (regex literal, slashes stripped)
	TokGlob             // bareword with glob metacharacters (*, ?, [)
	TokPipe             // |
	TokComma            // ,
	TokPlus             // +
	TokMinus            // -
	TokSlash            // / (arithmetic division, only in pipe context)
	TokPercent          // % (modulo, only in pipe context)
)

func (k TokenKind) String() string {
	switch k {
	case TokEOF:
		return "EOF"
	case TokWord:
		return "WORD"
	case TokOr:
		return "OR"
	case TokAnd:
		return "AND"
	case TokNot:
		return "NOT"
	case TokLParen:
		return "("
	case TokRParen:
		return ")"
	case TokEq:
		return "="
	case TokNe:
		return "!="
	case TokGt:
		return ">"
	case TokGte:
		return ">="
	case TokLt:
		return "<"
	case TokLte:
		return "<="
	case TokStar:
		return "*"
	case TokRegex:
		return "REGEX"
	case TokGlob:
		return "GLOB"
	case TokPipe:
		return "|"
	case TokComma:
		return ","
	case TokPlus:
		return "+"
	case TokMinus:
		return "-"
	case TokSlash:
		return "/"
	case TokPercent:
		return "%"
	default:
		return "UNKNOWN"
	}
}

// Token represents a lexical token.
type Token struct {
	Kind TokenKind
	Lit  string // for quoted strings: unescaped content without quotes
	Pos  int    // byte offset in input for error reporting
}

// Lexer tokenizes a query string.
type Lexer struct {
	input    string
	pos      int  // current position in input
	pipeMode bool // when true, '/' emits TokSlash instead of scanning regex
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

// SetPipeMode sets whether the lexer is in pipe mode.
// In pipe mode, '/' emits TokSlash (division) instead of scanning a regex literal.
func (l *Lexer) SetPipeMode(on bool) {
	l.pipeMode = on
}

// LexerState captures the lexer position for backtracking.
type LexerState struct {
	pos      int
	pipeMode bool
}

// Save returns a snapshot of the lexer state.
func (l *Lexer) Save() LexerState {
	return LexerState{pos: l.pos, pipeMode: l.pipeMode}
}

// Restore rewinds the lexer to a previously saved state.
func (l *Lexer) Restore(s LexerState) {
	l.pos = s.pos
	l.pipeMode = s.pipeMode
}

// Next returns the next token.
func (l *Lexer) Next() (Token, error) {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Kind: TokEOF, Pos: l.pos}, nil
	}

	startPos := l.pos
	ch := l.input[l.pos]

	// Single-character tokens
	switch ch {
	case '(':
		l.pos++
		return Token{Kind: TokLParen, Lit: "(", Pos: startPos}, nil
	case ')':
		l.pos++
		return Token{Kind: TokRParen, Lit: ")", Pos: startPos}, nil
	case '=':
		l.pos++
		return Token{Kind: TokEq, Lit: "=", Pos: startPos}, nil
	case '!':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return Token{Kind: TokNe, Lit: "!=", Pos: startPos}, nil
		}
		return Token{}, newParseError(startPos, ErrUnexpectedToken, "unexpected '!' (did you mean '!='?)")
	case '>':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return Token{Kind: TokGte, Lit: ">=", Pos: startPos}, nil
		}
		l.pos++
		return Token{Kind: TokGt, Lit: ">", Pos: startPos}, nil
	case '<':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return Token{Kind: TokLte, Lit: "<=", Pos: startPos}, nil
		}
		l.pos++
		return Token{Kind: TokLt, Lit: "<", Pos: startPos}, nil
	case '*':
		// Peek ahead: if followed by a bareword char or glob meta, this is a glob prefix (e.g. *error).
		if l.pos+1 < len(l.input) && isGlobBarewordChar(l.input[l.pos+1]) {
			return l.scanGlobBareword()
		}
		l.pos++
		return Token{Kind: TokStar, Lit: "*", Pos: startPos}, nil
	case '|':
		l.pos++
		return Token{Kind: TokPipe, Lit: "|", Pos: startPos}, nil
	case ',':
		l.pos++
		return Token{Kind: TokComma, Lit: ",", Pos: startPos}, nil
	case '+':
		l.pos++
		return Token{Kind: TokPlus, Lit: "+", Pos: startPos}, nil
	case '%':
		l.pos++
		return Token{Kind: TokPercent, Lit: "%", Pos: startPos}, nil
	case '-':
		if l.pipeMode {
			l.pos++
			return Token{Kind: TokMinus, Lit: "-", Pos: startPos}, nil
		}
		// In filter mode, '-' is a valid bareword char (e.g. "my-token")
		return l.scanBareword()
	case '"', '\'':
		return l.scanQuotedString(ch)
	case regexDelimiter:
		if l.pipeMode {
			l.pos++
			return Token{Kind: TokSlash, Lit: "/", Pos: startPos}, nil
		}
		return l.scanRegex()
	}

	// Bareword (may be keyword)
	return l.scanBareword()
}

// skipWhitespace advances past whitespace characters.
func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
		} else {
			break
		}
	}
}

// scanQuotedString scans a quoted string, processing escape sequences.
func (l *Lexer) scanQuotedString(quote byte) (Token, error) {
	startPos := l.pos
	l.pos++ // skip opening quote

	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		if ch == quote {
			l.pos++ // skip closing quote
			return Token{Kind: TokWord, Lit: sb.String(), Pos: startPos}, nil
		}

		if ch == '\\' {
			l.pos++
			if l.pos >= len(l.input) {
				return Token{}, newParseError(l.pos-1, ErrUnterminatedString, "unterminated string: escape at end of input")
			}

			escaped := l.input[l.pos]
			switch escaped {
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			case '\'':
				sb.WriteByte('\'')
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			default:
				return Token{}, newParseError(l.pos-1, ErrInvalidEscape, "invalid escape sequence: \\%c", escaped)
			}
			l.pos++
			continue
		}

		sb.WriteByte(ch)
		l.pos++
	}

	return Token{}, newParseError(startPos, ErrUnterminatedString, "unterminated string starting at position %d", startPos)
}

// scanBareword scans a bareword token, which may be a keyword or a glob pattern.
// If the bareword contains glob metacharacters (*, ?, [), it produces TokGlob.
func (l *Lexer) scanBareword() (Token, error) {
	startPos := l.pos
	hasGlobMeta := false

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if isBarewordChar(ch) {
			l.pos++
			continue
		}
		if ch != '*' && ch != '?' && ch != '[' {
			break
		}
		hasGlobMeta = true
		l.pos++
		// For '[', scan to closing ']'
		if ch == '[' {
			l.scanBracketClass()
		}
	}

	lit := l.input[startPos:l.pos]

	if hasGlobMeta {
		return Token{Kind: TokGlob, Lit: lit, Pos: startPos}, nil
	}

	kind := classifyWord(lit)
	return Token{Kind: kind, Lit: lit, Pos: startPos}, nil
}

// scanBracketClass advances past a bracket character class in a glob pattern (e.g. [abc]).
func (l *Lexer) scanBracketClass() {
	for l.pos < len(l.input) && l.input[l.pos] != ']' {
		l.pos++
	}
	if l.pos < len(l.input) {
		l.pos++ // skip ']'
	}
}

// scanGlobBareword scans a glob pattern starting with '*'.
func (l *Lexer) scanGlobBareword() (Token, error) {
	startPos := l.pos
	l.pos++ // skip leading '*'

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if isBarewordChar(ch) || ch == '*' || ch == '?' || ch == '[' {
			if ch == '[' {
				l.pos++
				for l.pos < len(l.input) && l.input[l.pos] != ']' {
					l.pos++
				}
				if l.pos < len(l.input) {
					l.pos++ // skip ']'
				}
				continue
			}
			l.pos++
		} else {
			break
		}
	}

	lit := l.input[startPos:l.pos]
	return Token{Kind: TokGlob, Lit: lit, Pos: startPos}, nil
}

// scanRegex scans a regex literal delimited by forward slashes.
// The pattern between slashes is returned as the token literal (slashes stripped).
// Escaped slashes (\/) within the pattern are unescaped.
func (l *Lexer) scanRegex() (Token, error) {
	startPos := l.pos
	l.pos++ // skip opening /

	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		if ch == regexDelimiter {
			l.pos++ // skip closing /
			return Token{Kind: TokRegex, Lit: sb.String(), Pos: startPos}, nil
		}

		if ch == '\\' && l.pos+1 < len(l.input) && l.input[l.pos+1] == regexDelimiter {
			// Escaped slash: \/ → /
			sb.WriteByte('/')
			l.pos += 2
			continue
		}

		sb.WriteByte(ch)
		l.pos++
	}

	return Token{}, newParseError(startPos, ErrUnterminatedRegex, "unterminated regex starting at position %d", startPos)
}

// isBarewordChar returns true if ch can be part of a bareword.
// Barewords exclude: whitespace, ()=*?"'/ and [ and comparison chars ><!
func isBarewordChar(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r':
		return false
	case '(', ')', '=', '*', '?', '[', '"', '\'', '/', '>', '<', '!':
		return false
	case '|', ',', '+', '%':
		return false
	default:
		return true
	}
}

// isGlobBarewordChar returns true if ch can start or continue a glob-extended bareword.
// This includes regular bareword chars plus glob metacharacters ? and [.
func isGlobBarewordChar(ch byte) bool {
	return isBarewordChar(ch) || ch == '?' || ch == '['
}

// classifyWord checks if a word is a keyword (case-insensitive).
func classifyWord(word string) TokenKind {
	upper := strings.ToUpper(word)
	switch upper {
	case "OR":
		return TokOr
	case "AND":
		return TokAnd
	case "NOT":
		return TokNot
	default:
		return TokWord
	}
}

// Peek returns the next token without consuming it.
func (l *Lexer) Peek() (Token, error) {
	savedPos := l.pos
	tok, err := l.Next()
	l.pos = savedPos
	return tok, err
}
