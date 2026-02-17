package querylang

import (
	"errors"
	"fmt"
)

// Lexer errors.
var (
	ErrUnterminatedString = errors.New("unterminated string")
	ErrUnterminatedRegex  = errors.New("unterminated regex")
	ErrInvalidEscape      = errors.New("invalid escape sequence")
	ErrInvalidRegex       = errors.New("invalid regex")
)

// Parser errors.
var (
	ErrEmptyQuery      = errors.New("empty query")
	ErrUnmatchedParen  = errors.New("unmatched parenthesis")
	ErrUnexpectedToken = errors.New("unexpected token")
	ErrUnexpectedEOF   = errors.New("unexpected end of query")
)

// ParseError provides detailed error information including position.
type ParseError struct {
	Pos     int    // byte offset in input
	Message string // human-readable error message
	Err     error  // underlying sentinel error (for errors.Is)
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at position %d: %s", e.Pos, e.Message)
}

func (e *ParseError) Unwrap() error {
	return e.Err
}

// newParseError creates a ParseError with the given position and sentinel error.
func newParseError(pos int, err error, msgFmt string, args ...any) *ParseError {
	return &ParseError{
		Pos:     pos,
		Message: fmt.Sprintf(msgFmt, args...),
		Err:     err,
	}
}
