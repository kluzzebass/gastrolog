package querylang

import (
	"regexp"
	"strings"
)

// CompileGlob converts a shell-style glob pattern to a compiled case-insensitive regex.
// Supported metacharacters: * (any chars), ? (single char), [abc] (char class).
// The resulting regex is anchored (^...$) and case-insensitive.
func CompileGlob(pattern string) (*regexp.Regexp, error) {
	re, err := globToRegex(pattern)
	if err != nil {
		return nil, err
	}
	compiled, err := regexp.Compile(re)
	if err != nil {
		return nil, err
	}
	return compiled, nil
}

// globToRegex converts a glob pattern string to a regex string.
func globToRegex(pattern string) (string, error) {
	var b strings.Builder
	b.WriteString("(?i)^")

	i := 0
	for i < len(pattern) {
		ch := pattern[i]
		switch ch {
		case '*':
			b.WriteString(".*")
			i++
		case '?':
			b.WriteByte('.')
			i++
		case '[':
			// Pass through character class as-is, including brackets.
			j := i + 1
			if j < len(pattern) && pattern[j] == '!' {
				j++ // skip negation
			}
			if j < len(pattern) && pattern[j] == ']' {
				j++ // literal ] at start of class
			}
			for j < len(pattern) && pattern[j] != ']' {
				j++
			}
			if j >= len(pattern) {
				return "", ErrInvalidGlob
			}
			// Write the character class, converting ! to ^ for negation.
			b.WriteByte('[')
			classBody := pattern[i+1 : j]
			if len(classBody) > 0 && classBody[0] == '!' {
				b.WriteByte('^')
				classBody = classBody[1:]
			}
			b.WriteString(classBody)
			b.WriteByte(']')
			i = j + 1 // skip past ']'
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
			i++
		}
	}

	b.WriteByte('$')
	return b.String(), nil
}

// ExtractGlobPrefix returns the literal prefix of a glob pattern — the characters
// before the first metacharacter (*, ?, [). This can be used for index acceleration
// via prefix lookup on sorted token indexes.
// Returns ("", false) if the pattern starts with a metacharacter.
func ExtractGlobPrefix(pattern string) (string, bool) {
	for i := range len(pattern) {
		switch pattern[i] {
		case '*', '?', '[':
			if i == 0 {
				return "", false
			}
			return strings.ToLower(pattern[:i]), true
		}
	}
	// No metacharacter found — entire pattern is a literal prefix.
	return strings.ToLower(pattern), true
}
