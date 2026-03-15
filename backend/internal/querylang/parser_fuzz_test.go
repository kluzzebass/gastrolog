package querylang

import "testing"

func FuzzParse(f *testing.F) {
	// Seed corpus: real query patterns covering various syntax features.
	seeds := []string{
		// Simple tokens
		"error",
		"hello",
		// KV predicates
		`level=error`,
		`status=200`,
		`host=web-01`,
		// Quoted values
		`"hello world"`,
		`message="request failed"`,
		// Boolean operators
		`level=error AND status=500`,
		`level=error OR level=warn`,
		`NOT level=debug`,
		`"hello world" AND status=200`,
		// Nested parens
		`(level=error OR level=warn) AND host=web-01`,
		`((level=error))`,
		`(a AND (b OR c)) AND d`,
		// Deeply nested
		`((((error))))`,
		// Regex patterns
		`/err.*/`,
		`/^[0-9]+$/`,
		`/foo|bar/`,
		// Glob patterns
		`host=web-*`,
		`app=my?app`,
		`level=err*`,
		// Wildcard predicates
		`*=error`,
		`host=*`,
		// Comparison operators
		`status>400`,
		`status>=500`,
		`status<200`,
		`status<=299`,
		`status!=404`,
		// Implicit AND
		`error timeout`,
		`level=error status=500`,
		// Edge cases
		``,
		`()`,
		`)`,
		`(`,
		`AND`,
		`OR`,
		`NOT`,
		`NOT NOT error`,
		`*=*`,
		`=`,
		`===`,
		`a= `,
		// Unicode / multi-byte
		"\xff\xfe",
		"\x00",
		// Long input
		`aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Parse must not panic. It should return either a valid AST or an error.
		_, _ = Parse(input)
	})
}
