package querylang

import "testing"

func FuzzToDNF(f *testing.F) {
	// Seed corpus: query strings that parse into ASTs of varying complexity.
	seeds := []string{
		"error",
		"error AND warn",
		"error OR warn",
		"NOT error",
		"(error OR warn) AND NOT debug",
		"(a AND b) OR (c AND d)",
		"NOT (a OR b)",
		"NOT (a AND b)",
		"NOT NOT error",
		"(a OR b) AND (c OR d) AND (e OR f)",
		"a AND b AND c AND d AND e",
		"a OR b OR c OR d OR e",
		"NOT (NOT (NOT error))",
		`level=error AND (status=500 OR status=503)`,
		`(a OR b) AND (c OR d) AND (e OR f) AND (g OR h)`,
		`NOT ((a AND b) OR (c AND d))`,
		"((a OR b) AND c) OR (d AND (e OR f))",
		`/err.*/ AND level=warn`,
		`host=web-* OR host=api-*`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		expr, err := Parse(input)
		if err != nil {
			return // unparseable input, skip
		}

		// ToDNF must not panic on any valid AST.
		dnf := ToDNF(expr)

		// Basic sanity: String() must not panic either.
		_ = dnf.String()
	})
}
