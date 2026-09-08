package querylang

import "testing"

func FuzzParseStringRoundTrip(f *testing.F) {
	// Seed corpus: queries that exercise various AST node types.
	seeds := []string{
		"error",
		`level=error`,
		`level=error AND status=500`,
		`level=error OR level=warn`,
		`NOT level=debug`,
		`(level=error OR level=warn) AND host=web-01`,
		`NOT NOT error`,
		`/err.*/`,
		`host=web-*`,
		`*=error`,
		`host=*`,
		`status>400`,
		`status>=500`,
		`status!=404`,
		`"hello world"`,
		`message="request failed"`,
		`a AND b AND c`,
		`a OR b OR c`,
		`(a OR b) AND (c OR d)`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// First parse.
		expr1, err1 := Parse(input)
		if err1 != nil {
			return // unparseable, skip
		}

		// String() is the wire format the coordinator forwards, so it must
		// parse back to the same expression.
		s := expr1.String()
		if s == "" {
			return
		}

		// Second parse must not panic.
		expr2, err2 := Parse(s)
		if err2 != nil {
			t.Fatalf("round-trip failed: Parse(%q) succeeded, but Parse(%q) returned error: %v",
				input, s, err2)
		}

		// Second serialization must equal first (stable round-trip), and the
		// two expressions must mean the same thing.
		if s2 := expr2.String(); s != s2 {
			t.Fatalf("unstable round-trip: first=%q, second=%q (original input=%q)", s, s2, input)
		}
		dnf1, dnf2 := ToDNF(expr1), ToDNF(expr2)
		if d1, d2 := dnf1.String(), dnf2.String(); d1 != d2 {
			t.Fatalf("round-trip changed meaning: %q → %q parses as %q, not %q", input, s, d2, d1)
		}
	})
}
