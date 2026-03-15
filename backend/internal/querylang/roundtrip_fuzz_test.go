package querylang

import (
	"strings"
	"testing"
)

// exprToQuery converts an AST back to parseable query syntax.
// Unlike String() which is diagnostic (e.g. "token(error)"), this produces
// a string the parser can consume for round-trip testing.
func exprToQuery(e Expr) string {
	switch v := e.(type) {
	case *PredicateExpr:
		switch v.Kind {
		case PredToken:
			return quoteIfNeeded(v.Value)
		case PredKV:
			k := quoteIfNeeded(v.Key)
			val := quoteIfNeeded(v.Value)
			return k + v.Op.String() + val
		case PredKeyExists:
			return quoteIfNeeded(v.Key) + "=*"
		case PredValueExists:
			return "*=" + quoteIfNeeded(v.Value)
		case PredRegex:
			return "/" + v.Value + "/"
		case PredGlob:
			return v.Value
		case PredExpr:
			return v.ExprLHS.String() + v.Op.String() + v.Value
		default:
			return quoteIfNeeded(v.Value)
		}
	case *NotExpr:
		return "NOT " + exprToQuery(v.Term)
	case *AndExpr:
		parts := make([]string, len(v.Terms))
		for i, t := range v.Terms {
			parts[i] = exprToQuery(t)
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	case *OrExpr:
		parts := make([]string, len(v.Terms))
		for i, t := range v.Terms {
			parts[i] = exprToQuery(t)
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	default:
		return ""
	}
}

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

		// Serialize back to parseable query syntax.
		s := exprToQuery(expr1)
		if s == "" {
			return
		}

		// Second parse must not panic.
		expr2, err2 := Parse(s)
		if err2 != nil {
			t.Fatalf("round-trip failed: Parse(%q) succeeded, but Parse(%q) returned error: %v",
				input, s, err2)
		}

		// Second serialization must equal first (stable round-trip).
		s2 := exprToQuery(expr2)
		if s != s2 {
			t.Fatalf("unstable round-trip: first=%q, second=%q (original input=%q)", s, s2, input)
		}
	})
}
