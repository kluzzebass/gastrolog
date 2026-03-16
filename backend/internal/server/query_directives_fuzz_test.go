package server

import (
	"gastrolog/internal/query"
	"testing"
)

func FuzzParseExpression(f *testing.F) {
	// Seed corpus: realistic directive combinations and edge cases.
	f.Add("")
	f.Add("error")
	f.Add("last=5m")
	f.Add("start=2024-01-01T00:00:00Z end=2024-12-31T23:59:59Z")
	f.Add("limit=100")
	f.Add("limit=-1")
	f.Add("limit=abc")
	f.Add("order=source_ts")
	f.Add("order=ingest_ts")
	f.Add("order=write_ts") // invalid — should produce error
	f.Add("order=bogus")
	f.Add("reverse=true")
	f.Add("reverse=false")
	f.Add("pos=42")
	f.Add("pos=0")
	f.Add("last=5m limit=100 order=source_ts reverse=true error")
	f.Add("start=2024-01-01T00:00:00Z end=2024-01-02T00:00:00Z limit=50")
	f.Add("source_start=2024-01-01T00:00:00Z source_end=2024-12-31T00:00:00Z")
	f.Add("ingest_start=1700000000 ingest_end=1700100000")
	f.Add("last=3d")
	f.Add("last=invalid")
	f.Add("start=not-a-time")
	f.Add("end=not-a-time")
	f.Add("foo=bar baz=qux")
	f.Add("level=error host=web-1 last=1h limit=500")
	f.Add("error | stats count")
	f.Add("error | stats count by level")
	f.Add(`key="value with spaces"`)
	f.Add("start=1700000000")
	f.Add("a]b[c")

	f.Fuzz(func(t *testing.T, expr string) {
		// Must not panic on any input. Errors are expected for malformed input.
		_, _, _ = parseExpression(expr)
	})
}

func FuzzApplyDirective(f *testing.F) {
	seeds := []struct {
		k, v string
	}{
		{"last", "5m"},
		{"last", "3d"},
		{"last", "bogus"},
		{"start", "2024-01-01T00:00:00Z"},
		{"start", "1700000000"},
		{"start", "nope"},
		{"end", "2024-12-31T23:59:59Z"},
		{"end", "garbage"},
		{"source_start", "2024-06-01T00:00:00Z"},
		{"source_end", "2024-06-01T00:00:00Z"},
		{"ingest_start", "1700000000"},
		{"ingest_end", "1700000000"},
		{"limit", "100"},
		{"limit", "-1"},
		{"limit", "abc"},
		{"pos", "0"},
		{"pos", "999999"},
		{"pos", "not_a_number"},
		{"order", "ingest_ts"},
		{"order", "source_ts"},
		{"order", "write_ts"},
		{"order", "bad"},
		{"reverse", "true"},
		{"reverse", "false"},
		{"reverse", "maybe"},
		{"unknown_key", "whatever"},
		{"", ""},
	}
	for _, s := range seeds {
		f.Add(s.k, s.v)
	}

	f.Fuzz(func(t *testing.T, k, v string) {
		var q query.Query
		_, _ = applyDirective(&q, k, v)
	})
}
