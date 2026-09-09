package querylang

import "testing"

// Expr.String() is the form the coordinator forwards to remote nodes, so every
// predicate kind must print as syntax the parser reads back as the same
// expression — the forms that would otherwise be misread as words (token,
// regex, glob, expression predicates), and the values the lexer cannot take as
// barewords (empty, spaces, pipes, quotes, operator characters).
func TestStringRoundTripsThroughTheParser(t *testing.T) {
	for _, src := range []string{
		`error`,
		`"disk error"`,
		`NOT error`,
		`error host=web-01`,
		`(level=error OR level=warn) AND NOT host=web-02`,
		`/disk.*full/`,
		`/a\/b/`,
		`web-*`,
		`err?r`,
		`host=web-*`,
		`*=timeout`,
		`*="a b"`,
		`*=err*`,
		`host=*`,
		`err*=*`,
		`host=""`,
		`host!=""`,
		`msg="a b"`,
		`msg="x|y"`,
		`msg="it's"`,
		`msg="say \"hi\""`,
		`msg="k=v"`,
		`"a key"=value`,
		`status>=500`,
		`len(message) > 100`,
		`len(host) > "a b"`,
		`len(host) > ""`,
	} {
		e1, err := Parse(src)
		if err != nil {
			t.Fatalf("parse %q: %v", src, err)
		}
		printed := e1.String()
		e2, err := Parse(printed)
		if err != nil {
			t.Errorf("%q prints as %q, which does not parse: %v", src, printed, err)
			continue
		}
		if again := e2.String(); again != printed {
			t.Errorf("%q prints as %q, which re-prints as %q", src, printed, again)
		}
		d1, d2 := ToDNF(e1), ToDNF(e2)
		if d1.String() != d2.String() {
			t.Errorf("%q prints as %q, which means %q rather than %q", src, printed, d2.String(), d1.String())
		}
	}
}

// The same holds for a whole pipeline: the filter and every operator's
// arguments must survive the trip.
func TestPipelineStringRoundTripsThroughTheParser(t *testing.T) {
	for _, src := range []string{
		`error | stats count`,
		`"disk error" | where host != "" | stats count by host`,
		`/disk/ | where msg = "a b" | stats sum(latency) as total by host | where total > 5 | sort -total | head 3`,
		`| where len(host) > "" | timechart 5 by level`,
		`| eval label = "" | stats count by label`,
	} {
		p1, err := ParsePipeline(src)
		if err != nil {
			t.Fatalf("parse %q: %v", src, err)
		}
		printed := p1.String()
		p2, err := ParsePipeline(printed)
		if err != nil {
			t.Errorf("%q prints as %q, which does not parse: %v", src, printed, err)
			continue
		}
		if again := p2.String(); again != printed {
			t.Errorf("%q prints as %q, which re-prints as %q", src, printed, again)
		}
	}
}
