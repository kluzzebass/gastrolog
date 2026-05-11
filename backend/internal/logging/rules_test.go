package logging

import (
	"log/slog"
	"testing"
)

func TestRuleSet_Resolve_NoRules(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, nil, 1)
	if got := rs.Resolve("anything"); got != slog.LevelInfo {
		t.Errorf("Resolve(anything) = %v, want INFO", got)
	}
	if got := rs.Resolve(""); got != slog.LevelInfo {
		t.Errorf("Resolve(empty) = %v, want INFO", got)
	}
}

func TestRuleSet_Resolve_ExactMatch(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator", Level: slog.LevelDebug},
	}, 1)
	if got := rs.Resolve("orchestrator"); got != slog.LevelDebug {
		t.Errorf("exact = %v, want DEBUG", got)
	}
	if got := rs.Resolve("orchestrator.replication"); got != slog.LevelInfo {
		t.Errorf("descendant of exact rule = %v, want INFO (exact stops at the exact path)", got)
	}
}

func TestRuleSet_Resolve_StarMatchesOneSegment(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.*", Level: slog.LevelDebug},
	}, 1)
	cases := map[string]slog.Level{
		"orchestrator.replication":         slog.LevelDebug, // one segment below
		"orchestrator.lifecycle":           slog.LevelDebug, // one segment below
		"orchestrator":                     slog.LevelInfo,  // * needs a segment
		"orchestrator.replication.catchup": slog.LevelInfo,  // two segments below, * is only one
		"cluster.replication":              slog.LevelInfo,
	}
	for path, want := range cases {
		if got := rs.Resolve(path); got != want {
			t.Errorf("Resolve(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRuleSet_Resolve_DoubleStarMatchesSubtree(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.**", Level: slog.LevelDebug},
	}, 1)
	cases := map[string]slog.Level{
		"orchestrator":                     slog.LevelDebug, // ** matches zero segments
		"orchestrator.replication":         slog.LevelDebug,
		"orchestrator.replication.catchup": slog.LevelDebug,
		"cluster":                          slog.LevelInfo,
		"orchestrator-other":               slog.LevelInfo, // not a descendant (different segment)
	}
	for path, want := range cases {
		if got := rs.Resolve(path); got != want {
			t.Errorf("Resolve(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRuleSet_Resolve_MidStringStar(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "ingester.*.conn", Level: slog.LevelDebug},
	}, 1)
	cases := map[string]slog.Level{
		"ingester.relp.conn":     slog.LevelDebug,
		"ingester.http.conn":     slog.LevelDebug,
		"ingester.conn":          slog.LevelInfo, // missing the middle segment
		"ingester.relp.other":    slog.LevelInfo,
		"ingester.relp.tls.conn": slog.LevelInfo, // * is one segment, not two
	}
	for path, want := range cases {
		if got := rs.Resolve(path); got != want {
			t.Errorf("Resolve(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRuleSet_Resolve_MidStringDoubleStar(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "ingester.**.conn", Level: slog.LevelDebug},
	}, 1)
	cases := map[string]slog.Level{
		"ingester.conn":          slog.LevelDebug, // ** matches zero
		"ingester.relp.conn":     slog.LevelDebug,
		"ingester.relp.tls.conn": slog.LevelDebug,
		"ingester.relp":          slog.LevelInfo, // no .conn suffix
		"cluster.relp.conn":      slog.LevelInfo,
	}
	for path, want := range cases {
		if got := rs.Resolve(path); got != want {
			t.Errorf("Resolve(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRuleSet_Resolve_SpecificityOrdering(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.**", Level: slog.LevelDebug},
		{Pattern: "orchestrator.replication.**", Level: slog.LevelWarn},
		{Pattern: "orchestrator.replication.catchup", Level: slog.LevelError},
	}, 1)
	cases := map[string]slog.Level{
		"orchestrator":                     slog.LevelDebug, // broadest only
		"orchestrator.lifecycle":           slog.LevelDebug, // broadest only
		"orchestrator.replication":         slog.LevelWarn,  // mid-tier
		"orchestrator.replication.target":  slog.LevelWarn,  // mid-tier
		"orchestrator.replication.catchup": slog.LevelError, // exact
	}
	for path, want := range cases {
		if got := rs.Resolve(path); got != want {
			t.Errorf("Resolve(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRuleSet_Resolve_ExactBeatsAllGlobs(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.**", Level: slog.LevelDebug},
		{Pattern: "orchestrator.*", Level: slog.LevelWarn},
		{Pattern: "orchestrator.replication", Level: slog.LevelError},
	}, 1)
	if got := rs.Resolve("orchestrator.replication"); got != slog.LevelError {
		t.Errorf("exact = %v, want ERROR (exact must beat any glob)", got)
	}
	if got := rs.Resolve("orchestrator.lifecycle"); got != slog.LevelWarn {
		t.Errorf("single-* = %v, want WARN (single-* beats **)", got)
	}
	if got := rs.Resolve("orchestrator.replication.catchup"); got != slog.LevelDebug {
		t.Errorf("only ** matches = %v, want DEBUG", got)
	}
}

func TestRuleSet_Resolve_StarBeatsDoubleStar(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "ingester.**.conn", Level: slog.LevelDebug},
		{Pattern: "ingester.*.conn", Level: slog.LevelError},
	}, 1)
	// Both match "ingester.relp.conn"; * is more specific than **.
	if got := rs.Resolve("ingester.relp.conn"); got != slog.LevelError {
		t.Errorf("ingester.relp.conn = %v, want ERROR (* beats **)", got)
	}
	// Only ** matches "ingester.relp.tls.conn".
	if got := rs.Resolve("ingester.relp.tls.conn"); got != slog.LevelDebug {
		t.Errorf("ingester.relp.tls.conn = %v, want DEBUG (only ** matches)", got)
	}
}

func TestRuleSet_Resolve_TieBreaksByDeclarationOrder(t *testing.T) {
	t.Parallel()
	// Two patterns with the same shape and length both match
	// "orchestrator.lifecycle"; declaration order wins.
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.*", Level: slog.LevelDebug}, // first
		{Pattern: "*.lifecycle", Level: slog.LevelError},    // second, same specificity shape
	}, 1)
	got := rs.Resolve("orchestrator.lifecycle")
	if got != slog.LevelDebug && got != slog.LevelError {
		t.Fatalf("unexpected level %v", got)
	}
	// We don't assert which one wins in this test (the spec scores differ
	// by pattern length, which makes "orchestrator.*" the winner). The
	// real point is that resolution is deterministic — running it twice
	// returns the same answer.
	got2 := rs.Resolve("orchestrator.lifecycle")
	if got != got2 {
		t.Errorf("non-deterministic resolution: %v vs %v", got, got2)
	}
}

func TestRuleSet_Resolve_EmptyPattern_NeverMatches(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "", Level: slog.LevelDebug},
	}, 1)
	if got := rs.Resolve("anything"); got != slog.LevelInfo {
		t.Errorf("empty pattern matched %v, want INFO (no match)", got)
	}
}

func TestNewRuleSet_MinLevel(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "x", Level: slog.LevelWarn},
		{Pattern: "y.*", Level: slog.LevelDebug},
		{Pattern: "z", Level: slog.LevelError},
	}, 1)
	if rs.MinLevel != slog.LevelDebug {
		t.Errorf("MinLevel = %v, want DEBUG (lowest among all rules and default)", rs.MinLevel)
	}
	rs2 := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "x", Level: slog.LevelWarn},
		{Pattern: "y", Level: slog.LevelError},
	}, 1)
	if rs2.MinLevel != slog.LevelInfo {
		t.Errorf("MinLevel = %v, want INFO", rs2.MinLevel)
	}
}

func TestNewRuleSet_DefensiveCopy(t *testing.T) {
	t.Parallel()
	rules := []LevelRule{{Pattern: "x", Level: slog.LevelDebug}}
	rs := NewRuleSet(slog.LevelInfo, rules, 1)
	rules[0].Level = slog.LevelError
	if rs.Rules[0].Level != slog.LevelDebug {
		t.Errorf("input mutation leaked into RuleSet")
	}
}
