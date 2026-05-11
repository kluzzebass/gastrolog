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
		t.Errorf("exact match = %v, want DEBUG", got)
	}
	if got := rs.Resolve("orchestrator.replication"); got != slog.LevelInfo {
		t.Errorf("descendant of exact rule = %v, want INFO (exact rules don't apply to descendants)", got)
	}
}

func TestRuleSet_Resolve_SubtreeGlob(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.*", Level: slog.LevelDebug},
	}, 1)
	cases := map[string]slog.Level{
		"orchestrator":                     slog.LevelDebug,
		"orchestrator.replication":         slog.LevelDebug,
		"orchestrator.replication.catchup": slog.LevelDebug,
		"cluster":                          slog.LevelInfo,
		"orchestrator-other":               slog.LevelInfo, // not a descendant
	}
	for path, want := range cases {
		if got := rs.Resolve(path); got != want {
			t.Errorf("Resolve(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRuleSet_Resolve_SpecificityWins(t *testing.T) {
	t.Parallel()
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.*", Level: slog.LevelDebug},
		{Pattern: "orchestrator.replication.*", Level: slog.LevelWarn},
		{Pattern: "orchestrator.replication.catchup", Level: slog.LevelError},
	}, 1)
	cases := map[string]slog.Level{
		"orchestrator":                     slog.LevelDebug, // broadest only
		"orchestrator.lifecycle":           slog.LevelDebug, // broadest only
		"orchestrator.replication":         slog.LevelWarn,  // mid-tier glob
		"orchestrator.replication.target":  slog.LevelWarn,  // mid-tier glob
		"orchestrator.replication.catchup": slog.LevelError, // exact wins over both globs
	}
	for path, want := range cases {
		if got := rs.Resolve(path); got != want {
			t.Errorf("Resolve(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestRuleSet_Resolve_ExactBeatsGlobAtSameLength(t *testing.T) {
	t.Parallel()
	// Both "orchestrator.replication" (exact) and "orchestrator.*" (glob)
	// could apply to path "orchestrator.replication". Exact must win
	// because it's more specific.
	rs := NewRuleSet(slog.LevelInfo, []LevelRule{
		{Pattern: "orchestrator.*", Level: slog.LevelDebug},
		{Pattern: "orchestrator.replication", Level: slog.LevelError},
	}, 1)
	if got := rs.Resolve("orchestrator.replication"); got != slog.LevelError {
		t.Errorf("exact = %v, want ERROR (exact must beat glob)", got)
	}
	if got := rs.Resolve("orchestrator.replication.x"); got != slog.LevelDebug {
		t.Errorf("descendant = %v, want DEBUG (exact rule doesn't cover descendants)", got)
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
	// When every rule is above default, MinLevel == Default.
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
	// Mutate the input slice — should not affect the rule set.
	rules[0].Level = slog.LevelError
	if rs.Rules[0].Level != slog.LevelDebug {
		t.Errorf("input mutation leaked into RuleSet")
	}
}
