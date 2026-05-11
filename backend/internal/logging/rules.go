package logging

import (
	"log/slog"
	"strings"
)

// LevelRule binds a component-path pattern to a minimum slog level.
//
// Two pattern shapes are recognised:
//
//   - Exact: a dotted path with no wildcard, e.g. "orchestrator.replication".
//     Matches only that component path, never descendants.
//   - Subtree glob: a path ending in ".*", e.g. "orchestrator.*". Matches
//     the prefix path itself AND every descendant.
//
// The empty pattern is invalid. Operators express the "applies to
// everything" rule through RuleSet.Default, not via a synthetic "*"
// pattern.
type LevelRule struct {
	Pattern string
	Level   slog.Level
}

// matches reports whether the rule's pattern covers the given component path.
func (r LevelRule) matches(path string) bool {
	if strings.HasSuffix(r.Pattern, ".*") {
		prefix := r.Pattern[:len(r.Pattern)-2]
		return path == prefix || strings.HasPrefix(path, prefix+".")
	}
	return r.Pattern == path
}

// specificity returns an ordering key: higher = more specific. Used by
// Resolve to pick the right rule when several match.
//
// Convention: exact-match patterns beat any glob with the same prefix
// length. So "orchestrator.replication" wins over "orchestrator.*" when
// both apply to path "orchestrator.replication".
func (r LevelRule) specificity() int {
	if strings.HasSuffix(r.Pattern, ".*") {
		return len(r.Pattern) - 2 // discount the trailing ".*"
	}
	return len(r.Pattern) + 1 // bonus to outrank a same-length glob
}

// RuleSet is the immutable resolution state of the filter handler:
// a fallback level plus an ordered list of overrides.
//
// RuleSet values are swapped atomically — every derived handler reads
// the same shared pointer, so a single store propagates the new rules
// across every logger in the process.
type RuleSet struct {
	// Default is the level used when no rule matches a component path
	// (or when the component path itself is empty).
	Default slog.Level
	// Rules are pattern -> level overrides. Order is not significant;
	// Resolve picks the most-specific match.
	Rules []LevelRule
	// Generation increments on every rule-set replacement. Derived
	// handlers compare against their cached generation to know when to
	// re-resolve their level.
	Generation uint64
	// MinLevel is the lowest level any rule (including Default) could
	// allow. Used by handlers that haven't pinned a componentPath yet
	// to decide whether to defer to Handle for record-attr inspection.
	// Computed by NewRuleSet.
	MinLevel slog.Level
}

// NewRuleSet builds a RuleSet with MinLevel pre-computed. Callers
// should always go through this constructor rather than constructing
// RuleSet literals directly.
func NewRuleSet(defaultLevel slog.Level, rules []LevelRule, generation uint64) RuleSet {
	minLevel := defaultLevel
	for _, r := range rules {
		if r.Level < minLevel {
			minLevel = r.Level
		}
	}
	copied := make([]LevelRule, len(rules))
	copy(copied, rules)
	return RuleSet{
		Default:    defaultLevel,
		Rules:      copied,
		Generation: generation,
		MinLevel:   minLevel,
	}
}

// Resolve returns the minimum level that applies to the given component
// path. Path "" returns Default.
//
// Resolution: the most-specific matching rule wins. See LevelRule.specificity.
func (rs RuleSet) Resolve(path string) slog.Level {
	if path == "" {
		return rs.Default
	}
	bestSpec := -1
	bestLevel := rs.Default
	for _, r := range rs.Rules {
		if !r.matches(path) {
			continue
		}
		if s := r.specificity(); s > bestSpec {
			bestSpec = s
			bestLevel = r.Level
		}
	}
	return bestLevel
}
