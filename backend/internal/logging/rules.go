package logging

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// ValidatePattern checks that a rule pattern parses under the grammar
// implemented by matchSegments. Returns an error with a human-readable
// reason on failure, or nil if the pattern is well-formed.
//
// Allowed segment shapes:
//   - "*"  — wildcard, exactly one segment
//   - "**" — wildcard, zero or more segments
//   - a literal of [a-z0-9_-]+
//
// Empty pattern is rejected; operators express "applies to everything"
// through the rule set's Default level. Empty segments (leading or
// consecutive dots) are rejected. Patterns are case-sensitive and
// lowercase by convention.
func ValidatePattern(pattern string) error {
	if pattern == "" {
		return errors.New("pattern must not be empty (use the default level instead)")
	}
	for seg := range strings.SplitSeq(pattern, ".") {
		if seg == "" {
			return fmt.Errorf("pattern %q has an empty segment", pattern)
		}
		if seg == "*" || seg == "**" {
			continue
		}
		for _, c := range seg {
			if !isValidSegmentChar(c) {
				return fmt.Errorf("pattern %q segment %q contains invalid character %q (allowed: a-z 0-9 _ -)", pattern, seg, c)
			}
		}
	}
	return nil
}

// ParseRuleSetSpec parses a comma-separated DSL into a RuleSet.
//
// Grammar:
//
//	spec       := entry ("," entry)*
//	entry      := key "=" level
//	key        := "default" | pattern
//	pattern    := one or more dotted segments, validated by ValidatePattern
//	level      := "debug" | "info" | "warn" | "warning" | "error" (case-insensitive)
//
// "default=info,orchestrator.**=debug,ingester.relp=warn" parses to
// {Default: INFO, Rules: [{orchestrator.**, DEBUG}, {ingester.relp, WARN}]}
// (declaration order preserved in Rules).
//
// Whitespace around entries, keys, and levels is trimmed. Empty entries
// (consecutive commas, trailing comma) are skipped.
//
// Generation is drawn from NextGeneration so the returned RuleSet can
// be installed directly via SetRuleSet without colliding with any
// previously-installed rule set in the process.
func ParseRuleSetSpec(spec string) (RuleSet, error) {
	defaultLevel := slog.LevelInfo
	var rules []LevelRule
	seenDefault := false
	seenPattern := map[string]bool{}

	for entry := range strings.SplitSeq(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		keyRaw, valRaw, ok := strings.Cut(entry, "=")
		if !ok {
			return RuleSet{}, fmt.Errorf("log level spec %q: entry %q missing '='", spec, entry)
		}
		key := strings.TrimSpace(keyRaw)
		valStr := strings.TrimSpace(valRaw)
		level, err := parseLevelName(valStr)
		if err != nil {
			return RuleSet{}, fmt.Errorf("log level spec %q: entry %q: %w", spec, entry, err)
		}
		if key == "default" {
			if seenDefault {
				return RuleSet{}, fmt.Errorf("log level spec %q: 'default' specified more than once", spec)
			}
			defaultLevel = level
			seenDefault = true
			continue
		}
		if err := ValidatePattern(key); err != nil {
			return RuleSet{}, fmt.Errorf("log level spec %q: %w", spec, err)
		}
		if seenPattern[key] {
			return RuleSet{}, fmt.Errorf("log level spec %q: pattern %q specified more than once", spec, key)
		}
		seenPattern[key] = true
		rules = append(rules, LevelRule{Pattern: key, Level: level})
	}

	return NewRuleSet(defaultLevel, rules, NextGeneration()), nil
}

// parseLevelName maps slog level names (case-insensitive, "warning"
// allowed as an alias for "warn") to slog.Level. Unknown names return
// an error.
func parseLevelName(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown level %q (allowed: debug, info, warn, error)", s)
	}
}

func isValidSegmentChar(c rune) bool {
	switch {
	case c >= 'a' && c <= 'z':
		return true
	case c >= '0' && c <= '9':
		return true
	case c == '-' || c == '_':
		return true
	default:
		return false
	}
}

// LevelRule binds a component-path pattern to a minimum slog level.
//
// Patterns are dot-separated, like the component paths they match. Each
// segment is either a literal (e.g. "orchestrator") or a wildcard:
//
//   - "*"  matches exactly one path segment (not zero, not multiple).
//   - "**" matches zero or more path segments.
//
// Wildcards may appear anywhere in the pattern, not just at the end.
//
// Examples:
//
//	"orchestrator"                  exact, matches only that path
//	"orchestrator.*"                direct children (one segment deep)
//	"orchestrator.**"               orchestrator itself + any descendant
//	"ingester.*.conn"               every ingester type's conn subsystem
//	"ingester.**.conn"              any conn anywhere under ingester
//	"**"                            everything (use RuleSet.Default instead)
//
// Empty pattern is invalid. Operators express the "applies to everything"
// rule through RuleSet.Default, not via "**".
type LevelRule struct {
	Pattern string
	Level   slog.Level
}

// matches reports whether the rule's pattern covers the given component path.
func (r LevelRule) matches(path string) bool {
	if r.Pattern == "" || path == "" {
		return false
	}
	return matchSegments(strings.Split(r.Pattern, "."), strings.Split(path, "."))
}

// matchSegments is a recursive backtracking matcher.
//
// "*" consumes exactly one path segment; "**" tries every possible
// consumption from 0 to len(path) and recurses. The greedy backtracking
// is fine for the pattern lengths we expect (a handful of segments) —
// no need for a compiled NFA.
func matchSegments(pat, path []string) bool {
	for len(pat) > 0 {
		head := pat[0]
		switch head {
		case "**":
			// Zero-or-more segments: try every split point and recurse on the tail.
			rest := pat[1:]
			for i := 0; i <= len(path); i++ {
				if matchSegments(rest, path[i:]) {
					return true
				}
			}
			return false
		case "*":
			if len(path) == 0 {
				return false
			}
			pat = pat[1:]
			path = path[1:]
		default:
			if len(path) == 0 || path[0] != head {
				return false
			}
			pat = pat[1:]
			path = path[1:]
		}
	}
	return len(path) == 0
}

// specificity returns an ordering key: higher = more specific.
//
// Scoring (per segment, summed):
//
//   - literal: +10000   (most specific; a concrete name commits to one value)
//   - "*":     +100     (committed to exactly one segment, value unconstrained)
//   - "**":    -10      (uncommitted across any number of segments)
//
// Then +len(Pattern) as a final discriminator so longer-named patterns
// outrank shorter ones at the same shape (e.g. "orchestrator.*" outranks
// "x.*"). Ties beyond that are broken in Resolve by declaration order
// (strict > on the specificity check, so the first equally-specific
// rule encountered wins).
func (r LevelRule) specificity() int {
	score := len(r.Pattern)
	for seg := range strings.SplitSeq(r.Pattern, ".") {
		switch seg {
		case "**":
			score -= 10
		case "*":
			score += 100
		default:
			score += 10000
		}
	}
	return score
}

// RuleSet is the immutable resolution state of the filter handler:
// a fallback level plus an ordered list of rules.
//
// RuleSet values are swapped atomically — every derived handler reads
// the same shared pointer, so a single store propagates the new rules
// across every logger in the process.
type RuleSet struct {
	// Default is the level used when no rule matches a component path
	// (or when the component path itself is empty).
	Default slog.Level
	// Rules are pattern -> level mappings. Order is significant only as
	// a tiebreaker when two rules have identical specificity scores; the
	// most-specific match always wins regardless of position.
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
// Resolution: the most-specific matching rule wins. See specificity for
// the scoring. Ties are broken by declaration order in the Rules slice
// (first one encountered wins).
func (rs RuleSet) Resolve(path string) slog.Level {
	if path == "" {
		return rs.Default
	}
	bestSpec := -1 << 30
	bestLevel := rs.Default
	matched := false
	for _, r := range rs.Rules {
		if !r.matches(path) {
			continue
		}
		s := r.specificity()
		if !matched || s > bestSpec {
			matched = true
			bestSpec = s
			bestLevel = r.Level
		}
	}
	return bestLevel
}
