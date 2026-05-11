package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/logging"
	"gastrolog/internal/logging/comp"
	"gastrolog/internal/system"
)

// PutLogLevels validates and persists a new cluster-wide LogLevelConfig.
// The mutation flows through the system config store's Raft FSM; once
// committed, every node's ComponentFilterHandler picks up the new rule
// set via its configSignal subscription (see app.WatchLogLevels).
func (s *SystemServer) PutLogLevels(
	ctx context.Context,
	req *connect.Request[apiv1.PutLogLevelsRequest],
) (*connect.Response[apiv1.PutLogLevelsResponse], error) {
	if req.Msg == nil || req.Msg.GetConfig() == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config is required"))
	}
	cfg := convert.LogLevelConfigFromProto(req.Msg.GetConfig())
	if err := validateLogLevelConfig(cfg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.sysStore.PutLogLevels(ctx, cfg); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("put log levels: %w", err))
	}
	echo, err := s.newSettingsMutationEcho(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&apiv1.PutLogLevelsResponse{Echo: echo}), nil
}

// validateLogLevelConfig checks that every rule's pattern parses under
// the gitignore-style glob grammar. Levels are not range-checked here
// — UNSPECIFIED arrives as 0 (== INFO), which is the documented
// default; any int64 is a valid slog level.
func validateLogLevelConfig(cfg system.LogLevelConfig) error {
	seen := make(map[string]bool, len(cfg.Rules))
	for i, r := range cfg.Rules {
		if err := logging.ValidatePattern(r.Pattern); err != nil {
			return fmt.Errorf("rule[%d]: %w", i, err)
		}
		if seen[r.Pattern] {
			return fmt.Errorf("rule[%d]: pattern %q appears more than once", i, r.Pattern)
		}
		seen[r.Pattern] = true
	}
	return nil
}

// ListLogComponents returns every component path declared in this
// binary's comp registry, with each path's effective level resolved
// against the current ComponentFilterHandler rule set.
//
// The registry is populated at binary startup as packages run their
// var initializers; in any reachable code state All() returns the full
// set the operator can target. Resolution is local: every node runs
// the same binary, so the path list and resolver semantics are
// identical, but the live rule set is whatever this node's
// ComponentFilterHandler currently holds.
func (s *SystemServer) ListLogComponents(
	_ context.Context,
	_ *connect.Request[apiv1.ListLogComponentsRequest],
) (*connect.Response[apiv1.ListLogComponentsResponse], error) {
	paths := comp.All()
	var rules logging.RuleSet
	if s.logFilter != nil {
		rules = s.logFilter.RuleSet()
	}

	out := make([]*apiv1.LogComponentInfo, 0, len(paths))
	for _, p := range paths {
		level := rules.Resolve(p.String())
		out = append(out, &apiv1.LogComponentInfo{
			Path:           p.String(),
			EffectiveLevel: convert.SlogLevelToProto(int64(level)),
			Source:         resolutionSource(rules, p.String()),
		})
	}
	// Defensive sort even though comp.All() already sorts — the wire
	// guarantees stable ordering across CLI/UI clients.
	slices.SortFunc(out, func(a, b *apiv1.LogComponentInfo) int {
		return strings.Compare(a.GetPath(), b.GetPath())
	})

	return connect.NewResponse(&apiv1.ListLogComponentsResponse{Components: out}), nil
}

// resolutionSource walks the rule set the same way Resolve does to
// report which rule (if any) produced the effective level for a path.
// Duplicates a small amount of logic from RuleSet.Resolve to avoid
// adding a return-source variant; the cost is one extra walk per
// component per ListLogComponents call, which is bounded.
func resolutionSource(rs logging.RuleSet, path string) apiv1.LogComponentLevelSource {
	bestSpec := -1 << 30
	bestSource := apiv1.LogComponentLevelSource_LOG_LEVEL_SOURCE_DEFAULT
	matched := false
	for _, r := range rs.Rules {
		if !ruleMatches(r, path) {
			continue
		}
		spec := ruleSpecificity(r)
		if !matched || spec > bestSpec {
			matched = true
			bestSpec = spec
			if isGlobPattern(r.Pattern) {
				bestSource = apiv1.LogComponentLevelSource_LOG_LEVEL_SOURCE_GLOB_RULE
			} else {
				bestSource = apiv1.LogComponentLevelSource_LOG_LEVEL_SOURCE_EXACT_RULE
			}
		}
	}
	return bestSource
}

// ruleMatches / ruleSpecificity duplicate the logic that lives on
// logging.LevelRule's unexported methods. They are kept here rather
// than exported from the logging package to keep that package's API
// surface narrow; the source-discrimination is a server-side
// presentation concern, not part of the filter's hot path.
func ruleMatches(r logging.LevelRule, path string) bool {
	if r.Pattern == "" || path == "" {
		return false
	}
	return matchSegmentsServer(strings.Split(r.Pattern, "."), strings.Split(path, "."))
}

func matchSegmentsServer(pat, path []string) bool {
	for len(pat) > 0 {
		head := pat[0]
		switch head {
		case "**":
			rest := pat[1:]
			for i := 0; i <= len(path); i++ {
				if matchSegmentsServer(rest, path[i:]) {
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

func ruleSpecificity(r logging.LevelRule) int {
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

func isGlobPattern(p string) bool {
	for seg := range strings.SplitSeq(p, ".") {
		if seg == "*" || seg == "**" {
			return true
		}
	}
	return false
}
