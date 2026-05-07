package orchestrator

import (
	"slices"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// gastrolog-4kkoo (Phase 5): tests for the per-route, priority-ordered,
// first-match-wins routing engine. The Phase-4 FilterCatchRest (`+`)
// kind is gone — its semantics are expressible as an explicit catch-all
// at the lowest priority.

// dest builds a single local destination for the given vault.
func dest(vaultID glid.GLID) []RouteDestination {
	return []RouteDestination{{VaultID: vaultID}}
}

// destOn builds a single remote destination on the given node.
func destOn(vaultID glid.GLID, nodeID string) []RouteDestination {
	return []RouteDestination{{VaultID: vaultID, NodeID: nodeID}}
}

// matchVaults extracts the destination vault IDs from a list of
// MatchResult entries.
func matchVaults(rs []MatchResult) []glid.GLID {
	out := make([]glid.GLID, len(rs))
	for i, r := range rs {
		out[i] = r.VaultID
	}
	return out
}

func TestCompileRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		expr      string
		wantKind  MatchKind
		wantError bool
	}{
		{name: "empty expression", expr: "", wantKind: MatchNone},
		{name: "catch-all", expr: "*", wantKind: MatchAll},
		{name: "simple kv expression", expr: "env=prod", wantKind: MatchExpr},
		{name: "complex expression", expr: "env=prod AND level=error", wantKind: MatchExpr},
		{name: "or expression", expr: "env=staging OR env=dev", wantKind: MatchExpr},
		{name: "key exists", expr: "env=*", wantKind: MatchExpr},
		{name: "value exists", expr: "*=error", wantKind: MatchExpr},
		{name: "not expression", expr: "NOT env=prod", wantKind: MatchExpr},
		{name: "token predicate rejected", expr: "error", wantError: true},
		{name: "token in and expression rejected", expr: "error AND env=prod", wantError: true},
		{name: "invalid syntax", expr: "env=prod AND", wantError: true},
		// gastrolog-4kkoo (Phase 5): "+" is no longer a special kind.
		// It now parses as a token predicate and is rejected.
		{name: "catch-the-rest plus rejected", expr: "+", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := CompileRoute(glid.New(), "test", 0, tt.expr, dest(glid.New()), "fanout")
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.Kind != tt.wantKind {
				t.Errorf("got kind %v, want %v", r.Kind, tt.wantKind)
			}
		})
	}
}

func TestRouteSetFirstMatchWins(t *testing.T) {
	t.Parallel()
	prodID := glid.New()
	stagingID := glid.New()
	catchAllID := glid.New()

	// Three routes, all priority 0, distinguished by name. RouteSet
	// sorts by (priority asc, name asc), so:
	//   archive (catch-all) < prod-errors < staging
	prodErrors, _ := CompileRoute(glid.New(), "prod-errors", 0, "env=prod AND level=error", dest(prodID), "fanout")
	staging, _ := CompileRoute(glid.New(), "staging", 0, "env=staging", dest(stagingID), "fanout")
	catchAll, _ := CompileRoute(glid.New(), "archive", 0, "*", dest(catchAllID), "fanout")

	rs := NewRouteSet([]*CompiledRoute{prodErrors, staging, catchAll})

	// archive sorts first lexicographically and is a MatchAll route, so
	// every record terminates there before reaching the more specific
	// routes. This is the load-bearing semantic — operators put the
	// catch-all at the lowest priority (highest value) explicitly.
	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod", "level": "error"}))
	if len(got) != 1 || got[0] != catchAllID {
		t.Errorf("with archive at name-tiebreak winner, expected [archive], got %v", got)
	}
}

func TestRouteSetPriorityWins(t *testing.T) {
	t.Parallel()
	prodID := glid.New()
	catchAllID := glid.New()

	// prod-errors at priority 10 (fires first), catch-all at 100.
	prodErrors, _ := CompileRoute(glid.New(), "prod-errors", 10, "env=prod AND level=error", dest(prodID), "fanout")
	catchAll, _ := CompileRoute(glid.New(), "archive", 100, "*", dest(catchAllID), "fanout")

	rs := NewRouteSet([]*CompiledRoute{prodErrors, catchAll})

	// Matching record fires prod-errors only.
	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod", "level": "error"}))
	if len(got) != 1 || got[0] != prodID {
		t.Errorf("expected [prod], got %v", got)
	}
	// Non-matching record falls through to catch-all.
	got = matchVaults(rs.Match(chunk.Attributes{"env": "staging"}))
	if len(got) != 1 || got[0] != catchAllID {
		t.Errorf("expected [archive], got %v", got)
	}
}

func TestRouteSetNoMatchDrops(t *testing.T) {
	t.Parallel()
	prodID := glid.New()

	prod, _ := CompileRoute(glid.New(), "prod", 0, "env=prod", dest(prodID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{prod})

	got := rs.Match(chunk.Attributes{"env": "staging"})
	if len(got) != 0 {
		t.Errorf("expected drop, got %v", got)
	}
}

func TestRouteSetMatchAllFires(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	all, _ := CompileRoute(glid.New(), "all", 0, "*", dest(vaultID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{all})

	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod"}))
	if len(got) != 1 || got[0] != vaultID {
		t.Errorf("MatchAll should fire on any record, got %v", got)
	}
}

func TestRouteSetMatchNoneSkips(t *testing.T) {
	t.Parallel()
	mutedID := glid.New()
	catchID := glid.New()

	muted, _ := CompileRoute(glid.New(), "a-muted", 0, "", dest(mutedID), "fanout")
	catchAll, _ := CompileRoute(glid.New(), "b-catch", 0, "*", dest(catchID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{muted, catchAll})

	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod"}))
	if len(got) != 1 || got[0] != catchID {
		t.Errorf("MatchNone should be skipped, falling to next route; got %v", got)
	}
}

func TestRouteSetMultiDestination(t *testing.T) {
	t.Parallel()
	vaultA := glid.New()
	vaultB := glid.New()

	fanout, _ := CompileRoute(glid.New(), "fan", 0, "*", []RouteDestination{
		{VaultID: vaultA},
		{VaultID: vaultB},
	}, "fanout")
	rs := NewRouteSet([]*CompiledRoute{fanout})

	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod"}))
	if !sameElements(got, []glid.GLID{vaultA, vaultB}) {
		t.Errorf("expected both destinations, got %v", got)
	}
}

func TestRouteSetMatchPreservesNodeIDAndDistribution(t *testing.T) {
	t.Parallel()
	localID := glid.New()
	remoteID := glid.New()

	r, _ := CompileRoute(glid.New(), "mixed", 0, "*", []RouteDestination{
		{VaultID: localID},
		{VaultID: remoteID, NodeID: "node-B"},
	}, "round-robin")
	rs := NewRouteSet([]*CompiledRoute{r})

	results := rs.Match(chunk.Attributes{"env": "prod"})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for _, m := range results {
		if m.RouteID != r.RouteID {
			t.Errorf("expected RouteID %v, got %v", r.RouteID, m.RouteID)
		}
		if m.Distribution != "round-robin" {
			t.Errorf("expected round-robin, got %q", m.Distribution)
		}
		if m.VaultID == localID && m.NodeID != "" {
			t.Errorf("local vault should have empty NodeID, got %q", m.NodeID)
		}
		if m.VaultID == remoteID && m.NodeID != "node-B" {
			t.Errorf("remote vault NodeID = %q, want node-B", m.NodeID)
		}
	}
}

func TestRouteSetNameTiebreaker(t *testing.T) {
	t.Parallel()
	aID := glid.New()
	bID := glid.New()

	// Both at priority 5; "a" sorts before "b" → a wins.
	a, _ := CompileRoute(glid.New(), "a-route", 5, "*", dest(aID), "fanout")
	b, _ := CompileRoute(glid.New(), "b-route", 5, "*", dest(bID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{b, a}) // intentionally reversed input

	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod"}))
	if len(got) != 1 || got[0] != aID {
		t.Errorf("name tiebreaker should pick a-route, got %v", got)
	}
}

func TestRouteSetRoutesReturnsSorted(t *testing.T) {
	t.Parallel()
	r1, _ := CompileRoute(glid.New(), "z", 100, "*", dest(glid.New()), "fanout")
	r2, _ := CompileRoute(glid.New(), "a", 1, "*", dest(glid.New()), "fanout")
	r3, _ := CompileRoute(glid.New(), "m", 50, "*", dest(glid.New()), "fanout")
	rs := NewRouteSet([]*CompiledRoute{r1, r2, r3})

	got := rs.Routes()
	if len(got) != 3 {
		t.Fatalf("got %d routes, want 3", len(got))
	}
	if got[0].Name != "a" || got[1].Name != "m" || got[2].Name != "z" {
		t.Errorf("got order %s,%s,%s; want a,m,z", got[0].Name, got[1].Name, got[2].Name)
	}
}

// gastrolog-4kkoo (Phase 5): synthetic-attribute injection.

func TestRouteSetMatchWithSourceInjectsSyntheticAttrs(t *testing.T) {
	t.Parallel()
	ingestVault := glid.New()
	retentionVault := glid.New()
	ingesterID := glid.New()

	// One route per source kind. Both at priority 0; route names are
	// chosen so name tiebreaker doesn't cross-contaminate.
	ingestRoute, err := CompileRoute(glid.New(), "a-ingest", 0,
		`_source="ingest"`,
		dest(ingestVault), "fanout")
	if err != nil {
		t.Fatalf("CompileRoute ingest: %v", err)
	}
	retentionRoute, err := CompileRoute(glid.New(), "b-retention", 0,
		`_source="retention"`,
		dest(retentionVault), "fanout")
	if err != nil {
		t.Fatalf("CompileRoute retention: %v", err)
	}
	rs := NewRouteSet([]*CompiledRoute{ingestRoute, retentionRoute})

	// Ingest source fires the ingest route, not retention.
	got := matchVaults(rs.MatchWithSource(chunk.Attributes{"env": "prod"}, SourceContext{
		Kind:       SourceIngest,
		IngesterID: ingesterID,
	}))
	if len(got) != 1 || got[0] != ingestVault {
		t.Errorf("ingest source: expected [ingest], got %v", got)
	}

	// Retention source fires the retention route.
	got = matchVaults(rs.MatchWithSource(chunk.Attributes{"env": "prod"}, SourceContext{
		Kind:    SourceRetention,
		VaultID: glid.New(),
		Reason:  "age",
	}))
	if len(got) != 1 || got[0] != retentionVault {
		t.Errorf("retention source: expected [retention], got %v", got)
	}

	// Plain Match (no source overlay) sees neither — nothing to drop into.
	got = matchVaults(rs.Match(chunk.Attributes{"env": "prod"}))
	if len(got) != 0 {
		t.Errorf("plain Match without source overlay should miss both routes, got %v", got)
	}
}

func TestRouteSetMatchOnSpecificIngester(t *testing.T) {
	t.Parallel()
	targetID := glid.New()
	otherID := glid.New()
	vaultID := glid.New()

	r, err := CompileRoute(glid.New(), "ing-specific", 0,
		`_ingester="`+targetID.String()+`"`,
		dest(vaultID), "fanout")
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	rs := NewRouteSet([]*CompiledRoute{r})

	// Records from the target ingester fire.
	got := matchVaults(rs.MatchWithSource(chunk.Attributes{}, SourceContext{
		Kind: SourceIngest, IngesterID: targetID,
	}))
	if len(got) != 1 || got[0] != vaultID {
		t.Errorf("target ingester: expected [vault], got %v", got)
	}

	// Records from another ingester drop.
	got = matchVaults(rs.MatchWithSource(chunk.Attributes{}, SourceContext{
		Kind: SourceIngest, IngesterID: otherID,
	}))
	if len(got) != 0 {
		t.Errorf("other ingester: expected drop, got %v", got)
	}
}

func TestRouteSetMatchOnRetentionReason(t *testing.T) {
	t.Parallel()
	archiveID := glid.New()
	vaultID := glid.New()

	r, err := CompileRoute(glid.New(), "age-archive", 0,
		`_source="retention" AND _reason="age"`,
		dest(archiveID), "fanout")
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	rs := NewRouteSet([]*CompiledRoute{r})

	// Retention by age fires.
	got := matchVaults(rs.MatchWithSource(chunk.Attributes{}, SourceContext{
		Kind: SourceRetention, VaultID: vaultID, Reason: "age",
	}))
	if len(got) != 1 || got[0] != archiveID {
		t.Errorf("retention by age: expected [archive], got %v", got)
	}

	// Retention by size doesn't fire.
	got = matchVaults(rs.MatchWithSource(chunk.Attributes{}, SourceContext{
		Kind: SourceRetention, VaultID: vaultID, Reason: "size",
	}))
	if len(got) != 0 {
		t.Errorf("retention by size: expected drop, got %v", got)
	}
}

func TestMatchWithSourceDoesNotMutateAttrs(t *testing.T) {
	t.Parallel()
	r, _ := CompileRoute(glid.New(), "all", 0, "*",
		dest(glid.New()), "fanout")
	rs := NewRouteSet([]*CompiledRoute{r})

	attrs := chunk.Attributes{"env": "prod"}
	original := len(attrs)
	rs.MatchWithSource(attrs, SourceContext{Kind: SourceIngest, IngesterID: glid.New()})

	// The caller's attrs map must not have synthetic keys spliced in.
	if len(attrs) != original {
		t.Errorf("attrs map mutated: expected %d entries, got %d", original, len(attrs))
	}
	for k := range attrs {
		if len(k) > 0 && k[0] == '_' {
			t.Errorf("synthetic key leaked into caller's attrs: %q", k)
		}
	}
}

// Per-expression behavior tests — preserved from the Phase-4 suite,
// adapted to the per-route shape.

func TestRouteSetCaseInsensitiveMatching(t *testing.T) {
	t.Parallel()
	prodID := glid.New()
	r, _ := CompileRoute(glid.New(), "prod", 0, "env=PROD", dest(prodID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{r})

	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod"}))
	if len(got) != 1 || got[0] != prodID {
		t.Errorf("should match case-insensitively: %v", got)
	}
	got = matchVaults(rs.Match(chunk.Attributes{"ENV": "PROD"}))
	if len(got) != 1 || got[0] != prodID {
		t.Errorf("should match case-insensitively with uppercase key: %v", got)
	}
}

func TestRouteSetNotExpression(t *testing.T) {
	t.Parallel()
	notProdID := glid.New()
	r, _ := CompileRoute(glid.New(), "not-prod", 0, "NOT env=prod", dest(notProdID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{r})

	got := matchVaults(rs.Match(chunk.Attributes{"env": "prod"}))
	if slices.Contains(got, notProdID) {
		t.Errorf("NOT env=prod should not match env=prod: %v", got)
	}
	got = matchVaults(rs.Match(chunk.Attributes{"env": "staging"}))
	if !slices.Contains(got, notProdID) {
		t.Errorf("NOT env=prod should match env=staging: %v", got)
	}
}

func TestRouteSetKeyExists(t *testing.T) {
	t.Parallel()
	hasEnvID := glid.New()
	r, _ := CompileRoute(glid.New(), "has-env", 0, "env=*", dest(hasEnvID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{r})

	got := matchVaults(rs.Match(chunk.Attributes{"env": "anything"}))
	if !slices.Contains(got, hasEnvID) {
		t.Errorf("env=* should match when env key exists: %v", got)
	}
	got = matchVaults(rs.Match(chunk.Attributes{"ENV": "anything"}))
	if !slices.Contains(got, hasEnvID) {
		t.Errorf("env=* should match ENV key case-insensitively: %v", got)
	}
	got = matchVaults(rs.Match(chunk.Attributes{"other": "value"}))
	if slices.Contains(got, hasEnvID) {
		t.Errorf("env=* should not match when env key is missing: %v", got)
	}
}

func TestRouteSetValueExists(t *testing.T) {
	t.Parallel()
	hasErrorID := glid.New()
	r, _ := CompileRoute(glid.New(), "has-error", 0, "*=error", dest(hasErrorID), "fanout")
	rs := NewRouteSet([]*CompiledRoute{r})

	got := matchVaults(rs.Match(chunk.Attributes{"level": "error"}))
	if !slices.Contains(got, hasErrorID) {
		t.Errorf("*=error should match when error value exists: %v", got)
	}
	got = matchVaults(rs.Match(chunk.Attributes{"level": "ERROR"}))
	if !slices.Contains(got, hasErrorID) {
		t.Errorf("*=error should match ERROR value case-insensitively: %v", got)
	}
	got = matchVaults(rs.Match(chunk.Attributes{"level": "info"}))
	if slices.Contains(got, hasErrorID) {
		t.Errorf("*=error should not match when error value is missing: %v", got)
	}
}

// Helper functions.

func sameElements(a, b []glid.GLID) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[glid.GLID]int)
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	return true
}

// Used by other test files in this package.
func containsUUID(slice []glid.GLID, id glid.GLID) bool { //nolint:unused // used by sibling test files
	return slices.Contains(slice, id)
}

// Suppress unused import if unused in this file but used in siblings.
var _ = destOn
