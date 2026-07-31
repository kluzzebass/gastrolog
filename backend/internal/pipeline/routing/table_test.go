package routing_test

import (
	"slices"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/record"
)

func sameElements(a, b []glid.GLID) bool {
	if len(a) != len(b) {
		return false
	}
	cp := slices.Clone(a)
	for _, x := range b {
		i := slices.Index(cp, x)
		if i < 0 {
			return false
		}
		cp = slices.Delete(cp, i, i+1)
	}
	return len(cp) == 0
}

func TestCompileRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		expr      string
		wantKind  routing.MatchKind
		wantError bool
	}{
		{name: "empty expression", expr: "", wantKind: routing.MatchNone},
		{name: "catch-all", expr: "*", wantKind: routing.MatchAll},
		{name: "simple kv expression", expr: "env=prod", wantKind: routing.MatchExpr},
		{name: "complex expression", expr: "env=prod AND level=error", wantKind: routing.MatchExpr},
		{name: "or expression", expr: "env=staging OR env=dev", wantKind: routing.MatchExpr},
		{name: "key exists", expr: "env=*", wantKind: routing.MatchExpr},
		{name: "value exists", expr: "*=error", wantKind: routing.MatchExpr},
		{name: "not expression", expr: "NOT env=prod", wantKind: routing.MatchExpr},
		{name: "token predicate rejected", expr: "error", wantError: true},
		{name: "token in and expression rejected", expr: "error AND env=prod", wantError: true},
		{name: "invalid syntax", expr: "env=prod AND", wantError: true},
		{name: "catch-the-rest plus rejected", expr: "+", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := routing.CompileRoute(glid.New(), "test", 0, tt.expr, []glid.GLID{glid.New()})
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

func TestTableFirstMatchWins(t *testing.T) {
	t.Parallel()
	prodID := glid.New()
	stagingID := glid.New()
	catchAllID := glid.New()

	prodErrors, _ := routing.CompileRoute(glid.New(), "prod-errors", 0, "env=prod AND level=error", []glid.GLID{prodID})
	staging, _ := routing.CompileRoute(glid.New(), "staging", 0, "env=staging", []glid.GLID{stagingID})
	catchAll, _ := routing.CompileRoute(glid.New(), "archive", 0, "*", []glid.GLID{catchAllID})

	table := routing.NewTable([]*routing.Route{prodErrors, staging, catchAll})

	got := table.Match(record.Attributes{"env": "prod", "level": "error"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != catchAllID {
		t.Errorf("with archive at name-tiebreak winner, expected [archive], got %v", got)
	}
}

func TestTablePriorityWins(t *testing.T) {
	t.Parallel()
	prodID := glid.New()
	catchAllID := glid.New()

	prodErrors, _ := routing.CompileRoute(glid.New(), "prod-errors", 10, "env=prod AND level=error", []glid.GLID{prodID})
	catchAll, _ := routing.CompileRoute(glid.New(), "archive", 100, "*", []glid.GLID{catchAllID})

	table := routing.NewTable([]*routing.Route{prodErrors, catchAll})

	got := table.Match(record.Attributes{"env": "prod", "level": "error"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != prodID {
		t.Errorf("expected [prod], got %v", got)
	}
	got = table.Match(record.Attributes{"env": "staging"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != catchAllID {
		t.Errorf("expected [archive], got %v", got)
	}
}

func TestTableNoMatchDrops(t *testing.T) {
	t.Parallel()
	prodID := glid.New()
	prod, _ := routing.CompileRoute(glid.New(), "prod", 0, "env=prod", []glid.GLID{prodID})
	table := routing.NewTable([]*routing.Route{prod})

	got := table.Match(record.Attributes{"env": "staging"}, routing.SourceContext{})
	if len(got) != 0 {
		t.Errorf("expected drop, got %v", got)
	}
}

func TestTableMatchAllFires(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	all, _ := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{vaultID})
	table := routing.NewTable([]*routing.Route{all})

	got := table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != vaultID {
		t.Errorf("MatchAll should fire on any record, got %v", got)
	}
}

func TestTableMatchNoneSkips(t *testing.T) {
	t.Parallel()
	mutedID := glid.New()
	catchID := glid.New()
	muted, _ := routing.CompileRoute(glid.New(), "a-muted", 0, "", []glid.GLID{mutedID})
	catchAll, _ := routing.CompileRoute(glid.New(), "b-catch", 0, "*", []glid.GLID{catchID})
	table := routing.NewTable([]*routing.Route{muted, catchAll})

	got := table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != catchID {
		t.Errorf("MatchNone should be skipped, falling to next route; got %v", got)
	}
}

func TestTableMultiDestination(t *testing.T) {
	t.Parallel()
	vaultA := glid.New()
	vaultB := glid.New()
	fanout, _ := routing.CompileRoute(glid.New(), "fan", 0, "*", []glid.GLID{vaultA, vaultB})
	table := routing.NewTable([]*routing.Route{fanout})

	got := table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{})
	if !sameElements(got, []glid.GLID{vaultA, vaultB}) {
		t.Errorf("expected both destinations, got %v", got)
	}
}

func TestTableNameTiebreaker(t *testing.T) {
	t.Parallel()
	aID := glid.New()
	bID := glid.New()
	a, _ := routing.CompileRoute(glid.New(), "a-route", 5, "*", []glid.GLID{aID})
	b, _ := routing.CompileRoute(glid.New(), "b-route", 5, "*", []glid.GLID{bID})
	table := routing.NewTable([]*routing.Route{b, a})

	got := table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != aID {
		t.Errorf("name tiebreaker should pick a-route, got %v", got)
	}
}

func TestTableMatchWithSourceInjectsSyntheticAttrs(t *testing.T) {
	t.Parallel()
	ingestVault := glid.New()
	retentionVault := glid.New()
	ingesterID := glid.New()

	ingestRoute, err := routing.CompileRoute(glid.New(), "a-ingest", 0,
		`_source="ingest"`, []glid.GLID{ingestVault})
	if err != nil {
		t.Fatalf("CompileRoute ingest: %v", err)
	}
	retentionRoute, err := routing.CompileRoute(glid.New(), "b-retention", 0,
		`_source="retention"`, []glid.GLID{retentionVault})
	if err != nil {
		t.Fatalf("CompileRoute retention: %v", err)
	}
	table := routing.NewTable([]*routing.Route{ingestRoute, retentionRoute})

	got := table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{
		Kind:       routing.SourceIngest,
		IngesterID: ingesterID,
	})
	if len(got) != 1 || got[0] != ingestVault {
		t.Errorf("ingest source: expected [ingest], got %v", got)
	}

	got = table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{
		Kind:    routing.SourceRetention,
		VaultID: glid.New(),
		Reason:  "age",
	})
	if len(got) != 1 || got[0] != retentionVault {
		t.Errorf("retention source: expected [retention], got %v", got)
	}

	got = table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{})
	if len(got) != 0 {
		t.Errorf("plain Match without source overlay should miss both routes, got %v", got)
	}
}

func TestTableMatchOnSpecificIngester(t *testing.T) {
	t.Parallel()
	targetID := glid.New()
	otherID := glid.New()
	vaultID := glid.New()

	r, err := routing.CompileRoute(glid.New(), "ing-specific", 0,
		`_ingester="`+targetID.String()+`"`, []glid.GLID{vaultID})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	table := routing.NewTable([]*routing.Route{r})

	got := table.Match(record.Attributes{}, routing.SourceContext{
		Kind: routing.SourceIngest, IngesterID: targetID,
	})
	if len(got) != 1 || got[0] != vaultID {
		t.Errorf("target ingester: expected [vault], got %v", got)
	}

	got = table.Match(record.Attributes{}, routing.SourceContext{
		Kind: routing.SourceIngest, IngesterID: otherID,
	})
	if len(got) != 0 {
		t.Errorf("other ingester: expected drop, got %v", got)
	}
}

func TestTableMatchOnRetentionReason(t *testing.T) {
	t.Parallel()
	archiveID := glid.New()
	vaultID := glid.New()

	r, err := routing.CompileRoute(glid.New(), "age-archive", 0,
		`_source="retention" AND _reason="age"`, []glid.GLID{archiveID})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	table := routing.NewTable([]*routing.Route{r})

	got := table.Match(record.Attributes{}, routing.SourceContext{
		Kind: routing.SourceRetention, VaultID: vaultID, Reason: "age",
	})
	if len(got) != 1 || got[0] != archiveID {
		t.Errorf("retention by age: expected [archive], got %v", got)
	}

	got = table.Match(record.Attributes{}, routing.SourceContext{
		Kind: routing.SourceRetention, VaultID: vaultID, Reason: "size",
	})
	if len(got) != 0 {
		t.Errorf("retention by size: expected drop, got %v", got)
	}
}

func TestMatchDoesNotMutateAttrs(t *testing.T) {
	t.Parallel()
	r, _ := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{glid.New()})
	table := routing.NewTable([]*routing.Route{r})

	attrs := record.Attributes{"env": "prod"}
	original := len(attrs)
	table.Match(attrs, routing.SourceContext{Kind: routing.SourceIngest, IngesterID: glid.New()})

	if len(attrs) != original {
		t.Errorf("attrs map mutated: expected %d entries, got %d", original, len(attrs))
	}
	for k := range attrs {
		if len(k) > 0 && k[0] == '_' {
			t.Errorf("synthetic key leaked into caller's attrs: %q", k)
		}
	}
}

func TestTableCaseInsensitiveMatching(t *testing.T) {
	t.Parallel()
	prodID := glid.New()
	r, _ := routing.CompileRoute(glid.New(), "prod", 0, "env=PROD", []glid.GLID{prodID})
	table := routing.NewTable([]*routing.Route{r})

	got := table.Match(record.Attributes{"env": "prod"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != prodID {
		t.Errorf("should match case-insensitively: %v", got)
	}
	got = table.Match(record.Attributes{"ENV": "PROD"}, routing.SourceContext{})
	if len(got) != 1 || got[0] != prodID {
		t.Errorf("should match case-insensitively with uppercase key: %v", got)
	}
}

func TestIngestSource(t *testing.T) {
	t.Parallel()
	ingesterID := glid.New()
	rec := &record.Record{
		EventID: record.EventID{IngesterID: ingesterID},
	}
	src := routing.IngestSource(rec)
	if src.Kind != routing.SourceIngest || src.IngesterID != ingesterID {
		t.Errorf("IngestSource = %+v, want ingest + ingester", src)
	}
	if routing.IngestSource(nil).Kind != routing.SourceIngest {
		t.Errorf("IngestSource(nil) should still be ingest kind")
	}
}

func TestRetentionSource(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	src := routing.RetentionSource(vaultID, "age")
	if src.Kind != routing.SourceRetention || src.VaultID != vaultID || src.Reason != "age" {
		t.Errorf("RetentionSource = %+v", src)
	}
}

// TestOverlaySemantics: the zero-copy srcOverlay must layer synthetic
// source attributes over a record's own attrs exactly: set synthetic keys
// shadow record attrs of the same name, unset synthetics fall through,
// and scan-based predicates (case-insensitive, glob keys, value-exists)
// see both layers without duplicates.
func TestOverlaySemantics(t *testing.T) {
	t.Parallel()
	vaultA := glid.New()
	srcVault := glid.New()

	mk := func(expr string) *routing.Table {
		r, err := routing.CompileRoute(glid.New(), "r", 0, expr, []glid.GLID{vaultA})
		if err != nil {
			t.Fatalf("compile %q: %v", expr, err)
		}
		return routing.NewTable([]*routing.Route{r})
	}
	retention := routing.RetentionSource(srcVault, "age")

	// Synthetic key matches via exact lookup.
	if got := mk("_source=retention").Match(record.Attributes{"app": "x"}, retention); len(got) != 1 {
		t.Fatal("_source=retention must match on retention source")
	}
	// Set synthetic shadows a record attr of the same name.
	if got := mk("_source=fake").Match(record.Attributes{"_source": "fake"}, retention); len(got) != 0 {
		t.Fatal("set synthetic must shadow record attr of the same name")
	}
	// Unset synthetic falls through to the record attr.
	if got := mk("_ingester=abc").Match(record.Attributes{"_ingester": "abc"}, retention); len(got) != 1 {
		t.Fatal("unset synthetic must fall through to record attr")
	}
	// Case-insensitive predicate finds a record attr through the overlay.
	if got := mk("APP=x").Match(record.Attributes{"app": "x"}, retention); len(got) != 1 {
		t.Fatal("case-insensitive key must find record attr through overlay")
	}
	// Value-exists scan sees the synthetic layer.
	if got := mk("*=age").Match(record.Attributes{"app": "x"}, retention); len(got) != 1 {
		t.Fatal("value-exists must see synthetic reason value")
	}
	// Key-exists via glob sees record attrs.
	if got := mk("ap*=x").Match(record.Attributes{"app": "x"}, retention); len(got) != 1 {
		t.Fatal("glob key must see record attrs through overlay")
	}
}

// BenchmarkMatchRoute measures per-record match cost: the srcOverlay
// replaced a full map copy per record (make + maps.Copy, ~10GB/run).
func BenchmarkMatchRoute(b *testing.B) {
	vaultA := glid.New()
	r, err := routing.CompileRoute(glid.New(), "r", 0, "env=prod AND level=error", []glid.GLID{vaultA})
	if err != nil {
		b.Fatal(err)
	}
	tbl := routing.NewTable([]*routing.Route{r})
	attrs := record.Attributes{
		"env": "prod", "level": "error", "app": "api", "host": "h1",
		"dc": "eu-1", "team": "core", "svc": "ingest", "ver": "1.2.3",
	}
	src := routing.RetentionSource(vaultA, "age")
	b.ReportAllocs()
	for b.Loop() {
		if got := tbl.Match(attrs, src); len(got) != 1 {
			b.Fatal("expected match")
		}
	}
}
