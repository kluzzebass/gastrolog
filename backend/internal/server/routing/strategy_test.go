package routing_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gastrolog/internal/server/routing"
)

// connectGenDir returns the path to the generated connect files.
func connectGenDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "api", "gen", "gastrolog", "v1", "gastrologv1connect")
}

// extractProcedureConstants parses all *.connect.go files and extracts
// every exported const whose name ends in "Procedure".
func extractProcedureConstants(t *testing.T) map[string]string {
	t.Helper()
	dir := connectGenDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read connect gen dir %s: %v", dir, err)
	}

	// constName → string value
	procedures := make(map[string]string)
	fset := token.NewFileSet()

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".connect.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || len(vs.Names) == 0 {
					continue
				}
				name := vs.Names[0].Name
				if !strings.HasSuffix(name, "Procedure") || !ast.IsExported(name) {
					continue
				}
				// Extract the string literal value.
				if len(vs.Values) > 0 {
					if lit, ok := vs.Values[0].(*ast.BasicLit); ok {
						val := strings.Trim(lit.Value, `"`)
						procedures[name] = val
					}
				}
			}
		}
	}
	return procedures
}

// TestAllProceduresDeclared verifies every generated *Procedure constant
// appears in DefaultRoutes(). This is the primary enforcement mechanism:
// if you add a new RPC to the proto and regenerate, this test fails until
// you classify it in routes.go.
func TestAllProceduresDeclared(t *testing.T) {
	generatedProcs := extractProcedureConstants(t)
	if len(generatedProcs) == 0 {
		t.Fatal("no procedure constants found — is the connect gen dir correct?")
	}

	routes := routing.DefaultRoutes()
	registry := routing.NewRegistry(routes)

	// Check every generated procedure is in the registry.
	for constName, procValue := range generatedProcs {
		if _, ok := registry.Lookup(procValue); !ok {
			t.Errorf("generated procedure %s (%s) not declared in DefaultRoutes()", constName, procValue)
		}
	}

	// Check every registry entry maps to a real generated constant
	// (catches stale/renamed entries).
	generatedValues := make(map[string]bool, len(generatedProcs))
	for _, v := range generatedProcs {
		generatedValues[v] = true
	}
	for _, proc := range registry.Procedures() {
		if !generatedValues[proc] {
			t.Errorf("registry entry %q does not match any generated *Procedure constant (stale?)", proc)
		}
	}
}

// TestStrategyValuesNonZero verifies every DefaultRoutes() entry has a
// non-zero Strategy (catches default-initialized entries).
func TestStrategyValuesNonZero(t *testing.T) {
	for proc, route := range routing.DefaultRoutes() {
		if route.Strategy == 0 {
			t.Errorf("procedure %s has zero Strategy", proc)
		}
	}
}

// TestNoProcedureDuplicates verifies no procedure appears twice.
// Since DefaultRoutes() returns a map, Go already prevents this at the
// language level. This test documents the invariant.
func TestNoProcedureDuplicates(t *testing.T) {
	routes := routing.DefaultRoutes()
	if len(routes) != routing.NewRegistry(routes).Len() {
		t.Error("duplicate procedures detected")
	}
}

// TestStrategyDistribution verifies the expected counts per strategy.
// Fails if a new RPC shifts counts without updating the test — forces
// the author to think about the classification.
func TestStrategyDistribution(t *testing.T) {
	counts := map[routing.Strategy]int{}
	for _, route := range routing.DefaultRoutes() {
		counts[route.Strategy]++
	}

	want := map[routing.Strategy]int{
		routing.RouteLocal:    41, // +1: WatchChunks (gastrolog-1jijm), +1: PreviewJSONLookup (gastrolog-4q2b3)
		routing.RouteLeader:   36, // +1: DeleteLookup
		routing.RouteTargeted: 12,
		routing.RouteFanOut:   7,
	}

	for strategy, expected := range want {
		got := counts[strategy]
		if got != expected {
			t.Errorf("%s: got %d, want %d", strategy, got, expected)
		}
	}

	// Verify total.
	total := 0
	for _, c := range counts {
		total += c
	}
	if total != 96 {
		t.Errorf("total procedures: got %d, want 96", total)
	}
}

// TestRouteTargetedHaveWrapResponse verifies that every unary RouteTargeted
// entry has a non-nil WrapResponse function (the interceptor needs this to
// deserialize forwarded responses).
func TestRouteTargetedHaveWrapResponse(t *testing.T) {
	for proc, route := range routing.DefaultRoutes() {
		if route.Strategy != routing.RouteTargeted {
			continue
		}
		if route.IsStreaming {
			// Streaming RouteTargeted (ExportVault) is handled by the
			// handler, not the interceptor.
			continue
		}
		if route.WrapResponse == nil {
			t.Errorf("unary RouteTargeted procedure %s has nil WrapResponse", proc)
		}
	}
}
