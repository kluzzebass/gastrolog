package server

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// The frontend queries the syntax service for its keyword sets, but carries an
// offline fallback. That fallback is a second copy of the served lists, so it
// is held to them here: the pipe functions it names must be exactly the
// aggregates, bin and scalars the service advertises.
func TestFrontendSyntaxFallbackMatchesTheServedPipeFunctions(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "..", "frontend", "src", "lib", "syntaxSets.ts"))
	if err != nil {
		t.Skipf("frontend source not available: %v", err)
	}
	block := regexp.MustCompile(`(?s)pipeFunctions: new Set\(\[(.*?)\]\)`).FindStringSubmatch(string(src))
	if block == nil {
		t.Fatal("pipeFunctions set not found in syntaxSets.ts")
	}
	var fallback []string
	for _, m := range regexp.MustCompile(`"([^"]+)"`).FindAllStringSubmatch(block[1], -1) {
		fallback = append(fallback, m[1])
	}

	served := slices.Concat(query.AggFuncNames, []string{"bin"}, querylang.ScalarFuncNames)
	slices.Sort(fallback)
	slices.Sort(served)
	if !slices.Equal(fallback, served) {
		t.Fatalf("frontend fallback pipe functions differ from the served set\nfallback: %s\nserved:   %s",
			strings.Join(fallback, " "), strings.Join(served, " "))
	}
}
