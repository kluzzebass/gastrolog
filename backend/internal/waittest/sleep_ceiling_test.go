package waittest_test

// Tests that synchronise on the clock instead of on the condition fail on
// busy machines while the code works, and the habit spreads one convenient
// sleep at a time — the flake hunt that prompted this package found five in
// one week. This guard freezes each package's count of time.Sleep call sites
// in test files: adding one fails here until its ceiling is raised in the
// same diff, turning a habit into a reviewed decision. A sleep that IS the
// subject — a simulated slow dependency, a duration under test, a poll pause
// inside a condition wait — is legitimate; raise the ceiling and say why in
// the commit. Counts below the ceiling pass, so removals cost nothing.

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// sleepCeilings freezes the test-file time.Sleep count per package at the
// value measured when the guard landed. Raise an entry deliberately; lower
// one whenever a cleanup allows.
var sleepCeilings = map[string]int{
	"internal/app":                   14,
	"internal/callgroup":             3,
	"internal/chunk/file":            1,
	"internal/cluster":               23,
	"internal/index":                 4,
	"internal/ingester/http":         1,
	"internal/ingester/limits":       1,
	"internal/ingester/relp":         1,
	"internal/ingester/self":         1,
	"internal/ingester/tail":         7,
	"internal/locktrack":             2,
	"internal/lookup":                4,
	"internal/multiraft":             2,
	"internal/orchestrator":          10,
	"internal/pipeline/chunking":     1,
	"internal/pipeline/collection":   3,
	"internal/pipeline/digestion":    1,
	"internal/raftgroup":             5,
	"internal/raftwal":               14,
	"internal/schedwatch":            1,
	"internal/server":                18,
	"internal/vaultraft":             2,
	"internal/vaultraft/vaultctlfsm": 5,
}

func TestSleepCountStaysWithinCeilings(t *testing.T) {
	t.Parallel()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this file")
	}
	backendRoot := filepath.Clean(filepath.Join(filepath.Dir(self), "..", ".."))

	counts := map[string]int{}
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(backendRoot, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path) //nolint:gosec // G304: paths come from walking our own source tree
			if err != nil {
				return err
			}
			n := 0
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "//") {
					continue
				}
				// The needle is split so this file does not count its own
				// search pattern.
				n += strings.Count(line, "time.Sleep"+"(")
			}
			if n > 0 {
				rel, err := filepath.Rel(backendRoot, filepath.Dir(path))
				if err != nil {
					return err
				}
				counts[rel] += n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for pkg, n := range counts {
		ceiling, known := sleepCeilings[pkg]
		if !known {
			t.Errorf("%s has %d time.Sleep call sites in tests and no ceiling — synchronise on the condition (waittest.For) instead, or add a ceiling here deliberately", pkg, n)
			continue
		}
		if n > ceiling {
			t.Errorf("%s has %d time.Sleep call sites in tests, ceiling is %d — synchronise on the condition (waittest.For) instead, or raise the ceiling here in the same change and say why", pkg, n, ceiling)
		}
	}
	for pkg := range sleepCeilings {
		if _, err := os.Stat(filepath.Join(backendRoot, pkg)); err != nil {
			t.Errorf("ceiling entry %q names a package directory that no longer exists; remove it", pkg)
		}
	}
}
