package comp

import (
	"bytes"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestPath_String(t *testing.T) {
	p := Root("orch_str").Sub("replication").Sub("catchup")
	if got, want := p.String(), "orch_str.replication.catchup"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestPath_Apply(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	scoped := Root("orch_apply").Sub("replication").Apply(logger)
	scoped.Info("hello")
	if !strings.Contains(buf.String(), "component=orch_apply.replication") {
		t.Errorf("expected component attr in output, got: %s", buf.String())
	}
}

func TestPath_Apply_NilLogger(t *testing.T) {
	if got := Root("apply_nil").Apply(nil); got != nil {
		t.Errorf("Apply(nil) = %v, want nil", got)
	}
}

func TestRoot_RejectsInvalidSegment(t *testing.T) {
	cases := []string{"", ".", "a.b", "a*", "*"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("Root(%q) did not panic", name)
				}
			}()
			_ = Root(name)
		})
	}
}

func TestSub_RejectsInvalidSegment(t *testing.T) {
	p := Root("sub_invalid")
	cases := []string{"", "a.b", "*"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("Sub(%q) did not panic", name)
				}
			}()
			_ = p.Sub(name)
		})
	}
}

func TestSub_RejectsZeroPath(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Sub on zero Path did not panic")
		}
	}()
	var p Path
	_ = p.Sub("anything")
}

// The auto-registration tests reset the registry to assert deterministic
// state. They run sequentially (no t.Parallel) and share the lock with
// every other Root/Sub call, so the other tests in this file that DON'T
// reset use distinct names to avoid clobbering each other's assertions.

func TestRoot_AutoRegisters(t *testing.T) {
	resetRegistryForTest()
	_ = Root("alpha")
	_ = Root("beta")
	got := All()
	if !containsPath(got, "alpha") || !containsPath(got, "beta") {
		t.Errorf("All() = %v, want to include alpha and beta", names(got))
	}
}

func TestSub_AutoRegisters(t *testing.T) {
	resetRegistryForTest()
	parent := Root("orch_reg")
	parent.Sub("replication")
	parent.Sub("replication").Sub("catchup")
	got := All()
	wantSubset := []string{"orch_reg", "orch_reg.replication", "orch_reg.replication.catchup"}
	for _, w := range wantSubset {
		if !containsPath(got, w) {
			t.Errorf("All() = %v, missing %q", names(got), w)
		}
	}
}

func TestRegistry_Dedups(t *testing.T) {
	resetRegistryForTest()
	_ = Root("dup")
	_ = Root("dup")
	_ = Root("dup").Sub("x")
	_ = Root("dup").Sub("x")
	got := All()
	if count := countPath(got, "dup"); count != 1 {
		t.Errorf("dup occurrences = %d, want 1", count)
	}
	if count := countPath(got, "dup.x"); count != 1 {
		t.Errorf("dup.x occurrences = %d, want 1", count)
	}
}

func TestAll_ReturnsSortedDefensiveCopy(t *testing.T) {
	resetRegistryForTest()
	_ = Root("zeta")
	_ = Root("alpha").Sub("y")
	_ = Root("alpha")
	a := All()
	b := All()
	if &a[0] == &b[0] {
		t.Error("All() returned the same underlying array twice; must be a copy")
	}
	sorted := slices.IsSortedFunc(a, func(x, y Path) int { return strings.Compare(x.s, y.s) })
	if !sorted {
		t.Errorf("All() not sorted: %v", names(a))
	}
}

func TestRegistry_ConcurrentSafe(t *testing.T) {
	resetRegistryForTest()
	const goroutines = 8
	const iterations = 50
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			parent := Root("concurrent")
			for i := range iterations {
				parent.Sub("child").Sub(itoa(g*iterations + i))
			}
		})
	}
	wg.Wait()
	got := All()
	if count := countPath(got, "concurrent"); count != 1 {
		t.Errorf("concurrent root duplicated: count = %d, want 1", count)
	}
	if count := countPath(got, "concurrent.child"); count != 1 {
		t.Errorf("concurrent.child duplicated: count = %d, want 1", count)
	}
	const wantLeaves = goroutines * iterations
	leaves := 0
	for _, p := range got {
		if strings.HasPrefix(p.s, "concurrent.child.") {
			leaves++
		}
	}
	if leaves != wantLeaves {
		t.Errorf("concurrent leaves = %d, want %d", leaves, wantLeaves)
	}
}

func containsPath(paths []Path, s string) bool {
	return slices.ContainsFunc(paths, func(p Path) bool { return p.s == s })
}

func countPath(paths []Path, s string) int {
	n := 0
	for _, p := range paths {
		if p.s == s {
			n++
		}
	}
	return n
}

func names(paths []Path) []string {
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = p.s
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b strings.Builder
	if n < 0 {
		b.WriteByte('-')
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	for i := len(digits) - 1; i >= 0; i-- {
		b.WriteByte(digits[i])
	}
	return b.String()
}
