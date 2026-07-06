package logging

import (
	"fmt"
	"testing"
	"time"
)

func TestThrottleFirstEmissionPassesThenSuppresses(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	th := Throttle{Interval: 30 * time.Second, Now: func() time.Time { return now }}

	if n, ok := th.Allow("vault-a"); !ok || n != 0 {
		t.Fatalf("first Allow = (%d,%v), want (0,true)", n, ok)
	}
	for range 5 {
		if _, ok := th.Allow("vault-a"); ok {
			t.Fatal("Allow inside interval must suppress")
		}
	}
	// Independent key is unaffected.
	if _, ok := th.Allow("vault-b"); !ok {
		t.Fatal("independent key suppressed")
	}

	now = now.Add(31 * time.Second)
	n, ok := th.Allow("vault-a")
	if !ok || n != 5 {
		t.Fatalf("post-interval Allow = (%d,%v), want (5,true)", n, ok)
	}
	// Counter reset after reporting.
	now = now.Add(31 * time.Second)
	if n, ok := th.Allow("vault-a"); !ok || n != 0 {
		t.Fatalf("Allow after quiet interval = (%d,%v), want (0,true)", n, ok)
	}
}

func TestThrottlePrunesIdleKeys(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	th := Throttle{Interval: time.Second, Now: func() time.Time { return now }}

	for i := range throttlePruneAbove + 10 {
		th.Allow(fmt.Sprintf("key-%d", i))
	}
	// All keys idle beyond the prune horizon; next new key triggers pruning.
	now = now.Add(10 * time.Second)
	th.Allow("fresh")
	th.mu.Lock()
	n := len(th.entries)
	th.mu.Unlock()
	if n > 2 {
		t.Fatalf("entries after prune = %d, want <= 2", n)
	}
}
