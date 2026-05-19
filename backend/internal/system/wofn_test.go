// W-of-N policy resolver tests (gastrolog-4xdvm).

package system

import (
	"errors"
	"testing"
)

func TestWOfNPolicyIsValid(t *testing.T) {
	t.Parallel()
	for _, p := range []WOfNPolicy{
		WOfNPolicyFull, WOfNPolicyMinusOne, WOfNPolicyQuorum, WOfNPolicyOne,
	} {
		if !p.IsValid() {
			t.Errorf("%q should be valid", p)
		}
	}
	for _, p := range []WOfNPolicy{"", "bogus", "WAITFORALL"} {
		if p.IsValid() {
			t.Errorf("%q should be invalid", p)
		}
	}
}

func TestWOfNPolicyResolve(t *testing.T) {
	t.Parallel()
	cases := []struct {
		policy WOfNPolicy
		n      int
		wantW  int
	}{
		// Full: every replica must ack.
		{WOfNPolicyFull, 1, 1},
		{WOfNPolicyFull, 3, 3},
		{WOfNPolicyFull, 5, 5},
		// Empty policy defaults to Full (preserves wait-for-all semantics).
		{"", 3, 3},
		// MinusOne: tolerates one straggler, clamped at 1.
		{WOfNPolicyMinusOne, 1, 1},
		{WOfNPolicyMinusOne, 2, 1},
		{WOfNPolicyMinusOne, 3, 2},
		{WOfNPolicyMinusOne, 5, 4},
		// Quorum: ceil(N / 2). Same as (N / 2) + 1 for integer math.
		{WOfNPolicyQuorum, 1, 1},
		{WOfNPolicyQuorum, 2, 2},
		{WOfNPolicyQuorum, 3, 2},
		{WOfNPolicyQuorum, 4, 3},
		{WOfNPolicyQuorum, 5, 3},
		// One: any single ack.
		{WOfNPolicyOne, 1, 1},
		{WOfNPolicyOne, 3, 1},
		{WOfNPolicyOne, 100, 1},
	}
	for _, c := range cases {
		got, err := c.policy.Resolve(c.n)
		if err != nil {
			t.Errorf("%q.Resolve(%d) error: %v", c.policy, c.n, err)
			continue
		}
		if got != c.wantW {
			t.Errorf("%q.Resolve(%d) = %d, want %d", c.policy, c.n, got, c.wantW)
		}
	}
}

func TestWOfNPolicyResolveAlwaysAtLeastOne(t *testing.T) {
	t.Parallel()
	// Durability floor: every valid resolution returns W >= 1. A
	// 0-ack write is never durable, even at the extremes of the
	// policy space (Quorum at N=1, MinusOne at N=1, etc.).
	for _, p := range []WOfNPolicy{
		WOfNPolicyFull, WOfNPolicyMinusOne, WOfNPolicyQuorum, WOfNPolicyOne, "",
	} {
		w, err := p.Resolve(1)
		if err != nil {
			t.Fatalf("%q.Resolve(1): %v", p, err)
		}
		if w < 1 {
			t.Errorf("%q.Resolve(1) = %d; want >= 1", p, w)
		}
	}
}

func TestWOfNPolicyResolveRejectsEmptyN(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, -1, -100} {
		if _, err := WOfNPolicyQuorum.Resolve(n); err == nil {
			t.Errorf("Resolve(%d) should error", n)
		}
	}
}

func TestWOfNPolicyResolveRejectsUnknownPolicy(t *testing.T) {
	t.Parallel()
	_, err := WOfNPolicy("bogus").Resolve(3)
	if err == nil {
		t.Fatal("expected error for unknown policy")
	}
	if !errorContains(err, "bogus") {
		t.Errorf("error should mention the policy: %v", err)
	}
}

func TestResolveWOfNPolicyFreeFunction(t *testing.T) {
	t.Parallel()
	got, err := ResolveWOfNPolicy("quorum", 5)
	if err != nil {
		t.Fatalf("ResolveWOfNPolicy: %v", err)
	}
	if got != 3 {
		t.Errorf("ResolveWOfNPolicy(quorum, 5) = %d, want 3", got)
	}
}

func errorContains(err error, sub string) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, err) && (sub == "" || stringContains(err.Error(), sub))
}

func stringContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
