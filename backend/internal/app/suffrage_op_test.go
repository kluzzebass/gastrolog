package app

// Two boolean inputs, three outcomes, and the wrong pairing is silent: a
// fresh joiner asking for no vote is an addition, but the same request from a
// current member is a demotion. Reading it as a demotion runs DemoteVoter
// against a node the configuration has never heard of, which fails quietly
// and leaves the joiner outside the cluster it just enrolled with.

import "testing"

func TestResolveSuffrageOp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		admitting bool
		voter     bool
		want      suffrageOp
	}{
		{"a fresh joiner entering as a learner", true, false, opAddNonvoter},
		{"a restarting voter refreshing its address", false, true, opAddVoter},
		{"a learner the promoter has caught up", false, true, opAddVoter},
		{"an operator demoting a member", false, false, opDemoteVoter},
		{"a joiner admitted straight as a voter", true, true, opAddVoter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveSuffrageOp(tc.admitting, tc.voter); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

// Stated on its own because it is the pairing that regressed: admission and
// demotion differ only in whether the caller supplied an address.
func TestAdmissionNeverDemotes(t *testing.T) {
	t.Parallel()

	for _, voter := range []bool{true, false} {
		if got := resolveSuffrageOp(true, voter); got == opDemoteVoter {
			t.Fatalf("admitting a node with voter=%v resolved to a demotion", voter)
		}
	}
}
