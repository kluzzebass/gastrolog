package app

// Two kinds of request reach the leader's suffrage machinery and they must
// not be confused. An operator's suffrage change on a member promotes or
// demotes it. A membership request comes from the node itself, carries no
// suffrage, and must never raise one: a node outside the configuration
// enters as a nonvoter, and a member is re-added as what it already is.

import "testing"

func TestResolveSuffrageOp(t *testing.T) {
	t.Parallel()

	if got := resolveSuffrageOp(true); got != opAddVoter {
		t.Fatalf("an operator promoting a member resolved to %s, want %s", got, opAddVoter)
	}
	if got := resolveSuffrageOp(false); got != opDemoteVoter {
		t.Fatalf("an operator demoting a member resolved to %s, want %s", got, opDemoteVoter)
	}
}

func TestResolveAdmissionOp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		suffrage string
		want     suffrageOp
		why      string
	}{
		{"a node outside the configuration", "", opAddNonvoter,
			"a vote is the cluster's to grant, never the joiner's to claim"},
		{"a learner asking again", "Nonvoter", opAddNonvoter,
			"asking twice must not promote a learner past the promoter's catch-up check"},
		{"a voter back at a new address", suffrageVoter, opAddVoter,
			"re-adding a voter as a nonvoter would cost the cluster a vote on every move"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveAdmissionOp(tc.suffrage); got != tc.want {
				t.Fatalf("got %s, want %s — %s", got, tc.want, tc.why)
			}
		})
	}
}

// A membership request comes from the node it names, so a resolution to a
// demotion would let a node's own request run DemoteVoter against it — and
// against a node outside the configuration, leave it outside.
func TestAdmissionNeverDemotes(t *testing.T) {
	t.Parallel()

	for _, suffrage := range []string{"", "Nonvoter", suffrageVoter} {
		if got := resolveAdmissionOp(suffrage); got == opDemoteVoter {
			t.Fatalf("admitting a node with suffrage %q resolved to a demotion", suffrage)
		}
	}
}
