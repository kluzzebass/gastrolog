// Tests for the fan-out fields on VaultConfig
// (gastrolog-nd6sz / gastrolog-hshgl).

package system

import (
	"strings"
	"testing"
)

func TestValidateFanOutFieldsAcceptsValidPolicies(t *testing.T) {
	t.Parallel()
	for _, p := range []WOfNPolicy{"", WOfNPolicyFull, WOfNPolicyMinusOne, WOfNPolicyQuorum, WOfNPolicyOne} {
		t.Run(string(p), func(t *testing.T) {
			v := VaultConfig{Name: "v1", WOfN: p}
			if err := v.ValidateFanOutFields(3); err != nil {
				t.Errorf("WOfN %q + N=3: %v", p, err)
			}
		})
	}
}

func TestValidateFanOutFieldsRejectsInvalidPolicy(t *testing.T) {
	t.Parallel()
	v := VaultConfig{Name: "v1", WOfN: "bogus"}
	err := v.ValidateFanOutFields(3)
	if err == nil {
		t.Fatal("expected error for bogus WOfN policy")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("error should mention the policy: %v", err)
	}
}

func TestValidateFanOutFieldsAcceptsNoPlacements(t *testing.T) {
	t.Parallel()
	// Memory / JSONL vaults legitimately have no placements.
	v := VaultConfig{Name: "v1", WOfN: WOfNPolicyQuorum}
	if err := v.ValidateFanOutFields(0); err != nil {
		t.Errorf("zero placements should be accepted: %v", err)
	}
}
