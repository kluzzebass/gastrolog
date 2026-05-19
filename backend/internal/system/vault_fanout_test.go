// Tests for the fan-out fields on VaultConfig (gastrolog-nd6sz).

package system

import (
	"strings"
	"testing"
)

func TestWriteModelResolveAndIsValid(t *testing.T) {
	t.Parallel()
	if got := WriteModelUnset.Resolve(); got != WriteModelLeaderDriven {
		t.Errorf("empty WriteModel resolved to %q, want LeaderDriven", got)
	}
	if got := WriteModelLeaderDriven.Resolve(); got != WriteModelLeaderDriven {
		t.Errorf("LeaderDriven resolved to %q", got)
	}
	if got := WriteModelFanOut.Resolve(); got != WriteModelFanOut {
		t.Errorf("FanOut resolved to %q", got)
	}
	for _, w := range []WriteModel{WriteModelUnset, WriteModelLeaderDriven, WriteModelFanOut} {
		if !w.IsValid() {
			t.Errorf("%q should be valid", w)
		}
	}
	for _, w := range []WriteModel{"fan-out", "Fanout", "bogus"} {
		if w.IsValid() {
			t.Errorf("%q should be invalid", w)
		}
	}
}

func TestValidateFanOutFieldsAcceptsLeaderDrivenWithAnyWOfN(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    VaultConfig
	}{
		{"unset everything", VaultConfig{Name: "v1"}},
		{"LeaderDriven explicit", VaultConfig{Name: "v1", WriteModel: WriteModelLeaderDriven}},
		{"LeaderDriven + bogus WOfN allowed (informational only)", VaultConfig{Name: "v1", WriteModel: WriteModelLeaderDriven, WOfN: ""}},
		{"LeaderDriven + canonical WOfN", VaultConfig{Name: "v1", WriteModel: WriteModelLeaderDriven, WOfN: WOfNPolicyQuorum}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.v.ValidateFanOutFields(0); err != nil {
				t.Errorf("ValidateFanOutFields: %v", err)
			}
		})
	}
}

func TestValidateFanOutFieldsAcceptsFanOutWithValidPolicy(t *testing.T) {
	t.Parallel()
	for _, p := range []WOfNPolicy{WOfNPolicyFull, WOfNPolicyMinusOne, WOfNPolicyQuorum, WOfNPolicyOne} {
		t.Run(string(p), func(t *testing.T) {
			v := VaultConfig{Name: "v1", WriteModel: WriteModelFanOut, WOfN: p}
			if err := v.ValidateFanOutFields(3); err != nil {
				t.Errorf("FanOut + %q + N=3: %v", p, err)
			}
		})
	}
}

func TestValidateFanOutFieldsRejectsFanOutWithoutPlacements(t *testing.T) {
	t.Parallel()
	v := VaultConfig{Name: "v1", WriteModel: WriteModelFanOut, WOfN: WOfNPolicyQuorum}
	err := v.ValidateFanOutFields(0)
	if err == nil {
		t.Fatal("expected error for FanOut + 0 placements")
	}
	if !strings.Contains(err.Error(), "non-empty Placements") {
		t.Errorf("error should mention placements: %v", err)
	}
}

func TestValidateFanOutFieldsRejectsInvalidWriteModel(t *testing.T) {
	t.Parallel()
	v := VaultConfig{Name: "v1", WriteModel: "bogus"}
	if err := v.ValidateFanOutFields(3); err == nil {
		t.Fatal("expected error for invalid WriteModel")
	}
}

func TestValidateFanOutFieldsRejectsInvalidWOfN(t *testing.T) {
	t.Parallel()
	v := VaultConfig{Name: "v1", WriteModel: WriteModelFanOut, WOfN: "bogus"}
	err := v.ValidateFanOutFields(3)
	if err == nil {
		t.Fatal("expected error for invalid WOfN under FanOut")
	}
}

func TestValidateFanOutFieldsFanOutDefaultPolicyResolvesToFull(t *testing.T) {
	t.Parallel()
	// FanOut + empty WOfN: resolves to Full at runtime. Validation
	// must accept this (the operator may flip WriteModel before
	// setting WOfN; the durability default is conservative).
	v := VaultConfig{Name: "v1", WriteModel: WriteModelFanOut}
	if err := v.ValidateFanOutFields(3); err != nil {
		t.Errorf("FanOut + default WOfN: %v", err)
	}
}
