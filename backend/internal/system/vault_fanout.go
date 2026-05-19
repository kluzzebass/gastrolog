// Vault config validation for the fan-out fields
// (gastrolog-2ujjh / gastrolog-nd6sz).
//
// Lives separate from vault.go so the validation surface for the new
// fields can grow without bloating the struct definition file. Today:
// two cross-field rules (WriteModel ↔ WOfN consistency at config
// time + Receiving-size sanity).

package system

import "fmt"

// ValidateFanOutFields enforces the cross-field invariants between
// WriteModel and WOfN. Returns nil iff:
//
//   - WriteModel is a canonical value (or the empty default).
//   - WOfN is a canonical value (or the empty default — resolves to
//     Full).
//   - For FanOut vaults: WOfN resolves successfully against
//     placementCount > 0 (the runtime W-of-N coordinator's
//     denominator).
//
// Called by config-write paths (CLI / UI / system FSM apply) before
// committing a VaultConfig change. Placement edits that would
// invalidate an existing FanOut vault should re-run this check; the
// caller can ignore the placement-count assertion for LeaderDriven
// vaults (their replication doesn't pivot on WOfN).
func (v *VaultConfig) ValidateFanOutFields(placementCount int) error {
	if !v.WriteModel.IsValid() {
		return fmt.Errorf("vault %s: invalid WriteModel %q", v.Name, v.WriteModel)
	}
	if v.WOfN != "" && !v.WOfN.IsValid() {
		return fmt.Errorf("vault %s: invalid WOfN policy %q", v.Name, v.WOfN)
	}
	if v.WriteModel.Resolve() != WriteModelFanOut {
		// LeaderDriven (or unset): WOfN is informational; no
		// runtime resolution is performed. Allow any setting so an
		// operator can pre-stage a policy before flipping the
		// WriteModel.
		return nil
	}
	if placementCount <= 0 {
		return fmt.Errorf("vault %s: FanOut WriteModel requires non-empty Placements", v.Name)
	}
	if _, err := v.WOfN.Resolve(placementCount); err != nil {
		return fmt.Errorf("vault %s: %w", v.Name, err)
	}
	return nil
}
