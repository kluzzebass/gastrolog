// Vault config validation for the fan-out fields
// (gastrolog-2ujjh / gastrolog-nd6sz / gastrolog-hshgl).
//
// Today: one cross-field rule — WOfN must resolve cleanly against the
// placement count. WriteModel was removed under gastrolog-hshgl
// (every vault is FanOut; no dispatch branch).

package system

import "fmt"

// ValidateFanOutFields enforces the cross-field invariants for the
// fan-out config. Returns nil iff:
//
//   - WOfN is a canonical value (or the empty default — resolves to
//     Full).
//   - WOfN resolves successfully against placementCount > 0.
//
// Called by config-write paths (CLI / UI / system FSM apply) before
// committing a VaultConfig change. Placement edits that would
// invalidate an existing vault should re-run this check.
func (v *VaultConfig) ValidateFanOutFields(placementCount int) error {
	if v.WOfN != "" && !v.WOfN.IsValid() {
		return fmt.Errorf("vault %s: invalid WOfN policy %q", v.Name, v.WOfN)
	}
	if placementCount <= 0 {
		// Memory / JSONL vaults legitimately have no placements.
		// Callers that care about cross-node durability check this
		// before invoking ValidateFanOutFields; we leave the
		// no-placements case as a no-op here.
		return nil
	}
	if _, err := v.WOfN.Resolve(placementCount); err != nil {
		return fmt.Errorf("vault %s: %w", v.Name, err)
	}
	return nil
}
