package orchestrator

import "gastrolog/internal/system"

// PlacementLeaderIsWriteAuthority reports whether ingest hot-path code may gate
// writes on VaultPlacement.Leader / VaultInstance.IsFollower for this vault.
//
// Locked Phase 0.6 (gastrolog-3c35d): V1 yes, V2 no — see
// docs/fan-out/v2/placement-leader-migration.md.
func PlacementLeaderIsWriteAuthority(writeModel system.VaultWriteModel) bool {
	return writeModel == system.VaultWriteModelV1
}

// PlacementLeaderIsWriteAuthorityForVault resolves the vault shell write model.
func PlacementLeaderIsWriteAuthorityForVault(v *Vault) bool {
	if v == nil {
		return true // unknown vault: preserve legacy gate until registered
	}
	return PlacementLeaderIsWriteAuthority(v.WriteModel)
}
