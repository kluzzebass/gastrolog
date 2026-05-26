package orchestrator

import (
	"testing"

	"gastrolog/internal/system"
)

func TestPlacementLeaderIsWriteAuthority(t *testing.T) {
	t.Parallel()
	if !PlacementLeaderIsWriteAuthority(system.VaultWriteModelV1) {
		t.Fatal("V1 must treat placement leader as write authority during transition")
	}
	if PlacementLeaderIsWriteAuthority(system.VaultWriteModelV2) {
		t.Fatal("V2 must not treat placement leader as write authority")
	}
}

func TestPlacementLeaderIsWriteAuthorityForVault(t *testing.T) {
	t.Parallel()
	v1 := &Vault{WriteModel: system.VaultWriteModelV1}
	if !PlacementLeaderIsWriteAuthorityForVault(v1) {
		t.Fatal("V1 vault shell")
	}
	v2 := &Vault{WriteModel: system.VaultWriteModelV2}
	if PlacementLeaderIsWriteAuthorityForVault(v2) {
		t.Fatal("V2 vault shell")
	}
	if !PlacementLeaderIsWriteAuthorityForVault(nil) {
		t.Fatal("nil vault defaults to legacy authority until registered")
	}
}
