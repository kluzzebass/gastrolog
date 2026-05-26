package orchestrator

import (
	"testing"

	"gastrolog/internal/system"
)

func TestPlacementLeaderIsWriteAuthority(t *testing.T) {
	t.Parallel()
	if !PlacementLeaderIsWriteAuthority(system.VaultWriteModelChunkAppend) {
		t.Fatal("chunk_append must treat placement leader as write authority during transition")
	}
	if PlacementLeaderIsWriteAuthority(system.VaultWriteModelSequenced) {
		t.Fatal("sequenced write model must not treat placement leader as write authority")
	}
}

func TestPlacementLeaderIsWriteAuthorityForVault(t *testing.T) {
	t.Parallel()
	chunkAppend := &Vault{WriteModel: system.VaultWriteModelChunkAppend}
	if !PlacementLeaderIsWriteAuthorityForVault(chunkAppend) {
		t.Fatal("chunk_append vault shell")
	}
	sequenced := &Vault{WriteModel: system.VaultWriteModelSequenced}
	if PlacementLeaderIsWriteAuthorityForVault(sequenced) {
		t.Fatal("sequenced vault shell")
	}
	if !PlacementLeaderIsWriteAuthorityForVault(nil) {
		t.Fatal("nil vault defaults to legacy authority until registered")
	}
}
