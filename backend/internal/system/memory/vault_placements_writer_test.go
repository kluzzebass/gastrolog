package memory

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// Vault placements have exactly one writer: the placement manager, through
// SetVaultPlacements. That call also mirrors the set onto VaultConfig.Placements
// because the orchestrator reads it there.
//
// PutVault stores the whole VaultConfig, so before gastrolog-kl8c3s it was a
// SECOND writer to that mirror: any client Putting a vault — a UI edit form that
// omits the field, a config import, or simply a read-modify-write round trip
// carrying a stale list — overwrote the mirror while the owner's copy kept the
// real value, and the two disagreed until the next placement event. Through
// Raft, on every node.
//
// These tests pin that a config write cannot move placements, whatever it sends.

func placementFor(nodeID string, leader bool) system.VaultPlacement {
	return system.VaultPlacement{StorageID: system.SyntheticStorageID(nodeID), Leader: leader}
}

func vaultPlacementsVia(t *testing.T, s *Store, id glid.GLID) ([]system.VaultPlacement, []system.VaultPlacement) {
	t.Helper()
	ctx := context.Background()
	owned, err := s.GetVaultPlacements(ctx, id)
	if err != nil {
		t.Fatalf("GetVaultPlacements: %v", err)
	}
	cfg, err := s.GetVault(ctx, id)
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	return owned, cfg.Placements
}

func assertPlacementsAgree(t *testing.T, s *Store, id glid.GLID, wantLen int, when string) {
	t.Helper()
	owned, mirrored := vaultPlacementsVia(t, s, id)
	if len(owned) != wantLen {
		t.Fatalf("%s: owner has %d placements, want %d", when, len(owned), wantLen)
	}
	if len(mirrored) != len(owned) {
		t.Fatalf("%s: VaultConfig.Placements has %d, owner has %d — the orchestrator "+
			"reads the mirror, so it is now looking at the wrong answer", when, len(mirrored), len(owned))
	}
	for i := range owned {
		if mirrored[i] != owned[i] {
			t.Fatalf("%s: placement %d differs: mirror=%+v owner=%+v", when, i, mirrored[i], owned[i])
		}
	}
}

func TestPutVaultCannotOverwritePlacements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewStore()
	id := glid.New()

	if err := s.PutVault(ctx, system.VaultConfig{ID: id, Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	placements := []system.VaultPlacement{placementFor("node-1", true), placementFor("node-2", false)}
	if err := s.SetVaultPlacements(ctx, id, placements); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}
	assertPlacementsAgree(t, s, id, 2, "after the owner set them")

	// The case that bit: a config write that simply omits placements. A UI edit
	// form or config import does exactly this.
	if err := s.PutVault(ctx, system.VaultConfig{ID: id, Name: "renamed", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault (no placements): %v", err)
	}
	assertPlacementsAgree(t, s, id, 2, "after a config write omitting placements")

	cfg, err := s.GetVault(ctx, id)
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if cfg.Name != "renamed" {
		t.Errorf("the config write itself was lost: name = %q, want %q", cfg.Name, "renamed")
	}
}

func TestPutVaultCannotInventPlacements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewStore()
	id := glid.New()

	if err := s.PutVault(ctx, system.VaultConfig{ID: id, Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := s.SetVaultPlacements(ctx, id, []system.VaultPlacement{placementFor("node-1", true)}); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	// A hand-written or stale placement list must not take effect.
	if err := s.PutVault(ctx, system.VaultConfig{
		ID: id, Name: "v", Type: system.VaultTypeMemory,
		Placements: []system.VaultPlacement{
			placementFor("node-7", true), placementFor("node-8", false), placementFor("node-9", false),
		},
	}); err != nil {
		t.Fatalf("PutVault (with placements): %v", err)
	}
	assertPlacementsAgree(t, s, id, 1, "after a config write carrying its own placements")

	_, mirrored := vaultPlacementsVia(t, s, id)
	if len(mirrored) == 1 && mirrored[0] != placementFor("node-1", true) {
		t.Errorf("client-supplied placement took effect: %+v", mirrored[0])
	}
}

// A vault created with placements already attached (the shape config import
// produces) must not seed them either — the placement manager assigns.
func TestPutVaultOnCreateIgnoresPlacements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewStore()
	id := glid.New()

	if err := s.PutVault(ctx, system.VaultConfig{
		ID: id, Name: "v", Type: system.VaultTypeMemory,
		Placements: []system.VaultPlacement{placementFor("node-1", true)},
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	assertPlacementsAgree(t, s, id, 0, "after creating a vault with placements attached")
}

// Round trip: read a config, change something unrelated, write it back. This is
// what every client does, and it must neither fail nor move placements — which
// is why PutVault re-derives rather than rejecting.
func TestPutVaultRoundTripPreservesPlacements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewStore()
	id := glid.New()

	if err := s.PutVault(ctx, system.VaultConfig{ID: id, Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := s.SetVaultPlacements(ctx, id, []system.VaultPlacement{placementFor("node-1", true)}); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	cfg, err := s.GetVault(ctx, id)
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	cfg.Name = "edited" // the read carries Placements; write it straight back
	if err := s.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault (round trip): %v", err)
	}

	assertPlacementsAgree(t, s, id, 1, "after a read-modify-write round trip")
	got, err := s.GetVault(ctx, id)
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if got.Name != "edited" {
		t.Errorf("round-tripped edit lost: name = %q, want %q", got.Name, "edited")
	}
}
