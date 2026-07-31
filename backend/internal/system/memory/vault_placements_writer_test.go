package memory

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// Vault placements have exactly one writer and exactly one representation:
// s.vaultPlacements, written through SetVaultPlacements. VaultConfig carries no
// placement field, so a config write has no placements to carry and "the mirror
// disagrees with the owner" is not a state the type system permits.
//
// These tests pin the outcome that matters — a config write does not disturb
// placements — plus the round trip every client performs.

func placementFor(nodeID string, leader bool) system.VaultPlacement {
	return system.VaultPlacement{StorageID: system.SyntheticStorageID(nodeID), Leader: leader}
}

func TestConfigWriteDoesNotDisturbPlacements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewStore()
	id := glid.New()

	if err := s.PutVault(ctx, system.VaultConfig{ID: id, Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	want := []system.VaultPlacement{placementFor("node-1", true), placementFor("node-2", false)}
	if err := s.SetVaultPlacements(ctx, id, want); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	// The case that bit before: a config write that says nothing about placement.
	if err := s.PutVault(ctx, system.VaultConfig{ID: id, Name: "renamed", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault (rename): %v", err)
	}

	got, err := s.GetVaultPlacements(ctx, id)
	if err != nil {
		t.Fatalf("GetVaultPlacements: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("placements after a config write = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("placement %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	cfg, err := s.GetVault(ctx, id)
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if cfg.Name != "renamed" {
		t.Errorf("the config write itself was lost: name = %q, want %q", cfg.Name, "renamed")
	}
}

// Read a config, change something unrelated, write it back — what every client
// does. It must neither fail nor move placements. This is why kl8c3s re-derived
// rather than rejecting; now there is nothing on the config to reject.
func TestConfigRoundTripPreservesPlacements(t *testing.T) {
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
	cfg.Name = "edited"
	if err := s.PutVault(ctx, *cfg); err != nil {
		t.Fatalf("PutVault (round trip): %v", err)
	}

	got, err := s.GetVaultPlacements(ctx, id)
	if err != nil {
		t.Fatalf("GetVaultPlacements: %v", err)
	}
	if len(got) != 1 || got[0] != placementFor("node-1", true) {
		t.Errorf("placements after a round trip = %+v, want the one the owner set", got)
	}
}

// Load's Runtime map is where readers now get placements, so it has to carry
// what the owner holds — an empty map there would make every vault look unplaced.
func TestLoadExposesPlacementsOnTheRuntime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewStore()
	id := glid.New()

	if err := s.PutVault(ctx, system.VaultConfig{ID: id, Name: "v", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	want := []system.VaultPlacement{placementFor("node-1", true)}
	if err := s.SetVaultPlacements(ctx, id, want); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	sys, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := sys.PlacementsFor(id)
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("System.PlacementsFor = %+v, want %+v", got, want)
	}
	if len(sys.PlacementsFor(glid.New())) != 0 {
		t.Error("an unknown vault must report no placements, not panic or invent")
	}
}
