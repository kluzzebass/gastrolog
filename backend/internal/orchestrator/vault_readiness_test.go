package orchestrator

import (
	"errors"
	"testing"

	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
)

func TestVaultReplicationReadinessErr_nilVault(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	err := vaultReplicationReadinessErr(vid, nil)
	if !errors.Is(err, ErrVaultNotFound) {
		t.Fatalf("got %v, want ErrVaultNotFound", err)
	}
}

func TestVaultReplicationReadinessErr_noTiers(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	v := &Vault{ID: vid, Tiers: nil}
	err := vaultReplicationReadinessErr(vid, v)
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("got %v, want ErrVaultNotReady", err)
	}
}

func TestVaultReplicationReadinessErr_fsmNotReady(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	tier := &TierInstance{
		TierID:     glid.New(),
		Type:       "memory",
		Chunks:     s.CM,
		Indexes:    s.IM,
		Query:      s.QE,
		IsFSMReady: func() bool { return false },
	}
	v := NewVault(vid, tier)
	if err := vaultReplicationReadinessErr(vid, v); !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("got %v, want ErrVaultNotReady", err)
	}
}

func TestVaultReplicationReadinessErr_ready(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	tier := &TierInstance{
		TierID:     glid.New(),
		Type:       "memory",
		Chunks:     s.CM,
		Indexes:    s.IM,
		Query:      s.QE,
		IsFSMReady: func() bool { return true },
	}
	v := NewVault(vid, tier)
	if err := vaultReplicationReadinessErr(vid, v); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}

func TestListAllChunkMetas_vaultNotReady(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	tier := &TierInstance{
		TierID:     glid.New(),
		Type:       "memory",
		Chunks:     s.CM,
		Indexes:    s.IM,
		Query:      s.QE,
		IsFSMReady: func() bool { return false },
	}
	o.RegisterVault(NewVault(vid, tier))
	_, err = o.ListAllChunkMetas(vid)
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("got %v, want ErrVaultNotReady", err)
	}
}
