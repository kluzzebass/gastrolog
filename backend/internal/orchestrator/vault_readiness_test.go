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

// Regression: ListChunks fans out to remote nodes; a node with the vault
// registered but no local tier placements must not fail ListAllChunkMetas
// with ErrVaultNotReady, or the UI sees 503 and empty chunks.
func TestListAllChunkMetas_noLocalTiersReturnsEmpty(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	o.RegisterVault(NewVault(vid))
	metas, err := o.ListAllChunkMetas(vid)
	if err != nil {
		t.Fatalf("ListAllChunkMetas: %v", err)
	}
	if metas != nil && len(metas) != 0 {
		t.Fatalf("expected nil or empty slice, got len=%d", len(metas))
	}
	if err := vaultReplicationReadinessErr(vid, o.vaults[vid]); !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("writes should still see not-ready, got %v", err)
	}
}
