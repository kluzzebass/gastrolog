package orchestrator

import (
	"context"
	"errors"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
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
	if len(metas) != 0 {
		t.Fatalf("expected nil or empty slice, got len=%d", len(metas))
	}
	if err := vaultReplicationReadinessErr(vid, o.vaults[vid]); !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("writes should still see not-ready, got %v", err)
	}
}

func TestSearch_ErrVaultNotReady(t *testing.T) {
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
	_, _, err = o.Search(context.Background(), vid, query.Query{}, nil)
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("Search: got %v, want ErrVaultNotReady", err)
	}
}

func TestAppendToTier_ErrVaultNotReady(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	tierID := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	tier := &TierInstance{
		TierID:     tierID,
		Type:       "memory",
		Chunks:     s.CM,
		Indexes:    s.IM,
		Query:      s.QE,
		IsFSMReady: func() bool { return false },
	}
	o.RegisterVault(NewVault(vid, tier))
	err = o.AppendToVault(vid, tierID, chunk.ChunkID{}, chunk.Record{Raw: []byte("x")})
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("AppendToVault: got %v, want ErrVaultNotReady", err)
	}
}

func TestLocalVaultsReplicationReady(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if !o.LocalVaultsReplicationReady() {
		t.Fatal("empty orchestrator should be replication-ready")
	}
	vid := glid.New()
	o.RegisterVault(NewVault(vid))
	if !o.LocalVaultsReplicationReady() {
		t.Fatal("routing-only vault (no tiers) should not block readiness")
	}
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
	if o.LocalVaultsReplicationReady() {
		t.Fatal("expected false when local tier FSM is not ready")
	}
}

func TestSearchReadyRegistry_skipsNotReadyVault(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	readyID := glid.New()
	sReady, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	o.RegisterVault(NewVault(readyID, &TierInstance{
		TierID:     glid.New(),
		Type:       "memory",
		Chunks:     sReady.CM,
		Indexes:    sReady.IM,
		Query:      sReady.QE,
		IsFSMReady: func() bool { return true },
	}))
	notReadyID := glid.New()
	sNR, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	o.RegisterVault(NewVault(notReadyID, &TierInstance{
		TierID:     glid.New(),
		Type:       "memory",
		Chunks:     sNR.CM,
		Indexes:    sNR.IM,
		Query:      sNR.QE,
		IsFSMReady: func() bool { return false },
	}))
	reg := &searchReadyRegistry{o: o}
	ids := reg.ListVaults()
	if len(ids) != 1 || ids[0] != readyID {
		t.Fatalf("ListVaults: got %v, want single ready vault %v", ids, readyID)
	}
}
