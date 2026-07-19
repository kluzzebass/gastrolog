package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestAppendUnlistedManifestSealed pins retention's candidate sourcing under
// lazy resolution (gastrolog-2kmgj6): sealed manifest entries the chunk
// manager has not resolved yet must appear as candidates; listed, unsealed,
// and cloud-backed entries must not duplicate or leak in. Without this, a
// restart empties cm.List() and retention starves exactly like the
// dead-retention incident.
func TestAppendUnlistedManifestSealed(t *testing.T) {
	t.Parallel()
	now := time.Now()
	listedID := chunk.NewChunkID()
	unlistedID := chunk.NewChunkID()
	unsealedID := chunk.NewChunkID()
	cloudID := chunk.NewChunkID()

	entries := []vaultctlfsm.ManifestEntry{
		{ID: listedID, State: chunk.ChunkStateSealed, SealedAt: now, RecordCount: 10},
		{ID: unlistedID, State: chunk.ChunkStateSealed, SealedAt: now.Add(-time.Hour), RecordCount: 20, DiskBytes: 4096},
		{ID: unsealedID, State: chunk.ChunkStateActive},
		{ID: cloudID, State: chunk.ChunkStateSealed, SealedAt: now, CloudBacked: true},
	}
	vaultInst := &VaultInstance{
		ManifestEntries: func() []vaultctlfsm.ManifestEntry { return entries },
	}

	metas := []chunk.ChunkMeta{{ID: listedID, Sealed: true, SealedAt: now}}
	out := appendUnlistedManifestSealed(metas, vaultInst)

	if len(out) != 2 {
		t.Fatalf("candidates = %d, want 2 (listed + unlisted sealed)", len(out))
	}
	var synthetic *chunk.ChunkMeta
	for i := range out {
		if out[i].ID == unlistedID {
			synthetic = &out[i]
		}
		if out[i].ID == unsealedID || out[i].ID == cloudID {
			t.Fatalf("ineligible entry %s leaked into candidates", out[i].ID)
		}
	}
	if synthetic == nil {
		t.Fatal("unlisted sealed manifest entry missing from candidates")
	}
	if !synthetic.Sealed || synthetic.SealedAt.IsZero() || synthetic.DiskBytes != 4096 {
		t.Fatalf("synthetic candidate lost fields: %+v", synthetic)
	}

	// Nil instance / callback: pass-through, no panic.
	if got := appendUnlistedManifestSealed(metas, nil); len(got) != 1 {
		t.Fatalf("nil instance: %d candidates, want 1", len(got))
	}
	if got := appendUnlistedManifestSealed(metas, &VaultInstance{}); len(got) != 1 {
		t.Fatalf("nil callback: %d candidates, want 1", len(got))
	}
}

// TestAppendUnlistedManifestSealedExcludesInFlightTransferPhantom pins
// gastrolog-2l918 review finding 3c: a manifest entry introduced by
// retention transfer disposition (TransferSourceVaultID set) with ZERO
// confirmed holders must NOT become a destination retention candidate —
// the bytes haven't landed on any home yet, so a short destination TTL
// firing on it would tombstone the transfer's own placeholder out from
// under the still-in-flight hand-off. An otherwise-identical entry that
// HAS earned at least one holder receipt is a normal candidate again.
func TestAppendUnlistedManifestSealedExcludesInFlightTransferPhantom(t *testing.T) {
	t.Parallel()
	now := time.Now()
	sourceVaultID := glid.New()
	phantomID := chunk.NewChunkID()
	landedID := chunk.NewChunkID()

	entries := []vaultctlfsm.ManifestEntry{
		// Zero holders: in-flight, must be excluded.
		{ID: phantomID, State: chunk.ChunkStateSealed, SealedAt: now, RecordCount: 5, TransferSourceVaultID: sourceVaultID},
		// Same shape but a holder has already acked: eligible again.
		{ID: landedID, State: chunk.ChunkStateSealed, SealedAt: now, RecordCount: 5, TransferSourceVaultID: sourceVaultID, Holders: []string{"node-A"}},
	}
	vaultInst := &VaultInstance{
		ManifestEntries: func() []vaultctlfsm.ManifestEntry { return entries },
	}

	out := appendUnlistedManifestSealed(nil, vaultInst)

	var sawPhantom, sawLanded bool
	for _, m := range out {
		if m.ID == phantomID {
			sawPhantom = true
		}
		if m.ID == landedID {
			sawLanded = true
		}
	}
	if sawPhantom {
		t.Error("zero-holder transfer-introduced entry must NOT be a retention candidate (phantom expiry wedge)")
	}
	if !sawLanded {
		t.Error("a transfer-introduced entry with a confirmed holder must rejoin normal candidacy")
	}
}
