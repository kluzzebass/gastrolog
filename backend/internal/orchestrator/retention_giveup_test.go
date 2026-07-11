package orchestrator

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestVaultRetentionGiveUpTTL pins the segment give-up bound's TTL source:
// the SHORTEST delete-disposition TTL wins (retention deletes at the first
// rule that fires); a route-disposition runner vetoes the bound entirely
// (routed records must reach their destinations — giving up their segment
// would drop them unrouted, a cardinal-rule violation); vaults with no TTL
// rule have no time bound.
func TestVaultRetentionGiveUpTTL(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	t.Run("shortest TTL wins", func(t *testing.T) {
		o := mootsFixture(t, vaultID, system.RetentionDispositionDelete, []retentionRule{
			{policy: chunk.NewTTLRetentionPolicy(10 * time.Minute)},
			{policy: chunk.NewTTLRetentionPolicy(3 * time.Minute)},
		})
		ttl, ok := o.vaultRetentionGiveUpTTL(vaultID)
		if !ok || ttl != 3*time.Minute {
			t.Fatalf("got (%v, %v), want (3m, true)", ttl, ok)
		}
	})

	t.Run("route disposition vetoes", func(t *testing.T) {
		o := mootsFixture(t, vaultID, system.RetentionDispositionRoute, []retentionRule{
			{policy: chunk.NewTTLRetentionPolicy(3 * time.Minute)},
		})
		if _, ok := o.vaultRetentionGiveUpTTL(vaultID); ok {
			t.Fatal("route disposition must veto the give-up bound")
		}
	})

	t.Run("size-only rules give no bound", func(t *testing.T) {
		o := mootsFixture(t, vaultID, system.RetentionDispositionDelete, []retentionRule{
			{policy: chunk.NewSizeRetentionPolicy(1)},
		})
		if _, ok := o.vaultRetentionGiveUpTTL(vaultID); ok {
			t.Fatal("no TTL rule means no time bound")
		}
	})

	t.Run("no retention runners: no bound", func(t *testing.T) {
		o := newTestOrch(t, Config{LocalNodeID: "node-A"})
		if _, ok := o.vaultRetentionGiveUpTTL(vaultID); ok {
			t.Fatal("vault without retention must not give up segments")
		}
	})
}

// TestChunkOnItsWayOut pins the doomed-pull gate: a sealed chunk flagged
// retention-pending, or with an in-flight delete, must never be scheduled for
// a replica catch-up pull — the bytes are being deleted on every home, so the
// pull fails on every peer (the constant failure stream of gastrolog-423tpt).
func TestChunkOnItsWayOut(t *testing.T) {
	t.Parallel()
	live := vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed}
	if chunkOnItsWayOut(live, nil) {
		t.Fatal("live sealed chunk must be pullable")
	}
	flagged := live
	flagged.RetentionPending = true
	if !chunkOnItsWayOut(flagged, nil) {
		t.Fatal("retention-pending chunk must not be pulled")
	}
	if !chunkOnItsWayOut(live, &vaultctlfsm.PendingDelete{ChunkID: live.ID}) {
		t.Fatal("chunk with in-flight delete must not be pulled")
	}
}
