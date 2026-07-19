package orchestrator

import (
	"log/slog"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// newSizeTriggerFixture wires a retentionRunner backed by fake chunk/index
// managers, plus a bare Orchestrator whose vaults map points the SAME
// managers at vaultID — so r.orch.IndexSizes(vaultID, id) (used by
// retentionRunner.chunkDiskClaims' fallback path) reaches the fixture's
// index manager exactly as findManagersForChunk does in production. This
// is the seam TestSweepSizeTriggerFallbackUsesIndexSizes exercises.
func newSizeTriggerFixture(t *testing.T, chunks []chunk.ChunkMeta, indexSizes map[chunk.ChunkID]map[string]int64) (*retentionRunner, *retentionFakeChunkManager) {
	t.Helper()
	cm := &retentionFakeChunkManager{chunks: chunks}
	im := &retentionFakeIndexManager{sizes: indexSizes}
	vaultID := glid.New()
	orch := &Orchestrator{vaults: map[glid.GLID]*Vault{
		vaultID: {ID: vaultID, Instance: &VaultInstance{VaultID: vaultID, Chunks: cm, Indexes: im}},
	}}
	r := &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
		cm:       cm,
		im:       im,
		orch:     orch,
		now:      time.Now,
		logger:   slog.Default(),
	}
	return r, cm
}

func sizeMetaAt(id chunk.ChunkID, start, end time.Time, bytes, diskBytes int64, cloudBacked bool) chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          id,
		WriteStart:  start,
		WriteEnd:    end,
		SealedAt:    end,
		Bytes:       bytes,
		DiskBytes:   diskBytes,
		CloudBacked: cloudBacked,
		Sealed:      true,
	}
}

// TestSweepSizeTriggerUsesDiskClaim drives a full sweep end-to-end and pins
// that the size trigger selects by DiskBytes, not logical Bytes: an older
// chunk that is cheap on disk (well compressed) must survive while a
// newer chunk that is expensive on disk (poorly compressed) gets drained,
// even though a logical-Bytes-driven policy would pick the opposite chunk.
func TestSweepSizeTriggerUsesDiskClaim(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cheapOnDisk := chunk.NewChunkID()     // older, huge logical Bytes, tiny DiskBytes
	expensiveOnDisk := chunk.NewChunkID() // newer, tiny logical Bytes, huge DiskBytes

	chunks := []chunk.ChunkMeta{
		sizeMetaAt(cheapOnDisk, base, base.Add(30*time.Minute), 5000, 100, false),
		sizeMetaAt(expensiveOnDisk, base.Add(time.Hour), base.Add(90*time.Minute), 100, 5000, false),
	}
	r, cm := newSizeTriggerFixture(t, chunks, nil)

	r.sweep([]retentionRule{{policy: chunk.NewSizeRetentionPolicy(200)}})

	if !slices.Contains(cm.deleted, expensiveOnDisk) {
		t.Fatalf("expected the disk-expensive chunk to be drained, deleted=%v", cm.deleted)
	}
	if slices.Contains(cm.deleted, cheapOnDisk) {
		t.Fatalf("the disk-cheap chunk must survive (a Bytes-driven policy would have wrongly deleted it): deleted=%v", cm.deleted)
	}
}

// TestSweepSizeTriggerFallbackUsesIndexSizes drives the DiskBytes-unset
// fallback through the real orchestrator seam: a legacy chunk with no
// DiskBytes recorded claims Bytes plus index sizes, sourced from
// r.orch.IndexSizes — not zero, and not Bytes alone.
func TestSweepSizeTriggerFallbackUsesIndexSizes(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	id := chunk.NewChunkID()

	chunks := []chunk.ChunkMeta{
		sizeMetaAt(id, base, base.Add(30*time.Minute), 300, 0, false),
	}
	// Claim = Bytes(300) + indexes(300) = 600, over the 500 budget. Without
	// the index-size fallback pulling in the extra 300, Bytes alone (300)
	// would fit under 500 and the chunk would wrongly survive.
	r, cm := newSizeTriggerFixture(t, chunks, map[chunk.ChunkID]map[string]int64{
		id: {"token": 300},
	})

	r.sweep([]retentionRule{{policy: chunk.NewSizeRetentionPolicy(500)}})

	if !slices.Contains(cm.deleted, id) {
		t.Fatalf("expected the fallback-path chunk (Bytes+indexes over budget) to be drained, deleted=%v", cm.deleted)
	}
}

// TestSweepSizeTriggerCloudBackedEvictedNeverSelected pins the intended
// consequence of the disk-claim switch: an evicted cloud-backed chunk
// (DiskBytes 0) is never destroyed by a size trigger, no matter how large
// its logical Bytes or how old it is — destroying it frees no local disk.
func TestSweepSizeTriggerCloudBackedEvictedNeverSelected(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	evicted := chunk.NewChunkID() // oldest, huge logical Bytes, evicted (DiskBytes 0)
	fresh := chunk.NewChunkID()   // newest, small, ordinary local chunk

	chunks := []chunk.ChunkMeta{
		sizeMetaAt(evicted, base, base.Add(30*time.Minute), 999_999_999, 0, true),
		sizeMetaAt(fresh, base.Add(time.Hour), base.Add(90*time.Minute), 100, 100, false),
	}
	// Budget so small that even the fresh chunk alone is over it — proves
	// the evicted chunk is skipped on its own (zero) claim, not merely
	// because there was budget headroom left over from the fresh chunk.
	r, cm := newSizeTriggerFixture(t, chunks, nil)

	r.sweep([]retentionRule{{policy: chunk.NewSizeRetentionPolicy(50)}})

	if slices.Contains(cm.deleted, evicted) {
		t.Fatalf("evicted cloud-backed chunk must never be selected by a size trigger, deleted=%v", cm.deleted)
	}
	if !slices.Contains(cm.deleted, fresh) {
		t.Fatalf("expected the ordinary over-budget chunk to be drained, deleted=%v", cm.deleted)
	}
}

// TestSweepSizeTriggerCloudBackedCachedEligible pins the other half of the
// cached-vs-evicted pin: a cached cloud-backed chunk (DiskBytes > 0) claims
// its cache bytes and is eligible for drain exactly like a file-vault chunk.
func TestSweepSizeTriggerCloudBackedCachedEligible(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cached := chunk.NewChunkID() // oldest, cached cloud-backed chunk
	fresh := chunk.NewChunkID()  // newest, small

	chunks := []chunk.ChunkMeta{
		sizeMetaAt(cached, base, base.Add(30*time.Minute), 5000, 600, true),
		sizeMetaAt(fresh, base.Add(time.Hour), base.Add(90*time.Minute), 100, 100, false),
	}
	r, cm := newSizeTriggerFixture(t, chunks, nil)

	r.sweep([]retentionRule{{policy: chunk.NewSizeRetentionPolicy(500)}})

	if !slices.Contains(cm.deleted, cached) {
		t.Fatalf("cached cloud-backed chunk must be eligible for drain, deleted=%v", cm.deleted)
	}
	if slices.Contains(cm.deleted, fresh) {
		t.Fatalf("fresh chunk fits the budget and must survive, deleted=%v", cm.deleted)
	}
}

// TestSweepSizeTriggerMixedVault drives every claim shape in one vault at
// once — DiskBytes-recorded, fallback (no DiskBytes), cached cloud-backed,
// evicted cloud-backed — through a real sweep and pins the exact
// keep/delete split by disk claim.
func TestSweepSizeTriggerMixedVault(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	oldestFallback := chunk.NewChunkID() // claim = 200 + 50 index = 250
	evictedCloud := chunk.NewChunkID()   // claim = 0
	midOnDisk := chunk.NewChunkID()      // claim = 300 (DiskBytes)
	newestCached := chunk.NewChunkID()   // claim = 150 (cached cloud)

	chunks := []chunk.ChunkMeta{
		sizeMetaAt(oldestFallback, base, base.Add(30*time.Minute), 200, 0, false),
		sizeMetaAt(evictedCloud, base.Add(time.Hour), base.Add(90*time.Minute), 8_000_000, 0, true),
		sizeMetaAt(midOnDisk, base.Add(2*time.Hour), base.Add(150*time.Minute), 900, 300, false),
		sizeMetaAt(newestCached, base.Add(3*time.Hour), base.Add(210*time.Minute), 2000, 150, true),
	}
	r, cm := newSizeTriggerFixture(t, chunks, map[chunk.ChunkID]map[string]int64{
		oldestFallback: {"token": 50},
	})

	// Budget 400: newest-first — newestCached(150) fits, budget=150.
	// midOnDisk(300): 150+300=450 > 400, skip (deleted). evictedCloud(0):
	// 150+0=150 <= 400, kept. oldestFallback(250): 150+250=400 <= 400, kept.
	r.sweep([]retentionRule{{policy: chunk.NewSizeRetentionPolicy(400)}})

	want := []chunk.ChunkID{midOnDisk}
	if len(cm.deleted) != len(want) {
		t.Fatalf("deleted = %v, want exactly %v", cm.deleted, want)
	}
	for _, id := range want {
		if !slices.Contains(cm.deleted, id) {
			t.Fatalf("expected %s among deleted, got %v", id, cm.deleted)
		}
	}
}
