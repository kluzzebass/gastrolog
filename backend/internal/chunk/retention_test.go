package chunk

import (
	"strings"
	"testing"
	"time"
)

// Helper to build a ChunkMeta with the given parameters.
func metaAt(id ChunkID, start, end time.Time, bytes int64) ChunkMeta {
	return ChunkMeta{
		ID:         id,
		WriteStart: start,
		WriteEnd:   end,
		SealedAt:   end,
		Bytes:      bytes,
		Sealed:     true,
	}
}

// idAt creates a unique ChunkID. The time parameter is unused but kept
// for test readability — UUIDv7 IDs are monotonic by creation order.
func idAt(_ time.Time) ChunkID {
	return NewChunkID()
}

// chunkIDsEqual returns true if two slices contain the same ChunkIDs in the same order.
func chunkIDsEqual(a, b []ChunkID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// chunkIDsEqualUnordered returns true if two slices contain the same set of ChunkIDs.
func chunkIDsEqualUnordered(a, b []ChunkID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[ChunkID]int, len(a))
	for _, id := range a {
		set[id]++
	}
	for _, id := range b {
		set[id]--
		if set[id] < 0 {
			return false
		}
	}
	return true
}

func formatIDs(ids []ChunkID) string {
	if len(ids) == 0 {
		return "[]"
	}
	var s strings.Builder
	s.WriteString("[")
	for i, id := range ids {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(id.String())
	}
	s.WriteString("]")
	return s.String()
}

// --- TTLRetentionPolicy ---

func TestTTLRetentionPolicy(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	maxAge := 24 * time.Hour

	id1 := idAt(base.Add(-72 * time.Hour)) // 3 days old
	id2 := idAt(base.Add(-48 * time.Hour)) // 2 days old
	id3 := idAt(base.Add(-12 * time.Hour)) // 12 hours old
	id4 := idAt(base.Add(-1 * time.Hour))  // 1 hour old

	tests := []struct {
		name   string
		chunks []ChunkMeta
		now    time.Time
		want   []ChunkID
	}{
		{
			name: "all within TTL",
			chunks: []ChunkMeta{
				metaAt(id3, base.Add(-12*time.Hour), base.Add(-11*time.Hour), 100),
				metaAt(id4, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 200),
			},
			now:  base,
			want: nil,
		},
		{
			name: "some expired",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-72*time.Hour), base.Add(-71*time.Hour), 100),
				metaAt(id2, base.Add(-48*time.Hour), base.Add(-47*time.Hour), 200),
				metaAt(id3, base.Add(-12*time.Hour), base.Add(-11*time.Hour), 300),
				metaAt(id4, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 400),
			},
			now:  base,
			want: []ChunkID{id1, id2},
		},
		{
			name: "all expired",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-72*time.Hour), base.Add(-71*time.Hour), 100),
				metaAt(id2, base.Add(-48*time.Hour), base.Add(-47*time.Hour), 200),
			},
			now:  base,
			want: []ChunkID{id1, id2},
		},
		{
			name:   "empty state",
			chunks: nil,
			now:    base,
			want:   nil,
		},
		{
			name: "single chunk within TTL",
			chunks: []ChunkMeta{
				metaAt(id4, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 100),
			},
			now:  base,
			want: nil,
		},
		{
			name: "single chunk expired",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-72*time.Hour), base.Add(-71*time.Hour), 100),
			},
			now:  base,
			want: []ChunkID{id1},
		},
	}

	policy := NewTTLRetentionPolicy(maxAge)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := policy.Apply(VaultState{Chunks: tt.chunks, Now: tt.now})
			if !chunkIDsEqual(got, tt.want) {
				t.Errorf("got %s, want %s", formatIDs(got), formatIDs(tt.want))
			}
		})
	}
}

func TestTTLRetentionPolicyUsesSealedAtNotWriteEnd(t *testing.T) {
	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	id := idAt(now)
	meta := ChunkMeta{
		ID:       id,
		WriteEnd: now.Add(-72 * time.Hour), // old records
		SealedAt: now.Add(-1 * time.Hour),  // sealed recently
		Sealed:   true,
	}
	policy := NewTTLRetentionPolicy(24 * time.Hour)
	got := policy.Apply(VaultState{Chunks: []ChunkMeta{meta}, Now: now})
	if len(got) != 0 {
		t.Fatalf("retention must anchor on SealedAt, not WriteEnd; got deletes %v", got)
	}
}

// --- SizeRetentionPolicy ---

func TestSizeRetentionPolicy(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	id1 := idAt(base.Add(-3 * time.Hour))
	id2 := idAt(base.Add(-2 * time.Hour))
	id3 := idAt(base.Add(-1 * time.Hour))

	tests := []struct {
		name     string
		chunks   []ChunkMeta
		maxBytes int64
		want     []ChunkID
	}{
		{
			name: "within budget",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 100),
				metaAt(id2, base.Add(-2*time.Hour), base.Add(-1*time.Hour+30*time.Minute), 200),
				metaAt(id3, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 300),
			},
			maxBytes: 1000,
			want:     nil,
		},
		{
			name: "over budget deletes oldest",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 400),
				metaAt(id2, base.Add(-2*time.Hour), base.Add(-1*time.Hour+30*time.Minute), 400),
				metaAt(id3, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 400),
			},
			maxBytes: 800,
			want:     []ChunkID{id1},
		},
		{
			name: "single chunk over budget is deleted",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 2000),
			},
			maxBytes: 500,
			want:     []ChunkID{id1},
		},
		{
			name:     "empty state",
			chunks:   nil,
			maxBytes: 500,
			want:     nil,
		},
		{
			name: "exactly at budget",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 200),
				metaAt(id2, base.Add(-2*time.Hour), base.Add(-1*time.Hour+30*time.Minute), 300),
			},
			maxBytes: 500,
			want:     nil,
		},
		{
			name: "all but newest deleted",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 500),
				metaAt(id2, base.Add(-2*time.Hour), base.Add(-1*time.Hour+30*time.Minute), 500),
				metaAt(id3, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 500),
			},
			maxBytes: 500,
			want:     []ChunkID{id1, id2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := NewSizeRetentionPolicy(tt.maxBytes)
			// Newest-first keep-set semantics are what this test pins, not the
			// claim formula itself — feed a claims map that mirrors each
			// fixture's logical Bytes so the numbers line up exactly as before
			// the drain trigger switched currencies.
			got := policy.Apply(VaultState{Chunks: tt.chunks, Now: base, Claims: claimsFromBytes(tt.chunks)})
			if !chunkIDsEqual(got, tt.want) {
				t.Errorf("got %s, want %s", formatIDs(got), formatIDs(tt.want))
			}
		})
	}
}

// claimsFromBytes builds a VaultState.Claims map that mirrors each chunk's
// logical Bytes — used by tests that pin keep-set/ordering semantics
// unrelated to the disk-claim formula itself.
func claimsFromBytes(chunks []ChunkMeta) map[ChunkID]int64 {
	claims := make(map[ChunkID]int64, len(chunks))
	for _, meta := range chunks {
		claims[meta.ID] = meta.Bytes
	}
	return claims
}

// metaWithClaim builds a ChunkMeta carrying the fields DiskClaim reads
// (Bytes, DiskBytes, CloudBacked) for size-drain-trigger tests that care
// about the claim formula, not just the logical byte count.
func metaWithClaim(id ChunkID, start, end time.Time, bytes, diskBytes int64, cloudBacked bool) ChunkMeta {
	m := metaAt(id, start, end, bytes)
	m.DiskBytes = diskBytes
	m.CloudBacked = cloudBacked
	return m
}

// TestSizeRetentionPolicyUsesDiskClaimNotLogicalBytes pins the measurement
// switch itself: a compressed chunk's logical Bytes wildly overstates what
// draining it reclaims. Selection must follow DiskBytes, not Bytes — a
// chunk that is cheap on disk but huge logically must not be evicted ahead
// of a chunk that is the reverse.
func TestSizeRetentionPolicyUsesDiskClaimNotLogicalBytes(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	// id1 (older) is logically small but claims a lot on disk (poor
	// compression); id2 (newer) is logically huge but claims almost
	// nothing (well compressed). A Bytes-driven policy would delete id2
	// first (newest-first keep, walking by Bytes) or keep id1 outright;
	// a claim-driven policy must delete id1 to stay under budget instead,
	// since id1 is what is actually consuming the disk.
	id1 := idAt(base.Add(-2 * time.Hour))
	id2 := idAt(base.Add(-1 * time.Hour))

	chunks := []ChunkMeta{
		metaWithClaim(id1, base.Add(-2*time.Hour), base.Add(-90*time.Minute), 100, 900, false),
		metaWithClaim(id2, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 5000, 100, false),
	}
	claims := map[ChunkID]int64{id1: 900, id2: 100}

	policy := NewSizeRetentionPolicy(500)
	got := policy.Apply(VaultState{Chunks: chunks, Now: base, Claims: claims})
	want := []ChunkID{id1}
	if !chunkIDsEqual(got, want) {
		t.Fatalf("got %s, want %s (claim-driven, not logical-bytes-driven)", formatIDs(got), formatIDs(want))
	}
}

// TestSizeRetentionPolicyFallbackPath pins the DiskBytes-unset fallback:
// when a caller precomputes Claims via DiskClaim for a chunk with no
// DiskBytes recorded, the claim is Bytes plus index sizes — and the policy
// just consumes whatever the caller computed, without reaching into Bytes
// itself.
func TestSizeRetentionPolicyFallbackPath(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	id1 := idAt(base.Add(-2 * time.Hour))
	id2 := idAt(base.Add(-1 * time.Hour))

	chunks := []ChunkMeta{
		metaWithClaim(id1, base.Add(-2*time.Hour), base.Add(-90*time.Minute), 300, 0, false),
		metaWithClaim(id2, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 300, 0, false),
	}
	// Fallback claim = Bytes + indexes, computed by the caller exactly as
	// DiskClaim would: 300 + 100 index bytes = 400 each.
	claims := map[ChunkID]int64{
		id1: DiskClaim(chunks[0], func(ChunkID) (map[string]int64, error) {
			return map[string]int64{"token": 100}, nil
		}),
		id2: DiskClaim(chunks[1], func(ChunkID) (map[string]int64, error) {
			return map[string]int64{"token": 100}, nil
		}),
	}

	policy := NewSizeRetentionPolicy(500)
	got := policy.Apply(VaultState{Chunks: chunks, Now: base, Claims: claims})
	want := []ChunkID{id1}
	if !chunkIDsEqual(got, want) {
		t.Fatalf("got %s, want %s", formatIDs(got), formatIDs(want))
	}
}

// TestSizeRetentionPolicyCloudBackedCachedVsEvicted pins the intended
// consequence of the disk-claim switch: an evicted cloud-backed chunk
// (DiskBytes 0) is never selected by a size trigger, no matter how large
// its logical Bytes — destroying it would free no local disk. A cached
// cloud-backed chunk (DiskBytes > 0) is eligible exactly like a file-vault
// chunk, since its cache file occupies real local disk.
func TestSizeRetentionPolicyCloudBackedCachedVsEvicted(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	evictedOld := idAt(base.Add(-3 * time.Hour))
	cachedMid := idAt(base.Add(-2 * time.Hour))
	freshSmall := idAt(base.Add(-1 * time.Hour))

	chunks := []ChunkMeta{
		// Evicted: huge logical Bytes, zero local disk claim.
		metaWithClaim(evictedOld, base.Add(-3*time.Hour), base.Add(-150*time.Minute), 10_000_000, 0, true),
		// Cached: claims real disk (its cache bytes).
		metaWithClaim(cachedMid, base.Add(-2*time.Hour), base.Add(-90*time.Minute), 5000, 600, true),
		// Ordinary local chunk, small.
		metaWithClaim(freshSmall, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 100, 100, false),
	}
	claims := map[ChunkID]int64{
		evictedOld: 0,
		cachedMid:  600,
		freshSmall: 100,
	}

	policy := NewSizeRetentionPolicy(500)
	got := policy.Apply(VaultState{Chunks: chunks, Now: base, Claims: claims})

	// Budget 500: newest-first, freshSmall (100) fits (budget=100), then
	// cachedMid (600) does not (100+600 > 500) so it is deleted, then
	// evictedOld (0) always fits regardless of order or budget already
	// spent — it must never be selected even though it is the oldest and
	// logically the largest chunk in the vault.
	want := []ChunkID{cachedMid}
	if !chunkIDsEqual(got, want) {
		t.Fatalf("got %s, want %s (evicted must survive, cached must be eligible)", formatIDs(got), formatIDs(want))
	}
	if slicesContainsChunkID(got, evictedOld) {
		t.Fatalf("evicted cloud-backed chunk must never be selected by a size trigger: got %s", formatIDs(got))
	}
}

// TestSizeRetentionPolicyMixedVault drives a vault with every claim shape at
// once — DiskBytes-recorded, fallback (no DiskBytes), cached cloud-backed,
// evicted cloud-backed — and pins the exact keep/delete split.
func TestSizeRetentionPolicyMixedVault(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	oldest := idAt(base.Add(-4 * time.Hour)) // fallback path, claim 250
	old := idAt(base.Add(-3 * time.Hour))    // evicted cloud-backed, claim 0
	mid := idAt(base.Add(-2 * time.Hour))    // DiskBytes recorded, claim 300
	newest := idAt(base.Add(-1 * time.Hour)) // cached cloud-backed, claim 150

	chunks := []ChunkMeta{
		metaWithClaim(oldest, base.Add(-4*time.Hour), base.Add(-3*time.Hour-30*time.Minute), 200, 0, false),
		metaWithClaim(old, base.Add(-3*time.Hour), base.Add(-2*time.Hour-30*time.Minute), 8_000_000, 0, true),
		metaWithClaim(mid, base.Add(-2*time.Hour), base.Add(-1*time.Hour-30*time.Minute), 900, 300, false),
		metaWithClaim(newest, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 2000, 150, true),
	}
	claims := map[ChunkID]int64{
		oldest: 250,
		old:    0,
		mid:    300,
		newest: 150,
	}

	// Budget 400: newest-first — newest(150) fits (150), mid(300) does not
	// (150+300=450 > 400) so mid is deleted, old(0) always fits, oldest(250)
	// does not fit alongside newest's 150 kept-budget (150+250=400 <= 400,
	// so it DOES fit) — walk it precisely below instead of eyeballing.
	policy := NewSizeRetentionPolicy(400)
	got := policy.Apply(VaultState{Chunks: chunks, Now: base, Claims: claims})

	// Newest-first walk: newest(150) -> budget=150 (kept). mid(300) ->
	// 150+300=450 > 400, skip (deleted). old(0) -> 150+0=150 <= 400, kept.
	// oldest(250) -> 150+250=400 <= 400, kept.
	want := []ChunkID{mid}
	if !chunkIDsEqual(got, want) {
		t.Fatalf("got %s, want %s", formatIDs(got), formatIDs(want))
	}
}

func slicesContainsChunkID(ids []ChunkID, target ChunkID) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

// --- CountRetentionPolicy ---

func TestCountRetentionPolicy(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	id1 := idAt(base.Add(-4 * time.Hour))
	id2 := idAt(base.Add(-3 * time.Hour))
	id3 := idAt(base.Add(-2 * time.Hour))
	id4 := idAt(base.Add(-1 * time.Hour))

	tests := []struct {
		name      string
		chunks    []ChunkMeta
		maxChunks int
		want      []ChunkID
	}{
		{
			name: "under count",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-4*time.Hour), base.Add(-3*time.Hour+30*time.Minute), 100),
				metaAt(id2, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 200),
			},
			maxChunks: 5,
			want:      nil,
		},
		{
			name: "exactly at count",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-4*time.Hour), base.Add(-3*time.Hour+30*time.Minute), 100),
				metaAt(id2, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 200),
				metaAt(id3, base.Add(-2*time.Hour), base.Add(-1*time.Hour+30*time.Minute), 300),
			},
			maxChunks: 3,
			want:      nil,
		},
		{
			name: "over count deletes oldest",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-4*time.Hour), base.Add(-3*time.Hour+30*time.Minute), 100),
				metaAt(id2, base.Add(-3*time.Hour), base.Add(-2*time.Hour+30*time.Minute), 200),
				metaAt(id3, base.Add(-2*time.Hour), base.Add(-1*time.Hour+30*time.Minute), 300),
				metaAt(id4, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 400),
			},
			maxChunks: 2,
			want:      []ChunkID{id1, id2},
		},
		{
			name:      "empty state",
			chunks:    nil,
			maxChunks: 3,
			want:      nil,
		},
		{
			name: "single chunk with max 1",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-4*time.Hour), base.Add(-3*time.Hour+30*time.Minute), 100),
			},
			maxChunks: 1,
			want:      nil,
		},
		{
			name: "single chunk with max 0",
			chunks: []ChunkMeta{
				metaAt(id1, base.Add(-4*time.Hour), base.Add(-3*time.Hour+30*time.Minute), 100),
			},
			maxChunks: 0,
			want:      nil, // maxChunks <= 0 returns nil early
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := NewCountRetentionPolicy(tt.maxChunks)
			got := policy.Apply(VaultState{Chunks: tt.chunks, Now: base})
			if !chunkIDsEqual(got, tt.want) {
				t.Errorf("got %s, want %s", formatIDs(got), formatIDs(tt.want))
			}
		})
	}
}

// --- CompositeRetentionPolicy ---

func TestCompositeRetentionPolicy(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	id1 := idAt(base.Add(-72 * time.Hour)) // old and large
	id2 := idAt(base.Add(-48 * time.Hour)) // old but small
	id3 := idAt(base.Add(-12 * time.Hour)) // recent and large
	id4 := idAt(base.Add(-1 * time.Hour))  // recent and small

	chunks := []ChunkMeta{
		metaAt(id1, base.Add(-72*time.Hour), base.Add(-71*time.Hour), 500),
		metaAt(id2, base.Add(-48*time.Hour), base.Add(-47*time.Hour), 100),
		metaAt(id3, base.Add(-12*time.Hour), base.Add(-11*time.Hour), 500),
		metaAt(id4, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 100),
	}

	t.Run("different policies flag different chunks", func(t *testing.T) {
		// TTL of 24h expires id1 and id2.
		// Count of 3 keeps newest 3 (id2, id3, id4), deletes id1.
		// Union: id1 (both), id2 (TTL only).
		ttl := NewTTLRetentionPolicy(24 * time.Hour)
		count := NewCountRetentionPolicy(3)
		composite := NewCompositeRetentionPolicy(ttl, count)

		got := composite.Apply(VaultState{Chunks: chunks, Now: base})
		want := []ChunkID{id1, id2}
		if !chunkIDsEqualUnordered(got, want) {
			t.Errorf("got %s, want %s (unordered)", formatIDs(got), formatIDs(want))
		}
	})

	t.Run("same chunk flagged by multiple policies no duplicates", func(t *testing.T) {
		// TTL of 24h expires id1 and id2.
		// Count of 2 keeps newest 2 (id3, id4), deletes id1 and id2.
		// Both policies flag id1 and id2, but no duplicates in result.
		ttl := NewTTLRetentionPolicy(24 * time.Hour)
		count := NewCountRetentionPolicy(2)
		composite := NewCompositeRetentionPolicy(ttl, count)

		got := composite.Apply(VaultState{Chunks: chunks, Now: base})
		want := []ChunkID{id1, id2}
		if !chunkIDsEqualUnordered(got, want) {
			t.Errorf("got %s, want %s (unordered)", formatIDs(got), formatIDs(want))
		}
		// Verify no duplicates.
		seen := make(map[ChunkID]bool)
		for _, id := range got {
			if seen[id] {
				t.Errorf("duplicate ID in result: %s", id)
			}
			seen[id] = true
		}
	})

	t.Run("empty state", func(t *testing.T) {
		ttl := NewTTLRetentionPolicy(24 * time.Hour)
		count := NewCountRetentionPolicy(2)
		composite := NewCompositeRetentionPolicy(ttl, count)

		got := composite.Apply(VaultState{Now: base})
		if len(got) != 0 {
			t.Errorf("expected no deletions, got %s", formatIDs(got))
		}
	})
}

// --- NeverRetainPolicy ---

func TestNeverRetainPolicy(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	id1 := idAt(base.Add(-72 * time.Hour))
	id2 := idAt(base.Add(-1 * time.Hour))

	t.Run("returns nil with chunks", func(t *testing.T) {
		policy := NeverRetainPolicy{}
		got := policy.Apply(VaultState{
			Chunks: []ChunkMeta{
				metaAt(id1, base.Add(-72*time.Hour), base.Add(-71*time.Hour), 1000),
				metaAt(id2, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 2000),
			},
			Now: base,
		})
		if got != nil {
			t.Errorf("expected nil, got %s", formatIDs(got))
		}
	})

	t.Run("returns nil with empty state", func(t *testing.T) {
		policy := NeverRetainPolicy{}
		got := policy.Apply(VaultState{Now: base})
		if got != nil {
			t.Errorf("expected nil, got %s", formatIDs(got))
		}
	})
}

// --- Edge cases: empty state for all policy types ---

func TestAllPolicies_EmptyState(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	empty := VaultState{Now: base}

	policies := []struct {
		name   string
		policy RetentionPolicy
	}{
		{"TTL", NewTTLRetentionPolicy(24 * time.Hour)},
		{"Size", NewSizeRetentionPolicy(1000)},
		{"Count", NewCountRetentionPolicy(5)},
		{"Composite", NewCompositeRetentionPolicy(
			NewTTLRetentionPolicy(24*time.Hour),
			NewCountRetentionPolicy(5),
		)},
		{"Never", NeverRetainPolicy{}},
	}

	for _, tt := range policies {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.policy.Apply(empty)
			if len(got) != 0 {
				t.Errorf("expected no deletions for empty state, got %s", formatIDs(got))
			}
		})
	}
}

// --- Edge case: RetentionPolicyFunc adapter ---

func TestRetentionPolicyFunc(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	id1 := idAt(base.Add(-1 * time.Hour))

	fn := RetentionPolicyFunc(func(state VaultState) []ChunkID {
		var result []ChunkID
		for _, m := range state.Chunks {
			result = append(result, m.ID)
		}
		return result
	})

	got := fn.Apply(VaultState{
		Chunks: []ChunkMeta{
			metaAt(id1, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 100),
		},
		Now: base,
	})

	if len(got) != 1 || got[0] != id1 {
		t.Errorf("expected [%s], got %s", id1, formatIDs(got))
	}
}
