package chunk

import (
	"slices"
	"testing"
	"time"
)

// permutations calls fn with every ordering of chunks (Heap's algorithm).
func permutations(chunks []ChunkMeta, fn func([]ChunkMeta)) {
	p := slices.Clone(chunks)
	c := make([]int, len(p))
	fn(slices.Clone(p))
	for i := 0; i < len(p); {
		if c[i] < i {
			if i%2 == 0 {
				p[0], p[i] = p[i], p[0]
			} else {
				p[c[i]], p[i] = p[i], p[c[i]]
			}
			fn(slices.Clone(p))
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
}

// oldestFirstFixture is six sealed chunks, oldest first, with distinct
// claims so a size budget that fits the newest two fits nothing else.
func oldestFirstFixture(base time.Time, start func(i int) time.Time) ([]ChunkMeta, map[ChunkID]int64) {
	chunks := make([]ChunkMeta, 6)
	claims := make(map[ChunkID]int64, len(chunks))
	for i := range chunks {
		chunks[i] = metaAt(NewChunkID(), start(i), base.Add(time.Duration(i)*time.Minute+30*time.Second), 100)
		claims[chunks[i].ID] = int64(100 + 10*i)
	}
	return chunks, claims
}

func idsOf(chunks []ChunkMeta) []ChunkID {
	ids := make([]ChunkID, len(chunks))
	for i := range chunks {
		ids[i] = chunks[i].ID
	}
	return ids
}

// assertOldestChosenInEveryOrder runs the count and size policies over every
// ordering of chunks (given oldest first) and requires both to delete exactly
// the oldest four, oldest first, without reordering the caller's slice.
func assertOldestChosenInEveryOrder(t *testing.T, chunks []ChunkMeta, claims map[ChunkID]int64, now time.Time) {
	t.Helper()
	want := idsOf(chunks[:4])
	budget := claims[chunks[4].ID] + claims[chunks[5].ID]
	policies := map[string]RetentionPolicy{
		"count": NewCountRetentionPolicy(2),
		"size":  NewSizeRetentionPolicy(budget),
	}
	permutations(chunks, func(in []ChunkMeta) {
		before := idsOf(in)
		for name, p := range policies {
			got := p.Apply(VaultState{Chunks: in, Now: now, Claims: claims})
			if !chunkIDsEqual(got, want) {
				t.Fatalf("%s policy over input order %s: got %s, want the four oldest %s",
					name, formatIDs(before), formatIDs(got), formatIDs(want))
			}
			if !chunkIDsEqual(idsOf(in), before) {
				t.Fatalf("%s policy reordered the caller's chunks", name)
			}
		}
	})
}

// TestCountAndSizePoliciesIgnoreInputOrder pins that the policies keeping the
// newest chunks decide by WriteStart, not by the position a chunk holds in
// the list the caller assembled.
func TestCountAndSizePoliciesIgnoreInputOrder(t *testing.T) {
	t.Parallel()
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	chunks, claims := oldestFirstFixture(base, func(i int) time.Time {
		return base.Add(time.Duration(i) * time.Minute)
	})
	assertOldestChosenInEveryOrder(t, chunks, claims, base.Add(time.Hour))
}

// TestCountAndSizePoliciesBreakWriteStartTiesByChunkID pins the tie-break:
// chunks sharing a WriteStart order by chunk ID, which orders by creation.
func TestCountAndSizePoliciesBreakWriteStartTiesByChunkID(t *testing.T) {
	t.Parallel()
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	t.Run("all-tied", func(t *testing.T) {
		t.Parallel()
		chunks, claims := oldestFirstFixture(base, func(int) time.Time { return base })
		assertOldestChosenInEveryOrder(t, chunks, claims, base.Add(time.Hour))
	})
	t.Run("tie-across-the-cut", func(t *testing.T) {
		t.Parallel()
		chunks, claims := oldestFirstFixture(base, func(i int) time.Time {
			return base.Add(time.Duration(min(i, 3)) * time.Minute)
		})
		assertOldestChosenInEveryOrder(t, chunks, claims, base.Add(time.Hour))
	})
}

// TestCountAndSizePoliciesZeroWriteStartIsOldest pins where a chunk with no
// recorded WriteStart lands: first, as the chunk stores' own List orders it.
func TestCountAndSizePoliciesZeroWriteStartIsOldest(t *testing.T) {
	t.Parallel()
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	chunks, claims := oldestFirstFixture(base, func(i int) time.Time {
		if i < 2 {
			return time.Time{}
		}
		return base.Add(time.Duration(i) * time.Minute)
	})
	assertOldestChosenInEveryOrder(t, chunks, claims, base.Add(time.Hour))
}

// TestTTLPolicyIsOrderIndependent pins that the age policy needs no
// ordering: every input order selects the same expired set.
func TestTTLPolicyIsOrderIndependent(t *testing.T) {
	t.Parallel()
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	chunks, _ := oldestFirstFixture(base, func(i int) time.Time {
		return base.Add(time.Duration(i) * time.Minute)
	})
	now := base.Add(time.Hour)
	ttl := NewTTLRetentionPolicy(now.Sub(chunks[3].SealedAt) - time.Second)
	want := idsOf(chunks[:4])
	permutations(chunks, func(in []ChunkMeta) {
		if got := ttl.Apply(VaultState{Chunks: in, Now: now}); !chunkIDsEqualUnordered(got, want) {
			t.Fatalf("ttl over input order %s: got %s, want %s", formatIDs(idsOf(in)), formatIDs(got), formatIDs(want))
		}
	})
}

// TestCompositePolicyOrderIndependent drives a count bound and an age bound
// together over every input order: the union is always the over-age chunks
// plus the oldest beyond the count.
func TestCompositePolicyOrderIndependent(t *testing.T) {
	t.Parallel()
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	chunks, _ := oldestFirstFixture(base, func(i int) time.Time {
		return base.Add(time.Duration(i) * time.Minute)
	})
	now := base.Add(time.Hour)
	composite := NewCompositeRetentionPolicy(
		NewTTLRetentionPolicy(now.Sub(chunks[1].SealedAt)-time.Second),
		NewCountRetentionPolicy(3),
	)
	want := idsOf(chunks[:3])
	permutations(chunks, func(in []ChunkMeta) {
		if got := composite.Apply(VaultState{Chunks: in, Now: now}); !chunkIDsEqualUnordered(got, want) {
			t.Fatalf("composite over input order %s: got %s, want %s", formatIDs(idsOf(in)), formatIDs(got), formatIDs(want))
		}
	})
}
