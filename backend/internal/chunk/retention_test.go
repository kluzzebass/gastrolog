package chunk

import (
	"testing"
	"time"
)

// Helper to build a ChunkMeta with the given parameters.
func metaAt(id ChunkID, start, end time.Time, bytes int64) ChunkMeta {
	return ChunkMeta{
		ID:      id,
		StartTS: start,
		EndTS:   end,
		Bytes:   bytes,
		Sealed:  true,
	}
}

// Helper to create deterministic ChunkIDs from a time.
func idAt(t time.Time) ChunkID {
	return ChunkIDFromTime(t)
}

// collectIDs extracts just the ChunkIDs from a slice for comparison.
func collectIDs(metas []ChunkMeta) []ChunkID {
	ids := make([]ChunkID, len(metas))
	for i, m := range metas {
		ids[i] = m.ID
	}
	return ids
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
	s := "["
	for i, id := range ids {
		if i > 0 {
			s += ", "
		}
		s += id.String()
	}
	s += "]"
	return s
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
			got := policy.Apply(StoreState{Chunks: tt.chunks, Now: tt.now})
			if !chunkIDsEqual(got, tt.want) {
				t.Errorf("got %s, want %s", formatIDs(got), formatIDs(tt.want))
			}
		})
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
			got := policy.Apply(StoreState{Chunks: tt.chunks, Now: base})
			if !chunkIDsEqual(got, tt.want) {
				t.Errorf("got %s, want %s", formatIDs(got), formatIDs(tt.want))
			}
		})
	}
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
			got := policy.Apply(StoreState{Chunks: tt.chunks, Now: base})
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

		got := composite.Apply(StoreState{Chunks: chunks, Now: base})
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

		got := composite.Apply(StoreState{Chunks: chunks, Now: base})
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

		got := composite.Apply(StoreState{Now: base})
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
		got := policy.Apply(StoreState{
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
		got := policy.Apply(StoreState{Now: base})
		if got != nil {
			t.Errorf("expected nil, got %s", formatIDs(got))
		}
	})
}

// --- Edge cases: empty state for all policy types ---

func TestAllPolicies_EmptyState(t *testing.T) {
	base := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	empty := StoreState{Now: base}

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

	fn := RetentionPolicyFunc(func(state StoreState) []ChunkID {
		var result []ChunkID
		for _, m := range state.Chunks {
			result = append(result, m.ID)
		}
		return result
	})

	got := fn.Apply(StoreState{
		Chunks: []ChunkMeta{
			metaAt(id1, base.Add(-1*time.Hour), base.Add(-30*time.Minute), 100),
		},
		Now: base,
	})

	if len(got) != 1 || got[0] != id1 {
		t.Errorf("expected [%s], got %s", id1, formatIDs(got))
	}
}
