package chunk

import (
	"testing"
	"time"
)

// reasonFixture is four sealed chunks A<B<C<D (oldest first), each claiming
// 100 bytes, and three triggers whose selections nest:
//
//	age   (maxAge 150m)      selects A
//	size  (maxBytes 200)     selects A, B
//	count (maxChunks 1)      selects A, B, C
//
// so every chunk the triggers select is selected by a different set of them.
type reasonFixture struct {
	a, b, c, d       ChunkID
	state            VaultState
	age, size, count RetentionPolicy
}

func newReasonFixture() reasonFixture {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	f := reasonFixture{a: NewChunkID(), b: NewChunkID(), c: NewChunkID(), d: NewChunkID()}
	ids := []ChunkID{f.a, f.b, f.c, f.d}
	claims := make(map[ChunkID]int64)
	var metas []ChunkMeta
	for i, id := range ids {
		start := base.Add(time.Duration(i) * time.Hour)
		metas = append(metas, metaAt(id, start, start.Add(time.Minute), 100))
		claims[id] = 100
	}
	f.state = VaultState{Chunks: metas, Now: base.Add(3 * time.Hour), Claims: claims}
	f.age = NewTTLRetentionPolicy(150 * time.Minute)
	f.size = NewSizeRetentionPolicy(200)
	f.count = NewCountRetentionPolicy(1)
	return f
}

func reasonsByID(matches []RetentionMatch) map[ChunkID]RetentionReason {
	out := make(map[ChunkID]RetentionReason, len(matches))
	for _, m := range matches {
		out[m.ID] = m.Reason
	}
	return out
}

func matchIDs(matches []RetentionMatch) []ChunkID {
	ids := make([]ChunkID, len(matches))
	for i, m := range matches {
		ids[i] = m.ID
	}
	return ids
}

func TestMatchRetentionNamesEachSingleTrigger(t *testing.T) {
	t.Parallel()
	f := newReasonFixture()
	cases := []struct {
		name   string
		policy RetentionPolicy
		want   RetentionReason
	}{
		{"age", f.age, RetentionReasonAge},
		{"size", f.size, RetentionReasonSize},
		{"count", f.count, RetentionReasonCount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := MatchRetention(c.policy, f.state)
			if want := c.policy.Apply(f.state); !chunkIDsEqual(matchIDs(got), want) {
				t.Fatalf("selected %s, want exactly Apply's %s", formatIDs(matchIDs(got)), formatIDs(want))
			}
			if len(got) == 0 {
				t.Fatal("fixture trigger selected nothing")
			}
			for _, m := range got {
				if m.Reason != c.want {
					t.Errorf("chunk %s reason = %q, want %q", m.ID, m.Reason, c.want)
				}
			}
		})
	}
}

// TestMatchRetentionCompositeTakesPrecedingReason builds the composite in the
// reverse of precedence order, so a reason that followed sub-policy order
// rather than precedence would name count for every chunk.
func TestMatchRetentionCompositeTakesPrecedingReason(t *testing.T) {
	t.Parallel()
	f := newReasonFixture()
	composite := NewCompositeRetentionPolicy(f.count, f.size, f.age)

	got := MatchRetention(composite, f.state)

	if want := composite.Apply(f.state); !chunkIDsEqual(matchIDs(got), want) {
		t.Fatalf("selected %s, want exactly Apply's %s in the same order", formatIDs(matchIDs(got)), formatIDs(want))
	}
	reasons := reasonsByID(got)
	want := map[ChunkID]RetentionReason{
		f.a: RetentionReasonAge,
		f.b: RetentionReasonSize,
		f.c: RetentionReasonCount,
	}
	for id, w := range want {
		if reasons[id] != w {
			t.Errorf("chunk %s reason = %q, want %q", id, reasons[id], w)
		}
	}
	if _, selected := reasons[f.d]; selected {
		t.Errorf("newest chunk must not be selected by any trigger")
	}
}

// TestMatchRetentionNestedComposite pins that a composite nested inside a
// composite still attributes each chunk to its own trigger.
func TestMatchRetentionNestedComposite(t *testing.T) {
	t.Parallel()
	f := newReasonFixture()
	nested := NewCompositeRetentionPolicy(f.count, NewCompositeRetentionPolicy(f.size, f.age))

	reasons := reasonsByID(MatchRetention(nested, f.state))
	if reasons[f.a] != RetentionReasonAge || reasons[f.b] != RetentionReasonSize || reasons[f.c] != RetentionReasonCount {
		t.Fatalf("nested composite reasons = %v", reasons)
	}
}

func TestMatchRetentionUntypedPolicyHasNoReason(t *testing.T) {
	t.Parallel()
	f := newReasonFixture()
	fn := RetentionPolicyFunc(func(VaultState) []ChunkID { return []ChunkID{f.d} })

	got := MatchRetention(NewCompositeRetentionPolicy(fn, f.count), f.state)
	reasons := reasonsByID(got)
	if r, ok := reasons[f.d]; !ok || r != "" {
		t.Fatalf("a chunk only an untyped policy selects must carry an empty reason, got %q (selected=%v)", r, ok)
	}
	if reasons[f.a] != RetentionReasonCount {
		t.Fatalf("a typed trigger must outrank the empty reason, got %q", reasons[f.a])
	}
}

func TestMatchRetentionEmptyVault(t *testing.T) {
	t.Parallel()
	f := newReasonFixture()
	composite := NewCompositeRetentionPolicy(f.age, f.size, f.count)
	if got := MatchRetention(composite, VaultState{Now: f.state.Now}); len(got) != 0 {
		t.Fatalf("empty vault selected %v", got)
	}
}

func TestPrecedingRetentionReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b, want RetentionReason
	}{
		{RetentionReasonAge, RetentionReasonSize, RetentionReasonAge},
		{RetentionReasonSize, RetentionReasonAge, RetentionReasonAge},
		{RetentionReasonCount, RetentionReasonSize, RetentionReasonSize},
		{RetentionReasonMemoryBudget, RetentionReasonCount, RetentionReasonCount},
		{"", RetentionReasonMemoryBudget, RetentionReasonMemoryBudget},
		{RetentionReasonCount, "", RetentionReasonCount},
		{"", "", ""},
	}
	for _, c := range cases {
		if got := PrecedingRetentionReason(c.a, c.b); got != c.want {
			t.Errorf("PrecedingRetentionReason(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}
