package sparkline_test

import (
	"testing"

	"gastrolog/internal/sparkline"
)

// rate is a named float64 type — proves the ~float64 constraint accepts
// domain types, not just the built-in kinds.
type rate float64

func TestBoundedRetainsLastN(t *testing.T) {
	t.Parallel()
	s := sparkline.New[int](3)
	for i := 1; i <= 5; i++ {
		s.Push(i)
	}
	got := s.Values()
	want := []int{3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Values() = %v, want %v", got, want)
		}
	}
	if s.Len() != 3 || s.Cap() != 3 {
		t.Fatalf("Len=%d Cap=%d, want 3/3", s.Len(), s.Cap())
	}
}

func TestUnboundedGrows(t *testing.T) {
	t.Parallel()
	s := sparkline.New[uint64](0) // no limit
	for i := range uint64(1000) {
		s.Push(i)
	}
	if s.Len() != 1000 || s.Cap() != 0 {
		t.Fatalf("Len=%d Cap=%d, want 1000/0", s.Len(), s.Cap())
	}
	got := s.Values()
	if got[0] != 0 || got[999] != 999 {
		t.Fatalf("ends = %d..%d, want 0..999", got[0], got[999])
	}
}

func TestGenericOverFloatAndNamedType(t *testing.T) {
	t.Parallel()
	s := sparkline.New[rate](2)
	s.Push(1.5)
	s.Push(2.5)
	s.Push(3.5)
	got := s.Values()
	if len(got) != 2 || got[0] != 2.5 || got[1] != 3.5 {
		t.Fatalf("Values() = %v, want [2.5 3.5]", got)
	}
}

func TestValuesIsDefensiveCopy(t *testing.T) {
	t.Parallel()
	s := sparkline.New[int](4)
	s.Push(1)
	s.Push(2)
	snap := s.Values()
	snap[0] = 999 // mutating the copy must not touch the sparkline
	s.Push(3)     // later pushes must not touch the earlier copy
	if v := s.Values(); v[0] != 1 {
		t.Fatalf("sparkline mutated through returned slice: %v", v)
	}
	if snap[0] != 999 || len(snap) != 2 {
		t.Fatalf("earlier snapshot changed by later Push: %v", snap)
	}
}

func TestEmptyAndReset(t *testing.T) {
	t.Parallel()
	s := sparkline.New[int](3)
	if s.Len() != 0 || s.Values() != nil {
		t.Fatalf("fresh sparkline not empty: len=%d vals=%v", s.Len(), s.Values())
	}
	s.Push(7)
	s.Push(8)
	s.Reset()
	if s.Len() != 0 || s.Values() != nil {
		t.Fatalf("after Reset not empty: len=%d vals=%v", s.Len(), s.Values())
	}
	s.Push(9) // usable after reset, capacity preserved
	if v := s.Values(); len(v) != 1 || v[0] != 9 {
		t.Fatalf("post-reset push = %v, want [9]", v)
	}
}

// TestWraparoundOrdering pushes many more than capacity through the ring and
// asserts the oldest-first ordering survives the head wrap.
func TestWraparoundOrdering(t *testing.T) {
	t.Parallel()
	s := sparkline.New[int](4)
	for i := range 10 {
		s.Push(i)
	}
	got := s.Values()
	want := []int{6, 7, 8, 9}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Values() = %v, want %v", got, want)
		}
	}
}
