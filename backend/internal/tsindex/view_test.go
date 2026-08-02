package tsindex

import (
	"testing"
)

// RawView is the version-1 TS-index section view: a search API over the raw
// contiguous [ts:i64][pos:u32] entry bytes, no header, no heap copy. It is
// what the GLCB section registry hands out for ITSI/STSI v1 sections; later
// section versions implement the same View interface over different bytes.

func rawEntries(t *testing.T, entries []Entry) []byte {
	t.Helper()
	Sort(entries)
	return EncodeAll(entries)
}

func TestRawViewSearchTS(t *testing.T) {
	t.Parallel()
	v, err := NewRawView(rawEntries(t, []Entry{
		{TS: 100, Pos: 7},
		{TS: 200, Pos: 3},
		{TS: 300, Pos: 9},
	}))
	if err != nil {
		t.Fatalf("NewRawView: %v", err)
	}

	cases := []struct {
		probe    int64
		wantRank uint32
		wantPos  uint32
		wantOK   bool
	}{
		{probe: 50, wantRank: 0, wantPos: 7, wantOK: true},  // before all → first
		{probe: 100, wantRank: 0, wantPos: 7, wantOK: true}, // exact first
		{probe: 150, wantRank: 1, wantPos: 3, wantOK: true}, // between
		{probe: 300, wantRank: 2, wantPos: 9, wantOK: true}, // exact last
		{probe: 301, wantOK: false},                         // past all
	}
	for _, c := range cases {
		rank, pos, ok := v.SearchTS(c.probe)
		if ok != c.wantOK || (ok && (rank != c.wantRank || pos != c.wantPos)) {
			t.Errorf("SearchTS(%d) = (%d, %d, %v), want (%d, %d, %v)",
				c.probe, rank, pos, ok, c.wantRank, c.wantPos, c.wantOK)
		}
	}
}

func TestRawViewLenAndEntryAt(t *testing.T) {
	t.Parallel()
	v, err := NewRawView(rawEntries(t, []Entry{{TS: 1, Pos: 10}, {TS: 2, Pos: 20}}))
	if err != nil {
		t.Fatalf("NewRawView: %v", err)
	}
	if v.Len() != 2 {
		t.Fatalf("Len = %d, want 2", v.Len())
	}
	if e := v.EntryAt(1); e.TS != 2 || e.Pos != 20 {
		t.Errorf("EntryAt(1) = %+v, want {TS:2 Pos:20}", e)
	}
}

// A section whose length is not a multiple of the entry size is structurally
// corrupt; refusing it at view construction keeps every later read in bounds.
func TestRawViewRejectsRaggedBytes(t *testing.T) {
	t.Parallel()
	if _, err := NewRawView(make([]byte, EntrySize+1)); err == nil {
		t.Fatal("ragged section accepted")
	}
}

// Empty is valid input for a view (a chunk can have zero records); it must
// answer not-found rather than reject, so callers need no special case.
func TestRawViewEmpty(t *testing.T) {
	t.Parallel()
	v, err := NewRawView(nil)
	if err != nil {
		t.Fatalf("NewRawView(nil): %v", err)
	}
	if v.Len() != 0 {
		t.Fatalf("Len = %d, want 0", v.Len())
	}
	if _, _, ok := v.SearchTS(0); ok {
		t.Error("SearchTS on empty view reported a hit")
	}
}

// RawView must satisfy the interface the registry hands out.
var _ View = RawView{}
