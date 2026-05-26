package spool

import "testing"

func TestReclaimable(t *testing.T) {
	t.Parallel()
	sealed := SegmentMeta{
		ID:          100,
		Window:      WindowID{Start: 100, End: 110},
		FirstSeq:    100,
		EndSeq:      110,
		LastSeq:     105,
		RecordCount: 3,
		Sealed:      true,
	}
	activeOpen := SegmentMeta{
		ID:          200,
		Window:      WindowID{Start: 200, End: 210},
		FirstSeq:    200,
		EndSeq:      210,
		LastSeq:     200,
		RecordCount: 1,
		Sealed:      false,
	}

	if err := Reclaimable(sealed, 110, WindowID{}); err != nil {
		t.Fatalf("sealed at watermark: %v", err)
	}
	if err := Reclaimable(sealed, 104, WindowID{}); err != ErrReclaimBlocked {
		t.Fatalf("above watermark err = %v, want %v", err, ErrReclaimBlocked)
	}
	if err := Reclaimable(sealed, 0, WindowID{}); err != ErrReclaimBlocked {
		t.Fatalf("zero watermark err = %v, want %v", err, ErrReclaimBlocked)
	}
	if err := Reclaimable(activeOpen, 999, WindowID{Start: 200, End: 210}); err != ErrReclaimBlocked {
		t.Fatalf("active open err = %v, want %v", err, ErrReclaimBlocked)
	}
	if err := Reclaimable(activeOpen, 999, WindowID{}); err != ErrSegmentNotSealed {
		t.Fatalf("unsealed err = %v, want %v", err, ErrSegmentNotSealed)
	}
}
