package spool

import "testing"

func TestReclaimable(t *testing.T) {
	t.Parallel()
	sealed := SegmentMeta{ID: 100, FirstSeq: 100, LastSeq: 105, RecordCount: 3, Sealed: true}
	activeOpen := SegmentMeta{ID: 200, FirstSeq: 200, LastSeq: 200, RecordCount: 1, Sealed: false}

	if err := Reclaimable(sealed, 105, 0); err != nil {
		t.Fatalf("sealed at watermark: %v", err)
	}
	if err := Reclaimable(sealed, 104, 0); err != ErrReclaimBlocked {
		t.Fatalf("above watermark err = %v, want %v", err, ErrReclaimBlocked)
	}
	if err := Reclaimable(sealed, 0, 0); err != ErrReclaimBlocked {
		t.Fatalf("zero watermark err = %v, want %v", err, ErrReclaimBlocked)
	}
	if err := Reclaimable(activeOpen, 999, 200); err != ErrReclaimBlocked {
		t.Fatalf("active open err = %v, want %v", err, ErrReclaimBlocked)
	}
	if err := Reclaimable(activeOpen, 999, 0); err != ErrSegmentNotSealed {
		t.Fatalf("unsealed err = %v, want %v", err, ErrSegmentNotSealed)
	}
}
