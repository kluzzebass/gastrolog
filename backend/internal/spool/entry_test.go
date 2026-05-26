package spool

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func TestSpoolIdxEntryRoundTrip(t *testing.T) {
	t.Parallel()
	want := IdxEntry{
		VaultSeq:   42,
		SourceTS:   time.Unix(1, 2).UTC(),
		IngestTS:   time.Unix(3, 4).UTC(),
		WriteTS:    time.Unix(5, 6).UTC(),
		RawOffset:  10,
		RawSize:    20,
		AttrOffset: 30,
		AttrSize:   40,
		IngestSeq:  7,
		IngesterID: glid.New(),
		NodeID:     glid.New(),
	}
	var buf [SpoolIdxEntrySize]byte
	EncodeIdxEntry(want, buf[:])
	got := DecodeIdxEntry(buf[:])
	if got != want {
		t.Fatalf("round trip mismatch:\nwant %+v\ngot  %+v", want, got)
	}
}

func TestRecordCountFromIdxSize(t *testing.T) {
	t.Parallel()
	if got := RecordCount(int64(IdxHeaderSize)); got != 0 {
		t.Fatalf("empty idx count = %d, want 0", got)
	}
	size := int64(IdxHeaderSize) + 2*int64(SpoolIdxEntrySize)
	if got := RecordCount(size); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	// Partial trailing entry is not counted (crash safety).
	partial := int64(IdxHeaderSize) + int64(SpoolIdxEntrySize) + 10
	if got := RecordCount(partial); got != 1 {
		t.Fatalf("partial tail count = %d, want 1", got)
	}
}

func TestSegmentMetaCoversSeq(t *testing.T) {
	t.Parallel()
	meta := SegmentMeta{ID: 100, FirstSeq: 100, LastSeq: 105, RecordCount: 3}
	for _, seq := range []uint64{99, 106} {
		if meta.CoversSeq(seq) {
			t.Fatalf("seq %d should not be covered", seq)
		}
	}
	for seq := uint64(100); seq <= 105; seq++ {
		if !meta.CoversSeq(seq) {
			t.Fatalf("seq %d should be covered", seq)
		}
	}
}

func TestBuildRecordPreservesVaultSeq(t *testing.T) {
	t.Parallel()
	entry := IdxEntry{VaultSeq: 99, IngestTS: time.Unix(1, 0).UTC(), IngestSeq: 1}
	rec := BuildRecord(entry, []byte("hi"), chunk.Attributes{"k": "v"})
	if rec.VaultSeq != 99 {
		t.Fatalf("VaultSeq = %d, want 99", rec.VaultSeq)
	}
}
