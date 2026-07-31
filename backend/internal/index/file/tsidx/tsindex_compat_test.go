package tsidx

import (
	"bytes"
	"testing"

	"gastrolog/internal/tsindex"
)

// goldenEntries and goldenBytes pin the 12-byte little-endian
// [ts:i64][pos:u32] × N wire layout independent of both tsidx and
// tsindex — these bytes are hand-computed, not produced by either
// package under test, so a bug shared by both encoder and decoder
// would still be caught.
//
//	(ts=100, pos=5)   -> 64 00 00 00 00 00 00 00 05 00 00 00
//	(ts=200, pos=2)   -> C8 00 00 00 00 00 00 00 02 00 00 00
//	(ts=300, pos=9)   -> 2C 01 00 00 00 00 00 00 09 00 00 00
//	(ts=300, pos=11)  -> 2C 01 00 00 00 00 00 00 0B 00 00 00 (TS tie, Pos breaks it)
var (
	goldenEntries = []tsindex.Entry{
		{TS: 100, Pos: 5},
		{TS: 200, Pos: 2},
		{TS: 300, Pos: 9},
		{TS: 300, Pos: 11},
	}
	goldenBytes = []byte{
		0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00,
		0xC8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
		0x2C, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00,
		0x2C, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00,
	}
)

// TestGoldenBytesPinFormat is the byte-compat proof for the 12-byte
// TS-index wire layout: it hardcodes the entries independent of both
// tsindex and tsidx, then asserts every read/write path in both packages
// agrees on them.
func TestGoldenBytesPinFormat(t *testing.T) {
	t.Parallel()

	if len(goldenBytes) != len(goldenEntries)*tsindex.EntrySize {
		t.Fatalf("fixture length mismatch: %d bytes for %d entries", len(goldenBytes), len(goldenEntries))
	}

	// tsindex.EncodeAll(goldenEntries) must reproduce the hand-computed
	// golden bytes exactly.
	if got := tsindex.EncodeAll(goldenEntries); !bytes.Equal(got, goldenBytes) {
		t.Fatalf("tsindex.EncodeAll(goldenEntries) = % x, want % x", got, goldenBytes)
	}

	// tsindex.Decode, entry by entry, must reproduce goldenEntries.
	for i, want := range goldenEntries {
		off := i * tsindex.EntrySize
		if got := tsindex.Decode(goldenBytes[off : off+tsindex.EntrySize]); got != want {
			t.Errorf("tsindex.Decode(entry %d) = %+v, want %+v", i, got, want)
		}
	}

	// tsidx.decodeRawEntries (the ported GLCB section decoder) must
	// reproduce goldenEntries too — this is the layout-unification seam:
	// tsidx no longer parses bytes itself, it delegates to tsindex.Decode.
	decoded, err := decodeRawEntries(goldenBytes)
	if err != nil {
		t.Fatalf("decodeRawEntries: %v", err)
	}
	if len(decoded) != len(goldenEntries) {
		t.Fatalf("decodeRawEntries len = %d, want %d", len(decoded), len(goldenEntries))
	}
	for i, want := range goldenEntries {
		if decoded[i] != want {
			t.Errorf("decodeRawEntries entry %d = %+v, want %+v", i, decoded[i], want)
		}
	}

	// tsidx.MmapView (via ViewFromSection, the in-process equivalent of an
	// mmap'd GLCB section) must read the same golden bytes identically to
	// tsindex.Decode / tsindex.EncodeAll above — no heap-allocated slice.
	mv, err := ViewFromSection(goldenBytes)
	if err != nil {
		t.Fatalf("ViewFromSection: %v", err)
	}
	if mv.Len() != uint32(len(goldenEntries)) { //nolint:gosec // G115: test fixture, small constant
		t.Fatalf("MmapView.Len() = %d, want %d", mv.Len(), len(goldenEntries))
	}
	for i, want := range goldenEntries {
		if got := mv.EntryAt(uint32(i)); got != want { //nolint:gosec // G115: test fixture, small constant
			t.Errorf("MmapView.EntryAt(%d) = %+v, want %+v", i, got, want)
		}
	}
}

// TestSearchAgreesWithTsindexFindStart cross-checks tsidx's two lookup
// paths (the decoded-slice FindStartPosition/FindStartRank and the
// mmap'd MmapView.SearchTS) against tsindex.FindStart operating directly
// on the same golden bytes. All three must agree on both position and
// found/not-found, proving the ported binary-search shape stayed
// behavior-identical to the pre-port hand-rolled version.
func TestSearchAgreesWithTsindexFindStart(t *testing.T) {
	t.Parallel()

	decoded, err := decodeRawEntries(goldenBytes)
	if err != nil {
		t.Fatalf("decodeRawEntries: %v", err)
	}
	mv, err := ViewFromSection(goldenBytes)
	if err != nil {
		t.Fatalf("ViewFromSection: %v", err)
	}

	for _, ts := range []int64{0, 50, 100, 150, 200, 250, 300, 301, 1000} {
		wantPos, wantOK := tsindex.FindStart(goldenBytes, ts)

		gotPos, gotOK := FindStartPosition(decoded, ts)
		if gotOK != wantOK || gotPos != uint64(wantPos) {
			t.Errorf("FindStartPosition(ts=%d) = (%d, %v), want (%d, %v) per tsindex.FindStart",
				ts, gotPos, gotOK, wantPos, wantOK)
		}

		_, mvPos, mvOK := mv.SearchTS(ts)
		if mvOK != wantOK || mvPos != wantPos {
			t.Errorf("MmapView.SearchTS(ts=%d) = (%d, %v), want (%d, %v) per tsindex.FindStart",
				ts, mvPos, mvOK, wantPos, wantOK)
		}
	}
}
