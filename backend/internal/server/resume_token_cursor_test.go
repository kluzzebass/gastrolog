package server

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
)

// The cursor's identity must survive the wire, or the next page falls back to
// timestamp-only resumption and ties are repeated.
func TestResumeTokenRoundTripsTheCursorEvent(t *testing.T) {
	ts := time.Date(2026, 9, 5, 12, 0, 0, 123456789, time.UTC)
	ev := chunk.EventID{IngesterID: glid.New(), NodeID: glid.New(), IngestTS: ts, IngestSeq: 42}
	in := &query.ResumeToken{HighwaterTS: ts, HighwaterEvent: ev}

	out, err := ProtoToResumeToken(ResumeTokenToProto(in))
	if err != nil || out == nil {
		t.Fatalf("round trip: token=%v err=%v", out, err)
	}
	if !out.HighwaterTS.Equal(ts) {
		t.Fatalf("HighwaterTS = %v, want %v", out.HighwaterTS, ts)
	}
	if out.HighwaterEvent.Compare(ev) != 0 || out.HighwaterEvent.NodeID != ev.NodeID {
		t.Fatalf("HighwaterEvent = %+v, want %+v", out.HighwaterEvent, ev)
	}
}

// On a page that merged local and remote records but showed no local one,
// the engine still holds a local record pulled ahead for comparison — and has
// already counted its position. Resuming from that position skips it; with
// remote records filling page after page, one local record vanishes per page.
// The page must hand on the local positions exactly as it received them.
func TestResumeTokenHoldsLocalPositionsWhenThePageShowedNoLocalRecord(t *testing.T) {
	vaultID := glid.New()
	chunkID := chunk.ChunkID(glid.New())
	pulledAhead := &query.ResumeToken{Positions: []query.MultiVaultPosition{{VaultID: vaultID, ChunkID: chunkID, Position: 5}}}
	mark := &emitMark{ts: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)}

	for _, tc := range []struct {
		name string
		held []query.MultiVaultPosition
	}{
		{"first page: no local position yet", nil},
		{"later page: positions from the previous page", []query.MultiVaultPosition{{VaultID: vaultID, ChunkID: chunkID, Position: 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := buildResumeTokenBytes(nil, func() *query.ResumeToken {
				tok := *pulledAhead
				return &tok
			}, mark, false, false, chunk.Record{}, tc.held, true)
			got, err := ProtoToLocalResumeToken(out)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(got.Positions) != len(tc.held) {
				t.Fatalf("token carries positions %v, want the held %v — not the pulled-ahead position 5", got.Positions, tc.held)
			}
			for i := range tc.held {
				if got.Positions[i] != tc.held[i] {
					t.Fatalf("position %d = %v, want held %v", i, got.Positions[i], tc.held[i])
				}
			}
			if !got.HighwaterTS.Equal(mark.ts) {
				t.Fatalf("highwater %v, want the merge's %v", got.HighwaterTS, mark.ts)
			}
		})
	}
}

// A page that showed a local record resumes that vault right after the last
// one shown, whatever the engine pulled ahead.
func TestResumeTokenPinsToTheLastLocalRecordShown(t *testing.T) {
	vaultID := glid.New()
	chunkID := chunk.ChunkID(glid.New())
	shown := chunk.Record{VaultID: vaultID, Ref: chunk.RecordRef{ChunkID: chunkID, Pos: 3}}
	out := buildResumeTokenBytes(nil, func() *query.ResumeToken {
		return &query.ResumeToken{Positions: []query.MultiVaultPosition{{VaultID: vaultID, ChunkID: chunkID, Position: 4}}}
	}, &emitMark{ts: time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)}, false, true, shown, nil, true)
	got, err := ProtoToLocalResumeToken(out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Positions) != 1 || got.Positions[0].Position != 3 {
		t.Fatalf("positions %v, want only the last shown record (pos 3)", got.Positions)
	}
}
