package server_test

// Through a gateway, consecutive pages of one search can be coordinated by
// different nodes. A page coordinated by a node that holds the vault locally
// mints a token carrying that vault's chunk positions; the next page, landing
// on a node where the same vault is remote, must still resume from it. Remote
// vaults resume at the coordinator's cursor, never at positions — a position
// blob is not a ResumeToken, and a peer handed one as its resume token can
// only refuse the page.

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/query"
	"gastrolog/internal/server"
)

func TestMultiNode_PageMintedByAnotherCoordinatorResumesOnARemoteVault(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data"}, WithoutVault("coord"))

	// Production-shaped records: every ingested record carries an event
	// identity, which is what lets the cursor resume strictly after the
	// boundary record instead of keeping its timestamp group.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	const total, pageLimit = 60, 25
	ingesterID, nodeID := glid.New(), glid.New()
	for i := range total {
		ts := t0.Add(time.Duration(i) * time.Second)
		h.Node(t, "data").vault.CM.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "rec-%02d", i),
			EventID: chunk.EventID{IngesterID: ingesterID, NodeID: nodeID, IngestTS: ts, IngestSeq: uint32(i)}, //nolint:gosec // G115: i < total
		})
	}
	start, end := t0, t0.Add((total+1)*time.Second)
	expr := fmt.Sprintf("start=%s end=%s limit=%d", start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), pageLimit)

	page := func(token []byte) (raws []string, last *gastrologv1.Record, next []byte) {
		t.Helper()
		req := &gastrologv1.SearchRequest{Query: &gastrologv1.Query{Expression: expr}, ResumeToken: token}
		stream, err := h.client.Search(context.Background(), connect.NewRequest(req))
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		for stream.Receive() {
			msg := stream.Msg()
			for _, r := range msg.Records {
				raws = append(raws, string(r.Raw))
				last = r
			}
			if msg.HasMore {
				next = msg.ResumeToken
			}
		}
		if err := stream.Err(); err != nil && err != io.EOF {
			t.Fatalf("page stream: %v", err)
		}
		return raws, last, next
	}

	first, last, _ := page(nil)
	if len(first) != pageLimit || last == nil {
		t.Fatalf("page 1 returned %d records, want %d", len(first), pageLimit)
	}

	// The token "data" mints when it coordinates page 1 itself: its own
	// vault's position for the last record shown, the cursor, the frozen
	// window. "coord" sees that vault as remote.
	token := server.ResumeTokenToProto(&query.ResumeToken{
		Positions: []query.MultiVaultPosition{{
			VaultID:  glid.FromBytes(last.Ref.VaultId),
			ChunkID:  chunk.ChunkID(glid.FromBytes(last.Ref.ChunkId)),
			Position: last.Ref.Pos,
		}},
		HighwaterTS: last.IngestTs.AsTime(),
		HighwaterEvent: chunk.EventID{
			IngesterID: glid.FromBytes(last.IngesterId),
			NodeID:     glid.FromBytes(last.NodeId),
			IngestTS:   last.IngestTs.AsTime(),
			IngestSeq:  last.IngestSeq,
		},
		FrozenStart: start,
		FrozenEnd:   end,
	})

	seen := map[string]bool{}
	for _, r := range first {
		seen[r] = true
	}
	for p := 2; token != nil; p++ {
		if p > 10 {
			t.Fatal("pagination did not terminate")
		}
		raws, _, next := page(token)
		for _, r := range raws {
			if seen[r] {
				t.Fatalf("page %d repeated %s", p, r)
			}
			seen[r] = true
		}
		token = next
	}
	if len(seen) != total {
		t.Fatalf("pages covered %d records, want %d", len(seen), total)
	}
}
