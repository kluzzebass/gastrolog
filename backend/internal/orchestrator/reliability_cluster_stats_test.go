package orchestrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/server"
)

// TestOrchRel_GetStatsCountsEachVaultOnceOnEveryNode drives GetStats and
// ListVaults on every node of a four-node cluster holding two vaults at
// different replication factors: vault A is homed on all four nodes, vault B
// on nodes {0,1,2}, and node 3 — the ingest origin — hosts no instance of B.
// Every node must report each vault's records once, and every node must report
// the same figures, node 3 included for the vault it holds no copy of.
func TestOrchRel_GetStatsCountsEachVaultOnceOnEveryNode(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(0, 1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	vA, vB := h.vaults[0], h.vaults[1]
	outsiderID := h.nodeIDs[3]

	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(outsiderID, total, "cluster-stats")
	h.waitSealedRecords(vA, h.nodeIDs[0], total)
	sealedB := h.waitSealedRecords(vB, h.nodeIDs[0], total)

	ctx := context.Background()
	servers := make(map[string]*server.VaultServer, len(h.nodeIDs))
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		servers[id] = server.NewVaultServer(n.orch, h.cfgStore, n.factories, nil, nil, nil, nil, nil, nil, nil, id, nil)
	}

	type figures struct {
		vaults, records, chunks, bytes, recordsA, recordsB int64
	}
	sample := func(id string) (figures, error) {
		resp, err := servers[id].GetStats(ctx, connect.NewRequest(&apiv1.GetStatsRequest{}))
		if err != nil {
			return figures{}, err
		}
		f := figures{
			vaults:  resp.Msg.TotalVaults,
			records: resp.Msg.TotalRecords,
			chunks:  resp.Msg.TotalChunks,
			bytes:   resp.Msg.TotalBytes,
		}
		for _, vs := range resp.Msg.VaultStats {
			switch glid.FromBytes(vs.Id) {
			case vA.id:
				f.recordsA = vs.RecordCount
			case vB.id:
				f.recordsB = vs.RecordCount
			}
		}
		return f, nil
	}

	// Followers apply the seal commands after the leader, so the figures are
	// compared once every node's vault-ctl FSM has caught up.
	var agreed figures
	h.waitProgress("GetStats agrees on every node and counts each vault once", 50*time.Millisecond, func() (string, bool) {
		var lines []string
		done := true
		for i, id := range h.nodeIDs {
			f, err := sample(id)
			if err != nil {
				lines = append(lines, fmt.Sprintf("%s: %v", h.nodes[id].label, err))
				done = false
				continue
			}
			lines = append(lines, fmt.Sprintf("%s: vaults=%d records=%d A=%d B=%d chunks=%d bytes=%d",
				h.nodes[id].label, f.vaults, f.records, f.recordsA, f.recordsB, f.chunks, f.bytes))
			if f.vaults != 2 || f.records != 2*total || f.recordsA != total || f.recordsB != total {
				done = false
			}
			if i == 0 {
				agreed = f
			} else if f != agreed {
				done = false
			}
		}
		return strings.Join(lines, "; "), done
	}, nil)

	if agreed.chunks < 2*int64(len(sealedB)) {
		t.Errorf("TotalChunks = %d, want at least %d (both vaults' sealed chunks)", agreed.chunks, 2*len(sealedB))
	}
	if agreed.bytes <= 0 {
		t.Errorf("TotalBytes = %d, want the logical size of the records", agreed.bytes)
	}

	for _, id := range h.nodeIDs {
		infos, err := servers[id].ListVaults(ctx, connect.NewRequest(&apiv1.ListVaultsRequest{}))
		if err != nil {
			t.Fatalf("ListVaults on %s: %v", h.nodes[id].label, err)
		}
		for _, info := range infos.Msg.Vaults {
			if info.RecordCount != total {
				t.Errorf("ListVaults on %s: vault %s RecordCount = %d, want %d",
					h.nodes[id].label, glid.FromBytes(info.Id), info.RecordCount, total)
			}
		}
	}
}
