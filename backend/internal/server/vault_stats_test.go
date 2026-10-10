package server_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

type fixedPeerVaultStats map[string]*gastrologv1.VaultStats

func (p fixedPeerVaultStats) FindVaultStats(vaultID string) *gastrologv1.VaultStats {
	return p[vaultID]
}

// newStatsServer registers a memory vault holding `records` records and a
// vault this node knows but can read no manifest for — registered with no
// instance, and no vault-ctl FSM in a single-node orchestrator.
func newStatsServer(t *testing.T, records int, peers server.PeerVaultStatsProvider) (*server.VaultServer, glid.GLID, glid.GLID) {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(5)})
	t0 := time.Now()
	for i := range records {
		if _, _, err := s.CM.Append(chunk.Record{IngestTS: t0.Add(time.Duration(i) * time.Second), Raw: []byte("0123456789")}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	held := glid.New()
	orch.RegisterVault(orchestrator.NewVaultFromComponents(held, s.CM, s.IM, s.QE))
	unreadable := glid.New()
	orch.RegisterVault(orchestrator.NewVault(unreadable, nil))
	return server.NewVaultServer(orch, nil, orchestrator.Factories{}, peers, nil, nil, nil, nil, nil, nil, "node-1", nil), held, unreadable
}

func TestGetStatsLeavesOutAVaultWithoutFigures(t *testing.T) {
	t.Parallel()
	srv, held, unreadable := newStatsServer(t, 12, nil)

	resp, err := srv.GetStats(context.Background(), connect.NewRequest(&gastrologv1.GetStatsRequest{}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if resp.Msg.TotalVaults != 1 {
		t.Errorf("TotalVaults = %d, want 1: a vault without figures is not counted as an empty one", resp.Msg.TotalVaults)
	}
	if resp.Msg.TotalRecords != 12 || resp.Msg.TotalChunks != 3 {
		t.Errorf("TotalRecords = %d TotalChunks = %d, want 12 and 3", resp.Msg.TotalRecords, resp.Msg.TotalChunks)
	}
	if resp.Msg.TotalBytes <= 0 {
		t.Errorf("TotalBytes = %d, want the logical size of the records", resp.Msg.TotalBytes)
	}
	for _, vs := range resp.Msg.VaultStats {
		if id := glid.FromBytes(vs.Id); id == unreadable {
			t.Errorf("VaultStats lists vault %s that has no figures on this node", id)
		} else if id != held {
			t.Errorf("VaultStats lists unexpected vault %s", id)
		}
	}

	if _, err := srv.GetStats(context.Background(), connect.NewRequest(&gastrologv1.GetStatsRequest{Vault: unreadable.String()})); err != nil {
		t.Errorf("GetStats(unreadable vault): %v", err)
	}
}

func TestGetStatsFallsBackToAPeerForAVaultWithoutAManifestHere(t *testing.T) {
	t.Parallel()
	peers := fixedPeerVaultStats{}
	srv, _, unreadable := newStatsServer(t, 5, peers)
	peers[unreadable.String()] = &gastrologv1.VaultStats{
		Id:          unreadable.ToProto(),
		RecordCount: 40,
		ChunkCount:  4,
		DataBytes:   400,
	}

	resp, err := srv.GetStats(context.Background(), connect.NewRequest(&gastrologv1.GetStatsRequest{}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if resp.Msg.TotalVaults != 2 || resp.Msg.TotalRecords != 45 || resp.Msg.TotalChunks != 5 {
		t.Errorf("got vaults=%d records=%d chunks=%d, want 2, 45, 5",
			resp.Msg.TotalVaults, resp.Msg.TotalRecords, resp.Msg.TotalChunks)
	}
}
