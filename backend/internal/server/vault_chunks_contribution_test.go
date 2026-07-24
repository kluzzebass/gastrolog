package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// stubChunkLister is a RemoteChunkLister that succeeds for every node
// except those named in dead, which return a transport error — modelling
// a paused or partitioned peer during a ListChunks fan-out.
type stubChunkLister struct {
	dead map[string]bool
}

func (s *stubChunkLister) ListChunks(_ context.Context, nodeID string, _ *gastrologv1.ForwardListChunksRequest) (*gastrologv1.ForwardListChunksResponse, error) {
	if s.dead[nodeID] {
		return nil, errors.New("connection refused")
	}
	return &gastrologv1.ForwardListChunksResponse{}, nil
}

// newContributionVaultServer builds a single real orchestrator hosting a
// memory vault placed across the local node plus the given remote node
// IDs, wired to lister for the cross-node ListChunks fan-out. Returns the
// server and the vault ID string for RPC calls.
func newContributionVaultServer(t *testing.T, localID string, remoteIDs []string, lister server.RemoteChunkLister) (*server.VaultServer, string) {
	t.Helper()
	ctx := context.Background()
	cfgStore := sysmem.NewStore()

	orch, err := orchestrator.New(orchestrator.Config{
		LocalNodeID:  localID,
		SystemLoader: cfgStore,
		SegmentsDir:  filepath.Join(t.TempDir(), "segments"),
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Stop() })

	v := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(10000)})
	vaultID := glid.New()
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, v.CM, v.IM, v.QE))

	placements := []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID(localID), Leader: true},
	}
	for _, rid := range remoteIDs {
		placements = append(placements, system.VaultPlacement{StorageID: system.SyntheticStorageID(rid)})
	}
	if err := cfgStore.PutVault(ctx, system.VaultConfig{
		ID:         vaultID,
		Name:       "vault-" + localID,
		Type:       system.VaultTypeMemory,
		Placements: placements,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := cfgStore.SetVaultPlacements(ctx, vaultID, placements); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	vs := server.NewVaultServer(orch, cfgStore, orchestrator.Factories{}, nil, lister, nil, nil, nil, localID, logger)
	return vs, vaultID.String()
}

// TestListChunks_PartialFanOutNamesDeadPeer pins gastrolog-1ic07: when a
// hosting peer fails during the ListChunks cross-node fan-out, the merged
// response must carry a ContributionReport naming that peer, so the
// inspector reads the chunk list as visibly partial instead of silently
// dropping the node.
func TestListChunks_PartialFanOutNamesDeadPeer(t *testing.T) {
	t.Parallel()

	const localID, aliveID, deadID = "node-local", "node-alive", "node-dead"
	lister := &stubChunkLister{dead: map[string]bool{deadID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{aliveID, deadID}, lister)

	resp, err := vs.ListChunks(context.Background(),
		connect.NewRequest(&gastrologv1.ListChunksRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}

	report := resp.Msg.GetContributionReport()
	if report == nil {
		t.Fatalf("ContributionReport = nil, want the dead peer named")
	}
	if len(report.Degraded) != 1 {
		t.Fatalf("Degraded = %d entries, want 1 (only the dead peer)", len(report.Degraded))
	}
	if report.Degraded[0].NodeId != deadID {
		t.Errorf("degraded node = %q, want %q", report.Degraded[0].NodeId, deadID)
	}
	if report.Degraded[0].Reason != "connection refused" {
		t.Errorf("degraded reason = %q, want %q", report.Degraded[0].Reason, "connection refused")
	}
}

// TestListChunks_FullFanOutOmitsReport pins the happy path: when every
// hosting peer contributes, the response omits the ContributionReport
// entirely so the inspector stays quiet (quiet-until-needed).
func TestListChunks_FullFanOutOmitsReport(t *testing.T) {
	t.Parallel()

	const localID, aliveID = "node-local", "node-alive"
	lister := &stubChunkLister{dead: map[string]bool{}} // nobody dead
	vs, vaultID := newContributionVaultServer(t, localID, []string{aliveID}, lister)

	resp, err := vs.ListChunks(context.Background(),
		connect.NewRequest(&gastrologv1.ListChunksRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if report := resp.Msg.GetContributionReport(); report != nil {
		t.Errorf("ContributionReport = %+v, want nil (all peers contributed)", report)
	}
}

// TestListChunks_ActiveOnlySkipsFanOut pins that the lightweight
// active-only poll never fans out, so it can never produce a degraded
// report even when a placement peer is dead — the report is a property
// of the full cross-node merge only.
func TestListChunks_ActiveOnlySkipsFanOut(t *testing.T) {
	t.Parallel()

	const localID, deadID = "node-local", "node-dead"
	lister := &stubChunkLister{dead: map[string]bool{deadID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{deadID}, lister)

	resp, err := vs.ListChunks(context.Background(),
		connect.NewRequest(&gastrologv1.ListChunksRequest{Vault: vaultID, ActiveOnly: true}))
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if report := resp.Msg.GetContributionReport(); report != nil {
		t.Errorf("ContributionReport = %+v, want nil (active-only never fans out)", report)
	}
}
