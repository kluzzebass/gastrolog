package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
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

// stubIndexer is a RemoteIndexer where a node in dead fails with a
// transport error, a node in holders returns index info (locating the
// chunk), and every other node returns not-found — the benign "I don't
// hold it" answer that must NOT count as degradation.
type stubIndexer struct {
	dead    map[string]bool
	holders map[string]bool
}

func (s *stubIndexer) GetIndexes(_ context.Context, nodeID string, _ *gastrologv1.ForwardGetIndexesRequest) (*gastrologv1.ForwardGetIndexesResponse, error) {
	if s.dead[nodeID] {
		return nil, errors.New("connection refused")
	}
	if s.holders[nodeID] {
		return &gastrologv1.ForwardGetIndexesResponse{
			Sealed:  true,
			Indexes: []*gastrologv1.IndexInfo{{Name: "token", Exists: true}},
		}, nil
	}
	return nil, connect.NewError(connect.CodeNotFound, chunk.ErrChunkNotFound)
}

// stubPipelineBacklog is a RemotePipelineBacklogGetter where a node in
// dead fails; every other node returns empty disk counts.
type stubPipelineBacklog struct {
	dead map[string]bool
}

func (s *stubPipelineBacklog) GetPipelineBacklogDisk(_ context.Context, nodeID string, _ *gastrologv1.ForwardGetPipelineBacklogRequest) (*gastrologv1.ForwardGetPipelineBacklogResponse, error) {
	if s.dead[nodeID] {
		return nil, errors.New("connection refused")
	}
	return &gastrologv1.ForwardGetPipelineBacklogResponse{}, nil
}

// stubExplainSearcher is a minimal RemoteSearcher whose Explain fails for
// any node in dead and returns an empty plan otherwise. Only Explain is
// exercised by the Explain fan-out test; the rest satisfy the interface.
type stubExplainSearcher struct {
	dead map[string]bool
}

func (s *stubExplainSearcher) Explain(_ context.Context, nodeID string, _ *gastrologv1.ForwardExplainRequest) (*gastrologv1.ForwardExplainResponse, error) {
	if s.dead[nodeID] {
		return nil, errors.New("connection refused")
	}
	return &gastrologv1.ForwardExplainResponse{}, nil
}

func (s *stubExplainSearcher) Search(_ context.Context, _ string, _ *gastrologv1.ForwardSearchRequest) (*gastrologv1.ForwardSearchResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *stubExplainSearcher) SearchStream(_ context.Context, _ string, _ *gastrologv1.ForwardSearchRequest) (<-chan []*gastrologv1.ExportRecord, *gastrologv1.TableResult, <-chan error, func() []byte, func() []*gastrologv1.HistogramBucket) {
	return nil, nil, nil, nil, nil
}

func (s *stubExplainSearcher) GetContext(_ context.Context, _ string, _ *gastrologv1.ForwardGetContextRequest) (*gastrologv1.ForwardGetContextResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *stubExplainSearcher) Follow(_ context.Context, _ string, _ *gastrologv1.ForwardFollowRequest) (<-chan *gastrologv1.ExportRecord, <-chan error) {
	return nil, nil
}

func (s *stubExplainSearcher) ExportToVault(_ context.Context, _ string, _ *gastrologv1.ForwardExportToVaultRequest) (*gastrologv1.ForwardExportToVaultResponse, error) {
	return nil, errors.New("not implemented")
}

// contributionDeps holds the remote fan-out stubs a contribution test
// wants wired; any may be nil.
type contributionDeps struct {
	lister     server.RemoteChunkLister
	indexer    server.RemoteIndexer
	pipeline   server.RemotePipelineBacklogGetter
	validator  server.RemoteVaultValidator
	reconciler server.RemoteCloudIndexReconciler
}

// newContributionVaultServer builds a single real orchestrator hosting a
// memory vault placed across the local node plus the given remote node
// IDs, wired to the supplied fan-out stubs. Each remote ID is also
// registered as a NodeConfig so cluster-wide fan-outs (pipeline backlog)
// that enumerate ListNodes can reach them. Returns the server and the
// vault ID string for RPC calls.
func newContributionVaultServer(t *testing.T, localID string, remoteIDs []string, deps contributionDeps) (*server.VaultServer, string) {
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

	// Register node configs for local + remotes so the cluster-node
	// enumeration (remoteClusterNodes → ListNodes) used by the pipeline
	// fan-out can see the peers. Node IDs are opaque strings here; parse
	// them as GLIDs where possible, otherwise skip registration (the
	// placement-based fan-outs don't need ListNodes).
	putNode := func(id string) {
		gid, perr := glid.ParseAny(id)
		if perr != nil {
			return
		}
		_ = cfgStore.PutNode(ctx, system.NodeConfig{ID: gid, Name: id})
	}
	putNode(localID)
	for _, rid := range remoteIDs {
		putNode(rid)
	}

	placements := []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID(localID), Leader: true},
	}
	for _, rid := range remoteIDs {
		placements = append(placements, system.VaultPlacement{StorageID: system.SyntheticStorageID(rid)})
	}
	if err := cfgStore.PutVault(ctx, system.VaultConfig{
		ID:   vaultID,
		Name: "vault-" + localID,
		Type: system.VaultTypeMemory,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := cfgStore.SetVaultPlacements(ctx, vaultID, placements); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	vs := server.NewVaultServer(orch, cfgStore, orchestrator.Factories{}, nil, deps.lister, deps.pipeline, nil, deps.indexer, deps.validator, deps.reconciler, localID, logger)
	return vs, vaultID.String()
}

// TestListChunks_PartialFanOutNamesDeadPeer: when a hosting peer fails
// during the ListChunks cross-node fan-out, the merged response must carry
// a ContributionReport naming that peer, so the inspector reads the chunk
// list as visibly partial instead of silently dropping the node.
func TestListChunks_PartialFanOutNamesDeadPeer(t *testing.T) {
	t.Parallel()

	const localID, aliveID, deadID = "node-local", "node-alive", "node-dead"
	lister := &stubChunkLister{dead: map[string]bool{deadID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{aliveID, deadID}, contributionDeps{lister: lister})

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
	vs, vaultID := newContributionVaultServer(t, localID, []string{aliveID}, contributionDeps{lister: lister})

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
	vs, vaultID := newContributionVaultServer(t, localID, []string{deadID}, contributionDeps{lister: lister})

	resp, err := vs.ListChunks(context.Background(),
		connect.NewRequest(&gastrologv1.ListChunksRequest{Vault: vaultID, ActiveOnly: true}))
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if report := resp.Msg.GetContributionReport(); report != nil {
		t.Errorf("ContributionReport = %+v, want nil (active-only never fans out)", report)
	}
}

// bogusChunkReq builds a GetIndexes request for a chunk that no local
// vault holds, forcing the cross-node fan-out path.
func bogusChunkReq(vaultID string) *connect.Request[gastrologv1.GetIndexesRequest] {
	cid := glid.New()
	return connect.NewRequest(&gastrologv1.GetIndexesRequest{
		Vault:   vaultID,
		ChunkId: cid.ToProto(),
	})
}

// TestGetIndexes_NotFoundPeersDoNotDegrade: peers that simply don't hold
// the chunk answer not-found — a benign non-answer. When a holder is
// found, the response carries its indexes and NO contribution report, even
// though other peers said not-found.
func TestGetIndexes_NotFoundPeersDoNotDegrade(t *testing.T) {
	t.Parallel()

	const localID, holderID, otherID = "node-local", "node-holder", "node-other"
	indexer := &stubIndexer{holders: map[string]bool{holderID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{holderID, otherID}, contributionDeps{indexer: indexer})

	resp, err := vs.GetIndexes(context.Background(), bogusChunkReq(vaultID))
	if err != nil {
		t.Fatalf("GetIndexes: %v", err)
	}
	if len(resp.Msg.Indexes) == 0 {
		t.Fatalf("Indexes empty, want the holder's index info")
	}
	if report := resp.Msg.GetContributionReport(); report != nil {
		t.Errorf("ContributionReport = %+v, want nil (chunk found; not-found peers are benign)", report)
	}
}

// TestGetIndexes_UncertainNotFoundCarriesReport pins that when the chunk
// is found nowhere AND a hosting peer was unreachable, the "not found"
// verdict is uncertain: the handler returns an empty response carrying
// the contribution report instead of a hard not-found error.
func TestGetIndexes_UncertainNotFoundCarriesReport(t *testing.T) {
	t.Parallel()

	const localID, deadID, otherID = "node-local", "node-dead", "node-other"
	indexer := &stubIndexer{dead: map[string]bool{deadID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{deadID, otherID}, contributionDeps{indexer: indexer})

	resp, err := vs.GetIndexes(context.Background(), bogusChunkReq(vaultID))
	if err != nil {
		t.Fatalf("GetIndexes: unexpected error (want empty-with-report, not a hard not-found): %v", err)
	}
	if len(resp.Msg.Indexes) != 0 {
		t.Errorf("Indexes = %d entries, want 0 (chunk located nowhere)", len(resp.Msg.Indexes))
	}
	report := resp.Msg.GetContributionReport()
	if report == nil || len(report.Degraded) != 1 || report.Degraded[0].NodeId != deadID {
		t.Fatalf("ContributionReport = %+v, want the dead peer named", report)
	}
}

// TestGetIndexes_CleanNotFoundWhenAllAnswer pins that when the chunk is
// found nowhere and every peer answered (none degraded), the handler
// keeps returning an authoritative not-found error — no false partial.
func TestGetIndexes_CleanNotFoundWhenAllAnswer(t *testing.T) {
	t.Parallel()

	const localID, aID, bID = "node-local", "node-a", "node-b"
	indexer := &stubIndexer{} // nobody holds it, nobody dead
	vs, vaultID := newContributionVaultServer(t, localID, []string{aID, bID}, contributionDeps{indexer: indexer})

	_, err := vs.GetIndexes(context.Background(), bogusChunkReq(vaultID))
	if err == nil {
		t.Fatal("GetIndexes: expected a not-found error, got nil")
	}
	// mapVaultError renders chunk-not-found via its message (asserted the
	// same way by TestMultiNode_GetIndexesNotFoundAnywhere); the point here
	// is that a clean not-found stays a hard error, never a silent
	// empty-with-report.
	if !strings.Contains(err.Error(), "chunk not found") {
		t.Errorf("error = %v, want a chunk-not-found error", err)
	}
}

// TestGetPipelineBacklog_PartialFanOutCarriesReport: when a peer fails
// during the pipeline-backlog disk fan-out, the cluster-wide segment
// totals omit that node, so the response carries a contribution report
// naming it. Node IDs must be GLIDs here — the pipeline fan-out enumerates
// ListNodes, not vault placements.
func TestGetPipelineBacklog_PartialFanOutCarriesReport(t *testing.T) {
	t.Parallel()

	localID := glid.New().String()
	deadID := glid.New().String()
	pipeline := &stubPipelineBacklog{dead: map[string]bool{deadID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{deadID}, contributionDeps{pipeline: pipeline})

	resp, err := vs.GetPipelineBacklog(context.Background(),
		connect.NewRequest(&gastrologv1.GetPipelineBacklogRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("GetPipelineBacklog: %v", err)
	}
	report := resp.Msg.GetContributionReport()
	if report == nil || len(report.Degraded) != 1 || report.Degraded[0].NodeId != deadID {
		t.Fatalf("ContributionReport = %+v, want the dead peer named", report)
	}
}

// TestGetPipelineBacklog_FullFanOutOmitsReport pins the happy path: every
// peer contributing its segment counts leaves the response report-free.
func TestGetPipelineBacklog_FullFanOutOmitsReport(t *testing.T) {
	t.Parallel()

	localID := glid.New().String()
	aliveID := glid.New().String()
	pipeline := &stubPipelineBacklog{dead: map[string]bool{}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{aliveID}, contributionDeps{pipeline: pipeline})

	resp, err := vs.GetPipelineBacklog(context.Background(),
		connect.NewRequest(&gastrologv1.GetPipelineBacklogRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("GetPipelineBacklog: %v", err)
	}
	if report := resp.Msg.GetContributionReport(); report != nil {
		t.Errorf("ContributionReport = %+v, want nil (all peers contributed)", report)
	}
}

// TestExplain_PartialFanOutCarriesReport: a vault whose leader is a remote
// node is fanned out to that node; when the peer fails, the merged plan
// omits its chunks and the response carries a contribution report naming
// it.
func TestExplain_PartialFanOutCarriesReport(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const localID, deadID = "node-local", "node-dead"

	orch, err := orchestrator.New(orchestrator.Config{
		LocalNodeID:  localID,
		SystemLoader: sysmem.NewStore(),
		SegmentsDir:  filepath.Join(t.TempDir(), "segments"),
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Stop() })

	cfgStore := sysmem.NewStore()
	// A vault led by the dead remote node — the local orch never registers
	// it, so it is a purely-remote vault the Explain fan-out must reach.
	remoteVaultID := glid.New()
	placements := []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID(deadID), Leader: true},
	}
	if err := cfgStore.PutVault(ctx, system.VaultConfig{
		ID:   remoteVaultID,
		Name: "vault-remote",
		Type: system.VaultTypeMemory,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := cfgStore.SetVaultPlacements(ctx, remoteVaultID, placements); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	searcher := &stubExplainSearcher{dead: map[string]bool{deadID: true}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	qs := server.NewQueryServer(orch, cfgStore, searcher, localID, nil, nil, 0, 0, 0, logger)

	resp, err := qs.Explain(ctx, connect.NewRequest(&gastrologv1.ExplainRequest{
		Query: &gastrologv1.Query{Expression: ""},
	}))
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	report := resp.Msg.GetContributionReport()
	if report == nil || len(report.Degraded) != 1 || report.Degraded[0].NodeId != deadID {
		t.Fatalf("ContributionReport = %+v, want the dead peer named", report)
	}
}
