package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// stubQuerySearcher is a RemoteSearcher for the collect* fan-out tests.
// SearchStream returns a successful empty stream for every vault; Search
// fails for any node in deadSearch and otherwise returns a one-row table.
type stubQuerySearcher struct {
	deadSearch map[string]bool
}

func (s *stubQuerySearcher) Search(_ context.Context, nodeID string, _ *apiv1.ForwardSearchRequest) (*apiv1.ForwardSearchResponse, error) {
	if s.deadSearch[nodeID] {
		return nil, errors.New("connection refused")
	}
	return &apiv1.ForwardSearchResponse{
		TableResult: &apiv1.TableResult{
			Columns: []string{"count"},
			Rows:    []*apiv1.TableRow{{Values: []string{"1"}}},
		},
	}, nil
}

func (s *stubQuerySearcher) SearchStream(_ context.Context, _ string, _ *apiv1.ForwardSearchRequest) (
	<-chan []*apiv1.ExportRecord, *apiv1.TableResult, <-chan error, func() []byte, func() []*apiv1.HistogramBucket,
) {
	rc := make(chan []*apiv1.ExportRecord)
	close(rc)
	ec := make(chan error)
	close(ec)
	return rc, nil, ec, func() []byte { return nil }, func() []*apiv1.HistogramBucket { return nil }
}

func (s *stubQuerySearcher) GetContext(_ context.Context, _ string, _ *apiv1.ForwardGetContextRequest) (*apiv1.ForwardGetContextResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *stubQuerySearcher) Explain(_ context.Context, _ string, _ *apiv1.ForwardExplainRequest) (*apiv1.ForwardExplainResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *stubQuerySearcher) Follow(_ context.Context, _ string, _ *apiv1.ForwardFollowRequest) (<-chan *apiv1.ExportRecord, <-chan error) {
	return nil, nil
}

func (s *stubQuerySearcher) ExportToVault(_ context.Context, _ string, _ *apiv1.ForwardExportToVaultRequest) (*apiv1.ForwardExportToVaultResponse, error) {
	return nil, errors.New("not implemented")
}

// newQueryServerWithRemoteVaults builds a QueryServer whose local orch
// leads no vault, with the given vault IDs each placed (leader) on a
// distinct remote node so the collect* fan-outs treat them as remote.
// Returns the server and the remote node ID for each vault (index-aligned).
func newQueryServerWithRemoteVaults(t *testing.T, searcher RemoteSearcher, vaultIDs []glid.GLID) (*QueryServer, []string) {
	t.Helper()
	ctx := context.Background()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: "node-local"})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Stop() })

	nodeIDs := make([]string, len(vaultIDs))
	for i, vid := range vaultIDs {
		nodeID := "node-remote-" + string(rune('a'+i))
		nodeIDs[i] = nodeID
		placements := []system.VaultPlacement{
			{StorageID: system.SyntheticStorageID(nodeID), Leader: true},
		}
		if err := cfgStore.PutVault(ctx, system.VaultConfig{
			ID:   vid,
			Name: "vault-" + nodeID,
			Type: system.VaultTypeMemory,
		}); err != nil {
			t.Fatalf("PutVault: %v", err)
		}
		if err := cfgStore.SetVaultPlacements(ctx, vid, placements); err != nil {
			t.Fatalf("SetVaultPlacements: %v", err)
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	qs := NewQueryServer(orch, cfgStore, searcher, "node-local", nil, nil, 0, 0, 0, logger)
	return qs, nodeIDs
}

func allVaultsQuery() query.Query {
	// Empty filter — selects every vault (local + remote).
	return query.Query{}
}

// TestCollectRemote_ReportsContributors pins gastrolog-20lrg(a): a merged
// search records which remote vaults contributed. Under fail-on-remote-
// failure the fanned-out set IS the contributor set, so a successful
// two-vault fan-out returns both vault IDs.
func TestCollectRemote_ReportsContributors(t *testing.T) {
	t.Parallel()

	v1, v2 := glid.New(), glid.New()
	qs, _ := newQueryServerWithRemoteVaults(t, &stubQuerySearcher{}, []glid.GLID{v1, v2})

	_, _, contributors := qs.collectRemote(context.Background(), allVaultsQuery(), nil)
	got := map[glid.GLID]bool{}
	for _, id := range contributors {
		got[id] = true
	}
	if len(contributors) != 2 || !got[v1] || !got[v2] {
		t.Fatalf("contributors = %v, want both %s and %s", contributors, v1, v2)
	}
}

// TestCollectRemote_NoRemoteVaultsEmptyContributors pins that a purely
// local search (no remote vaults in scope) reports no contributors — the
// happy-path signal the UI shows nothing for.
func TestCollectRemote_NoRemoteVaultsEmptyContributors(t *testing.T) {
	t.Parallel()

	qs, _ := newQueryServerWithRemoteVaults(t, &stubQuerySearcher{}, nil)
	_, _, contributors := qs.collectRemote(context.Background(), allVaultsQuery(), nil)
	if len(contributors) != 0 {
		t.Fatalf("contributors = %v, want empty (no remote vaults)", contributors)
	}
}

func statsPipeline(t *testing.T) *querylang.Pipeline {
	t.Helper()
	pipeline, err := querylang.ParsePipeline("| stats count")
	if err != nil {
		t.Fatalf("parse pipeline: %v", err)
	}
	if pipeline == nil || len(pipeline.Pipes) == 0 {
		t.Fatal("expected a stats pipeline from '| stats count'")
	}
	return pipeline
}

// TestCollectRemotePipeline_FailsOnRemoteFailure pins gastrolog-20lrg(b):
// a failed remote vault fails the whole pipeline query (fail-hard), never
// a silent partial aggregate.
func TestCollectRemotePipeline_FailsOnRemoteFailure(t *testing.T) {
	t.Parallel()

	v1, v2 := glid.New(), glid.New()
	searcher := &stubQuerySearcher{}
	qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{v1, v2})
	// Kill the second vault's node.
	searcher.deadSearch = map[string]bool{nodeIDs[1]: true}

	_, err := qs.collectRemotePipeline(context.Background(), allVaultsQuery(), statsPipeline(t))
	if err == nil {
		t.Fatal("collectRemotePipeline: expected an error when a remote vault fails, got nil")
	}
}

// TestCollectRemotePipeline_HappyPathMerges pins that when every remote
// vault answers, the pipeline fan-out returns their table results and no
// error.
func TestCollectRemotePipeline_HappyPathMerges(t *testing.T) {
	t.Parallel()

	v1, v2 := glid.New(), glid.New()
	qs, _ := newQueryServerWithRemoteVaults(t, &stubQuerySearcher{}, []glid.GLID{v1, v2})

	results, err := qs.collectRemotePipeline(context.Background(), allVaultsQuery(), statsPipeline(t))
	if err != nil {
		t.Fatalf("collectRemotePipeline: unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d tables, want 2 (one per remote vault)", len(results))
	}
}
