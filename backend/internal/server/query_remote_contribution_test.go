package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// stubQuerySearcher is a RemoteSearcher for the collect* fan-out tests. By
// default SearchStream returns a successful empty stream for every vault and
// Search a one-row table, or a one-bucket histogram when asked for one. Each
// map names nodes that answer otherwise.
type stubQuerySearcher struct {
	// deadSearch nodes fail Search as an unreachable peer does.
	deadSearch map[string]bool
	// deadStream nodes end their record stream with that failure.
	deadStream map[string]bool
	// notReady nodes answer every request that the vault is not ready.
	notReady map[string]bool
	// streamRecords nodes stream this many records before their answer ends.
	streamRecords map[string]int
	// notReadyAfterRecords nodes end their record stream, after its records,
	// with a not-ready answer.
	notReadyAfterRecords map[string]bool
}

func stubNotReady(nodeID string) error {
	return fmt.Errorf("search stream from %s: %w", nodeID, orchestrator.ErrVaultNotReady)
}

func (s *stubQuerySearcher) Search(_ context.Context, nodeID string, req *apiv1.ForwardSearchRequest) (*apiv1.ForwardSearchResponse, error) {
	if s.notReady[nodeID] {
		return nil, stubNotReady(nodeID)
	}
	if s.deadSearch[nodeID] {
		return nil, errors.New("connection refused")
	}
	if req.GetHistogramGrid() != nil {
		return &apiv1.ForwardSearchResponse{
			Histogram: []*apiv1.HistogramBucket{{TimestampMs: req.GetHistogramGrid().GetStartNanos() / 1e6, Count: 1}},
		}, nil
	}
	return &apiv1.ForwardSearchResponse{
		TableResult: &apiv1.TableResult{
			Columns: []string{"count"},
			Rows:    []*apiv1.TableRow{{Values: []string{"1"}}},
		},
	}, nil
}

func (s *stubQuerySearcher) SearchStream(_ context.Context, nodeID string, _ *apiv1.ForwardSearchRequest) (
	<-chan []*apiv1.ExportRecord, *apiv1.TableResult, <-chan error, func() []byte,
) {
	rc := make(chan []*apiv1.ExportRecord, 1)
	ec := make(chan error, 1)
	if n := s.streamRecords[nodeID]; n > 0 && !s.notReady[nodeID] {
		batch := make([]*apiv1.ExportRecord, n)
		for i := range batch {
			batch[i] = &apiv1.ExportRecord{Raw: fmt.Appendf(nil, "%s-%d", nodeID, i)}
		}
		rc <- batch
	}
	switch {
	case s.notReady[nodeID], s.notReadyAfterRecords[nodeID]:
		ec <- stubNotReady(nodeID)
	case s.deadStream[nodeID]:
		ec <- fmt.Errorf("search stream from %s: connection refused", nodeID)
	}
	close(rc)
	close(ec)
	return rc, nil, ec, func() []byte { return nil }
}

func (s *stubQuerySearcher) ListChunks(_ context.Context, _ string, _ *apiv1.ForwardListChunksRequest) (*apiv1.ForwardListChunksResponse, error) {
	return &apiv1.ForwardListChunksResponse{}, nil
}

func (s *stubQuerySearcher) GetContext(_ context.Context, nodeID string, _ *apiv1.ForwardGetContextRequest) (*apiv1.ForwardGetContextResponse, error) {
	if s.notReady[nodeID] {
		return nil, stubNotReady(nodeID)
	}
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

// drainRemote runs a collectRemote fan-out to its end and returns the records
// it merged, the contributors it reports once drained, and the first error it
// yielded.
func drainRemote(t *testing.T, qs *QueryServer, q query.Query) (records int, contributors []glid.GLID, err error) {
	t.Helper()
	it, contributorsFn := qs.collectRemote(context.Background(), q)
	if it != nil {
		for _, recErr := range it {
			if recErr != nil {
				err = recErr
				break
			}
			records++
		}
	}
	if contributorsFn != nil {
		contributors = contributorsFn()
	}
	return records, contributors, err
}

// TestCollectRemote_ReportsContributors pins that a merged search records
// which remote vaults contributed. Every vault answering, a two-vault fan-out
// returns both vault IDs.
func TestCollectRemote_ReportsContributors(t *testing.T) {
	t.Parallel()

	v1, v2 := glid.New(), glid.New()
	qs, _ := newQueryServerWithRemoteVaults(t, &stubQuerySearcher{}, []glid.GLID{v1, v2})

	_, contributors, err := drainRemote(t, qs, allVaultsQuery())
	if err != nil {
		t.Fatalf("remote stream: %v", err)
	}
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
	_, contributors, _ := drainRemote(t, qs, allVaultsQuery())
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

// TestCollectRemotePipeline_FailsOnRemoteFailure pins that a failed remote
// vault fails the whole pipeline query (fail-hard), never a silent partial
// aggregate.
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

// A remote vault whose leader answers that it is not ready is left out of a
// merged search, as a vault the coordinator leads is while not ready: the
// other vaults' records arrive, no error is raised, and the vault is not
// reported as a contributor.
func TestCollectRemote_LeavesOutNotReadyVault(t *testing.T) {
	t.Parallel()

	v1, v2, v3 := glid.New(), glid.New(), glid.New()
	searcher := &stubQuerySearcher{}
	qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{v1, v2, v3})
	searcher.streamRecords = map[string]int{nodeIDs[0]: 2, nodeIDs[1]: 2, nodeIDs[2]: 2}
	searcher.notReady = map[string]bool{nodeIDs[1]: true}

	records, contributors, err := drainRemote(t, qs, allVaultsQuery())
	if err != nil {
		t.Fatalf("remote stream: %v", err)
	}
	if records != 4 {
		t.Errorf("records = %d, want 4 (the two ready vaults' records)", records)
	}
	if want := sortedGLIDs(v1, v3); !slices.Equal(contributors, want) {
		t.Errorf("contributors = %v, want %v (the not-ready vault left out)", contributors, want)
	}
}

// With every remote vault not ready the merge yields nothing and no error,
// and reports no contributor.
func TestCollectRemote_EveryVaultNotReady(t *testing.T) {
	t.Parallel()

	v1, v2 := glid.New(), glid.New()
	searcher := &stubQuerySearcher{}
	qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{v1, v2})
	searcher.notReady = map[string]bool{nodeIDs[0]: true, nodeIDs[1]: true}

	records, contributors, err := drainRemote(t, qs, allVaultsQuery())
	if err != nil {
		t.Fatalf("remote stream: %v", err)
	}
	if records != 0 || len(contributors) != 0 {
		t.Errorf("records = %d, contributors = %v; want none of either", records, contributors)
	}
}

// A not-ready answer that ends a stream which already delivered records
// leaves that vault's share incomplete, so it fails the search.
func TestCollectRemote_NotReadyAfterRecordsFails(t *testing.T) {
	t.Parallel()

	v1, v2 := glid.New(), glid.New()
	searcher := &stubQuerySearcher{}
	qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{v1, v2})
	searcher.streamRecords = map[string]int{nodeIDs[0]: 2}
	searcher.notReadyAfterRecords = map[string]bool{nodeIDs[0]: true}

	_, _, err := drainRemote(t, qs, allVaultsQuery())
	if !errors.Is(err, orchestrator.ErrVaultNotReady) {
		t.Fatalf("err = %v, want the vault's not-ready failure", err)
	}
}

// Leaving a not-ready vault out never hides a real failure of another vault.
func TestCollectRemote_RealFailureBesideNotReadyFails(t *testing.T) {
	t.Parallel()

	v1, v2 := glid.New(), glid.New()
	searcher := &stubQuerySearcher{}
	qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{v1, v2})
	searcher.notReady = map[string]bool{nodeIDs[0]: true}
	searcher.deadStream = map[string]bool{nodeIDs[1]: true}

	_, _, err := drainRemote(t, qs, allVaultsQuery())
	if err == nil || errors.Is(err, orchestrator.ErrVaultNotReady) {
		t.Fatalf("err = %v, want the other vault's stream failure", err)
	}
}

// A pipeline leaves a not-ready remote vault's table out, still fails on a
// real remote failure beside it, and with every remote vault not ready
// answers from the coordinator's own share alone.
func TestCollectRemotePipeline_LeavesOutNotReadyVault(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		notReady   []int
		dead       []int
		wantTables int
		wantErr    bool
	}{
		{"one not ready", []int{1}, nil, 1, false},
		{"every vault not ready", []int{0, 1}, nil, 0, false},
		{"not ready beside a failure", []int{0}, []int{1}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			searcher := &stubQuerySearcher{notReady: map[string]bool{}, deadSearch: map[string]bool{}}
			qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{glid.New(), glid.New()})
			for _, i := range tc.notReady {
				searcher.notReady[nodeIDs[i]] = true
			}
			for _, i := range tc.dead {
				searcher.deadSearch[nodeIDs[i]] = true
			}

			results, err := qs.collectRemotePipeline(context.Background(), allVaultsQuery(), statsPipeline(t))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if len(results) != tc.wantTables {
				t.Errorf("tables = %d, want %d", len(results), tc.wantTables)
			}
		})
	}
}

// A histogram counts a not-ready remote vault as adding nothing, and still
// shows none when another remote vault fails to answer.
func TestCollectRemoteHistogram_LeavesOutNotReadyVault(t *testing.T) {
	t.Parallel()

	grid := &apiv1.HistogramGrid{StartNanos: 1_000_000_000, EndNanos: 61_000_000_000, Buckets: 50}
	for _, tc := range []struct {
		name      string
		notReady  []int
		dead      []int
		wantOK    bool
		wantCount int64
	}{
		{"one not ready", []int{1}, nil, true, 1},
		{"every vault not ready", []int{0, 1}, nil, true, 0},
		{"not ready beside a failure", []int{0}, []int{1}, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			searcher := &stubQuerySearcher{notReady: map[string]bool{}, deadSearch: map[string]bool{}}
			qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{glid.New(), glid.New()})
			for _, i := range tc.notReady {
				searcher.notReady[nodeIDs[i]] = true
			}
			for _, i := range tc.dead {
				searcher.deadSearch[nodeIDs[i]] = true
			}

			q := allVaultsQuery()
			buckets, ok := qs.collectRemoteHistogram(context.Background(), q, grid, qs.remoteHistogramVaults(context.Background(), q))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			var count int64
			for _, b := range buckets {
				count += b.GetCount()
			}
			if count != tc.wantCount {
				t.Errorf("count = %d, want %d", count, tc.wantCount)
			}
		})
	}
}

func sortedGLIDs(ids ...glid.GLID) []glid.GLID {
	return slices.SortedFunc(slices.Values(ids), func(a, b glid.GLID) int { return strings.Compare(a.String(), b.String()) })
}

// Context around a record in a vault not ready on its leader is unavailable
// for now — the answer a vault not ready on this node gets from the vault
// RPCs — never an internal fault.
func TestGetContext_AnchorInNotReadyRemoteVaultIsUnavailable(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	searcher := &stubQuerySearcher{}
	qs, nodeIDs := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{vaultID})
	searcher.notReady = map[string]bool{nodeIDs[0]: true}

	_, err := qs.GetContext(context.Background(), connect.NewRequest(&apiv1.GetContextRequest{
		Ref: &apiv1.RecordRef{VaultId: vaultID.ToProto(), ChunkId: glid.New().ToProto()},
	}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("code = %v, want %v (err: %v)", got, connect.CodeUnavailable, err)
	}
}
