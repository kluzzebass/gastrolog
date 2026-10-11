package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memjson "gastrolog/internal/index/memory/json"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

// unmaterializedChunks reports its active chunk as not locally materialized,
// the shape an open pipeline chunk has, so order=source_ts sorts it in memory.
type unmaterializedChunks struct{ chunk.ChunkManager }

func (unmaterializedChunks) HasLocalContent(chunk.ChunkID) bool { return false }

// newSortOverflowEngine returns an engine over one unindexed chunk of n
// records whose source order differs from its ingest order, with its
// per-query memory ceiling lowered to limit.
func newSortOverflowEngine(t *testing.T, n int, limit int64) *query.Engine {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(1_000_000)})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingester := glid.New()
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Second)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts,
			WriteTS:  ts,
			SourceTS: t0.Add(time.Duration((i*7)%n) * time.Second),
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ts, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Raw:      fmt.Appendf(nil, "r%d", i),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	jsonIdx := memjson.NewIndexer(cm)
	im := indexmem.NewManagerWithJSON(
		[]index.Indexer{tokIdx, attrIdx, kvIdx, jsonIdx}, tokIdx, attrIdx, kvIdx, jsonIdx, nil)
	e := query.New(unmaterializedChunks{cm}, im, nil)
	e.SetMemoryLimit(limit)
	return e
}

// engineSwapSearch is the production Search handler dispatching to an engine
// the test supplies.
type engineSwapSearch struct {
	*QueryServer
	eng *query.Engine
}

func (h engineSwapSearch) Search(ctx context.Context, req *connect.Request[apiv1.SearchRequest], stream *connect.ServerStream[apiv1.SearchResponse]) error {
	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return errInvalidArg(err)
	}
	return h.dispatchSearch(ctx, h.eng, q, pipeline, req.Msg.ResumeToken, time.Now(), stream)
}

func searchCode(t *testing.T, eng *query.Engine, expr string) (int, error) {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	qs := NewQueryServer(orch, nil, nil, "node-1", nil, nil, 0, 0, 0, nil)
	path, handler := gastrologv1connect.NewQueryServiceHandler(engineSwapSearch{QueryServer: qs, eng: eng})
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := gastrologv1connect.NewQueryServiceClient(srv.Client(), srv.URL)

	stream, err := client.Search(context.Background(), connect.NewRequest(&apiv1.SearchRequest{
		Query: &apiv1.Query{Expression: expr},
	}))
	if err != nil {
		return 0, err
	}
	n := 0
	for stream.Receive() {
		n += len(stream.Msg().GetRecords())
	}
	return n, stream.Err()
}

// A plain order=source_ts search whose sort-key buffer outgrows the memory
// budget reaches the client as ResourceExhausted.
func TestSearchMemoryLimitSurfacesAsResourceExhausted(t *testing.T) {
	const n = 200

	// Premise: with room for the sort keys the same search succeeds.
	if got, err := searchCode(t, newSortOverflowEngine(t, n, 64*n*2), "order=source_ts"); err != nil || got != n {
		t.Fatalf("premise: search with room returned %d records, err %v; want %d", got, err, n)
	}

	_, err := searchCode(t, newSortOverflowEngine(t, n, 64*n/2), "order=source_ts")
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatal("expected the search to fail against its memory budget")
	}
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("code = %v, want %v (err: %v)", got, connect.CodeResourceExhausted, err)
	}
}

// mapSearchError is the mapping every searchDirect error passes through, for
// local and remote iterators alike; followError is the follow stream's. A
// memory overflow here or on the peer that ran its share is ResourceExhausted;
// anything else is not.
func TestQueryErrorMappingReportsMemoryOverflowAsResourceExhausted(t *testing.T) {
	memErr := &query.MemoryLimitError{Consumer: "sort buffer", Limit: query.MaxQueryMemoryBytes}
	cases := []struct {
		name string
		err  error
		want connect.Code
	}{
		{"local MemoryLimitError", fmt.Errorf("vault v chunk c: %w", memErr), connect.CodeResourceExhausted},
		{"peer ResourceExhausted status", fmt.Errorf("search stream from node-2: %w", status.Error(codes.ResourceExhausted, memErr.Error())), connect.CodeResourceExhausted},
		{"peer Internal status", fmt.Errorf("search stream from node-2: %w", status.Error(codes.Internal, "disk on fire")), connect.CodeInternal},
		{"local other failure", errors.New("disk on fire"), connect.CodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := connect.CodeOf(mapSearchError(tc.err)); got != tc.want {
				t.Errorf("mapSearchError: code = %v, want %v", got, tc.want)
			}
			if got := connect.CodeOf(followError(tc.err)); got != tc.want {
				t.Errorf("followError: code = %v, want %v", got, tc.want)
			}
			if got := connect.CodeOf(errQueryExecution(tc.err)); got != tc.want {
				t.Errorf("errQueryExecution: code = %v, want %v", got, tc.want)
			}
		})
	}
}
