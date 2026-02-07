package server_test

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
)

// embeddedTransport routes requests directly to an http.Handler using pipes
// for streaming support. This mirrors the implementation in repl/client_embedded.go.
type embeddedTransport struct {
	handler http.Handler
}

func (t *embeddedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	pr, pw := io.Pipe()
	rw := &pipeResponseWriter{
		pw:       pw,
		header:   make(http.Header),
		headerCh: make(chan struct{}),
	}

	go func() {
		defer pw.Close()
		t.handler.ServeHTTP(rw, req)
		rw.WriteHeader(http.StatusOK)
	}()

	<-rw.headerCh

	return &http.Response{
		StatusCode:    rw.statusCode,
		Status:        http.StatusText(rw.statusCode),
		Header:        rw.header,
		Body:          pr,
		ContentLength: -1,
		Request:       req,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
	}, nil
}

type pipeResponseWriter struct {
	pw         *io.PipeWriter
	header     http.Header
	statusCode int
	headerOnce sync.Once
	headerCh   chan struct{}
}

func (w *pipeResponseWriter) Header() http.Header { return w.header }

func (w *pipeResponseWriter) WriteHeader(code int) {
	w.headerOnce.Do(func() {
		w.statusCode = code
		if w.headerCh != nil {
			close(w.headerCh)
		}
	})
}

func (w *pipeResponseWriter) Write(data []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.pw.Write(data)
}

func (w *pipeResponseWriter) Flush() {}

func TestEmbeddedTransportSearch(t *testing.T) {
	// Create orchestrator with a store
	orch := orchestrator.New(orchestrator.Config{})

	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	im := indexmem.NewManager([]index.Indexer{tokIdx, attrIdx, kvIdx}, tokIdx, attrIdx, kvIdx, nil)

	// Add some records
	t0 := time.Now()
	for i := 0; i < 5; i++ {
		cm.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", query.New(cm, im, nil))

	// Create server
	srv := server.New(orch, nil, orchestrator.Factories{}, server.Config{})
	handler := srv.Handler()

	// Create client with embedded transport (like REPL uses)
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	client := gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")

	// Run search
	t.Log("Running search via embedded transport...")
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{},
	}))
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	count := 0
	for stream.Receive() {
		msg := stream.Msg()
		count += len(msg.Records)
		t.Logf("Received batch of %d records", len(msg.Records))
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("Stream error: %v", err)
	}

	t.Logf("Total: %d records", count)
	if count != 5 {
		t.Errorf("expected 5 records, got %d", count)
	}
}
