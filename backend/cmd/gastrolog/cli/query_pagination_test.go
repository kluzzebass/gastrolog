package cli

// A search larger than one page streams in pages, each request carrying the
// previous page's resume token. If a request goes out without the token, the
// server answers it as page 1 again, with a token again, and the client pages
// forever: every query over one page — `--count` included — never ends.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/server"
)

// pagedQueryService serves pages[i] for the request that presents tokens[i]
// (nil for the first page), and records every token it was sent.
type pagedQueryService struct {
	gastrologv1connect.UnimplementedQueryServiceHandler
	pages  [][]*gastrologv1.Record
	tokens [][]byte
	mu     sync.Mutex
	seen   [][]byte
}

func (s *pagedQueryService) Search(_ context.Context, req *connect.Request[gastrologv1.SearchRequest], stream *connect.ServerStream[gastrologv1.SearchResponse]) error {
	s.mu.Lock()
	got := req.Msg.GetResumeToken()
	s.seen = append(s.seen, got)
	s.mu.Unlock()
	page := 0
	for i, tok := range s.tokens {
		if bytes.Equal(tok, got) {
			page = i
		}
	}
	var next []byte
	if page+1 < len(s.pages) {
		next = s.tokens[page+1]
	}
	return stream.Send(&gastrologv1.SearchResponse{Records: s.pages[page], ResumeToken: next})
}

func TestStreamSearchCarriesEachPagesTokenAndStops(t *testing.T) {
	t.Parallel()
	rec := func(raw string) *gastrologv1.Record { return &gastrologv1.Record{Raw: []byte(raw)} }
	svc := &pagedQueryService{
		pages:  [][]*gastrologv1.Record{{rec("a"), rec("b")}, {rec("c"), rec("d")}, {rec("e")}},
		tokens: [][]byte{nil, []byte("after-page-1"), []byte("after-page-2")},
	}
	mux := http.NewServeMux()
	mux.Handle(gastrologv1connect.NewQueryServiceHandler(svc))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var got []string
	err := streamSearch(context.Background(), server.NewClient(srv.URL), "last=1h", 0, func(resp *gastrologv1.SearchResponse) error {
		for _, r := range resp.GetRecords() {
			got = append(got, string(r.GetRaw()))
		}
		if len(got) > 50 {
			t.Fatalf("still paging after %d records: the pages are repeating", len(got))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("streamSearch: %v", err)
	}

	if want := []string{"a", "b", "c", "d", "e"}; len(got) != len(want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.seen) != 3 {
		t.Fatalf("server saw %d requests, want 3", len(svc.seen))
	}
	for i, want := range svc.tokens {
		if !bytes.Equal(svc.seen[i], want) {
			t.Errorf("request %d carried token %q, want %q", i+1, svc.seen[i], want)
		}
	}
}
