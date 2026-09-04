package server

import (
	"context"
	"strings"
	"sync"
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/querylang"
)

// recordingSearcher captures every expression the coordinator forwards.
type recordingSearcher struct {
	stubQuerySearcher
	mu   sync.Mutex
	sent []string
}

func (r *recordingSearcher) Search(ctx context.Context, nodeID string, req *apiv1.ForwardSearchRequest) (*apiv1.ForwardSearchResponse, error) {
	r.mu.Lock()
	r.sent = append(r.sent, req.GetQuery())
	r.mu.Unlock()
	return r.stubQuerySearcher.Search(ctx, nodeID, req)
}

func (r *recordingSearcher) forwarded(t *testing.T) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sent) == 0 {
		t.Fatal("nothing was forwarded")
	}
	return r.sent[0]
}

// The remote parses the forwarded string exactly as a client expression, so it
// must round-trip: the filter ahead of the directives, the pipes behind one
// pipe each, and nothing that reads as an operator.
func TestForwardedPipelineExpressionRoundTrips(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, expr, wantFilter string
		wantPipes              int
	}{
		{"filter and stats", "level=error start=2026-09-04T21:26:10Z end=2026-09-04T21:26:40Z | stats count", "level=error", 1},
		{"no filter", "start=2026-09-04T21:26:10Z end=2026-09-04T21:26:40Z | stats count", "", 1},
		{"two pipes", "host=web-1 last=1h | where level=error | stats count by host", "host=web-1", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q, pipeline, err := ParseExpression(tc.expr)
			if err != nil || pipeline == nil {
				t.Fatalf("parse %q: pipeline=%v err=%v", tc.expr, pipeline, err)
			}
			got := forwardedPipelineExpression(q, pipeline)

			rq, rp, err := ParseExpression(got)
			if err != nil {
				t.Fatalf("remote cannot parse forwarded %q: %v", got, err)
			}
			if rp == nil || len(rp.Pipes) != tc.wantPipes {
				t.Fatalf("forwarded %q parsed to %d pipes, want %d", got, pipeCount(rp), tc.wantPipes)
			}
			gotFilter := ""
			if rq.BoolExpr != nil {
				gotFilter = rq.BoolExpr.String()
			}
			if gotFilter != tc.wantFilter {
				t.Fatalf("forwarded %q carries filter %q, want %q", got, gotFilter, tc.wantFilter)
			}
			if rq.Start.IsZero() || rq.End.IsZero() {
				t.Fatalf("forwarded %q lost its absolute time bounds", got)
			}
		})
	}
}

// A vault_id= predicate selects which node to ask; the remote is told the
// vault through VaultId, so the predicate must not travel in the expression.
func TestForwardedPipelineExpressionDropsVaultPredicate(t *testing.T) {
	t.Parallel()

	v := glid.New()
	q, pipeline, err := ParseExpression("vault_id=" + v.String() + " level=error last=1h | stats count")
	if err != nil || pipeline == nil {
		t.Fatalf("parse: %v", err)
	}
	got := forwardedPipelineExpression(q, pipeline)
	if strings.Contains(got, "vault_id") {
		t.Fatalf("forwarded %q still carries the vault predicate", got)
	}
	rq, rp, err := ParseExpression(got)
	if err != nil || rp == nil || len(rp.Pipes) != 1 {
		t.Fatalf("remote cannot parse forwarded %q: pipes=%d err=%v", got, pipeCount(rp), err)
	}
	if rq.BoolExpr == nil || rq.BoolExpr.String() != "level=error" {
		t.Fatalf("forwarded %q filter = %v, want level=error", got, rq.BoolExpr)
	}
}

// End to end through collectRemotePipeline: what the coordinator actually
// sends to a remote vault must be parseable by that remote.
func TestCollectRemotePipelineForwardsAParseableExpression(t *testing.T) {
	t.Parallel()

	v := glid.New()
	searcher := &recordingSearcher{}
	qs, _ := newQueryServerWithRemoteVaults(t, searcher, []glid.GLID{v})

	q, pipeline, err := ParseExpression("ingester_type=scatterbox start=2026-09-04T21:26:10Z end=2026-09-04T21:26:40Z | stats count")
	if err != nil || pipeline == nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := qs.collectRemotePipeline(context.Background(), q, pipeline); err != nil {
		t.Fatalf("collectRemotePipeline: %v", err)
	}
	sent := searcher.forwarded(t)
	if _, rp, err := ParseExpression(sent); err != nil || rp == nil || len(rp.Pipes) != 1 {
		t.Fatalf("remote would reject forwarded %q: pipes=%d err=%v", sent, pipeCount(rp), err)
	}
}

func pipeCount(p *querylang.Pipeline) int {
	if p == nil {
		return 0
	}
	return len(p.Pipes)
}
