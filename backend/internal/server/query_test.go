package server_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
	"github.com/google/uuid"
)

// newQueryTestSetup creates an orchestrator with a memory vault containing
// numRecords records, wires up a QueryServiceClient via embeddedTransport,
// and returns the client for streaming RPCs.
func newQueryTestSetup(t *testing.T, numRecords int) gastrologv1connect.QueryServiceClient {
	t.Helper()

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})

	t0 := time.Now()
	for i := range numRecords {
		s.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
		})
	}

	defaultID := uuid.Must(uuid.NewV7())
	orch.RegisterVault(orchestrator.NewVault(defaultID, s.CM, s.IM, s.QE))

	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	return gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")
}

func TestQueryServerSearch(t *testing.T) {
	client := newQueryTestSetup(t, 5)

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
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("Stream error: %v", err)
	}

	if count != 5 {
		t.Errorf("expected 5 records, got %d", count)
	}
}

func TestSearchInvalidQuery(t *testing.T) {
	client := newQueryTestSetup(t, 0)

	// "start=not-a-time" will fail in applyDirective → parseTime.
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: "start=not-a-time"},
	}))
	if err != nil {
		// Connect may surface the error on initial call for server-stream RPCs.
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
		}
		return
	}
	// Or it may surface on the stream.
	for stream.Receive() {
	}
	if err := stream.Err(); err == nil {
		t.Fatal("expected error for invalid query expression")
	} else if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

func TestSearchInvalidResumeToken(t *testing.T) {
	client := newQueryTestSetup(t, 0)

	// Corrupt bytes that aren't a valid protobuf ResumeToken.
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query:       &gastrologv1.Query{},
		ResumeToken: []byte{0xFF, 0xFE, 0xFD, 0x01, 0x02, 0x03},
	}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
		}
		return
	}
	for stream.Receive() {
	}
	if err := stream.Err(); err == nil {
		t.Fatal("expected error for corrupt resume token")
	} else if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

func TestSearchContextCancellation(t *testing.T) {
	client := newQueryTestSetup(t, 100)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Search(ctx, connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{},
	}))
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Receive one batch, then cancel.
	if stream.Receive() {
		cancel()
	} else {
		cancel()
		t.Skip("no records received before cancel")
	}

	// Drain remaining — should terminate cleanly.
	for stream.Receive() {
	}
	// Either CodeCanceled or nil (clean exit) is acceptable.
	if err := stream.Err(); err != nil {
		code := connect.CodeOf(err)
		if code != connect.CodeCanceled && code != connect.CodeUnknown {
			t.Fatalf("unexpected error code %v: %v", code, err)
		}
	}
}

func TestFollowInvalidQuery(t *testing.T) {
	client := newQueryTestSetup(t, 0)

	stream, err := client.Follow(context.Background(), connect.NewRequest(&gastrologv1.FollowRequest{
		Query: &gastrologv1.Query{Expression: "start=not-a-time"},
	}))
	if err != nil {
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
		}
		return
	}
	for stream.Receive() {
	}
	if err := stream.Err(); err == nil {
		t.Fatal("expected error for invalid query expression")
	} else if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

func TestFollowRejectsAggregatingOps(t *testing.T) {
	client := newQueryTestSetup(t, 0)

	cases := []struct {
		name string
		expr string
	}{
		{"stats", "* | stats count"},
		{"sort", "* | sort timestamp"},
		{"tail", "* | tail 10"},
		{"slice", "* | slice 0 10"},
		{"timechart", "* | timechart count span=1h"},
		{"barchart", "* | barchart count by source"},
		{"donut", "* | donut count by source"},
		{"map", "* | map count by source"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream, err := client.Follow(ctx, connect.NewRequest(&gastrologv1.FollowRequest{
				Query: &gastrologv1.Query{Expression: tc.expr},
			}))
			if err != nil {
				if connect.CodeOf(err) != connect.CodeInvalidArgument {
					t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
				}
				return
			}
			for stream.Receive() {
			}
			if err := stream.Err(); err == nil {
				t.Fatalf("expected error for %s in follow mode", tc.name)
			} else if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
			}
		})
	}
}

func TestSearchHeadLimitsResults(t *testing.T) {
	client := newQueryTestSetup(t, 100)

	// Exact reproduction of what the frontend sends: proto-level limit=100
	// (page size) with a | head 10 pipeline in the expression.
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{
			Expression: "reverse=true | head 10",
			Limit:      100,
		},
	}))
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	count := 0
	for stream.Receive() {
		count += len(stream.Msg().Records)
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("Stream error: %v", err)
	}

	if count != 10 {
		t.Errorf("expected 10 records from | head 10, got %d", count)
	}
}

func TestSearchHeadBeforeStats(t *testing.T) {
	client := newQueryTestSetup(t, 100)

	// Pipeline: | head 10 | stats count — head should truncate before aggregation.
	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{
			Expression: "reverse=true | head 10 | stats count",
			Limit:      100,
		},
	}))
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	var countVal string
	for stream.Receive() {
		msg := stream.Msg()
		if msg.TableResult != nil && len(msg.TableResult.Rows) > 0 {
			row := msg.TableResult.Rows[0]
			if len(row.Values) > 0 {
				countVal = row.Values[0]
			}
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("Stream error: %v", err)
	}
	if countVal != "10" {
		t.Errorf("head 10 | stats count = %q, want \"10\"", countVal)
	}
}

// TestQueryStringRoundTrip verifies that Query.String() preserves nanosecond
// precision so remote nodes compute identical time ranges. A regression here
// causes cross-node histogram bucket misalignment and missing records.
func TestQueryStringRoundTrip(t *testing.T) {
	t.Parallel()

	// Simulate what "last=5m" produces: nanosecond-precise timestamps.
	now := time.Now()
	original := "start=" + now.Add(-5*time.Minute).Format(time.RFC3339Nano) +
		" end=" + now.Format(time.RFC3339Nano) + " reverse=true"

	q, _, err := server.ParseExpression(original)
	if err != nil {
		t.Fatalf("ParseExpression: %v", err)
	}

	// Round-trip: serialize with String(), re-parse on "remote" node.
	serialized := q.String()
	q2, _, err := server.ParseExpression(serialized)
	if err != nil {
		t.Fatalf("ParseExpression round-trip: %v", err)
	}

	if !q.Start.Equal(q2.Start) {
		t.Errorf("Start lost precision: %v → %v (diff %v)", q.Start, q2.Start, q.Start.Sub(q2.Start))
	}
	if !q.End.Equal(q2.End) {
		t.Errorf("End lost precision: %v → %v (diff %v)", q.End, q2.End, q.End.Sub(q2.End))
	}
	if q.IsReverse != q2.IsReverse {
		t.Errorf("IsReverse: %v → %v", q.IsReverse, q2.IsReverse)
	}
}

func TestExplainInvalidQuery(t *testing.T) {
	client := newQueryTestSetup(t, 0)

	_, err := client.Explain(context.Background(), connect.NewRequest(&gastrologv1.ExplainRequest{
		Query: &gastrologv1.Query{Expression: "start=not-a-time"},
	}))
	if err == nil {
		t.Fatal("expected error for invalid query expression")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}
