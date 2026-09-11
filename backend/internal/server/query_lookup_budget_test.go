package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/lookup"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"

	"connectrpc.com/connect"
)

// newLookupQuerySetup wires a query client over a vault whose records each
// carry a distinct "ip" attribute, with one HTTP lookup table registered
// against srvURL. The lookup opts into private destinations because the test
// endpoint listens on loopback.
func newLookupQuerySetup(t *testing.T, srvURL string, numRecords int) gastrologv1connect.QueryServiceClient {
	t.Helper()

	cfgStore := sysmem.NewStore()
	ss, err := system.LoadServerSettings(context.Background(), cfgStore)
	if err != nil {
		t.Fatalf("LoadServerSettings: %v", err)
	}
	ss.Lookup.HTTPLookups = []system.HTTPLookupConfig{{
		Name:                     "probe",
		URLTemplate:              srvURL + "/{value}",
		Parameters:               []system.HTTPLookupParam{{Name: "value"}},
		AllowPrivateDestinations: true,
	}}
	if err := system.SaveServerSettings(context.Background(), cfgStore, ss); err != nil {
		t.Fatalf("SaveServerSettings: %v", err)
	}

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	v := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100_000),
	})
	t0 := time.Now().Add(-time.Duration(numRecords) * time.Second)
	for i := range numRecords {
		v.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("test-record"),
			Attrs:    chunk.Attributes{"ip": fmt.Sprintf("198.51.100.%d.%d", i/256, i%256)},
		})
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(glid.New(), v.CM, v.IM, v.QE))

	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{NoAuth: true})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewQueryServiceClient(httpClient, "http://embedded")
}

// countEnriched runs the streaming lookup query and reports how many returned
// records carry the enrichment field.
func countEnriched(t *testing.T, client gastrologv1connect.QueryServiceClient) (records, enriched int) {
	t.Helper()

	stream, err := client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: "| lookup probe ip"},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	for stream.Receive() {
		for _, rec := range stream.Msg().GetRecords() {
			records++
			if rec.GetAttrs()["ip_team"] != "" {
				enriched++
			}
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream: %v", err)
	}
	return records, enriched
}

// TestSearchLookupIsBudgeted drives the real streaming query path — the one a
// bare "| lookup" takes, which never enters the engine's pipeline entry points
// — and holds it to the per-query outbound bound.
func TestSearchLookupIsBudgeted(t *testing.T) {
	if testing.Short() {
		t.Skip("issues one outbound request per distinct value up to the budget")
	}

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"team":"platform"}`))
	}))
	defer srv.Close()

	// Premise: below the bound every record is enriched, so a shortfall above
	// it is the budget and not broken wiring.
	client := newLookupQuerySetup(t, srv.URL, 20)
	records, enriched := countEnriched(t, client)
	if records != 20 || enriched != 20 {
		t.Fatalf("under the bound: %d/%d records enriched, want 20/20 (%d requests)", enriched, records, requests.Load())
	}

	requests.Store(0)
	const over = lookup.MaxOutboundPerQuery + 50
	client = newLookupQuerySetup(t, srv.URL, over)
	records, enriched = countEnriched(t, client)
	if records != over {
		t.Fatalf("got %d records, want %d", records, over)
	}
	if enriched != lookup.MaxOutboundPerQuery {
		t.Errorf("%d records enriched, want the bound %d", enriched, lookup.MaxOutboundPerQuery)
	}
	if got := requests.Load(); got != lookup.MaxOutboundPerQuery {
		t.Errorf("endpoint received %d requests, want the bound %d", got, lookup.MaxOutboundPerQuery)
	}
}
