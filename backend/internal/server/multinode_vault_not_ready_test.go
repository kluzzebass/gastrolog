package server_test

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

// holdNotReady re-registers node's vault behind a readiness gate the test
// controls, closed: the vault is not ready on its leader until the test
// stores true.
func holdNotReady(t *testing.T, node multinodeTestNode) *atomic.Bool {
	t.Helper()
	if node.vaultID.IsZero() {
		t.Fatalf("node %s has no vault to hold", node.nodeID)
	}
	ready := &atomic.Bool{}
	node.orch.RegisterVault(orchestrator.NewVault(node.vaultID, &orchestrator.VaultInstance{
		VaultID: node.vaultID,
		Type:    "memory",
		Chunks:  node.vault.CM,
		Indexes: node.vault.IM,
		Query:   node.vault.QE,
		ManifestReadFacet: orchestrator.ManifestReadFacet{
			IsFSMReady: ready.Load,
		},
	}))
	return ready
}

// coordinatedFrom orders nodes so the coordinator, the node the client is
// connected to, comes first.
func coordinatedFrom(coordinator string, nodes []string) []string {
	return append([]string{coordinator}, slices.DeleteFunc(slices.Clone(nodes), func(n string) bool { return n == coordinator })...)
}

// searchView is what a client sees of one search: the raw payloads of the
// records it was sent, the vaults reported as contributing, and the histogram
// total.
type searchView struct {
	raws         []string
	contributors map[glid.GLID]bool
	histTotal    int64
	sawHist      bool
}

func searchViewOf(t *testing.T, h *multiNodeHarness, expr string) searchView {
	t.Helper()
	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: expr},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	view := searchView{contributors: map[glid.GLID]bool{}}
	for stream.Receive() {
		for _, r := range stream.Msg().Records {
			view.raws = append(view.raws, string(r.Raw))
		}
		for _, raw := range stream.Msg().ContributingVaultIds {
			view.contributors[glid.FromBytes(raw)] = true
		}
		if hist := stream.Msg().Histogram; len(hist) > 0 {
			view.sawHist = true
			for _, b := range hist {
				view.histTotal += b.Count
			}
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("search %q failed: %v", expr, err)
	}
	slices.Sort(view.raws)
	return view
}

func statsCount(t *testing.T, h *multiNodeHarness) string {
	t.Helper()
	rows := searchTable(t, h.client, histWindow+" | stats count").GetRows()
	if len(rows) != 1 || len(rows[0].GetValues()) != 1 {
		t.Fatalf("stats count rows = %v, want one row of one value", rows)
	}
	return rows[0].GetValues()[0]
}

func rawsFromNodes(all []string, nodes ...string) []string {
	return slices.DeleteFunc(slices.Clone(all), func(raw string) bool {
		return !slices.ContainsFunc(nodes, func(n string) bool { return strings.HasPrefix(raw, n+"-") })
	})
}

var notReadyClusterNodes = []string{"node-1", "node-2", "node-3", "node-4"}

// notReadyVaultLeader leads the vault the tests hold not ready.
const notReadyVaultLeader = "node-3"

// A search that reaches a vault not yet ready on its leader answers the same
// from every node: the other vaults' records, histogram and aggregate, with
// that vault left out and not reported as contributing — whether the client
// is on the vault's leader or on any other node. Once the vault is ready, it
// is searched again from both.
func TestMultiNode_VaultNotReadyOnItsLeaderLeftOutFromEveryNode(t *testing.T) {
	t.Parallel()

	type observed struct {
		coordinator     string
		notReady, ready searchView
		notReadyCount   string
		readyCount      string
		vaultID         glid.GLID
	}
	var runs []observed
	for _, coordinator := range []string{"node-1", notReadyVaultLeader} {
		h := setupMultiNode(t, coordinatedFrom(coordinator, notReadyClusterNodes), WithClusterSearch())
		seedHistogramVaults(t, h, notReadyClusterNodes)
		gate := holdNotReady(t, h.Node(t, notReadyVaultLeader))

		run := observed{coordinator: coordinator, vaultID: h.Node(t, notReadyVaultLeader).vaultID}
		run.notReady = searchViewOf(t, h, histWindow)
		run.notReadyCount = statsCount(t, h)
		gate.Store(true)
		run.ready = searchViewOf(t, h, histWindow)
		run.readyCount = statsCount(t, h)
		runs = append(runs, run)
	}

	const perVault = histErrPerVault + histInfoPerVault
	all := runs[0].ready.raws
	if len(all) != len(notReadyClusterNodes)*perVault {
		t.Fatalf("premise: with every vault ready the search returned %d records, want %d", len(all), len(notReadyClusterNodes)*perVault)
	}
	wantNotReady := rawsFromNodes(all, "node-1", "node-2", "node-4")
	for _, run := range runs {
		if !slices.Equal(run.notReady.raws, wantNotReady) {
			t.Errorf("from %s: records while not ready = %d %v, want the %d records of the ready vaults",
				run.coordinator, len(run.notReady.raws), run.notReady.raws, len(wantNotReady))
		}
		if run.notReady.contributors[run.vaultID] {
			t.Errorf("from %s: the not-ready vault is reported as contributing", run.coordinator)
		}
		if !run.notReady.sawHist || run.notReady.histTotal != int64(len(wantNotReady)) {
			t.Errorf("from %s: histogram while not ready = %d (shown %v), want %d",
				run.coordinator, run.notReady.histTotal, run.notReady.sawHist, len(wantNotReady))
		}
		if want := strconv.Itoa(len(wantNotReady)); run.notReadyCount != want {
			t.Errorf("from %s: stats count while not ready = %s, want %s", run.coordinator, run.notReadyCount, want)
		}
		if !slices.Equal(run.ready.raws, all) {
			t.Errorf("from %s: records once ready = %d, want all %d", run.coordinator, len(run.ready.raws), len(all))
		}
		if want := strconv.Itoa(len(all)); run.readyCount != want {
			t.Errorf("from %s: stats count once ready = %s, want %s", run.coordinator, run.readyCount, want)
		}
	}
	if !runs[0].ready.contributors[runs[0].vaultID] {
		t.Error("from node-1: the vault, once ready, is not reported as contributing")
	}
	if !slices.Equal(runs[0].notReady.raws, runs[1].notReady.raws) {
		t.Error("the not-ready vault's leader and another node answer the same search differently")
	}
}

// With every vault in the cluster not ready, a search from any node answers
// with nothing and no error.
func TestMultiNode_EveryVaultNotReady(t *testing.T) {
	t.Parallel()

	for _, coordinator := range []string{"node-1", notReadyVaultLeader} {
		t.Run("from "+coordinator, func(t *testing.T) {
			t.Parallel()
			h := setupMultiNode(t, coordinatedFrom(coordinator, notReadyClusterNodes), WithClusterSearch())
			seedHistogramVaults(t, h, notReadyClusterNodes)
			for _, id := range notReadyClusterNodes {
				holdNotReady(t, h.Node(t, id))
			}

			view := searchViewOf(t, h, histWindow)
			if len(view.raws) != 0 || len(view.contributors) != 0 || view.histTotal != 0 {
				t.Errorf("records = %d, contributors = %v, histogram total = %d; want none",
					len(view.raws), view.contributors, view.histTotal)
			}
			if got := statsCount(t, h); got != "0" {
				t.Errorf("stats count = %s, want 0", got)
			}
		})
	}
}

// A vault left out for not being ready never hides a real failure of another
// vault: a peer whose share of the query overflows its memory budget still
// fails the search, with the code that says so.
func TestMultiNode_VaultNotReadyDoesNotHideRealFailure(t *testing.T) {
	t.Parallel()
	nodes := []string{"coord", "data-1", "data-2", "data-3"}
	const perNode = 400
	const expr = "| stats count by id"
	setup := func(opts ...mnOption) *multiNodeHarness {
		h := setupMultiNode(t, nodes, append([]mnOption{WithoutVault("coord"), WithClusterSearch()}, opts...)...)
		for _, id := range nodes[1:] {
			addMNDistinctRecords(t, h.Node(t, id), id, perNode)
		}
		holdNotReady(t, h.Node(t, "data-2"))
		return h
	}

	if rows := len(searchTable(t, setup().client, expr).GetRows()); rows != 2*perNode {
		t.Fatalf("premise: %s returned %d groups, want %d from the two ready vaults", expr, rows, 2*perNode)
	}

	err := searchExpectingError(t, setup(WithPeerQueryMemoryLimit(8<<10)).client, expr)
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("code = %v, want %v (err: %v)", got, connect.CodeResourceExhausted, err)
	}
}

// On a single node, a vault that is not ready is left out of a search beside
// a ready one, without an error, and is searched once it is ready.
func TestSingleNode_VaultNotReadyLeftOut(t *testing.T) {
	t.Parallel()

	orch, err := orchestrator.New(orchestrator.Config{LocalNodeID: "solo"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = orch.Stop() })
	vaultNode := func(prefix string) multinodeTestNode {
		v := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(10000)})
		n := multinodeTestNode{nodeID: "solo", orch: orch, vaultID: glid.New(), vault: v}
		orch.RegisterVault(orchestrator.NewVaultFromComponents(n.vaultID, v.CM, v.IM, v.QE))
		addMNRecords(t, n, prefix, 5, nil)
		return n
	}
	vaultNode("ready")
	gate := holdNotReady(t, vaultNode("held"))

	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{NoAuth: true})
	client := gastrologv1connect.NewQueryServiceClient(&http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}, "http://embedded")
	raws := func() []string {
		var out []string
		for _, r := range searchAll(t, client, "") {
			out = append(out, string(r.Raw))
		}
		slices.Sort(out)
		return out
	}

	if got, want := raws(), []string{"ready-0", "ready-1", "ready-2", "ready-3", "ready-4"}; !slices.Equal(got, want) {
		t.Fatalf("records while not ready = %v, want %v", got, want)
	}
	gate.Store(true)
	if got := raws(); len(got) != 10 {
		t.Fatalf("records once ready = %d %v, want 10", len(got), got)
	}
}
