package server_test

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
)

// Resource-owner routing (gastrolog-51ge9): an imperative action naming a
// resource must reach the node that owns the resource, resolved by the
// BACKEND from replicated state — no X-Target-Node header from the client,
// and no config mutation tunnelled through Raft to get there.
//
// TriggerIngester is the converted consumer. Every node in these tests has
// the ingester registered in its orchestrator, so a test only passes when
// routing actually picked the owner — not because the owner was the only
// node that could have served the call.

// fakeTriggerable is an ingester that records Trigger() calls. Trigger is
// invoked synchronously by the handler, so tests never wait on anything.
type fakeTriggerable struct {
	mu    sync.Mutex
	count int
}

func (f *fakeTriggerable) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	<-ctx.Done()
	return nil
}

func (f *fakeTriggerable) Trigger() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
}

func (f *fakeTriggerable) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

// mnSystemClientFor builds a SystemService client whose server runs AS
// nodeID, with a forwarder that can reach every other harness node. This is
// what makes "correct from every node" testable: the same request issued at
// any node must land on the resource owner.
func mnSystemClientFor(t *testing.T, h *multiNodeHarness, nodeID string) gastrologv1connect.SystemServiceClient {
	t.Helper()
	node := h.Node(t, nodeID)
	srv := server.New(node.orch, h.cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil, server.Config{
		NodeID:           nodeID,
		RoutingForwarder: newDirectUnaryForwarder(t, h.nodes, h.cfgStore, nodeID, t.TempDir()),
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")
}

// registerFakeIngesterEverywhere registers the same triggerable ingester on
// every node's orchestrator and returns the per-node recorders.
func registerFakeIngesterEverywhere(t *testing.T, h *multiNodeHarness, ingID glid.GLID) map[string]*fakeTriggerable {
	t.Helper()
	fakes := make(map[string]*fakeTriggerable, len(h.nodes))
	for id, node := range h.nodes {
		f := &fakeTriggerable{}
		fakes[id] = f
		node.orch.RegisterIngester(ingID, "burst", "scatterbox", f)
	}
	return fakes
}

// seedIngester writes the replicated config + alive state that the owner
// resolver reads.
func seedIngester(t *testing.T, h *multiNodeHarness, ingID glid.GLID, aliveOn ...string) {
	t.Helper()
	ctx := context.Background()
	if err := h.cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID:       ingID,
		Name:     "burst",
		Type:     "scatterbox",
		Enabled:  true,
		AllNodes: true,
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	for _, nodeID := range aliveOn {
		if err := h.cfgStore.SetIngesterAlive(ctx, ingID, nodeID, true); err != nil {
			t.Fatalf("SetIngesterAlive(%s): %v", nodeID, err)
		}
	}
}

func triggerCounts(fakes map[string]*fakeTriggerable) map[string]int {
	counts := make(map[string]int, len(fakes))
	for id, f := range fakes {
		counts[id] = f.Count()
	}
	return counts
}

// TestTriggerIngester_ReachesOwnerFromEveryNode: the action is issued at
// each node in turn, with no X-Target-Node header anywhere, and always
// executes on the single node running the ingester.
func TestTriggerIngester_ReachesOwnerFromEveryNode(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	ingID := glid.New()
	fakes := registerFakeIngesterEverywhere(t, h, ingID)
	seedIngester(t, h, ingID, "data-2")

	nodeIDs := []string{"coord", "data-1", "data-2"}
	for _, from := range nodeIDs {
		client := mnSystemClientFor(t, h, from)
		_, err := client.TriggerIngester(context.Background(),
			connect.NewRequest(&gastrologv1.TriggerIngesterRequest{Id: ingID.Bytes()}))
		if err != nil {
			t.Fatalf("TriggerIngester from %s: %v", from, err)
		}
	}

	got := triggerCounts(fakes)
	if got["data-2"] != len(nodeIDs) {
		t.Errorf("owner data-2 triggered %d times, want %d (counts: %v)", got["data-2"], len(nodeIDs), got)
	}
	for _, id := range []string{"coord", "data-1"} {
		if got[id] != 0 {
			t.Errorf("non-owner %s was triggered %d times (counts: %v)", id, got[id], got)
		}
	}
}

// TestTriggerIngester_PluralOwners is the ingester-HA shape: several nodes
// run the same ingester. A node that owns it serves locally; a node that
// does not picks the same owner as every other non-owner node.
func TestTriggerIngester_PluralOwners(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	ingID := glid.New()
	fakes := registerFakeIngesterEverywhere(t, h, ingID)
	seedIngester(t, h, ingID, "data-1", "data-2")

	// An owner node serves locally rather than hopping.
	for _, owner := range []string{"data-1", "data-2"} {
		before := fakes[owner].Count()
		client := mnSystemClientFor(t, h, owner)
		if _, err := client.TriggerIngester(context.Background(),
			connect.NewRequest(&gastrologv1.TriggerIngesterRequest{Id: ingID.Bytes()})); err != nil {
			t.Fatalf("TriggerIngester from owner %s: %v", owner, err)
		}
		if fakes[owner].Count() != before+1 {
			t.Errorf("owner %s did not serve locally (counts: %v)", owner, triggerCounts(fakes))
		}
	}

	// A non-owner forwards, deterministically, to the first owner.
	client := mnSystemClientFor(t, h, "coord")
	if _, err := client.TriggerIngester(context.Background(),
		connect.NewRequest(&gastrologv1.TriggerIngesterRequest{Id: ingID.Bytes()})); err != nil {
		t.Fatalf("TriggerIngester from coord: %v", err)
	}
	got := triggerCounts(fakes)
	if got["coord"] != 0 {
		t.Errorf("non-owner coord served the action itself (counts: %v)", got)
	}
	if got["data-1"] != 2 {
		t.Errorf("expected the forwarded action on data-1 (first owner), counts: %v", got)
	}
	if got["data-2"] != 1 {
		t.Errorf("data-2 should not have received the forwarded action, counts: %v", got)
	}
}

// TestTriggerIngester_UnknownIngester: an ingester that is absent from
// replicated config gets a definitive NotFound from every node, instead of
// whichever node received the request running the action locally.
func TestTriggerIngester_UnknownIngester(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))

	known := glid.New()
	fakes := registerFakeIngesterEverywhere(t, h, known)
	seedIngester(t, h, known, "data-2")

	ghost := glid.New()
	for _, from := range []string{"coord", "data-1", "data-2"} {
		client := mnSystemClientFor(t, h, from)
		_, err := client.TriggerIngester(context.Background(),
			connect.NewRequest(&gastrologv1.TriggerIngesterRequest{Id: ghost.Bytes()}))
		if err == nil {
			t.Fatalf("TriggerIngester(unknown) from %s: expected an error", from)
		}
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("TriggerIngester(unknown) from %s: got %v (%v), want CodeNotFound", from, connect.CodeOf(err), err)
		}
	}

	for id, count := range triggerCounts(fakes) {
		if count != 0 {
			t.Errorf("node %s ran a trigger for an unknown ingester (%d times)", id, count)
		}
	}
}

// TestTriggerIngester_MalformedID: a request whose ID is not a GLID gets
// the handler's InvalidArgument, not a routing failure.
func TestTriggerIngester_MalformedID(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	client := mnSystemClientFor(t, h, "coord")
	_, err := client.TriggerIngester(context.Background(),
		connect.NewRequest(&gastrologv1.TriggerIngesterRequest{Id: []byte("nope")}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("got %v (%v), want CodeInvalidArgument", connect.CodeOf(err), err)
	}
}

// TestTriggerIngester_ConfiguredButNotRunning: the ingester exists in
// config but no node has reported it alive. There is no owner to route to,
// so the receiving node runs the handler and reports what it sees rather
// than the routing layer inventing a target.
func TestTriggerIngester_ConfiguredButNotRunning(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))

	ingID := glid.New()
	seedIngester(t, h, ingID) // no alive nodes

	client := mnSystemClientFor(t, h, "coord")
	_, err := client.TriggerIngester(context.Background(),
		connect.NewRequest(&gastrologv1.TriggerIngesterRequest{Id: ingID.Bytes()}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("got %v (%v), want the handler's CodeNotFound", connect.CodeOf(err), err)
	}
}
