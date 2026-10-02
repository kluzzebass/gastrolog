package orchestrator_test

// A blackholed cloud endpoint — the address resolves, connections are
// accepted, nothing ever answers — must cost the cluster nothing beyond that
// one vault's uploads. Vault registration runs under the orchestrator
// registry write lock inside the FSM apply path, and Go's write-preferring
// RWMutex lets one stalled writer starve every reader, so a single store
// dial on the construction path turns one dead endpoint into a node that
// silently ingests nothing. The blackhole shape matters: a refused
// connection fails in microseconds and hides the bug; an accepted-then-hung
// one is the quadrant where it lives.

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// blackholeEndpoint accepts TCP connections and never answers them. release
// (run automatically at cleanup) closes the listener and every held
// connection so an SDK call hung against it fails over to a fast refusal
// instead of pinning teardown.
type blackholeEndpoint struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func startBlackholeEndpoint(t *testing.T) *blackholeEndpoint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	b := &blackholeEndpoint{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.conns = append(b.conns, conn)
			b.mu.Unlock()
		}
	}()
	t.Cleanup(b.release)
	return b
}

func (b *blackholeEndpoint) url() string { return "http://" + b.ln.Addr().String() }

func (b *blackholeEndpoint) release() {
	_ = b.ln.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.conns {
		_ = c.Close()
	}
	b.conns = nil
}

func TestOrchPipeline_BlackholedCloudStoreNeverStallsIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	bh := startBlackholeEndpoint(t)
	h := newOrchRelHarness(t, 4,
		withMatchAllRoute(0),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	ctx := context.Background()
	vA := h.vaults[0]

	// A cloud-backed vault whose store is the blackhole. The service config
	// is valid — only the endpoint is dead.
	csID := glid.New()
	if err := h.cfgStore.PutCloudService(ctx, system.CloudService{
		ID:        csID,
		Name:      "orch-rel-blackholed-store",
		Provider:  "s3",
		Bucket:    "orch-rel-blackhole",
		Region:    "us-east-1",
		Endpoint:  bh.url(),
		AccessKey: "key",
		SecretKey: "secret",
	}); err != nil {
		t.Fatalf("PutCloudService: %v", err)
	}
	vB := vaultSpec{label: "B", id: glid.New(), nodeIdxs: []int{0, 1, 2}}
	if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
		ID:               vB.id,
		Name:             "orch-rel-vault-" + vB.label,
		Type:             system.VaultTypeFile,
		StorageClass:     harnessStorageClass,
		RotationPolicyID: h.rotationPolicyID,
		CloudServiceID:   &csID,
	}); err != nil {
		t.Fatalf("PutVault %s: %v", vB.label, err)
	}
	placements := make([]system.VaultPlacement, 0, len(vB.nodeIdxs))
	for pos, idx := range vB.nodeIdxs {
		placements = append(placements, system.VaultPlacement{
			StorageID: h.nodes[h.nodeIDs[idx]].fileStorageID.String(),
			Leader:    pos == 0,
		})
	}
	if err := h.cfgStore.SetVaultPlacements(ctx, vB.id, placements); err != nil {
		t.Fatalf("SetVaultPlacements %s: %v", vB.label, err)
	}
	h.vaults = append(h.vaults, vB)
	sys, err := h.cfgStore.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var vaultCfg system.VaultConfig
	for i := range sys.Config.Vaults {
		if sys.Config.Vaults[i].ID == vB.id {
			vaultCfg = sys.Config.Vaults[i]
		}
	}

	// Registration must complete promptly on every node while the endpoint
	// is black: any store dial on the construction path sits inside this
	// call, under the registry write lock, for the SDK's full retry budget.
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		done := make(chan error, 1)
		go func() { done <- n.orch.AddVault(ctx, vaultCfg, n.factories) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("AddVault on %s: %v", n.label, err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("AddVault on %s stalled beyond 20s against a blackholed cloud endpoint: vault construction is dialing the store", n.label)
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
	}

	// The bystander vault's ingest must be untouched: records submitted on a
	// non-home node travel the full pipeline to sealed, queryable GLCBs
	// while vault B's endpoint stays black.
	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(h.nodeIDs[3], total, "blackhole-bystander")
	h.waitSealedRecords(vA, h.nodeIDs[0], total)
	h.waitSearchable(vA, h.nodeIDs[1], total)
}
