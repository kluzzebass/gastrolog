package orchestrator_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// addCloudCacheVault writes a cloud-backed pipeline vault bound to a "memory"
// cloud service, with the given cache policy, placed on nodeIdxs (first
// is leader), routes every record to it, and drives the dispatcher-equivalent
// fan-out on every node.
func (h *orchRelHarness) addCloudCacheVault(label string, nodeIdxs []int, cacheBudget string) vaultSpec {
	h.t.Helper()
	v := h.putCloudVault(label, cacheBudget)
	v.nodeIdxs = nodeIdxs
	h.setVaultPlacements(v, nodeIdxs)
	h.vaults = append(h.vaults, v)
	h.addVaultOnEveryNode(v)
	return v
}

// putCloudVault writes a cloud-backed pipeline vault bound to a "memory" cloud
// service, with the given cache policy and no placement, and routes every
// record to it.
func (h *orchRelHarness) putCloudVault(label, cacheBudget string) vaultSpec {
	h.t.Helper()
	ctx := context.Background()
	csID := glid.New()
	if err := h.cfgStore.PutCloudService(ctx, system.CloudService{
		ID: csID, Name: "orch-rel-memory-" + label, Provider: "memory",
	}); err != nil {
		h.t.Fatalf("PutCloudService: %v", err)
	}
	v := vaultSpec{label: label, id: glid.New()}
	if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
		Enabled:          true,
		ID:               v.id,
		Name:             "orch-rel-vault-" + label,
		Type:             system.VaultTypeFile,
		StorageClass:     harnessStorageClass,
		RotationPolicyID: h.rotationPolicyID,
		CloudServiceID:   &csID,
		CacheEviction:    "lru",
		CacheBudget:      cacheBudget,
	}); err != nil {
		h.t.Fatalf("PutVault %s: %v", label, err)
	}
	h.addMatchAllRoute(v)
	return v
}

// addVaultOnEveryNode drives the dispatcher-equivalent fan-out of a vault
// written to the config store: AddVault and a routing reload on every node.
func (h *orchRelHarness) addVaultOnEveryNode(v vaultSpec) {
	h.t.Helper()
	ctx := context.Background()
	sys, err := h.cfgStore.Load(ctx)
	if err != nil {
		h.t.Fatalf("Load: %v", err)
	}
	var vaultCfg system.VaultConfig
	for i := range sys.Config.Vaults {
		if sys.Config.Vaults[i].ID == v.id {
			vaultCfg = sys.Config.Vaults[i]
		}
	}
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if err := n.orch.AddVault(ctx, vaultCfg, n.factories); err != nil {
			h.t.Fatalf("AddVault on %s: %v", n.label, err)
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			h.t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
	}
}

// TestOrchPipeline_CloudCachedCopyEvictsOnEveryHome drives the cache
// policy on a real 4-node cluster: a cloud-backed vault with RF 3 and a cache
// budget no chunk fits. Every home — not only the uploading placement leader —
// must drop its pipeline GLCB of every uploaded chunk, withdraw its holder
// receipt for it, and still serve every record by re-fetching from the shared
// bucket. Segment release must not be pinned by the withdrawn receipts.
func TestOrchPipeline_CloudCachedCopyEvictsOnEveryHome(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	t.Parallel()

	h := newOrchRelHarness(t, 4,
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
		withSharedCloudStore(blobstore.NewMemory()),
	)
	homes := []int{0, 1, 2}
	v := h.addCloudCacheVault("C", homes, "1B")

	const total = 3 * pipelineChunkMaxRecords
	h.submitIngestRecords(h.nodeIDs[3], total, "cloud-cache")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], total)

	h.waitProgress("every sealed chunk uploaded, evicted on every home, receipts withdrawn",
		100*time.Millisecond, func() (string, bool) {
			sub := h.vaultCtlSubFSM(v, h.nodeIDs[0])
			if sub == nil {
				return "no vault-ctl FSM", false
			}
			var uploaded, held, onDisk int
			for _, want := range entries {
				e := sub.Get(want.ID)
				if e == nil {
					continue
				}
				if e.CloudBacked {
					uploaded++
				}
				held += len(e.Holders)
				for _, idx := range homes {
					if _, err := os.Stat(h.pipelineGLCBPath(h.nodeIDs[idx], v, e.ID)); err == nil {
						onDisk++
					}
				}
			}
			done := uploaded == len(entries) && held == 0 && onDisk == 0
			return fmt.Sprintf("uploaded=%d/%d holder_receipts=%d pipeline_glcbs=%d",
				uploaded, len(entries), held, onDisk), done
		}, func() { h.dumpPipelineState(v) })

	// Every home reads every record back through the uncached path while
	// eviction keeps running. (The harness has no cross-node search
	// forwarding, so the non-home node is not queried.)
	for _, idx := range homes {
		h.waitSearchable(v, h.nodeIDs[idx], total)
	}

	// Zero chunk holders must not pin the segments: an uploaded chunk is the
	// durable copy that supersedes them.
	h.waitRegistryDrained(v, homes)

	residency := h.nodes[h.nodeIDs[1]].orch.ChunkResidency(v.id, entries[0].ID)
	if len(residency) != 0 {
		t.Fatalf("residency of an evicted cloud-backed chunk = %v, want none", residency)
	}
}
