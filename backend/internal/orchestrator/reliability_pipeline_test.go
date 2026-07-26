package orchestrator_test

// Rubicon E3 (gastrolog-18f9r): 4+ node cluster acceptance for the V3
// pipeline. These tests run the FULL production write path across real
// in-process nodes: ingest on any node → routing → durable local segment →
// distribution publish through vault-ctl Raft → home-side collection over the
// real PullSegment gRPC transport → leader-planned open-chunk manifest →
// sealed GLCB built on every home → cross-node query.
//
// The harness wiring (withPipelineCluster) mirrors production: a shared
// PeerConns pool per node (static address resolution instead of cluster-ctl Raft),
// the cluster PullSegment server bound to Orchestrator.ServeSegmentPull, and
// vault-ctl apply forwarding for origin publishes from non-leader nodes.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/chatterbox"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/query"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// pipelineTestCompletePolicy completes a working segment on every commit (any
// non-empty segment exceeds 1 byte), so a finite ingest batch fully drains
// into published segments without waiting on size/age thresholds.
var pipelineTestCompletePolicy = segmentation.CompletePolicy{MaxBytes: 1}

// pipelineChunkMaxRecords seals the open-chunk manifest at exactly this many
// records (the planner splits segment refs so a manifest never overshoots),
// making sealed-chunk counts deterministic for record counts that are a
// multiple of it.
const pipelineChunkMaxRecords = 10

// vaultCtlSubFSM returns the vault-ctl sub-FSM for a vault on a node, or nil.
func (h *orchRelHarness) vaultCtlSubFSM(v vaultSpec, nodeID string) *vaultctlfsm.FSM {
	n := h.nodes[nodeID]
	if n == nil || n.groupMgr == nil {
		return nil
	}
	g := n.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(v.id))
	if g == nil {
		return nil
	}
	vfsm, ok := g.FSM.(*vaultraft.FSM)
	if !ok || vfsm == nil {
		return nil
	}
	return vfsm.VaultFSM(v.id)
}

// sealedPipelineChunks returns the vault's Sealed chunk entries in the
// vault-ctl FSM as observed on a node.
func (h *orchRelHarness) sealedPipelineChunks(v vaultSpec, nodeID string) []vaultctlfsm.ManifestEntry {
	sub := h.vaultCtlSubFSM(v, nodeID)
	if sub == nil {
		return nil
	}
	var out []vaultctlfsm.ManifestEntry
	for _, e := range sub.List() {
		if e.State == chunk.ChunkStateSealed {
			out = append(out, e)
		}
	}
	return out
}

// waitSealedRecords waits until a node's FSM shows the vault's sealed chunks
// covering exactly wantRecords records in total, then returns the sealed
// entries. Progress metric: sealed record total + sealed chunk count.
func (h *orchRelHarness) waitSealedRecords(v vaultSpec, nodeID string, wantRecords int64) []vaultctlfsm.ManifestEntry {
	h.t.Helper()
	var entries []vaultctlfsm.ManifestEntry
	what := fmt.Sprintf("vault %s on %s: sealed records reaching exactly %d", v.label, h.nodes[nodeID].label, wantRecords)
	h.waitProgress(what, 50*time.Millisecond, func() (string, bool) {
		entries = h.sealedPipelineChunks(v, nodeID)
		var total int64
		for _, e := range entries {
			total += e.RecordCount
		}
		return fmt.Sprintf("sealed_records=%d sealed_chunks=%d", total, len(entries)), total == wantRecords
	}, func() { h.dumpPipelineState(v) })
	return entries
}

// dumpPipelineState logs per-node vault-ctl FSM pipeline state (published
// segments, chunk entries, open/sealed manifests) for post-mortem on a
// convergence failure.
func (h *orchRelHarness) dumpPipelineState(v vaultSpec) {
	h.t.Helper()
	for i, id := range h.nodeIDs {
		sub := h.vaultCtlSubFSM(v, id)
		if sub == nil {
			h.t.Logf("node-%d: vault-ctl FSM <nil>", i+1)
			continue
		}
		var states []string
		for _, e := range sub.List() {
			states = append(states, fmt.Sprintf("%s=%v/%d", e.ID, e.State, e.RecordCount))
		}
		openInfo := "<nil>"
		if open := sub.OpenChunk(); open != nil {
			openInfo = fmt.Sprintf("chunk=%s records=%d refs=%d", open.ChunkID, open.TotalRecords, len(open.Refs))
		}
		sealedInfo := "<nil>"
		if sm := sub.SealedManifest(); sm != nil {
			sealedInfo = fmt.Sprintf("chunk=%s records=%d", sm.ChunkID, sm.TotalRecords)
		}
		h.t.Logf("node-%d: segments=%d chunks=%v open=%s sealedPending=%s",
			i+1, len(sub.ListCompletedSegments()), states, openInfo, sealedInfo)
	}
}

// waitSealedRecordsAtLeast waits until the vault's sealed chunks cover at
// least wantRecords records (for nondeterministic sources like chatterbox).
// Progress metric: sealed record total.
func (h *orchRelHarness) waitSealedRecordsAtLeast(v vaultSpec, nodeID string, wantRecords int64) int64 {
	h.t.Helper()
	var last int64
	what := fmt.Sprintf("vault %s on %s: sealed records reaching >= %d", v.label, h.nodes[nodeID].label, wantRecords)
	h.waitProgress(what, 50*time.Millisecond, func() (string, bool) {
		last = 0
		for _, e := range h.sealedPipelineChunks(v, nodeID) {
			last += e.RecordCount
		}
		return fmt.Sprintf("sealed_records=%d", last), last >= wantRecords
	}, func() {
		if sub := h.vaultCtlSubFSM(v, nodeID); sub != nil {
			states := map[chunk.ChunkState]int{}
			var total int64
			for _, e := range sub.List() {
				states[e.State]++
				total += e.RecordCount
			}
			h.t.Logf("vault %s on %s FSM census at stall: entries_by_state=%v total_records=%d sealed_manifest=%v open_chunk=%v",
				v.label, h.nodes[nodeID].label, states, total, sub.SealedManifest() != nil, sub.OpenChunk() != nil)
		}
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 1)
		h.t.Logf("goroutine profile at stall:\n%s", stacks.String())
	})
	return last
}

// pipelineGLCBPath is the on-disk location of a pipeline-built sealed GLCB on
// a node: <home>/segments/<vaultID>/chunks/<chunkID>/data.glcb. Mirrors
// originRoot + VaultSpec.ChunkRoot in the orchestrator.
func (h *orchRelHarness) pipelineGLCBPath(nodeID string, v vaultSpec, chunkID chunk.ChunkID) string {
	chunkRoot := h.nodes[nodeID].home + "/segments/" + v.id.String() + "/chunks"
	return chunking.ChunkGLCBPath(chunkRoot, chunkID)
}

// pipelineVaultRoot is the segment staging root for a vault on a node.
func (h *orchRelHarness) pipelineVaultRoot(nodeID string, v vaultSpec) string {
	return h.nodes[nodeID].home + "/segments/" + v.id.String()
}

func countHeadSegmentFiles(root string) (int, error) {
	ids, err := paths.ListSegmentIDs(paths.HeadDir(root))
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

// assertHeadBounded checks head/ does not grow unbounded relative to the
// vault-ctl completed-segment registry (gastrolog-3vlse).
func (h *orchRelHarness) assertHeadBounded(v vaultSpec, homeIdxs []int, registrySegments int, allowance int) {
	h.t.Helper()
	for _, idx := range homeIdxs {
		nodeID := h.nodeIDs[idx]
		root := h.pipelineVaultRoot(nodeID, v)
		headCount, err := countHeadSegmentFiles(root)
		if err != nil {
			h.t.Fatalf("vault %s home %s: list head/: %v", v.label, h.nodes[nodeID].label, err)
		}
		limit := registrySegments + allowance
		if headCount > limit {
			h.t.Fatalf("vault %s home %s: head/ has %d files, want <= registry_segments(%d)+allowance(%d)=%d",
				v.label, h.nodes[nodeID].label, headCount, registrySegments, allowance, limit)
		}
		h.t.Logf("vault %s home %s: head_files=%d registry_segments=%d",
			v.label, h.nodes[nodeID].label, headCount, registrySegments)
	}
}

// waitGLCBsOnHomes blocks until every home node holds a GLCB file for every
// given chunk, then asserts the bytes are identical across homes (the build
// is deterministic, so divergence means a real bug). Progress metric: the
// count of (chunk, home) pairs whose GLCB file exists on disk.
func (h *orchRelHarness) waitGLCBsOnHomes(v vaultSpec, homeIdxs []int, entries []vaultctlfsm.ManifestEntry) {
	h.t.Helper()
	total := len(entries) * len(homeIdxs)
	what := fmt.Sprintf("vault %s: GLCBs for %d chunks on %d homes", v.label, len(entries), len(homeIdxs))
	h.waitProgress(what, 50*time.Millisecond, func() (string, bool) {
		present := 0
		var missing []string
		for _, e := range entries {
			for _, idx := range homeIdxs {
				nodeID := h.nodeIDs[idx]
				if _, err := os.Stat(h.pipelineGLCBPath(nodeID, v, e.ID)); err == nil {
					present++
				} else {
					missing = append(missing, fmt.Sprintf("%s@%s", e.ID, h.nodes[nodeID].label))
				}
			}
		}
		return fmt.Sprintf("glcbs_present=%d/%d missing=%v", present, total, missing), present == total
	}, func() { h.dumpPipelineState(v) })

	// All files exist (GLCBs are renamed into place atomically); assert
	// byte-identity across homes.
	for _, e := range entries {
		var refHash [sha256.Size]byte
		var refNode string
		for _, idx := range homeIdxs {
			nodeID := h.nodeIDs[idx]
			data, err := os.ReadFile(h.pipelineGLCBPath(nodeID, v, e.ID))
			if err != nil {
				h.t.Fatalf("vault %s chunk %s: GLCB unreadable on home %s after convergence: %v",
					v.label, e.ID, h.nodes[nodeID].label, err)
			}
			sum := sha256.Sum256(data)
			if refNode == "" {
				refHash, refNode = sum, h.nodes[nodeID].label
				continue
			}
			if sum != refHash {
				h.t.Fatalf("vault %s chunk %s: GLCB bytes differ between homes %s and %s",
					v.label, e.ID, refNode, h.nodes[nodeID].label)
			}
		}
	}
}

// searchRecords runs a match-all search for the vault on a node and returns
// the raw record bodies.
func (h *orchRelHarness) searchRecords(v vaultSpec, nodeID string) [][]byte {
	h.t.Helper()
	n := h.nodes[nodeID]
	seq, _, err := n.orch.Search(context.Background(), v.id, query.Query{}, nil)
	if err != nil {
		h.t.Fatalf("Search vault %s on %s: %v", v.label, n.label, err)
	}
	var out [][]byte
	for rec, err := range seq {
		if err != nil {
			h.t.Fatalf("Search vault %s on %s: iteration: %v", v.label, n.label, err)
		}
		out = append(out, bytes.Clone(rec.Raw))
	}
	return out
}

// submitIngestRecords pushes count records through the pipeline routing path
// on the given node, waiting for each durability ack. Returns the bodies.
func (h *orchRelHarness) submitIngestRecords(nodeID string, count int, prefix string) map[string]bool {
	h.t.Helper()
	n := h.nodes[nodeID]
	bodies := make(map[string]bool, count)
	for i := range count {
		body := fmt.Sprintf("%s-%03d", prefix, i)
		ack := make(chan error, 1)
		rec := chunk.Record{
			IngestTS: time.Now().UTC(),
			Attrs:    map[string]string{"seq": strconv.Itoa(i)},
			Raw:      []byte(body),
		}
		if err := n.orch.SubmitIngest(context.Background(), rec, ack); err != nil {
			h.t.Fatalf("SubmitIngest %d on %s: %v", i, n.label, err)
		}
		select {
		case err := <-ack:
			if err != nil {
				h.t.Fatalf("SubmitIngest %d on %s: ack: %v", i, n.label, err)
			}
		case <-time.After(orchHarnessStallWindow):
			// A durability ack is a single progress event; waiting the shared
			// stall window on it is the degenerate form of waitProgress.
			h.t.Fatalf("SubmitIngest %d on %s: no ack within stall window %s", i, n.label, orchHarnessStallWindow)
		}
		bodies[body] = true
	}
	return bodies
}

// TestOrchPipeline_ClusterIngestToSealedGLCB is the Rubicon E3 core
// acceptance: on a 4-node cluster with a vault homed on 3 of the 4 nodes,
// records ingested on the NON-home node travel the full pipeline and end as
// byte-identical sealed GLCBs on every home, queryable there.
func TestOrchPipeline_ClusterIngestToSealedGLCB(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	homeIdxs := []int{0, 1, 2}
	ingestNode := h.nodeIDs[3] // not a home for v

	const total = 3 * pipelineChunkMaxRecords
	bodies := h.submitIngestRecords(ingestNode, total, "e3-acceptance")

	// All records land in sealed manifests: the planner seals at exactly
	// pipelineChunkMaxRecords records, and total is a multiple of it.
	entries := h.waitSealedRecords(v, h.nodeIDs[0], total)
	if len(entries) != 3 {
		t.Fatalf("expected 3 sealed chunks, got %d", len(entries))
	}

	// Every home holds every sealed GLCB, byte-identical across homes.
	h.waitGLCBsOnHomes(v, homeIdxs, entries)

	// The ingest node is not a home: no pipeline GLCB may exist there.
	for _, e := range entries {
		if _, err := os.Stat(h.pipelineGLCBPath(ingestNode, v, e.ID)); !os.IsNotExist(err) {
			t.Fatalf("non-home ingest node holds GLCB for chunk %s (err=%v)", e.ID, err)
		}
	}

	// Cross-node query: records ingested on node-4 are served from a home
	// (follower home, not the placement leader) with full fidelity.
	h.waitSearchable(v, h.nodeIDs[1], total)
	got := h.searchRecords(v, h.nodeIDs[1])
	counts := make(map[string]int, len(got))
	for _, raw := range got {
		counts[string(raw)]++
	}
	for body, n := range counts {
		if !bodies[body] {
			t.Errorf("query returned unexpected record %q (x%d)", body, n)
		} else if n != 1 {
			t.Errorf("query returned record %q %d times, want once", body, n)
		}
		delete(bodies, body)
	}
	for body := range bodies {
		t.Errorf("query missing ingested record %q", body)
	}
}

// TestOrchPipeline_PlacementChurnConverges moves a vault home from one node
// to another mid-stream and verifies the next sealed chunk lands on the NEW
// home set without manual repair: the joining home collects the segments it
// never originated (build-time CollectMissing nudge) and builds the GLCB.
func TestOrchPipeline_PlacementChurnConverges(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	ingestNode := h.nodeIDs[3]
	ctx := context.Background()

	// Phase 1: one sealed chunk on the original home set {0,1,2}.
	h.submitIngestRecords(ingestNode, pipelineChunkMaxRecords, "pre-churn")
	first := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	h.waitGLCBsOnHomes(v, []int{0, 1, 2}, first)

	// Phase 2: churn the placement — node-3 leaves the home set, node-4
	// joins. In production the config dispatcher reacts to
	// NotifyVaultPlacementsSet by building/removing the local instance and
	// re-firing the route/pipeline reload on every node; emulate that fan-out
	// here (the dispatcher itself is covered by app-level tests).
	newHomes := []int{0, 1, 3}
	h.setVaultPlacements(v, newHomes)
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if id == h.nodeIDs[3] {
			if err := n.orch.AddVaultInstance(ctx, v.id, n.factories); err != nil {
				t.Fatalf("AddVaultInstance on %s: %v", n.label, err)
			}
		}
		if id == h.nodeIDs[2] {
			n.orch.RemoveVaultInstance(v.id)
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
	}

	// Phase 3: the next chunk converges on the new home set. The ingest node
	// stays node-4 — which is now ALSO a home, exercising the
	// origin-equals-home local-holder path alongside remote collection.
	h.submitIngestRecords(ingestNode, pipelineChunkMaxRecords, "post-churn")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	var second []vaultctlfsm.ManifestEntry
	firstIDs := map[chunk.ChunkID]bool{}
	for _, e := range first {
		firstIDs[e.ID] = true
	}
	for _, e := range entries {
		if !firstIDs[e.ID] {
			second = append(second, e)
		}
	}
	if len(second) != 1 {
		t.Fatalf("expected exactly 1 post-churn sealed chunk, got %d", len(second))
	}
	h.waitGLCBsOnHomes(v, newHomes, second)

	// The departed home must not build the post-churn chunk.
	if _, err := os.Stat(h.pipelineGLCBPath(h.nodeIDs[2], v, second[0].ID)); !os.IsNotExist(err) {
		t.Fatalf("departed home built post-churn GLCB (err=%v)", err)
	}

	// Query on the JOINED home returns the post-churn records.
	h.waitProgress("post-churn records searchable on joined home", 50*time.Millisecond, func() (string, bool) {
		postChurn := 0
		for _, raw := range h.searchRecords(v, h.nodeIDs[3]) {
			if bytes.HasPrefix(raw, []byte("post-churn-")) {
				postChurn++
			}
		}
		return fmt.Sprintf("post_churn_records=%d/%d", postChurn, pipelineChunkMaxRecords),
			postChurn == pipelineChunkMaxRecords
	}, func() { h.dumpPipelineState(v) })
}

// TestOrchPipeline_IngesterReassignmentKeepsFlowing runs a live synthetic
// ingester (chatterbox) through the full seven-stage pipeline, then moves it
// to another node — the orchestrator-level effect of a singleton
// reassignment (the Raft-assignment dispatch itself is covered by app-level
// dispatcher tests). Sealed-chunk progress must continue after the move.
func TestOrchPipeline_IngesterReassignmentKeepsFlowing(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	ingID := glid.New()
	desired := []orchestrator.IngesterDesired{{
		ID:   ingID,
		Name: "chatter",
		Type: "chatterbox",
		Build: func() (ingestion.Ingester, error) {
			return chatterbox.NewIngester(ingID, map[string]string{
				"minInterval": "1ms",
				"maxInterval": "5ms",
			}, slog.New(slog.DiscardHandler))
		},
	}}

	// Run on node-4 (non-home origin) first.
	if err := h.nodes[h.nodeIDs[3]].orch.ReconcileIngesters(desired); err != nil {
		t.Fatalf("start ingester on node-4: %v", err)
	}
	before := h.waitSealedRecordsAtLeast(v, h.nodeIDs[0], pipelineChunkMaxRecords)

	// Reassign: stop on node-4, start on node-2 (a home node).
	if err := h.nodes[h.nodeIDs[3]].orch.ReconcileIngesters(nil); err != nil {
		t.Fatalf("stop ingester on node-4: %v", err)
	}
	if err := h.nodes[h.nodeIDs[1]].orch.ReconcileIngesters(desired); err != nil {
		t.Fatalf("start ingester on node-2: %v", err)
	}

	// Flow continues: sealed records keep growing past the pre-move total by
	// at least one full chunk, proving the new origin publishes and the homes
	// collect from it.
	h.waitSealedRecordsAtLeast(v, h.nodeIDs[0], before+pipelineChunkMaxRecords)

	// Every sealed chunk has its GLCB on all homes.
	entries := h.sealedPipelineChunks(v, h.nodeIDs[0])
	h.waitGLCBsOnHomes(v, []int{0, 1, 2}, entries)
}

// pipelinePlannerHealth summarizes vault-ctl FSM pipeline state on the leader
// for soak assertions (gastrolog-1cedo).
type pipelinePlannerHealth struct {
	RegistrySegments int
	RegistryRecords  int64
	PendingRecords   int64 // registry records not yet fully consumed by planner
	OpenRecords      int64
	OpenRefs         int
	SealedChunks     int
	SealedRecords    int64
}

func pipelinePlannerHealthFromFSM(sub *vaultctlfsm.FSM) pipelinePlannerHealth {
	var h pipelinePlannerHealth
	if sub == nil {
		return h
	}
	for _, entry := range sub.ListCompletedSegments() {
		h.RegistrySegments++
		h.RegistryRecords += int64(entry.RecordCount)
		if next, ok := sub.ResumeRecordNumber(entry.SegmentID); ok {
			if next >= entry.RecordCount {
				continue
			}
			h.PendingRecords += int64(entry.RecordCount) - int64(next)
		} else {
			h.PendingRecords += int64(entry.RecordCount)
		}
	}
	if open := sub.OpenChunk(); open != nil {
		h.OpenRecords = int64(open.TotalRecords)
		h.OpenRefs = len(open.Refs)
	}
	for _, e := range sub.List() {
		if e.State == chunk.ChunkStateSealed {
			h.SealedChunks++
			h.SealedRecords += e.RecordCount
		}
	}
	return h
}

// TestOrchPipeline_SustainedIngestManifestKeepsPace runs steady synthetic
// ingest through the full pipeline and asserts the vault leader's planner
// keeps the open manifest near rotation policy instead of letting millions
// of registry records stall behind a tiny manifest (gastrolog-1cedo /
// regression for gastrolog-3bn3q).
func TestOrchPipeline_SustainedIngestManifestKeepsPace(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline soak test")
	}

	const (
		warmupDuration         = 3 * time.Second
		soakDuration           = 20 * time.Second
		sampleInterval         = 250 * time.Millisecond
		maxPendingRegistryFrac = 98 // percent: nearly all records stuck with no sealing
		minRegistryForRatio    = int64(200)
		minSealedChunksGain    = 2
		minSealedRecordsGain   = int64(2 * pipelineChunkMaxRecords)
	)

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	leaderNode := h.nodeIDs[0]

	ingID := glid.New()
	desired := []orchestrator.IngesterDesired{{
		ID:   ingID,
		Name: "soak-chatter",
		Type: "chatterbox",
		Build: func() (ingestion.Ingester, error) {
			return chatterbox.NewIngester(ingID, map[string]string{
				"minInterval": "1ms",
				"maxInterval": "5ms",
			}, slog.New(slog.DiscardHandler))
		},
	}}

	ingestNode := h.nodeIDs[3]
	if err := h.nodes[ingestNode].orch.ReconcileIngesters(desired); err != nil {
		t.Fatalf("start ingester on %s: %v", h.nodes[ingestNode].label, err)
	}
	t.Cleanup(func() {
		_ = h.nodes[ingestNode].orch.ReconcileIngesters(nil)
	})

	initial := pipelinePlannerHealthFromFSM(h.vaultCtlSubFSM(v, leaderNode))
	initialSealedChunks := initial.SealedChunks
	initialSealedRecords := initial.SealedRecords

	maxPending := initial.PendingRecords
	deadline := time.Now().Add(warmupDuration + soakDuration)
	lastSealedChunks := initialSealedChunks
	lastSealProgress := time.Now()

	for time.Now().Before(deadline) {
		health := pipelinePlannerHealthFromFSM(h.vaultCtlSubFSM(v, leaderNode))
		if health.PendingRecords > maxPending {
			maxPending = health.PendingRecords
		}
		// Live failure mode: registry grows but sealing stalls with ~all records
		// still pending (millions of orphans, ~1 sealed chunk). Detection is
		// stall-based, not calibrated to how fast a healthy soak "should" seal:
		// every sealed-chunk increment resets the stall clock, so slow-under-
		// contention sealing never trips this — only a genuine sealing wedge
		// (no new sealed chunk for the shared stall window while ~all registry
		// records stay pending) does. The end-of-soak waitSealedRecordsAtLeast
		// below catches the same wedge when the soak is shorter than the
		// stall window.
		if health.SealedChunks != lastSealedChunks {
			lastSealedChunks = health.SealedChunks
			lastSealProgress = time.Now()
		}
		if health.RegistryRecords >= minRegistryForRatio &&
			time.Since(lastSealProgress) >= orchHarnessStallWindow {
			pendingPct := health.PendingRecords * 100 / health.RegistryRecords
			if pendingPct >= maxPendingRegistryFrac {
				h.dumpPipelineState(v)
				t.Fatalf("vault %s: sealing stalled for %s with %d%% of registry records pending (%d/%d, sealed chunks stuck at %d); "+
					"open_records=%d open_refs=%d",
					v.label, time.Since(lastSealProgress).Round(time.Millisecond), pendingPct,
					health.PendingRecords, health.RegistryRecords, health.SealedChunks,
					health.OpenRecords, health.OpenRefs)
			}
		}
		time.Sleep(sampleInterval)
	}

	gotSealed := h.waitSealedRecordsAtLeast(v, leaderNode, initialSealedRecords+minSealedRecordsGain)
	final := pipelinePlannerHealthFromFSM(h.vaultCtlSubFSM(v, leaderNode))
	if gain := final.SealedChunks - initialSealedChunks; gain < minSealedChunksGain {
		h.dumpPipelineState(v)
		t.Fatalf("vault %s: sealed chunks gained %d in %s, want >= %d (initial=%d final=%d sealed_records=%d)",
			v.label, gain, warmupDuration+soakDuration, minSealedChunksGain, initialSealedChunks, final.SealedChunks, gotSealed)
	}
	if gain := gotSealed - initialSealedRecords; gain < minSealedRecordsGain {
		h.dumpPipelineState(v)
		t.Fatalf("vault %s: sealed records gained %d in %s, want >= %d (initial=%d final=%d)",
			v.label, gain, warmupDuration+soakDuration, minSealedRecordsGain, initialSealedRecords, gotSealed)
	}

	// Every sealed chunk should materialize on all homes.
	entries := h.sealedPipelineChunks(v, leaderNode)
	h.waitGLCBsOnHomes(v, []int{0, 1, 2}, entries)

	const headAllowance = 8 // working/pre-head + in-flight segments
	h.assertHeadBounded(v, []int{0, 1, 2}, final.RegistrySegments, headAllowance)

	t.Logf("vault %s soak ok: max_pending=%d sealed_chunks %d→%d sealed_records %d→%d registry_segments=%d registry_records=%d",
		v.label, maxPending,
		initialSealedChunks, final.SealedChunks,
		initialSealedRecords, gotSealed,
		final.RegistrySegments, final.RegistryRecords)
}
