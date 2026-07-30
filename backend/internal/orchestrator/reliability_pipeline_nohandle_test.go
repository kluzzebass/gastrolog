package orchestrator_test

// Multi-node coverage for the vault-ctl no-handle startup window.
// A route can land on a node before that node's vault-ctl group for
// the destination vault exists (dispatcher fan-out has no cross-concern
// ordering guarantee), leaving the node an Origin with the fail-closed
// noHandlePublisher. Records ingested during that window are durable local
// segments whose publishes are refused; when the group lands and the route
// reload re-registers the vault with the real VaultCtlPublisher, every
// refused segment must republish — retry drain plus the registration's
// stranded rescan — and travel the full pipeline to sealed, queryable GLCBs
// on the homes. Before the fix the noop publisher swallowed those publishes
// as successes and the upgrade had nothing to republish: durable segments
// invisible to the cluster until the origin restarted.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/system"
)

// addRuntimeVault writes a new file-backed pipeline vault to the shared
// config store mid-test — placements on the given node indexes (first is
// leader), the shared rotation policy — and returns its vaultSpec plus the
// composed VaultConfig (with placements) as each node's dispatcher would see
// it. The caller drives the dispatcher-equivalent fan-out (AddVault /
// ReloadFilters) per node, in whatever order the scenario needs.
func (h *orchRelHarness) addRuntimeVault(label string, nodeIdxs []int) (vaultSpec, system.VaultConfig) {
	h.t.Helper()
	ctx := context.Background()
	v := vaultSpec{label: label, id: glid.New(), nodeIdxs: nodeIdxs}
	if err := h.cfgStore.PutVault(ctx, system.VaultConfig{
		ID:               v.id,
		Name:             "orch-rel-vault-" + label,
		Type:             system.VaultTypeFile,
		StorageClass:     harnessStorageClass,
		RotationPolicyID: h.rotationPolicyID,
	}); err != nil {
		h.t.Fatalf("PutVault %s: %v", label, err)
	}
	placements := make([]system.VaultPlacement, 0, len(nodeIdxs))
	for pos, idx := range nodeIdxs {
		n := h.nodes[h.nodeIDs[idx]]
		placements = append(placements, system.VaultPlacement{
			StorageID: n.fileStorageID.String(),
			Leader:    pos == 0,
		})
	}
	if err := h.cfgStore.SetVaultPlacements(ctx, v.id, placements); err != nil {
		h.t.Fatalf("SetVaultPlacements %s: %v", label, err)
	}
	h.vaults = append(h.vaults, v)

	sys, err := h.cfgStore.Load(ctx)
	if err != nil {
		h.t.Fatalf("Load after PutVault %s: %v", label, err)
	}
	for i := range sys.Config.Vaults {
		if sys.Config.Vaults[i].ID == v.id {
			return v, sys.Config.Vaults[i]
		}
	}
	h.t.Fatalf("vault %s missing from composed config after PutVault", label)
	return v, system.VaultConfig{}
}

// addMatchAllRoute writes an enabled match-all route targeting the vault.
func (h *orchRelHarness) addMatchAllRoute(v vaultSpec) {
	h.t.Helper()
	if err := h.cfgStore.PutRoute(context.Background(), system.RouteConfig{
		ID:   glid.New(),
		Name: "orch-rel-route-" + v.label,
		Stages: []system.RouteStage{
			{Match: &system.MatchStage{Expression: "*"}},
		},
		Destinations: []glid.GLID{v.id},
		Enabled:      true,
	}); err != nil {
		h.t.Fatalf("PutRoute %s: %v", v.label, err)
	}
}

// countCompletedRecords sums RecordCount over the completed/ segments under a
// vault staging root — the durable on-disk truth of what the origin holds.
func countCompletedRecords(root string) (int64, error) {
	ids, err := paths.ListSegmentIDs(paths.CompletedDir(root))
	if err != nil {
		return 0, err
	}
	var total int64
	for id := range ids {
		hdr, err := segment.ReadHeader(paths.CompletedSegment(root, id))
		if err != nil {
			return 0, err
		}
		total += int64(hdr.RecordCount)
	}
	return total, nil
}

// TestOrchPipeline_NoHandleWindowRepublishesAfterUpgrade drives the window on
// a real 4-node cluster: vault B arrives at runtime, homes get the full
// dispatcher fan-out, the ingest node gets ONLY the route reload (no vault-ctl
// group yet), records flow during the window, and the late group arrival plus
// route reload must make every windowed record sealed and queryable — no
// restart, no completed-channel overflow, and no record duplicated by the
// overlapping republish paths.
func TestOrchPipeline_NoHandleWindowRepublishesAfterUpgrade(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	ctx := context.Background()
	homeIdxs := []int{0, 1, 2}
	ingestNode := h.nodeIDs[3]

	v, vaultCfg := h.addRuntimeVault("B", homeIdxs)
	h.addMatchAllRoute(v)

	// Homes get the full fan-out: AddVault (registers the vault, creates the
	// vault-ctl group with symmetric seeding across all four nodes, builds
	// the local instance) then the route reload.
	for _, idx := range homeIdxs {
		n := h.nodes[h.nodeIDs[idx]]
		if err := n.orch.AddVault(ctx, vaultCfg, n.factories); err != nil {
			t.Fatalf("AddVault on %s: %v", n.label, err)
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
	}
	// The ingest node gets ONLY the route reload: Origin registered, no
	// vault-ctl group — the no-handle window is open.
	if err := h.nodes[ingestNode].orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters on ingest node: %v", err)
	}
	if h.vaultCtlSubFSM(v, ingestNode) != nil {
		t.Fatal("test precondition broken: ingest node already has a vault-ctl group for vault B")
	}

	// Ingest during the window. Each ack is a durable local segment commit;
	// every publish is refused fail-closed.
	const total = 2 * pipelineChunkMaxRecords
	bodies := h.submitIngestRecords(ingestNode, total, "no-handle-window")

	// Durable truth on the origin: all windowed records sit in completed/.
	root := h.pipelineVaultRoot(ingestNode, v)
	h.waitProgress("origin completed/ holding all windowed records", 25*time.Millisecond, func() (string, bool) {
		n, err := countCompletedRecords(root)
		if err != nil {
			return fmt.Sprintf("completed_records=? err=%v", err), false
		}
		return fmt.Sprintf("completed_records=%d/%d", n, total), n == total
	}, func() { h.dumpPipelineState(v) })

	// Nothing may have reached the registry during the window — a fail-closed
	// refusal is not a publish.
	if sub := h.vaultCtlSubFSM(v, h.nodeIDs[0]); sub != nil {
		if entries := sub.ListCompletedSegments(); len(entries) != 0 {
			t.Fatalf("registry holds %d segments during the no-handle window, want 0", len(entries))
		}
	}

	// The window closes: the vault fan-out reaches the ingest node (AddVault
	// creates its vault-ctl group; no local placement, so metadata only) and
	// the route reload production re-fires re-registers the pipeline vault
	// with the real publisher.
	n4 := h.nodes[ingestNode]
	if err := n4.orch.AddVault(ctx, vaultCfg, n4.factories); err != nil {
		t.Fatalf("AddVault on ingest node: %v", err)
	}
	if err := n4.orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters on ingest node after group arrival: %v", err)
	}

	// Every windowed record becomes sealed and queryable on the homes.
	entries := h.waitSealedRecords(v, h.nodeIDs[0], total)
	h.waitGLCBsOnHomes(v, homeIdxs, entries)
	h.waitSearchable(v, h.nodeIDs[1], total)

	// Exactly-once: the overlapping republish paths (retry drain + stranded
	// rescan on re-registration) must not duplicate a record.
	counts := make(map[string]int)
	for _, raw := range h.searchRecords(v, h.nodeIDs[1]) {
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
		t.Errorf("query missing windowed record %q", body)
	}
}
