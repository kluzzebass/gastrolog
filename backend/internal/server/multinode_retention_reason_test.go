package server_test

import (
	"context"
	"fmt"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// TestMultiNode_RetentionReasonRoutesFromEveryNode pins that `_reason` is
// matched by the cluster-wide routing table on whichever node runs the
// retention sweep: from every data node, retention records of one source
// vault reach the vault named by their reason's route, including vaults
// homed on other nodes.
func TestMultiNode_RetentionReasonRoutesFromEveryNode(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline test")
	}
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithoutVault("coord"))
	ctx := context.Background()

	d1 := h.Node(t, "data-1")
	d2 := h.Node(t, "data-2")
	d3 := h.Node(t, "data-3")
	source := d1.vaultID

	destination := map[chunk.RetentionReason]glid.GLID{
		chunk.RetentionReasonAge:   d2.vaultID,
		chunk.RetentionReasonSize:  d3.vaultID,
		chunk.RetentionReasonCount: d1.vaultID,
	}
	for reason, dest := range destination {
		expr := fmt.Sprintf(`_source="retention" AND _vault=%q AND _reason=%q`, source.String(), reason)
		if err := h.cfgStore.PutRoute(ctx, system.RouteConfig{
			ID:           glid.New(),
			Name:         "retain-" + string(reason),
			Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: expr}}},
			Destinations: []glid.GLID{dest},
			Enabled:      true,
		}); err != nil {
			t.Fatalf("PutRoute %s: %v", reason, err)
		}
	}
	for _, n := range []multinodeTestNode{d1, d2, d3} {
		startMNRouteStatsNode(t, n)
	}

	// Distinct per-reason counts so a misattributed record shows up as a
	// count landing on the wrong destination.
	perReason := map[chunk.RetentionReason]int{
		chunk.RetentionReasonAge:   1,
		chunk.RetentionReasonSize:  2,
		chunk.RetentionReasonCount: 3,
	}
	for _, sweeper := range []multinodeTestNode{d1, d2, d3} {
		for reason, n := range perReason {
			for range n {
				rec := chunk.Record{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("expired")}
				if err := sweeper.orch.SubmitRetentionRecord(ctx, source, rec, reason); err != nil {
					t.Fatalf("submit %s on %s: %v", reason, sweeper.nodeID, err)
				}
			}
		}
		// A retention submit returns only after routing on the submitting
		// node has counted the record, so its counters are final here.
		stats := sweeper.orch.VaultRouteStatsList()
		for reason, dest := range destination {
			var got int64
			if s := stats[dest]; s != nil {
				got = s.Matched
			}
			if want := int64(perReason[reason]); got != want {
				t.Errorf("on %s: _reason=%q destination matched %d, want %d", sweeper.nodeID, reason, got, want)
			}
		}
		if s := sweeper.orch.GetRouteStats(); s.Unmatched != 0 {
			t.Errorf("on %s: %d retention records matched no route", sweeper.nodeID, s.Unmatched)
		}
	}

	// A retention record with no reason matches none of the reason routes.
	rec := chunk.Record{Attrs: chunk.Attributes{"k": "v"}, Raw: []byte("expired")}
	if err := d1.orch.SubmitRetentionRecord(ctx, source, rec, ""); err != nil {
		t.Fatalf("submit without reason: %v", err)
	}
	if s := d1.orch.GetRouteStats(); s.Unmatched != 1 {
		t.Errorf("a retention record without a reason must match no reason route, got Unmatched=%d", s.Unmatched)
	}
}
