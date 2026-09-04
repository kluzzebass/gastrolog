package server_test

import (
	"fmt"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// A filtered aggregation from a coordinator that does not hold the vault must
// reach the remote in a form its parser accepts and return the remote's
// answer. The coordinator used to forward the filter twice — once ahead of
// the directives and once as the first pipe segment — and the remote rejected
// the second copy as an unknown pipe operator.
func TestMultiNode_FilteredPipelineFansOutToARemoteVault(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithoutVault("coord"))
	data := h.Node(t, "data-1")
	t0 := time.Now().Add(-time.Minute)
	addMNRecordsFromTime(t, data, "bad", 7, t0, map[string]string{"level": "error"})
	addMNRecordsFromTime(t, data, "fine", 5, t0.Add(10*time.Second), map[string]string{"level": "info"})

	// Premise: the unfiltered aggregation already fans out and sees everything.
	if got := singleCount(t, searchTable(t, h.client, "| stats count")); got != "12" {
		t.Fatalf("unfiltered count = %s, want 12", got)
	}

	if got := singleCount(t, searchTable(t, h.client, "level=error | stats count")); got != "7" {
		t.Fatalf("filtered count = %s, want 7", got)
	}
	vaultScoped := fmt.Sprintf("vault_id=%s level=error | stats count", data.vaultID)
	if got := singleCount(t, searchTable(t, h.client, vaultScoped)); got != "7" {
		t.Fatalf("vault-scoped filtered count = %s, want 7", got)
	}
	byLevel := tableToMap(t, searchTable(t, h.client, "level=error OR level=info | stats count by level"), "level", "count")
	if byLevel["error"] != "7" || byLevel["info"] != "5" {
		t.Fatalf("count by level = %v, want error=7 info=5", byLevel)
	}
}

func singleCount(t *testing.T, table *gastrologv1.TableResult) string {
	t.Helper()
	if table == nil || len(table.Rows) != 1 || len(table.Rows[0].Values) != 1 {
		t.Fatalf("expected a one-cell count table, got %v", table)
	}
	return table.Rows[0].Values[0]
}
