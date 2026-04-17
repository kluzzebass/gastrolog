package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

func TestPeerState_Delete(t *testing.T) {
	ps := NewPeerState(time.Minute)
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	ps.Update("node-b", &gastrologv1.NodeStats{}, time.Now())

	if got := ps.Get("node-a"); got == nil {
		t.Fatal("precondition: node-a should be present")
	}

	ps.Delete("node-a")

	if got := ps.Get("node-a"); got != nil {
		t.Errorf("Get(node-a) after Delete: want nil, got %v", got)
	}
	if got := ps.Get("node-b"); got == nil {
		t.Error("Get(node-b): unrelated entry was removed")
	}
}

// TestPeerState_Delete_Missing verifies Delete on an unknown node is a no-op,
// not a panic.
func TestPeerState_Delete_Missing(t *testing.T) {
	ps := NewPeerState(time.Minute)
	ps.Delete("never-existed") // must not panic
}

// TestPeerState_Delete_SurvivesLaterUpdate verifies that after Delete, a
// subsequent Update for the same node restores it (contrast with
// MarkUnreachable which is also restored by later updates — these are both
// idempotent from a data-freshness perspective).
func TestPeerState_Delete_SurvivesLaterUpdate(t *testing.T) {
	ps := NewPeerState(time.Minute)
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	ps.Delete("node-a")
	ps.Update("node-a", &gastrologv1.NodeStats{}, time.Now())
	if got := ps.Get("node-a"); got == nil {
		t.Error("Update after Delete should restore the entry")
	}
}

func TestPeerJobState_Delete(t *testing.T) {
	pjs := NewPeerJobState(time.Minute)
	pjs.Update("node-a", []*gastrologv1.Job{{Id: []byte("j1")}}, time.Now())
	pjs.Update("node-b", []*gastrologv1.Job{{Id: []byte("j2")}}, time.Now())

	all := pjs.GetAll()
	if _, ok := all["node-a"]; !ok {
		t.Fatal("precondition: node-a should be present")
	}

	pjs.Delete("node-a")

	all = pjs.GetAll()
	if _, ok := all["node-a"]; ok {
		t.Error("GetAll after Delete(node-a): entry still present")
	}
	if _, ok := all["node-b"]; !ok {
		t.Error("GetAll after Delete(node-a): unrelated entry was removed")
	}
}

// TestPeerJobState_Delete_Missing verifies Delete on an unknown node is a
// no-op.
func TestPeerJobState_Delete_Missing(t *testing.T) {
	pjs := NewPeerJobState(time.Minute)
	pjs.Delete("never-existed") // must not panic
}
