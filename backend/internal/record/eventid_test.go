package record

import (
	"slices"
	"testing"
	"time"

	"gastrolog/internal/glid"
)

func TestEventIDCompareTotalOrder(t *testing.T) {
	t.Parallel()

	ts0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ts1 := ts0.Add(time.Second)

	nodeA := glid.New()
	nodeB := glid.New()
	ingA := glid.New()
	ingB := glid.New()

	ids := []EventID{
		{IngesterID: ingA, NodeID: nodeA, IngestTS: ts0, IngestSeq: 0},
		{IngesterID: ingA, NodeID: nodeA, IngestTS: ts0, IngestSeq: 1},
		{IngesterID: ingA, NodeID: nodeB, IngestTS: ts0, IngestSeq: 0},
		{IngesterID: ingB, NodeID: nodeA, IngestTS: ts0, IngestSeq: 0},
		{IngesterID: ingA, NodeID: nodeA, IngestTS: ts1, IngestSeq: 0},
	}

	got := slices.Clone(ids)
	slices.SortFunc(got, func(a, b EventID) int { return a.Compare(b) })

	for i := 1; i < len(got); i++ {
		if got[i-1].Compare(got[i]) >= 0 {
			t.Fatalf("sort order broken at %d: %v vs %v", i, got[i-1], got[i])
		}
		if !got[i-1].Less(got[i]) {
			t.Fatalf("Less inconsistent at %d", i)
		}
	}

	for _, id := range ids {
		if id.Compare(id) != 0 {
			t.Fatalf("self-compare: %v", id)
		}
		if id.Less(id) {
			t.Fatalf("self-Less: %v", id)
		}
	}
}

func TestEventIDCompareFieldPrecedence(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	base := EventID{
		IngesterID: glid.New(),
		NodeID:     glid.New(),
		IngestTS:   ts,
		IngestSeq:  5,
	}

	olderTS := base
	olderTS.IngestTS = ts.Add(-time.Nanosecond)
	if base.Compare(olderTS) <= 0 {
		t.Fatal("IngestTS should dominate order")
	}

	otherNode := base
	otherNode.NodeID = glid.New()
	if base.Compare(otherNode) == 0 {
		t.Fatal("NodeID should break ties on IngestTS")
	}

	otherIngester := base
	otherIngester.IngesterID = glid.New()
	if base.Compare(otherIngester) == 0 {
		t.Fatal("IngesterID should break ties on IngestTS and NodeID")
	}

	otherSeq := base
	otherSeq.IngestSeq = base.IngestSeq + 1
	if base.Compare(otherSeq) >= 0 {
		t.Fatal("IngestSeq should break ties on timestamp and GLIDs")
	}
}
