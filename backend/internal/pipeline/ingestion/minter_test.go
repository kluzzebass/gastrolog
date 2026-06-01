package ingestion

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
)

func TestMinterMonotonicIngestSeq(t *testing.T) {
	t.Parallel()

	ingesterID := glid.New()
	nodeID := glid.New()
	m := NewMinter(ingesterID, nodeID)

	before := time.Now().UTC()
	for want := range uint32(100) {
		id := m.Mint()
		if id.NodeID != nodeID {
			t.Fatalf("node ID = %v, want %v", id.NodeID, nodeID)
		}
		if id.IngesterID != ingesterID {
			t.Fatalf("ingester ID = %v, want %v", id.IngesterID, ingesterID)
		}
		if id.IngestTS.Before(before) {
			t.Fatalf("IngestTS %v before mint window %v", id.IngestTS, before)
		}
		if id.IngestSeq != want {
			t.Fatalf("IngestSeq = %d, want %d", id.IngestSeq, want)
		}
	}
}

func TestMinterIndependentInstances(t *testing.T) {
	t.Parallel()

	nodeID := glid.New()
	mA := NewMinter(glid.New(), nodeID)
	mB := NewMinter(glid.New(), nodeID)

	a0 := mA.Mint()
	b0 := mB.Mint()
	a1 := mA.Mint()

	if a0.IngestSeq != 0 || b0.IngestSeq != 0 || a1.IngestSeq != 1 {
		t.Fatalf("seq not independent per instance: a0=%d b0=%d a1=%d", a0.IngestSeq, b0.IngestSeq, a1.IngestSeq)
	}
	if a0.Compare(b0) == 0 || a0.Compare(a1) == 0 {
		t.Fatal("distinct mints must produce distinct EventIDs")
	}
}

func TestMinterConcurrent(t *testing.T) {
	t.Parallel()

	m := NewMinter(glid.New(), glid.New())

	const workers = 8
	const perWorker = 250
	ids := make([]uint32, workers*perWorker)

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range perWorker {
				id := m.Mint()
				ids[id.IngestSeq]++
			}
		}()
	}
	wg.Wait()

	for seq, count := range ids {
		if count != 1 {
			t.Fatalf("IngestSeq %d seen %d times, want 1", seq, count)
		}
	}
}
