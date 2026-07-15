package raftwal

// Coverage for gastrolog-1io54g: submit latency accounting on the shared WAL.

import (
	"testing"

	hraft "github.com/hashicorp/raft"
)

func TestAppendLatencyAccounting(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("g1")
	baseCount, _ := w.AppendTotals() // group registration itself submits
	for i := uint64(1); i <= 5; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}

	count, totalNanos := w.AppendTotals()
	if count != baseCount+5 {
		t.Fatalf("count = %d, want %d", count, baseCount+5)
	}
	if totalNanos == 0 {
		t.Fatal("totalNanos = 0, want > 0")
	}
	max1 := w.TakeMaxAppendLatency()
	if max1 == 0 {
		t.Fatal("max = 0, want > 0")
	}
	// Max resets on take; totals do not.
	if again := w.TakeMaxAppendLatency(); again != 0 {
		t.Fatalf("max after take = %d, want 0 (reset on read)", again)
	}
	count2, _ := w.AppendTotals()
	if count2 != count {
		t.Fatalf("count after take = %d, want %d (totals are cumulative)", count2, count)
	}
}
