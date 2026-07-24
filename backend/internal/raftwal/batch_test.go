package raftwal

// Coverage for gastrolog-53lk2 (batched StoreLogs): a StoreLogs call is ONE
// submit through the batch writer and ONE WAL record, so an N-entry batch is
// atomic on disk — replay after a torn or corrupted write never surfaces a
// half-applied batch.

import (
	"fmt"
	"os"
	"sync"
	"testing"

	hraft "github.com/hashicorp/raft"
)

func makeBatch(lo, hi uint64, term uint64, payloadLen int) []*hraft.Log {
	logs := make([]*hraft.Log, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		data := make([]byte, payloadLen)
		for j := range data {
			data[j] = byte(i + uint64(j))
		}
		logs = append(logs, &hraft.Log{Index: i, Term: term, Type: hraft.LogCommand, Data: data})
	}
	return logs
}

func TestStoreLogsSingleSubmitPerBatch(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("batch-submit")
	baseCount, _ := w.AppendTotals() // group registration submits once

	if err := gs.StoreLogs(makeBatch(1, 100, 1, 32)); err != nil {
		t.Fatal(err)
	}
	count, _ := w.AppendTotals()
	if count != baseCount+1 {
		t.Fatalf("StoreLogs(100) made %d submits, want 1", count-baseCount)
	}

	last, _ := gs.LastIndex()
	if last != 100 {
		t.Fatalf("last=%d want 100", last)
	}
}

func TestStoreLogsEmptyBatchNoop(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("batch-empty")
	baseCount, _ := w.AppendTotals()

	if err := gs.StoreLogs(nil); err != nil {
		t.Fatalf("StoreLogs(nil): %v", err)
	}
	if err := gs.StoreLogs([]*hraft.Log{}); err != nil {
		t.Fatalf("StoreLogs(empty): %v", err)
	}

	count, _ := w.AppendTotals()
	if count != baseCount {
		t.Fatalf("empty batches submitted %d ops, want 0", count-baseCount)
	}
	first, _ := gs.FirstIndex()
	last, _ := gs.LastIndex()
	if first != 0 || last != 0 {
		t.Fatalf("first=%d last=%d after empty batches, want 0/0", first, last)
	}
}

func TestStoreLogsBatchRoundTripAndReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	gs := w.GroupStore("batch-rt")
	logs := makeBatch(1, 50, 3, 64)
	logs[10].Extensions = []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := gs.StoreLogs(logs); err != nil {
		t.Fatal(err)
	}

	verify := func(gs *GroupStore, phase string) {
		t.Helper()
		first, _ := gs.FirstIndex()
		last, _ := gs.LastIndex()
		if first != 1 || last != 50 {
			t.Fatalf("%s: first=%d last=%d, want 1..50", phase, first, last)
		}
		var got hraft.Log
		for i := uint64(1); i <= 50; i++ {
			if err := gs.GetLog(i, &got); err != nil {
				t.Fatalf("%s: GetLog(%d): %v", phase, i, err)
			}
			want := logs[i-1]
			if got.Index != want.Index || got.Term != want.Term || got.Type != want.Type ||
				string(got.Data) != string(want.Data) || string(got.Extensions) != string(want.Extensions) {
				t.Fatalf("%s: log %d mismatch: got %+v want %+v", phase, i, got, want)
			}
		}
	}
	verify(gs, "live")

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	verify(w2.GroupStore("batch-rt"), "replayed")
}

// A torn tail inside a batch record must drop the WHOLE batch on replay —
// earlier records survive, and no prefix of the torn batch is applied.
func TestStoreLogsTornTailDropsWholeBatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	gs := w.GroupStore("batch-torn")
	if err := gs.StoreLogs(makeBatch(1, 3, 1, 40)); err != nil {
		t.Fatal(err)
	}
	if err := gs.StoreLogs(makeBatch(4, 8, 1, 40)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Tear the tail of the newest segment: the last record is the 4..8
	// batch; cutting into it must invalidate its CRC.
	seg := newestSegmentPath(t, dir)
	fi, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	truncateFileTail(t, seg, fi.Size()-5)

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	gs2 := w2.GroupStore("batch-torn")
	last, _ := gs2.LastIndex()
	if last != 3 {
		t.Fatalf("last=%d after torn batch, want 3 (first batch only)", last)
	}
	var log hraft.Log
	for i := uint64(1); i <= 3; i++ {
		if err := gs2.GetLog(i, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", i, err)
		}
	}
	// NO entry of the torn batch may survive — a half-applied batch would
	// surface indices 4.. here.
	for i := uint64(4); i <= 8; i++ {
		if err := gs2.GetLog(i, &log); err != hraft.ErrLogNotFound {
			t.Fatalf("GetLog(%d): want ErrLogNotFound (torn batch dropped whole), got %v", i, err)
		}
	}
}

// A flipped byte anywhere in a batch payload fails the record CRC and drops
// the whole batch — never a partial application.
func TestStoreLogsCorruptedBatchDropsWholeBatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("batch-corrupt")
	if err := gs.StoreLogs(makeBatch(1, 5, 1, 100)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte near the end of the file — well inside the batch record's
	// payload (the batch is the last and by far the largest record).
	seg := newestSegmentPath(t, dir)
	data, err := os.ReadFile(seg) //nolint:gosec // G304: test-controlled path
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-50] ^= 0xFF
	if err := os.WriteFile(seg, data, 0o644); err != nil { //nolint:gosec // G306: test file
		t.Fatal(err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	gs2 := w2.GroupStore("batch-corrupt")
	first, _ := gs2.FirstIndex()
	last, _ := gs2.LastIndex()
	if first != 0 || last != 0 {
		t.Fatalf("first=%d last=%d after corrupted batch, want 0/0 (whole batch dropped)", first, last)
	}
	var log hraft.Log
	for i := uint64(1); i <= 5; i++ {
		if err := gs2.GetLog(i, &log); err != hraft.ErrLogNotFound {
			t.Fatalf("GetLog(%d): want ErrLogNotFound, got %v", i, err)
		}
	}
}

// A batch record larger than the segment target still lands as one atomic
// record (on a fresh segment), and survives replay.
func TestStoreLogsHugeBatchExceedsSegmentTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{SegmentTargetSize: 4096}
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	gs := w.GroupStore("batch-huge")
	// Seed so the active segment is non-empty and rotation logic engages.
	if err := gs.StoreLog(&hraft.Log{Index: 1, Term: 1, Data: []byte("seed")}); err != nil {
		t.Fatal(err)
	}
	// ~200 entries x ~85B encoded ≈ 17KB record — several times the target.
	if err := gs.StoreLogs(makeBatch(2, 201, 2, 60)); err != nil {
		t.Fatal(err)
	}
	// And another huge batch to exercise rotation directly after an
	// oversized segment.
	if err := gs.StoreLogs(makeBatch(202, 401, 2, 60)); err != nil {
		t.Fatal(err)
	}

	verify := func(gs *GroupStore, phase string) {
		t.Helper()
		first, _ := gs.FirstIndex()
		last, _ := gs.LastIndex()
		if first != 1 || last != 401 {
			t.Fatalf("%s: first=%d last=%d, want 1..401", phase, first, last)
		}
		var log hraft.Log
		for i := uint64(1); i <= 401; i++ {
			if err := gs.GetLog(i, &log); err != nil {
				t.Fatalf("%s: GetLog(%d): %v", phase, i, err)
			}
			if log.Index != i {
				t.Fatalf("%s: GetLog(%d) returned index %d", phase, i, log.Index)
			}
		}
	}
	verify(gs, "live")

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	verify(w2.GroupStore("batch-huge"), "replayed")
}

// Concurrent batched StoreLogs across groups stay isolated and ordered.
func TestStoreLogsBatchMultiGroupConcurrent(t *testing.T) {
	t.Parallel()
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const numGroups = 8
	const batches = 10
	const perBatch = 20

	var wg sync.WaitGroup
	errs := make(chan error, numGroups)
	for g := range numGroups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gs := w.GroupStore(fmt.Sprintf("batch-mg-%d", g))
			for b := range batches {
				lo := uint64(b*perBatch + 1)
				hi := lo + perBatch - 1
				if err := gs.StoreLogs(makeBatch(lo, hi, uint64(g+1), 24)); err != nil {
					errs <- fmt.Errorf("group %d batch %d: %w", g, b, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for g := range numGroups {
		gs := w.GroupStore(fmt.Sprintf("batch-mg-%d", g))
		first, _ := gs.FirstIndex()
		last, _ := gs.LastIndex()
		if first != 1 || last != batches*perBatch {
			t.Fatalf("group %d: first=%d last=%d, want 1..%d", g, first, last, batches*perBatch)
		}
		var log hraft.Log
		for i := uint64(1); i <= batches*perBatch; i++ {
			if err := gs.GetLog(i, &log); err != nil {
				t.Fatalf("group %d GetLog(%d): %v", g, i, err)
			}
			if log.Term != uint64(g+1) {
				t.Fatalf("group %d GetLog(%d): term=%d want %d (cross-group leak?)", g, i, log.Term, g+1)
			}
		}
	}
}
