package raftwal

// High-signal contract tests for raftwal: reference-model equivalence, full
// index scans after compaction/reopen, and concurrent stable+log stress.
// Run with: go test ./internal/raftwal -race

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	hraft "github.com/hashicorp/raft"
)

// refLogStore mirrors production log index bounds and DeleteRange semantics
// (hashicorp/raft InmemStore-style) against encoded payloads in a map.
type refLogStore struct {
	logs       map[uint64][]byte
	firstIndex uint64
	lastIndex  uint64
}

func newRefLogStore() *refLogStore {
	return &refLogStore{logs: make(map[uint64][]byte)}
}

func (r *refLogStore) storeLog(log *hraft.Log) {
	payload := encodelog(log)
	r.logs[log.Index] = payload
	if r.firstIndex == 0 || log.Index < r.firstIndex {
		r.firstIndex = log.Index
	}
	if log.Index > r.lastIndex {
		r.lastIndex = log.Index
	}
}

func (r *refLogStore) deleteRange(lo, hi uint64) {
	if hi < lo {
		return
	}
	for i := lo; i <= hi; i++ {
		delete(r.logs, i)
	}
	if lo <= r.firstIndex {
		r.firstIndex = hi + 1
	}
	if hi >= r.lastIndex {
		r.lastIndex = lo - 1
	}
	if r.firstIndex > r.lastIndex {
		r.firstIndex = 0
		r.lastIndex = 0
	}
}

func (r *refLogStore) assertEqual(t *testing.T, gs *GroupStore) {
	t.Helper()
	f, err := gs.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	l, err := gs.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if f != r.firstIndex || l != r.lastIndex {
		t.Fatalf("bounds: got first=%d last=%d, ref first=%d last=%d", f, l, r.firstIndex, r.lastIndex)
	}
	if r.lastIndex == 0 {
		if len(r.logs) != 0 {
			t.Fatalf("empty bounds but ref has %d keys", len(r.logs))
		}
		return
	}
	for idx := r.firstIndex; idx <= r.lastIndex; idx++ {
		wantEnc, ok := r.logs[idx]
		if !ok {
			t.Fatalf("ref model missing index %d (first=%d last=%d)", idx, r.firstIndex, r.lastIndex)
		}
		var got, want hraft.Log
		if err := gs.GetLog(idx, &got); err != nil {
			t.Fatalf("GetLog(%d): %v", idx, err)
		}
		if err := decodelog(wantEnc, &want); err != nil {
			t.Fatalf("decodelog ref %d: %v", idx, err)
		}
		if got.Index != want.Index || got.Term != want.Term || got.Type != want.Type ||
			string(got.Data) != string(want.Data) || string(got.Extensions) != string(want.Extensions) {
			t.Fatalf("log %d: got %+v / want %+v", idx, got, want)
		}
	}
	if len(r.logs) != int(r.lastIndex-r.firstIndex+1) {
		t.Fatalf("ref map size %d vs span %d", len(r.logs), r.lastIndex-r.firstIndex+1)
	}
}

// Random interleaved append + prefix delete must match an independent reference model.
func TestHarnessRandomAppendPrefixDeleteMatchesReference(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Open(dir, harnessWalConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	gs := w.GroupStore("invariant-ref")
	ref := newRefLogStore()
	rng := rand.New(rand.NewChaCha8([32]byte{9: 42}))

	var next uint64 = 1
	const steps = 600
	for s := 0; s < steps; s++ {
		if rng.IntN(10) < 7 {
			log := &hraft.Log{
				Index: next,
				Term:  1 + uint64(s%5),
				Type:  hraft.LogCommand,
				Data:  []byte(fmt.Sprintf("e-%d", s)),
			}
			if err := gs.StoreLog(log); err != nil {
				t.Fatalf("step %d StoreLog: %v", s, err)
			}
			ref.storeLog(log)
			next++
			continue
		}
		if ref.firstIndex == 0 || ref.lastIndex <= ref.firstIndex {
			continue
		}
		// Delete a contiguous prefix of the *current* live suffix [first..last],
		// leaving at least one entry so append indices stay aligned with Raft.
		span := int(ref.lastIndex - ref.firstIndex) // >= 1
		extra := rng.IntN(span)                      // 0 .. span-1
		kEnd := ref.firstIndex + uint64(extra)       // first .. last-1
		if err := gs.DeleteRange(ref.firstIndex, kEnd); err != nil {
			t.Fatalf("step %d DeleteRange(%d,%d): %v", s, ref.firstIndex, kEnd, err)
		}
		ref.deleteRange(ref.firstIndex, kEnd)
		if ref.lastIndex == 0 {
			next = 1
		}
		ref.assertEqual(t, gs)
	}
	ref.assertEqual(t, gs)
}

// Every index in [FirstIndex, LastIndex] must be readable after compaction + reopen.
func TestHarnessFullIndexScanAfterCompactionAndReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := harnessWalConfig()
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	gs := w.GroupStore("scan-full")
	for i := uint64(1); i <= 15; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	big := make([]byte, 900)
	for i := uint64(16); i <= 35; i++ {
		if err := gs.StoreLog(&hraft.Log{Index: i, Term: 2, Data: big}); err != nil {
			t.Fatalf("StoreLog %d: %v", i, err)
		}
	}
	if err := gs.DeleteRange(1, 6); err != nil {
		t.Fatal(err)
	}
	if w.LastCompactionStats().ReclaimedSegments == 0 {
		t.Fatal("expected compaction")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	gs2 := w2.GroupStore("scan-full")
	first, _ := gs2.FirstIndex()
	last, _ := gs2.LastIndex()
	var log hraft.Log
	for idx := first; idx <= last; idx++ {
		if err := gs2.GetLog(idx, &log); err != nil {
			t.Fatalf("GetLog(%d): %v", idx, err)
		}
	}
}

// Concurrent writers on multiple groups interleaved with stable keys; verify
// final durable state after quiesce (no races on committed values).
func TestHarnessConcurrentStableLogMultiGroup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := harnessWalConfig()
	w, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const nGroups = 12
	const perGroup = 80
	type result struct {
		g   int
		err error
	}
	ch := make(chan result, nGroups)
	var wg sync.WaitGroup

	stores := make([]*GroupStore, nGroups)
	for g := range nGroups {
		stores[g] = w.GroupStore(fmt.Sprintf("conc-stable-%d", g))
	}

	for g := range nGroups {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			gs := stores[g]
			uKey := []byte(fmt.Sprintf("u-%d", g))
			sKey := []byte(fmt.Sprintf("s-%d", g))
			for i := uint64(1); i <= perGroup; i++ {
				if err := gs.StoreLog(&hraft.Log{
					Index: i,
					Term:  uint64(g + 1),
					Type:  hraft.LogCommand,
					Data:  []byte(fmt.Sprintf("g%d-%d", g, i)),
				}); err != nil {
					ch <- result{g, err}
					return
				}
				if err := gs.SetUint64(uKey, i*uint64(g+17)); err != nil {
					ch <- result{g, err}
					return
				}
				if err := gs.Set(sKey, []byte(fmt.Sprintf("v-%d-%d", g, i))); err != nil {
					ch <- result{g, err}
					return
				}
			}
			ch <- result{g, nil}
		}()
	}
	wg.Wait()
	close(ch)
	for res := range ch {
		if res.err != nil {
			t.Fatalf("group %d: %v", res.g, res.err)
		}
	}

	for g := range nGroups {
		gs := w.GroupStore(fmt.Sprintf("conc-stable-%d", g))
		first, _ := gs.FirstIndex()
		last, _ := gs.LastIndex()
		if first != 1 || last != perGroup {
			t.Fatalf("group %d: first=%d last=%d want 1..%d", g, first, last, perGroup)
		}
		uKey := []byte(fmt.Sprintf("u-%d", g))
		sKey := []byte(fmt.Sprintf("s-%d", g))
		n, _ := gs.GetUint64(uKey)
		if want := perGroup * uint64(g+17); n != want {
			t.Fatalf("group %d: GetUint64=%d want %d", g, n, want)
		}
		val, _ := gs.Get(sKey)
		if string(val) != fmt.Sprintf("v-%d-%d", g, perGroup) {
			t.Fatalf("group %d: Get=%q want v-%d-%d", g, val, g, perGroup)
		}
		var log hraft.Log
		for i := uint64(1); i <= perGroup; i++ {
			if err := gs.GetLog(i, &log); err != nil {
				t.Fatalf("group %d GetLog %d: %v", g, i, err)
			}
			if want := fmt.Sprintf("g%d-%d", g, i); string(log.Data) != want {
				t.Fatalf("group %d log %d: data=%q want %q", g, i, log.Data, want)
			}
		}
	}
}
