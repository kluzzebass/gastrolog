package query

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memjson "gastrolog/internal/index/memory/json"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/manifest"
	"gastrolog/internal/querylang"
)

// unmaterializedCM reports its active chunk as not locally materialized, the
// shape an open pipeline chunk has: no source index, so order=source_ts puts
// it through buildSortedScanner.
type unmaterializedCM struct {
	chunk.ChunkManager
}

func (unmaterializedCM) HasLocalContent(chunk.ChunkID) bool { return false }

type unindexedVault struct {
	cm chunk.ChunkManager
	im index.IndexManager
}

type unindexedRegistry struct {
	order  []glid.GLID
	vaults map[glid.GLID]unindexedVault
}

func (r *unindexedRegistry) ListVaults() []glid.GLID { return r.order }
func (r *unindexedRegistry) ChunkManager(id glid.GLID) chunk.ChunkManager {
	if v, ok := r.vaults[id]; ok {
		return v.cm
	}
	return nil
}
func (r *unindexedRegistry) IndexManager(id glid.GLID) index.IndexManager {
	if v, ok := r.vaults[id]; ok {
		return v.im
	}
	return nil
}
func (r *unindexedRegistry) Reader() manifest.Reader           { return manifest.NewProjectingReader(r) }
func (r *unindexedRegistry) IndexReader() manifest.IndexReader { return nil }

// SearchChunkMetas exposes only the active chunk so the projection and the
// active head are not both listed.
func (r *unindexedRegistry) SearchChunkMetas(id glid.GLID) []chunk.ChunkMeta {
	v, ok := r.vaults[id]
	if !ok {
		return nil
	}
	if a := v.cm.Active(); a != nil {
		return []chunk.ChunkMeta{*a}
	}
	return nil
}

// newUnindexedVault builds a memory vault whose single active chunk holds n
// records; record i carries SourceTS t0 + (i*stride + offset) seconds so that
// several such vaults interleave perfectly in source order.
func newUnindexedVault(t *testing.T, n, stride, offset int) unindexedVault {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(1_000_000)})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ingester := glid.New()
	for i := range n {
		ingestTS := t0.Add(time.Duration(i) * time.Second)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ingestTS,
			WriteTS:  ingestTS,
			SourceTS: t0.Add(time.Duration(i*stride+offset) * time.Second),
			EventID:  chunk.EventID{IngesterID: ingester, IngestTS: ingestTS, IngestSeq: uint32(i + 1)}, //nolint:gosec // G115: small loop index
			Attrs:    chunk.Attributes{"host": fmt.Sprintf("host-%d", offset), "level": fmt.Sprintf("l%d", i)},
			Raw:      fmt.Appendf(nil, "v%d-r%d", offset, i),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	jsonIdx := memjson.NewIndexer(cm)
	im := indexmem.NewManagerWithJSON(
		[]index.Indexer{tokIdx, attrIdx, kvIdx, jsonIdx}, tokIdx, attrIdx, kvIdx, jsonIdx, nil)
	return unindexedVault{cm: unmaterializedCM{cm}, im: im}
}

func newUnindexedEngine(t *testing.T, vaults, perVault int, memLimit int64) *Engine {
	t.Helper()
	reg := &unindexedRegistry{vaults: map[glid.GLID]unindexedVault{}}
	for v := range vaults {
		id := glid.New()
		reg.order = append(reg.order, id)
		reg.vaults[id] = newUnindexedVault(t, perVault, vaults, v)
	}
	e := NewWithRegistry(reg, nil)
	e.memLimit = memLimit
	return e
}

func firstSearchError(it func(func(chunk.Record, error) bool)) error {
	for _, err := range it {
		if err != nil {
			return err
		}
	}
	return nil
}

// An unindexed chunk whose sort keys outgrow the budget fails the search with
// a MemoryLimitError that names the sort buffer. It is the query's size, not
// the vault's health, so it is not reported as a vault read failure.
func TestSortedScannerOverflowNamesTheSortBuffer(t *testing.T) {
	const perVault = 200
	e := newUnindexedEngine(t, 1, perVault, sortedEntryBytes*perVault/2)
	it, _ := e.Search(context.Background(), Query{OrderBy: OrderBySourceTS}, nil)
	err := firstSearchError(it)

	memErr, ok := errors.AsType[*MemoryLimitError](err)
	if !ok {
		t.Fatalf("expected *MemoryLimitError from the sort-key buffer, got %T: %v", err, err)
	}
	if memErr.Consumer != consumerSortBuffer {
		t.Errorf("overflow names %q, want %q", memErr.Consumer, consumerSortBuffer)
	}
	if re, ok := errors.AsType[*VaultReadError](err); ok {
		t.Errorf("memory overflow reported as a read failure of vault %s chunk %s", re.VaultID, re.ChunkID)
	}
}

// One ledger per query: K unindexed chunks, each under the limit on its own,
// are held at the same time by the source-order merge, so together they
// overflow it.
func TestSortKeyBuffersShareOneQueryBudget(t *testing.T) {
	const (
		vaults   = 4
		perVault = 100
	)
	perChunk := int64(sortedEntryBytes * perVault)
	limit := perChunk + perChunk/2 // each chunk fits; any two do not

	// Premise: with room for every chunk at once, the merge interleaves all K
	// vaults, so every sorted scanner holds its keys at the same time.
	roomy := newUnindexedEngine(t, vaults, perVault, int64(vaults)*perChunk)
	it, _ := roomy.Search(context.Background(), Query{OrderBy: OrderBySourceTS}, nil)
	var raws []string
	for rec, err := range it {
		if err != nil {
			t.Fatalf("premise: search with room for every chunk failed: %v", err)
		}
		raws = append(raws, string(rec.Raw))
	}
	if len(raws) != vaults*perVault {
		t.Fatalf("premise: got %d records, want %d", len(raws), vaults*perVault)
	}
	for i := range vaults {
		if want := fmt.Sprintf("v%d-r0", i); raws[i] != want {
			t.Fatalf("premise: record %d = %q, want %q: chunks were not interleaved", i, raws[i], want)
		}
	}

	e := newUnindexedEngine(t, vaults, perVault, limit)
	it, _ = e.Search(context.Background(), Query{OrderBy: OrderBySourceTS}, nil)
	if _, ok := errors.AsType[*MemoryLimitError](firstSearchError(it)); !ok {
		t.Fatalf("query held %d sort-key buffers of %d bytes at once against a %d-byte budget and was not refused",
			vaults, perChunk, limit)
	}
}

// The ledger a caller hands RunPipelineWithRemote is the one the pipeline's
// own scan charges for sort keys.
func TestSortKeysChargeTheCallerBudget(t *testing.T) {
	const perVault = 200
	perChunk := int64(sortedEntryBytes * perVault)

	pipeline, err := querylang.ParsePipeline("| stats count by host")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q := Query{BoolExpr: pipeline.Filter, OrderBy: OrderBySourceTS}

	// Premise: the scan runs the sorted scanner, which a caller ledger with
	// room for the keys lets through.
	e := newUnindexedEngine(t, 1, perVault, 0)
	if _, err := e.RunPipelineWithRemote(context.Background(), q, pipeline, nil, &Budget{limit: 4 * perChunk}); err != nil {
		t.Fatalf("premise: pipeline with room for its sort keys failed: %v", err)
	}

	caller := &Budget{limit: perChunk / 2}
	_, err = e.RunPipelineWithRemote(context.Background(), q, pipeline, nil, caller)
	memErr, ok := errors.AsType[*MemoryLimitError](err)
	if !ok {
		t.Fatalf("pipeline over %d bytes of sort keys succeeded against a %d-byte caller budget: %v", perChunk, perChunk/2, err)
	}
	if memErr.Consumer != consumerSortBuffer {
		t.Errorf("overflow names %q, want %q", memErr.Consumer, consumerSortBuffer)
	}
}

// A sorted scanner hands its keys back when it is done with them — drained
// or abandoned — so a ledger shared by the parts of one query reflects what
// is held now, not everything that was ever held.
func TestSortKeysReturnToTheLedgerWhenTheScanEnds(t *testing.T) {
	const perVault = 200
	perChunk := int64(sortedEntryBytes * perVault)
	e := newUnindexedEngine(t, 1, perVault, 0)

	t.Run("drained", func(t *testing.T) {
		b := &Budget{limit: perChunk + perChunk/2}
		for pass := range 3 {
			it, _ := e.Search(context.Background(), Query{OrderBy: OrderBySourceTS}.WithBudget(b), nil)
			n := 0
			for _, err := range it {
				if err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
				n++
			}
			if n != perVault {
				t.Fatalf("pass %d: %d records, want %d", pass, n, perVault)
			}
			if used := b.used.Load(); used != 0 {
				t.Fatalf("pass %d: ledger holds %d bytes after the scan ended", pass, used)
			}
		}
	})

	t.Run("abandoned", func(t *testing.T) {
		b := &Budget{limit: perChunk + perChunk/2}
		it, _ := e.Search(context.Background(), Query{OrderBy: OrderBySourceTS}.WithBudget(b), nil)
		for _, err := range it {
			if err != nil {
				t.Fatal(err)
			}
			if b.used.Load() != perChunk {
				t.Fatalf("ledger holds %d bytes mid-scan, want the %d-byte key buffer", b.used.Load(), perChunk)
			}
			break
		}
		if used := b.used.Load(); used != 0 {
			t.Fatalf("ledger holds %d bytes after the scan was abandoned", used)
		}
	})

	t.Run("overflowed", func(t *testing.T) {
		b := &Budget{limit: perChunk / 2}
		it, _ := e.Search(context.Background(), Query{OrderBy: OrderBySourceTS}.WithBudget(b), nil)
		if _, ok := errors.AsType[*MemoryLimitError](firstSearchError(it)); !ok {
			t.Fatal("premise: expected the scan to overflow")
		}
		if used := b.used.Load(); used != 0 {
			t.Fatalf("ledger holds %d bytes after the overflowing scan ended", used)
		}
	})
}

// The histogram charges the ledger of the query it accompanies, and returns
// everything it charged when its buckets are built.
func TestHistogramChargesTheQueryLedger(t *testing.T) {
	const perVault = 200
	perChunk := int64(sortedEntryBytes * perVault)
	e := newUnindexedEngine(t, 1, perVault, 0)
	q := Query{OrderBy: OrderBySourceTS, BoolExpr: &querylang.PredicateExpr{Kind: querylang.PredKV, Key: "host", Value: "host-0"}}

	roomy := &Budget{limit: 4 * perChunk}
	if got := e.ComputeHistogram(context.Background(), q.WithBudget(roomy), 10); len(got) == 0 {
		t.Fatal("premise: a histogram with room on its ledger produced no buckets")
	}
	if used := roomy.used.Load(); used != 0 {
		t.Fatalf("ledger holds %d bytes after the histogram finished", used)
	}

	tight := &Budget{limit: perChunk / 2}
	if got := e.ComputeHistogram(context.Background(), q.WithBudget(tight), 10); got != nil {
		t.Fatalf("histogram over %d bytes of sort keys succeeded against a %d-byte query ledger", perChunk, perChunk/2)
	}
	if used := tight.used.Load(); used != 0 {
		t.Fatalf("ledger holds %d bytes after the abandoned histogram", used)
	}
}

// The page-1 histogram runs on its own goroutine beside the record scan, both
// on the query's one ledger. Run under -race: the ledger must take concurrent
// charges and releases, and end empty once both are done.
func TestHistogramAndScanShareOneLedgerConcurrently(t *testing.T) {
	const perVault = 300
	e := newUnindexedEngine(t, 2, perVault, 0)
	b := e.NewBudget()
	q := Query{OrderBy: OrderBySourceTS, BoolExpr: &querylang.PredicateExpr{Kind: querylang.PredKeyExists, Key: "host"}}.WithBudget(b)

	const rounds = 20
	var wg sync.WaitGroup
	errs := make(chan error, 2*rounds)
	for range rounds {
		wg.Go(func() {
			if len(e.ComputeHistogram(context.Background(), q, 10)) == 0 {
				errs <- errors.New("histogram produced no buckets")
			}
		})
		wg.Go(func() {
			it, _ := e.Search(context.Background(), q, nil)
			n := 0
			for _, err := range it {
				if err != nil {
					errs <- err
					return
				}
				n++
			}
			if n != 2*perVault {
				errs <- fmt.Errorf("scan returned %d records, want %d", n, 2*perVault)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if used := b.used.Load(); used != 0 {
		t.Fatalf("ledger holds %d bytes after every histogram and scan finished", used)
	}
}

// A sub-account charges through to its parent against the parent's ceiling,
// and close returns whatever it still holds.
func TestSubAccountChargesItsParent(t *testing.T) {
	parent := &Budget{limit: 100}
	if err := parent.Charge(consumerRecordBuffer, 40); err != nil {
		t.Fatal(err)
	}
	sub := parent.subAccount()
	if err := sub.Charge(consumerTimechartState, 50); err != nil {
		t.Fatalf("charge inside the parent's room failed: %v", err)
	}
	if err := sub.Charge(consumerTimechartState, 20); err == nil {
		t.Fatal("sub-account charge past the parent's ceiling succeeded")
	}
	sub.Release(30)
	if got := parent.used.Load(); got != 80 {
		t.Fatalf("parent holds %d after sub-account release, want 80", got)
	}
	sub.close()
	if got := parent.used.Load(); got != 40 {
		t.Fatalf("parent holds %d after close, want the 40 it charged itself", got)
	}
	sub.close()
	if got := parent.used.Load(); got != 40 {
		t.Fatalf("second close changed the parent to %d", got)
	}
}
