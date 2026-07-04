package query_test

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

// cloudBackedCM wraps a ChunkManager so that List() returns all chunks
// with CloudBacked=true. This simulates a cloud-backed vault for testing.
type cloudBackedCM struct {
	chunk.ChunkManager
}

func (c *cloudBackedCM) List() ([]chunk.ChunkMeta, error) {
	metas, err := c.ChunkManager.List()
	if err != nil {
		return nil, err
	}
	for i := range metas {
		metas[i].CloudBacked = true
	}
	return metas, nil
}

func (c *cloudBackedCM) Meta(id chunk.ChunkID) (chunk.ChunkMeta, error) {
	m, err := c.ChunkManager.Meta(id)
	if err != nil {
		return m, err
	}
	m.CloudBacked = true
	return m, nil
}

func (c *cloudBackedCM) ingestRankView() (chunk.IngestTSRankView, bool) {
	rankCM, ok := c.ChunkManager.(chunk.IngestTSRankView)
	return rankCM, ok
}

func (c *cloudBackedCM) IngestTSRankLen(id chunk.ChunkID) (uint64, error) {
	if rankCM, ok := c.ingestRankView(); ok {
		return rankCM.IngestTSRankLen(id)
	}
	return 0, chunk.ErrIngestTSRankIndex
}

func (c *cloudBackedCM) IngestTSRankAt(id chunk.ChunkID, rank uint64) (int64, uint32, error) {
	if rankCM, ok := c.ingestRankView(); ok {
		return rankCM.IngestTSRankAt(id, rank)
	}
	return 0, 0, chunk.ErrIngestTSRankIndex
}

func (c *cloudBackedCM) FindIngestTSRank(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	if rankCM, ok := c.ingestRankView(); ok {
		return rankCM.FindIngestTSRank(id, ts)
	}
	return 0, false, nil
}

// TestCloudBackedChunksIncludedInSearch verifies that cloud-backed chunks
// participate in search results. This is the regression test for the bug
// where cloud chunks were "deferred" during heap priming but the lazy
// priming was never implemented — deferredChunks was written but never read.
func TestCloudBackedChunksIncludedInSearch(t *testing.T) {
	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	// Local vault: 5 records at t0+0s through t0+4s.
	localVaultID := glid.New()
	local := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 5 {
		local.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "local-record-%d", i),
		})
	}
	local.CM.Seal()
	reg.vaults[localVaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{local.CM, local.IM}

	// Cloud-backed vault: 5 records at t0+5s through t0+9s.
	cloudVaultID := glid.New()
	cloud := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 5 {
		cloud.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i+5) * time.Second),
			Raw:      fmt.Appendf(nil, "cloud-record-%d", i),
		})
	}
	cloud.CM.Seal()
	reg.vaults[cloudVaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{&cloudBackedCM{cloud.CM}, cloud.IM}

	eng := query.NewWithRegistry(reg, nil)

	// Search with no limit — all records must be returned.
	iter, _ := eng.Search(context.Background(), query.Query{}, nil)
	count := 0
	for _, err := range iter {
		if err != nil {
			t.Fatalf("search error: %v", err)
		}
		count++
	}
	if count != 10 {
		t.Errorf("unlimited search: expected 10 records (5 local + 5 cloud), got %d", count)
	}

	// Search WITH a limit — cloud records must still participate in the merge
	// so that timestamp ordering is correct across vaults.
	iter, _ = eng.Search(context.Background(), query.Query{Limit: 3}, nil)
	count = 0
	for _, err := range iter {
		if err != nil {
			t.Fatalf("limited search error: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("limited search: expected 3 records, got %d", count)
	}
}

// TestCloudBackedTimestampOrdering verifies that cloud-backed and local
// records are merge-sorted correctly by timestamp. Cloud records that are
// chronologically earlier than local records must appear first.
func TestCloudBackedTimestampOrdering(t *testing.T) {
	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	// Cloud-backed vault has EARLIER records (t0+0s through t0+2s).
	cloudVaultID := glid.New()
	cloud := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 3 {
		cloud.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "cloud-%d", i),
		})
	}
	cloud.CM.Seal()
	reg.vaults[cloudVaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{&cloudBackedCM{cloud.CM}, cloud.IM}

	// Local vault has LATER records (t0+3s through t0+5s).
	localVaultID := glid.New()
	local := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 3 {
		local.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i+3) * time.Second),
			Raw:      fmt.Appendf(nil, "local-%d", i),
		})
	}
	local.CM.Seal()
	reg.vaults[localVaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{local.CM, local.IM}

	eng := query.NewWithRegistry(reg, nil)

	// Limit=3: the first 3 records should all be from the cloud-backed
	// vault (they have earlier timestamps). If cloud chunks were skipped,
	// we'd get local records instead — wrong ordering.
	iter, _ := eng.Search(context.Background(), query.Query{Limit: 3}, nil)
	var raws []string
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("search error: %v", err)
		}
		raws = append(raws, string(rec.Raw))
	}
	if len(raws) != 3 {
		t.Fatalf("expected 3 records, got %d", len(raws))
	}
	for i, raw := range raws {
		expected := fmt.Sprintf("cloud-%d", i)
		if raw != expected {
			t.Errorf("record %d: got %q, want %q (cloud records should sort first)", i, raw, expected)
		}
	}
}

// orderedRegistry pins ListVaults order. The multi-vault merge must not
// depend on registry enumeration order (gastrolog-33bkl7).
type orderedRegistry struct {
	*testRegistry
	order []glid.GLID
}

func (r *orderedRegistry) ListVaults() []glid.GLID {
	return append([]glid.GLID(nil), r.order...)
}

// TestMultiVaultLimitOrderingIndependentOfRegistryOrder pins gastrolog-33bkl7
// deterministically: with the LATER-timestamped vault enumerating first, the
// lazy-prime merge treated the concatenated per-vault chunk lists as
// non-overlapping planner order, opened the later vault, satisfied Limit,
// and never opened the earlier cloud-backed vault — silently dropping the
// earliest records from search results.
func TestMultiVaultLimitOrderingIndependentOfRegistryOrder(t *testing.T) {
	base := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	cloudVaultID := glid.New()
	cloud := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 3 {
		cloud.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "cloud-%d", i),
		})
	}
	cloud.CM.Seal()
	base.vaults[cloudVaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{&cloudBackedCM{cloud.CM}, cloud.IM}

	localVaultID := glid.New()
	local := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 3 {
		local.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i+3) * time.Second),
			Raw:      fmt.Appendf(nil, "local-%d", i),
		})
	}
	local.CM.Seal()
	base.vaults[localVaultID] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{local.CM, local.IM}

	// Pathological enumeration order: later-timestamped vault first.
	reg := &orderedRegistry{testRegistry: base, order: []glid.GLID{localVaultID, cloudVaultID}}
	eng := query.NewWithRegistry(reg, nil)

	iter, _ := eng.Search(context.Background(), query.Query{Limit: 3}, nil)
	var raws []string
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("search error: %v", err)
		}
		raws = append(raws, string(rec.Raw))
	}
	if len(raws) != 3 {
		t.Fatalf("expected 3 records, got %d", len(raws))
	}
	for i, raw := range raws {
		expected := fmt.Sprintf("cloud-%d", i)
		if raw != expected {
			t.Errorf("record %d: got %q, want %q (earliest records must win regardless of vault order)", i, raw, expected)
		}
	}
}
