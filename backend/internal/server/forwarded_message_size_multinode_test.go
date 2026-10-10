package server_test

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// appendSizedRecords appends count records to node's vault, each body size
// bytes long and starting with "<prefix>-<index>|".
func appendSizedRecords(t *testing.T, node multinodeTestNode, prefix string, count, size int, t0 time.Time) []string {
	t.Helper()
	keys := make([]string, count)
	for i := range count {
		keys[i] = fmt.Sprintf("%s-%04d", prefix, i)
		raw := bytes.Repeat([]byte{'x'}, size)
		copy(raw, keys[i]+"|")
		ts := t0.Add(time.Duration(i) * time.Millisecond)
		if _, _, err := node.vault.CM.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: raw}); err != nil {
			t.Fatalf("append %s: %v", keys[i], err)
		}
	}
	return keys
}

// TestMultiNode_ForwardedSearchCarriesLargeRecords searches, through a
// vault-less coordinator, three remote vaults whose forwarded responses
// exceed gRPC's default 4 MiB receive cap: two whose 200-record batches run
// to 4.8 MiB, and one holding a record larger than the 10 MiB HTTP ingest
// body. Every record arrives exactly once and whole.
func TestMultiNode_ForwardedSearchCarriesLargeRecords(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"},
		WithoutVault("coord"), WithClusterSearch())

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	sizes := map[string]int{}
	for _, id := range []string{"data-1", "data-2"} {
		for _, k := range appendSizedRecords(t, h.Node(t, id), id, 220, 24<<10, t0) {
			sizes[k] = 24 << 10
		}
	}
	data3 := h.Node(t, "data-3")
	for _, k := range appendSizedRecords(t, data3, "data-3-small", 10, 512, t0) {
		sizes[k] = 512
	}
	for _, k := range appendSizedRecords(t, data3, "data-3-large", 1, 12<<20, t0.Add(time.Second)) {
		sizes[k] = 12 << 20
	}

	seen := map[string]int{}
	for _, rec := range searchAll(t, h.client, "") {
		key, _, ok := bytes.Cut(rec.GetRaw(), []byte("|"))
		if !ok {
			t.Fatalf("record without a key prefix: %.40q", rec.GetRaw())
		}
		seen[string(key)]++
		if want := sizes[string(key)]; len(rec.GetRaw()) != want {
			t.Errorf("%s arrived with %d bytes, want %d", key, len(rec.GetRaw()), want)
		}
	}
	for k := range sizes {
		if seen[k] != 1 {
			t.Errorf("%s arrived %d times, want once", k, seen[k])
		}
	}
	if len(seen) != len(sizes) {
		t.Errorf("search returned %d distinct records, want %d", len(seen), len(sizes))
	}
}

// TestMultiNode_ForwardedPipelineCarriesLargeTable runs a stats query through
// a vault-less coordinator over three remote vaults, each of whose tables
// encodes to more than gRPC's default 4 MiB receive cap. Every group arrives
// with every node's contribution.
func TestMultiNode_ForwardedPipelineCarriesLargeTable(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"},
		WithoutVault("coord"), WithClusterSearch())

	const groups = 4500
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	keys := make([]string, groups)
	for i := range keys {
		keys[i] = fmt.Sprintf("%05d-%s", i, bytes.Repeat([]byte{'k'}, 1<<10))
	}
	for _, id := range []string{"data-1", "data-2", "data-3"} {
		node := h.Node(t, id)
		for i, k := range keys {
			ts := t0.Add(time.Duration(i) * time.Millisecond)
			if _, _, err := node.vault.CM.Append(chunk.Record{
				IngestTS: ts, WriteTS: ts, Raw: []byte(id), Attrs: chunk.Attributes{"k": k},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	counts := tableToMap(t, searchTable(t, h.client, "| stats count by k"), "k", "count")
	if len(counts) != groups {
		t.Fatalf("table has %d groups, want %d", len(counts), groups)
	}
	for _, k := range keys {
		if n, err := strconv.Atoi(counts[k]); err != nil || n != 3 {
			t.Fatalf("group %.12q count = %q, want 3", k, counts[k])
		}
	}
}
