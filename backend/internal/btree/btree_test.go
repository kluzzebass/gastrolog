package btree_test

import (
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"gastrolog/internal/btree"
)

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.bt")
}

func mustCreate(t *testing.T) (*btree.Tree[int64, uint32], string) {
	t.Helper()
	path := tempPath(t)
	tree, err := btree.Create(path, btree.Int64Uint32)
	if err != nil {
		t.Fatal(err)
	}
	return tree, path
}

func TestCreateAndOpen(t *testing.T) {
	tree, path := mustCreate(t)
	if tree.Count() != 0 {
		t.Fatalf("count = %d, want 0", tree.Count())
	}
	if tree.Height() != 1 {
		t.Fatalf("height = %d, want 1", tree.Height())
	}
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}

	tree, err := btree.Open(path, btree.Int64Uint32)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	if tree.Count() != 0 {
		t.Fatalf("count after reopen = %d, want 0", tree.Count())
	}
}

func TestInsertAndFindGE(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	// Non-monotonic inserts.
	keys := []int64{300, 100, 500, 200, 400}
	for i, k := range keys {
		if err := tree.Insert(k, uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	if tree.Count() != 5 {
		t.Fatalf("count = %d, want 5", tree.Count())
	}

	tests := []struct {
		target  int64
		wantKey int64
		wantVal uint32
		wantNil bool
	}{
		{target: 50, wantKey: 100, wantVal: 1},
		{target: 100, wantKey: 100, wantVal: 1},
		{target: 101, wantKey: 200, wantVal: 3},
		{target: 500, wantKey: 500, wantVal: 2},
		{target: 501, wantNil: true},
	}
	for _, tt := range tests {
		it, err := tree.FindGE(tt.target)
		if err != nil {
			t.Fatalf("FindGE(%d): %v", tt.target, err)
		}
		if tt.wantNil {
			if it.Valid() {
				t.Errorf("FindGE(%d): want invalid, got (%d, %d)", tt.target, it.Key(), it.Value())
			}
			continue
		}
		if !it.Valid() {
			t.Fatalf("FindGE(%d): want valid", tt.target)
		}
		if it.Key() != tt.wantKey || it.Value() != tt.wantVal {
			t.Errorf("FindGE(%d) = (%d, %d), want (%d, %d)", tt.target, it.Key(), it.Value(), tt.wantKey, tt.wantVal)
		}
	}
}

func TestDuplicateKeys(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	for i := range uint32(10) {
		if err := tree.Insert(100, i); err != nil {
			t.Fatal(err)
		}
	}

	it, err := tree.FindGE(100)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for it.Valid() && it.Key() == 100 {
		if it.Value() != uint32(count) {
			t.Fatalf("entry %d: value = %d, want %d", count, it.Value(), count)
		}
		count++
		it.Next()
	}
	if count != 10 {
		t.Fatalf("found %d duplicates, want 10", count)
	}
}

func TestScan(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	keys := []int64{50, 30, 40, 10, 20}
	for i, k := range keys {
		if err := tree.Insert(k, uint32(i)); err != nil {
			t.Fatal(err)
		}
	}

	it, err := tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for it.Valid() {
		got = append(got, it.Key())
		it.Next()
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	want := []int64{10, 20, 30, 40, 50}
	if !slices.Equal(got, want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}
}

func TestScanEmpty(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	it, err := tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if it.Valid() {
		t.Fatal("scan on empty tree should be invalid")
	}
}

func TestSplitLeaf(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	// Insert enough entries to force at least one leaf split.
	n := 500
	for i := range n {
		if err := tree.Insert(int64(n-i), uint32(i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if tree.Count() != uint64(n) {
		t.Fatalf("count = %d, want %d", tree.Count(), n)
	}
	if tree.Height() < 2 {
		t.Fatalf("height = %d, want >= 2 after %d inserts", tree.Height(), n)
	}

	// Verify sorted scan.
	it, err := tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var prev int64
	var count int
	for it.Valid() {
		if it.Key() < prev {
			t.Fatalf("entry %d: key %d < previous %d", count, it.Key(), prev)
		}
		prev = it.Key()
		count++
		it.Next()
	}
	if count != n {
		t.Fatalf("scanned %d entries, want %d", count, n)
	}
}

func TestLargeRandomInserts(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	rng := rand.New(rand.NewPCG(42, 0))
	n := 50_000

	type kv struct {
		key   int64
		value uint32
	}
	entries := make([]kv, n)
	for i := range n {
		entries[i] = kv{key: rng.Int64N(1_000_000), value: uint32(i)}
		if err := tree.Insert(entries[i].key, entries[i].value); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if tree.Count() != uint64(n) {
		t.Fatalf("count = %d, want %d", tree.Count(), n)
	}

	// Sort expected entries by (key, value).
	slices.SortFunc(entries, func(a, b kv) int {
		if c := a.key - b.key; c != 0 {
			if c < 0 {
				return -1
			}
			return 1
		}
		if a.value < b.value {
			return -1
		}
		if a.value > b.value {
			return 1
		}
		return 0
	})

	// Verify full scan matches sorted order.
	it, err := tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if !it.Valid() {
			t.Fatalf("scan ended at entry %d, want %d", i, n)
		}
		if it.Key() != entries[i].key || it.Value() != entries[i].value {
			t.Fatalf("entry %d: got (%d, %d), want (%d, %d)", i, it.Key(), it.Value(), entries[i].key, entries[i].value)
		}
		it.Next()
	}
	if it.Valid() {
		t.Fatal("scan has extra entries")
	}

	// Spot-check FindGE.
	for range 100 {
		target := rng.Int64N(1_000_000)
		it, err := tree.FindGE(target)
		if err != nil {
			t.Fatal(err)
		}
		if !it.Valid() {
			// All entries should be < target.
			for _, e := range entries {
				if e.key >= target {
					t.Fatalf("FindGE(%d) invalid but entry (%d, %d) exists", target, e.key, e.value)
				}
			}
			continue
		}
		if it.Key() < target {
			t.Fatalf("FindGE(%d) returned key %d", target, it.Key())
		}
	}
}

func TestPersistence(t *testing.T) {
	path := tempPath(t)

	// Create and populate.
	tree, err := btree.Create(path, btree.Int64Uint32)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		if err := tree.Insert(int64(i*10), uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tree.Sync(); err != nil {
		t.Fatal(err)
	}
	tree.Close()

	// Reopen and verify.
	tree, err = btree.Open(path, btree.Int64Uint32)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if tree.Count() != 1000 {
		t.Fatalf("count = %d, want 1000", tree.Count())
	}

	it, err := tree.FindGE(500)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Valid() || it.Key() != 500 || it.Value() != 50 {
		t.Fatalf("FindGE(500) = (%d, %d), want (500, 50)", it.Key(), it.Value())
	}

	// Scan all and verify count.
	it, err = tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for it.Valid() {
		count++
		it.Next()
	}
	if count != 1000 {
		t.Fatalf("scanned %d, want 1000", count)
	}
}

func TestFindGEAcrossLeafBoundary(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	// Insert entries with gaps to ensure FindGE must cross a leaf boundary.
	for i := range 340 {
		if err := tree.Insert(int64(i), uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Gap: no entries for keys 340-999.
	for i := range 340 {
		if err := tree.Insert(int64(1000+i), uint32(1000+i)); err != nil {
			t.Fatal(err)
		}
	}

	// FindGE for a key in the gap.
	it, err := tree.FindGE(500)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Valid() {
		t.Fatal("FindGE(500): want valid")
	}
	if it.Key() != 1000 {
		t.Fatalf("FindGE(500) = %d, want 1000", it.Key())
	}
}

func TestCodecMismatch(t *testing.T) {
	path := tempPath(t)

	tree, err := btree.Create(path, btree.Int64Uint32)
	if err != nil {
		t.Fatal(err)
	}
	tree.Close()

	// Try to open with a different codec.
	wrongCodec := btree.Codec[int32, int32]{
		KeySize: 4,
		ValSize: 4,
		PutKey:  func([]byte, int32) {},
		Key:     func([]byte) int32 { return 0 },
		PutVal:  func([]byte, int32) {},
		Val:     func([]byte) int32 { return 0 },
	}
	_, err = btree.Open(path, wrongCodec)
	if err == nil {
		t.Fatal("expected codec mismatch error")
	}
}

func TestDeleteSingle(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	for i := range 10 {
		if err := tree.Insert(int64(i*10), uint32(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete from middle.
	ok, err := tree.Delete(50)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Delete(50): want true")
	}
	if tree.Count() != 9 {
		t.Fatalf("count = %d, want 9", tree.Count())
	}

	// Verify 50 is gone.
	it, err := tree.FindGE(50)
	if err != nil {
		t.Fatal(err)
	}
	if it.Valid() && it.Key() == 50 {
		t.Fatal("key 50 should have been deleted")
	}

	// Delete non-existent key.
	ok, err = tree.Delete(999)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Delete(999): want false")
	}
}

func TestDeleteAll(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	n := 500
	keys := make([]int64, n)
	for i := range n {
		keys[i] = int64(i)
		if err := tree.Insert(int64(i), uint32(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete all entries.
	for _, k := range keys {
		ok, err := tree.Delete(k)
		if err != nil {
			t.Fatalf("Delete(%d): %v", k, err)
		}
		if !ok {
			t.Fatalf("Delete(%d): want true", k)
		}
	}

	if tree.Count() != 0 {
		t.Fatalf("count = %d, want 0", tree.Count())
	}
	if tree.Height() != 1 {
		t.Fatalf("height = %d, want 1 after deleting all", tree.Height())
	}

	it, err := tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if it.Valid() {
		t.Fatal("scan should be invalid after deleting all")
	}
}

func TestDeleteAndReinsert(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	n := 1000
	for i := range n {
		if err := tree.Insert(int64(i), uint32(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete every other entry.
	for i := 0; i < n; i += 2 {
		ok, err := tree.Delete(int64(i))
		if err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
		if !ok {
			t.Fatalf("Delete(%d): want true", i)
		}
	}
	if tree.Count() != uint64(n/2) {
		t.Fatalf("count = %d, want %d", tree.Count(), n/2)
	}

	// Re-insert deleted entries.
	for i := 0; i < n; i += 2 {
		if err := tree.Insert(int64(i), uint32(i+n)); err != nil {
			t.Fatal(err)
		}
	}
	if tree.Count() != uint64(n) {
		t.Fatalf("count = %d, want %d", tree.Count(), n)
	}

	// Verify sorted scan has all entries.
	it, err := tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var count int
	var prev int64 = -1
	for it.Valid() {
		if it.Key() < prev {
			t.Fatalf("entry %d: key %d < previous %d", count, it.Key(), prev)
		}
		prev = it.Key()
		count++
		it.Next()
	}
	if count != n {
		t.Fatalf("scanned %d entries, want %d", count, n)
	}
}

func TestDeleteLargeRandom(t *testing.T) {
	tree, _ := mustCreate(t)
	defer tree.Close()

	rng := rand.New(rand.NewPCG(99, 0))
	n := 10_000

	keys := make([]int64, n)
	for i := range n {
		keys[i] = rng.Int64N(100_000)
		if err := tree.Insert(keys[i], uint32(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Delete the first half of inserted keys.
	deleted := 0
	for i := range n / 2 {
		ok, err := tree.Delete(keys[i])
		if err != nil {
			t.Fatalf("Delete(%d): %v", keys[i], err)
		}
		if ok {
			deleted++
		}
	}

	if tree.Count() != uint64(n-deleted) {
		t.Fatalf("count = %d, want %d", tree.Count(), n-deleted)
	}

	// Verify scan is sorted.
	it, err := tree.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var count int
	var prev int64 = -1
	for it.Valid() {
		if it.Key() < prev {
			t.Fatalf("entry %d: key %d < previous %d", count, it.Key(), prev)
		}
		prev = it.Key()
		count++
		it.Next()
	}
	if count != int(tree.Count()) {
		t.Fatalf("scanned %d entries, want %d", count, tree.Count())
	}
}

func BenchmarkInsert(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.bt")
	tree, err := btree.Create(path, btree.Int64Uint32)
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()

	rng := rand.New(rand.NewPCG(1, 0))
	b.ResetTimer()
	var i uint32
	for b.Loop() {
		if err := tree.Insert(rng.Int64(), i); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkFindGE(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.bt")
	tree, err := btree.Create(path, btree.Int64Uint32)
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()

	rng := rand.New(rand.NewPCG(1, 0))
	for i := range 100_000 {
		if err := tree.Insert(rng.Int64(), uint32(i)); err != nil { //nolint:gosec // benchmark
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for b.Loop() {
		it, err := tree.FindGE(rng.Int64())
		if err != nil {
			b.Fatal(err)
		}
		_ = it.Valid()
	}
}
