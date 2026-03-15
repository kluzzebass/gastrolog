package btree

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// FuzzBTreeOps interprets a random byte sequence as a series of B+ tree
// operations (insert, delete, lookup) and verifies invariants after all
// operations complete: Count matches actual entries, Scan produces sorted
// keys, and FindGE returns correct results.
func FuzzBTreeOps(f *testing.F) {
	// Seed corpus: a few hand-crafted operation sequences.
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0})                         // one insert
	f.Add([]byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1}) // insert then delete
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0}) // multiple inserts
	f.Add(make([]byte, 100)) // zeros

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.bt")

		tree, err := Create(path, Int64Uint32)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		defer func() { _ = tree.Close() }()

		// Track expected state: key → list of values (duplicates allowed).
		expected := make(map[int64][]uint32)

		// Each operation is 9 bytes: 1 byte opcode + 8 bytes key (for insert: last 4 of key are value).
		// Op 0: insert (key = bytes[1:9] as int64, value = bytes[5:9] as uint32)
		// Op 1: delete (key = bytes[1:9] as int64)
		// Op 2: findGE (key = bytes[1:9] as int64) — just verify no panic
		const opSize = 9
		for len(data) >= opSize {
			op := data[0] % 3
			// Parse key as int64 from bytes 1..9.
			keyBytes := data[1:opSize]
			key := int64(uint64(keyBytes[0]) | uint64(keyBytes[1])<<8 |
				uint64(keyBytes[2])<<16 | uint64(keyBytes[3])<<24 |
				uint64(keyBytes[4])<<32 | uint64(keyBytes[5])<<40 |
				uint64(keyBytes[6])<<48 | uint64(keyBytes[7])<<56)

			data = data[opSize:]

			switch op {
			case 0: // insert
				val := uint32(uint64(keyBytes[4]) | uint64(keyBytes[5])<<8 |
					uint64(keyBytes[6])<<16 | uint64(keyBytes[7])<<24)
				if err := tree.Insert(key, val); err != nil {
					t.Fatalf("insert: %v", err)
				}
				expected[key] = append(expected[key], val)

			case 1: // delete
				found, err := tree.Delete(key)
				if err != nil {
					t.Fatalf("delete: %v", err)
				}
				vals := expected[key]
				if len(vals) > 0 {
					if !found {
						t.Fatalf("delete(%d): expected found=true", key)
					}
					// Remove first occurrence.
					expected[key] = vals[1:]
					if len(expected[key]) == 0 {
						delete(expected, key)
					}
				} else {
					if found {
						t.Fatalf("delete(%d): expected found=false", key)
					}
				}

			case 2: // findGE — just ensure no panic/error
				it, err := tree.FindGE(key)
				if err != nil {
					t.Fatalf("findGE: %v", err)
				}
				if it.Valid() {
					if Int64Uint32.Compare(it.Key(), key) < 0 {
						t.Fatalf("findGE(%d): returned key %d which is less", key, it.Key())
					}
				}
			}

			// Limit total ops to prevent huge trees in fuzzing.
			if tree.Count() > 500 {
				break
			}
		}

		// Flush to disk and re-verify.
		if err := tree.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}

		// Invariant 1: Count matches expected.
		var expectedCount uint64
		for _, vals := range expected {
			expectedCount += uint64(len(vals))
		}
		if tree.Count() != expectedCount {
			t.Fatalf("count: got %d, want %d", tree.Count(), expectedCount)
		}

		// Invariant 2: Scan produces sorted keys and correct count.
		it, err := tree.Scan()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		var scanKeys []int64
		var scanCount uint64
		for it.Valid() {
			scanKeys = append(scanKeys, it.Key())
			scanCount++
			it.Next()
		}
		if it.Err() != nil {
			t.Fatalf("scan iteration error: %v", it.Err())
		}
		if scanCount != expectedCount {
			t.Fatalf("scan count: got %d, want %d", scanCount, expectedCount)
		}
		if !slices.IsSortedFunc(scanKeys, Int64Uint32.Compare) {
			t.Fatalf("scan keys not sorted")
		}

		// Invariant 3: Close and reopen, verify Count persists.
		if err := tree.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		tree2, err := Open(path, Int64Uint32)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer func() { _ = tree2.Close() }()

		if tree2.Count() != expectedCount {
			t.Fatalf("reopen count: got %d, want %d", tree2.Count(), expectedCount)
		}

		// Suppress unused import if os is only used via TempDir.
		_ = os.ErrNotExist
	})
}
