package raftwal_test

import (
	"fmt"
	"sync"
	"testing"

	"gastrolog/internal/raftwal"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// BenchmarkStoreLogs_WAL measures StoreLogs throughput with the shared WAL
// across N concurrent groups, each writing B logs per operation.
func BenchmarkStoreLogs_WAL(b *testing.B) {
	for _, numGroups := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("groups=%d", numGroups), func(b *testing.B) {
			dir := b.TempDir()
			w, err := raftwal.Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			stores := make([]*raftwal.GroupStore, numGroups)
			for i := range numGroups {
				stores[i] = w.GroupStore(fmt.Sprintf("group-%d", i))
			}

			b.ResetTimer()
			var wg sync.WaitGroup
			for g := range numGroups {
				wg.Add(1)
				go func() {
					defer wg.Done()
					gs := stores[g]
					for i := range b.N {
						_ = gs.StoreLogs([]*hraft.Log{{
							Index: uint64(i + 1),
							Term:  1,
							Type:  hraft.LogCommand,
							Data:  make([]byte, 256),
						}})
					}
				}()
			}
			wg.Wait()
		})
	}
}

// BenchmarkStoreLogs_BoltDB measures StoreLogs throughput with per-group
// boltdb, for comparison with the WAL benchmark.
func BenchmarkStoreLogs_BoltDB(b *testing.B) {
	for _, numGroups := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("groups=%d", numGroups), func(b *testing.B) {
			dir := b.TempDir()

			stores := make([]*raftboltdb.BoltStore, numGroups)
			for i := range numGroups {
				bs, err := raftboltdb.New(raftboltdb.Options{
					Path: fmt.Sprintf("%s/group-%d.db", dir, i),
				})
				if err != nil {
					b.Fatal(err)
				}
				defer bs.Close()
				stores[i] = bs
			}

			b.ResetTimer()
			var wg sync.WaitGroup
			for g := range numGroups {
				wg.Add(1)
				go func() {
					defer wg.Done()
					bs := stores[g]
					for i := range b.N {
						_ = bs.StoreLogs([]*hraft.Log{{
							Index: uint64(i + 1),
							Term:  1,
							Type:  hraft.LogCommand,
							Data:  make([]byte, 256),
						}})
					}
				}()
			}
			wg.Wait()
		})
	}
}
