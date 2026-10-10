package orchestrator

// A retention sweep's candidates are the chunks the leader's chunk store
// lists plus the sealed manifest entries it does not: a leader that lacks a
// local copy (a restart before local resolution, a placement change, a lost
// copy) still has to enforce the vault's bounds over them. The count and size
// policies keep the newest chunks, so whatever mix of local and manifest-only
// candidates a sweep sees, the chunks it deletes must be the oldest ones.

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// leaderCopyPatterns are the shapes of "which chunks the leader holds
// locally" a sweep can meet, over chunks seeded oldest first.
var leaderCopyPatterns = []struct {
	name     string
	onLeader func(i, n int) bool
}{
	{"all-local", func(int, int) bool { return true }},
	{"interleaved", func(i, _ int) bool { return i%2 == 1 }},
	{"leader-lacks-oldest", func(i, n int) bool { return i >= n/2 }},
	{"leader-lacks-newest", func(i, n int) bool { return i < n/2 }},
	{"leader-holds-only-newest", func(i, n int) bool { return i == n-1 }},
	{"no-local-copies", func(int, int) bool { return false }},
}

// seedOldestFirst seeds n chunks with strictly increasing WriteStart; the
// leader holds a local copy of chunk i only where onLeader says so.
func (c *boundCluster) seedOldestFirst(n int, onLeader func(i, n int) bool) []chunk.ChunkID {
	c.t.Helper()
	starts := make([]time.Time, n)
	for i := range starts {
		starts[i] = c.base.Add(time.Duration(c.seeded) * time.Minute)
		c.seeded++
	}
	return c.seedAt(starts, time.Now(), func(i int) bool { return onLeader(i, n) })
}

// assertPendingExactly checks the leader's committed pending deletes are
// exactly want, naming any retained chunk deleted in an older one's place.
func (c *boundCluster) assertPendingExactly(label string, all, want []chunk.ChunkID) {
	t := c.t
	t.Helper()
	got := c.pendingOnLeader()
	if sameChunkSet(got, want) {
		return
	}
	pending := make(map[chunk.ChunkID]bool, len(got))
	for _, id := range got {
		pending[id] = true
	}
	var deletedKept, keptExpired []int
	for i, id := range all {
		switch {
		case pending[id] && !slices.Contains(want, id):
			deletedKept = append(deletedKept, i)
		case !pending[id] && slices.Contains(want, id):
			keptExpired = append(keptExpired, i)
		}
	}
	t.Fatalf("%s: retention deleted chunks %v (0 = oldest) the bound keeps, and kept older chunks %v in their place",
		label, deletedKept, keptExpired)
}

func clusterShapes() [][]string {
	return [][]string{fourNodes[:1], fourNodes}
}

// TestRetentionCountDeletesOldestWhateverLeaderHolds: eight chunks against a
// count bound of three must lose exactly the five oldest, whichever of them
// the leader holds locally.
func TestRetentionCountDeletesOldestWhateverLeaderHolds(t *testing.T) {
	t.Parallel()
	for _, nodeIDs := range clusterShapes() {
		for _, p := range leaderCopyPatterns {
			t.Run(fmt.Sprintf("%d-nodes/%s", len(nodeIDs), p.name), func(t *testing.T) {
				t.Parallel()
				c := newBoundCluster(t, nodeIDs, len(nodeIDs))
				ids := c.seedOldestFirst(8, p.onLeader)

				c.log.hold(nodeIDs...)
				c.runner.sweep(countBoundRules(3))

				c.assertPendingExactly("count bound", ids, ids[:5])
				c.assertUncapped("count bound")
			})
		}
	}
}

// TestRetentionSizeKeepsNewestWhateverLeaderHolds runs the size bound over
// the same shapes. Locally listed chunks claim their on-disk bytes and
// manifest-only ones their logical bytes plus indexes, so the claims are
// mixed; the budget is exactly the newest three chunks' claims.
func TestRetentionSizeKeepsNewestWhateverLeaderHolds(t *testing.T) {
	t.Parallel()
	for _, nodeIDs := range clusterShapes() {
		for _, p := range leaderCopyPatterns {
			t.Run(fmt.Sprintf("%d-nodes/%s", len(nodeIDs), p.name), func(t *testing.T) {
				t.Parallel()
				c := newBoundCluster(t, nodeIDs, len(nodeIDs))
				ids := c.seedOldestFirst(8, p.onLeader)

				claims := c.sweepClaims()
				var budget int64
				for _, id := range ids[5:] {
					if claims[id] <= 0 {
						t.Fatalf("fixture: chunk %s has claim %d, want > 0", id, claims[id])
					}
					budget += claims[id]
				}
				if p.name == "interleaved" && claims[ids[0]] == claims[ids[1]] {
					t.Fatalf("fixture: local and manifest-only claims must differ, both %d", claims[ids[0]])
				}
				size := chunk.NewSizeRetentionPolicy(budget)

				c.log.hold(nodeIDs...)
				c.runner.sweep([]retentionRule{{policy: size}})

				c.assertPendingExactly("size bound", ids, ids[:5])
			})
		}
	}
}

// sweepClaims is the disk claim the leader's next sweep assigns each
// candidate, local and manifest-only alike.
func (c *boundCluster) sweepClaims() map[chunk.ChunkID]int64 {
	c.t.Helper()
	metas, err := c.leader().cm.List()
	if err != nil {
		c.t.Fatal(err)
	}
	return c.runner.chunkDiskClaims(appendUnlistedManifestSealed(metas, c.leader().inst))
}

// TestRetentionOrderAfterLeaderRestartBeforeLocalResolution restarts the
// placement leader holding a local copy of only one chunk; the fresh
// process's first sweep sees almost every candidate from the manifest alone.
func TestRetentionOrderAfterLeaderRestartBeforeLocalResolution(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	ids := c.seedOldestFirst(10, func(i, _ int) bool { return i == 6 })

	c.restartLeader()
	c.log.hold(fourNodes...)
	c.runner.sweep(countBoundRules(4))

	c.assertPendingExactly("after restart", ids, ids[:6])
	c.assertUncapped("after restart")
}

// TestRetentionOrderUnderSteadyIngestWithoutLeaderCopies seals chunks round
// after round, only some of which ever reach the leader's disk, with every
// round's deletes finalizing before the next: the manifest must always hold
// exactly the newest chunks.
func TestRetentionOrderUnderSteadyIngestWithoutLeaderCopies(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	rules := countBoundRules(3)

	var all []chunk.ChunkID
	finalized := 0
	for round := range 6 {
		label := fmt.Sprintf("round %d", round)
		all = append(all, c.seedOldestFirst(3, func(i, _ int) bool { return (i+round)%3 == 0 })...)
		c.log.hold(fourNodes...)
		c.runner.sweep(rules)
		kept := all[max(0, len(all)-3):]
		expired := all[finalized : len(all)-len(kept)]
		c.assertPendingExactly(label, all, expired)
		c.log.release(fourNodes...)
		c.log.waitProposed(expired, fourNodes)
		finalized = len(all) - len(kept)
		c.assertManifest(label, kept, nil)
		c.assertUncapped(label)
	}
}

// TestRetentionTiesInWriteStartResolveByChunkID seals six chunks with the
// same WriteStart, half of them manifest-only on the leader. Chunk IDs order
// by creation, so the earliest-created chunks are the ones the bound drops,
// on every run.
func TestRetentionTiesInWriteStartResolveByChunkID(t *testing.T) {
	t.Parallel()
	for _, nodeIDs := range clusterShapes() {
		t.Run(fmt.Sprintf("%d-nodes", len(nodeIDs)), func(t *testing.T) {
			t.Parallel()
			c := newBoundCluster(t, nodeIDs, len(nodeIDs))
			starts := slices.Repeat([]time.Time{c.base}, 6)
			ids := c.seedAt(starts, time.Now(), func(i int) bool { return i%2 == 0 })
			if !slices.IsSortedFunc(ids, chunk.ChunkID.Compare) {
				t.Fatal("fixture: chunk IDs must order by creation")
			}

			c.log.hold(nodeIDs...)
			c.runner.sweep(countBoundRules(2))

			c.assertPendingExactly("tied WriteStart", ids, ids[:4])
		})
	}
}
