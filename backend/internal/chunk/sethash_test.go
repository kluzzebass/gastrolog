// SetHash tests for the seal-time reconcile fast path
// (gastrolog-37k2b).
//
// Properties asserted:
//   - Order-independence (commutative XOR): same EventIDs in any
//     traversal order produce the same set-hash.
//   - Empty set: zero accumulator.
//   - Distinguishes distinct EventIDs even when they share
//     IngesterID + NodeID + IngestTS (the IngestSeq disambiguates).
//   - Distinguishes per-field changes (IngesterID, NodeID, IngestTS,
//     IngestSeq each contribute to the hash).
//   - SetHashOf and SetHasher produce identical results for the same
//     input set.
//   - Adding a duplicate EventID twice cancels (XOR property) — not a
//     production invariant, but documented behavior worth pinning.

package chunk

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
)

func setHashEventID(t *testing.T, ingester, node glid.GLID, ts time.Time, seq uint32) EventID {
	t.Helper()
	return EventID{
		IngesterID: ingester,
		NodeID:     node,
		IngestTS:   ts,
		IngestSeq:  seq,
	}
}

func TestSetHashEmptySetIsZero(t *testing.T) {
	t.Parallel()
	if got := SetHashOf(nil); got != (SetHash{}) {
		t.Errorf("empty set hash = %x, want zero", got)
	}
	if got := SetHashOf([]EventID{}); got != (SetHash{}) {
		t.Errorf("empty slice hash = %x, want zero", got)
	}
}

func TestSetHashIsOrderIndependent(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()
	ids := []EventID{
		setHashEventID(t, ing, node, now, 1),
		setHashEventID(t, ing, node, now.Add(1), 2),
		setHashEventID(t, ing, node, now.Add(2), 3),
	}
	forward := SetHashOf(ids)
	reverse := SetHashOf([]EventID{ids[2], ids[1], ids[0]})
	shuffled := SetHashOf([]EventID{ids[1], ids[2], ids[0]})

	if forward == (SetHash{}) {
		t.Fatal("non-empty set hashed to zero")
	}
	if forward != reverse {
		t.Errorf("reverse-order hash differs: forward=%x reverse=%x", forward, reverse)
	}
	if forward != shuffled {
		t.Errorf("shuffled-order hash differs: forward=%x shuffled=%x", forward, shuffled)
	}
}

func TestSetHashDistinguishesDistinctSets(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()

	setA := []EventID{
		setHashEventID(t, ing, node, now, 1),
		setHashEventID(t, ing, node, now, 2),
	}
	setB := []EventID{
		setHashEventID(t, ing, node, now, 1),
		setHashEventID(t, ing, node, now, 3),
	}
	hA, hB := SetHashOf(setA), SetHashOf(setB)
	if hA == hB {
		t.Error("distinct sets produced identical hashes")
	}
}

func TestSetHashChangesPerField(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()
	base := setHashEventID(t, ing, node, now, 1)
	baseHash := SetHashOf([]EventID{base})

	cases := []struct {
		name string
		mod  EventID
	}{
		{"different IngesterID", setHashEventID(t, glid.New(), node, now, 1)},
		{"different NodeID", setHashEventID(t, ing, glid.New(), now, 1)},
		{"different IngestTS", setHashEventID(t, ing, node, now.Add(time.Nanosecond), 1)},
		{"different IngestSeq", setHashEventID(t, ing, node, now, 2)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if SetHashOf([]EventID{c.mod}) == baseHash {
				t.Errorf("%s did not change the hash", c.name)
			}
		})
	}
}

func TestSetHashStreamingMatchesBulk(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()
	ids := []EventID{
		setHashEventID(t, ing, node, now, 1),
		setHashEventID(t, ing, node, now.Add(1), 2),
		setHashEventID(t, ing, node, now.Add(2), 3),
	}
	h := NewSetHasher()
	for _, id := range ids {
		h.Add(id)
	}
	if h.Sum() != SetHashOf(ids) {
		t.Error("streaming SetHasher and SetHashOf disagree")
	}
}

func TestSetHashReset(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()
	h := NewSetHasher()
	h.Add(setHashEventID(t, ing, node, now, 1))
	h.Add(setHashEventID(t, ing, node, now, 2))
	if h.Sum() == (SetHash{}) {
		t.Fatal("non-empty accumulator hashed to zero")
	}
	h.Reset()
	if h.Sum() != (SetHash{}) {
		t.Errorf("Reset did not zero accumulator: %x", h.Sum())
	}
}

func TestSetHashDuplicateAddCancels(t *testing.T) {
	t.Parallel()
	// XOR property: adding the same EventID twice cancels to zero.
	// Not a production invariant (the production caller walks a B+ tree
	// where each EventID appears once), but documented behavior worth
	// pinning so future readers don't trip on the algebra.
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()
	id := setHashEventID(t, ing, node, now, 1)
	h := NewSetHasher()
	h.Add(id)
	h.Add(id)
	if h.Sum() != (SetHash{}) {
		t.Errorf("double-add did not cancel: %x", h.Sum())
	}
}

func TestSetHashConvergenceAcrossReplicas(t *testing.T) {
	t.Parallel()
	// The seal-time reconcile fast path: three Receiving replicas
	// each compute their local set-hash over identical EventID sets
	// (in their own traversal order from each replica's local
	// ingestBT). The three hashes must match → seal completes
	// without falling into the Merkle slow path.
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()
	ids := []EventID{
		setHashEventID(t, ing, node, now, 1),
		setHashEventID(t, ing, node, now.Add(1), 2),
		setHashEventID(t, ing, node, now.Add(2), 3),
		setHashEventID(t, ing, node, now.Add(3), 4),
		setHashEventID(t, ing, node, now.Add(4), 5),
	}

	replicaA := SetHashOf(ids)
	replicaB := SetHashOf([]EventID{ids[2], ids[0], ids[4], ids[1], ids[3]})
	replicaC := SetHashOf([]EventID{ids[4], ids[3], ids[2], ids[1], ids[0]})

	if replicaA != replicaB || replicaB != replicaC {
		t.Errorf("3-replica fast path did not converge: A=%x B=%x C=%x", replicaA, replicaB, replicaC)
	}
}

func TestSetHashDivergenceDropsToSlowPath(t *testing.T) {
	t.Parallel()
	// Symmetric scenario: one replica is missing a record. Its hash
	// must differ from the others, signaling the Merkle slow path
	// should activate.
	now := time.Unix(0, 1_700_000_000_000_000_000)
	ing, node := glid.New(), glid.New()
	full := []EventID{
		setHashEventID(t, ing, node, now, 1),
		setHashEventID(t, ing, node, now.Add(1), 2),
		setHashEventID(t, ing, node, now.Add(2), 3),
	}
	missing := []EventID{full[0], full[2]} // missing full[1]

	if SetHashOf(full) == SetHashOf(missing) {
		t.Error("missing-record set hashed identically to full set")
	}
}
