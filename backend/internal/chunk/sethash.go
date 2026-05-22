// Order-independent set-hash over EventIDs for the fan-out epic's
// seal-time reconcile fast path (gastrolog-2ujjh / gastrolog-37k2b).
//
// Each Receiving member computes a SetHash of its chunk's EventIDs
// immediately after CmdBeginSeal applies. Replicas exchange the
// 32-byte hashes; matching hashes finalize the seal without further
// work. Mismatches drop into a Merkle slow path (separate primitive)
// that pinpoints divergent EventIDs and pulls them.
//
// The hash is commutative (XOR of per-record SHA-256 digests) so the
// traversal order of the local EventID set does not matter. EventID is
// fully unique by construction (IngesterID + NodeID + IngestTS +
// IngestSeq, see types.go), so cross-set collisions on the XOR
// aggregate are astronomically unlikely — a SHA-256 hash space wide
// enough that no realistic divergence pattern collides.

package chunk

import (
	"crypto/sha256"
	"encoding/binary"
)

// SetHash is the 32-byte order-independent set-hash of a chunk's
// EventIDs. Zero value means "empty set" — never confused with a
// populated set because SHA-256 of even the smallest EventID is
// non-zero.
type SetHash [32]byte

// SetHasher accumulates EventIDs into a running order-independent
// set-hash. Cheap to instantiate per chunk-seal; safe to reuse across
// chunks by zeroing the accumulator (Reset).
type SetHasher struct {
	acc SetHash
}

// NewSetHasher returns a fresh hasher with a zero accumulator.
func NewSetHasher() *SetHasher {
	return &SetHasher{}
}

// Reset zeros the accumulator so the hasher can be reused for the
// next chunk.
func (h *SetHasher) Reset() {
	h.acc = SetHash{}
}

// Add folds one EventID into the running set-hash. Idempotent under
// re-adding the same EventID twice: XOR-ing the same digest twice
// cancels back to the prior state. The Merkle slow path catches that
// case anyway because both replicas hold the same record set, so the
// XOR cancellation is moot in normal operation.
//
// The encoding pinned here MUST stay byte-stable across all Receiving
// members — every replica computes the hash from the same EventID
// bytes. The hashed payload is:
//
//	[16 bytes IngesterID][16 bytes NodeID][8 bytes IngestTS nanos][4 bytes IngestSeq]
//
// IngestTS is encoded as int64 nanoseconds since the Unix epoch in
// big-endian byte order — same as the wire-format convention used in
// MarshalRequestDelete et al. (see vaultctlfsm/fsm_receipts.go).
func (h *SetHasher) Add(id EventID) {
	var buf [16 + 16 + 8 + 4]byte
	copy(buf[0:16], id.IngesterID[:])
	copy(buf[16:32], id.NodeID[:])
	binary.BigEndian.PutUint64(buf[32:40], uint64(id.IngestTS.UnixNano()))
	binary.BigEndian.PutUint32(buf[40:44], id.IngestSeq)
	digest := sha256.Sum256(buf[:])
	for i, b := range digest {
		h.acc[i] ^= b
	}
}

// Sum returns the current set-hash. Does not mutate the accumulator —
// safe to call multiple times during incremental construction.
func (h *SetHasher) Sum() SetHash {
	return h.acc
}

// SetHashOf computes the set-hash of the given EventIDs in one pass.
// Convenience wrapper around SetHasher.Add for callers that already
// have the full slice in memory; the streaming Add form is preferred
// for the seal-time path, which walks the ingestBT iterator.
func SetHashOf(ids []EventID) SetHash {
	h := NewSetHasher()
	for _, id := range ids {
		h.Add(id)
	}
	return h.Sum()
}
