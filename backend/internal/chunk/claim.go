package chunk

// IndexSizeLookup resolves the on-disk sizes of a chunk's index files, keyed
// by index name. DiskClaim only calls it for chunks with no directly
// recorded DiskBytes (legacy chunks that pre-date DiskBytes tracking); a nil
// lookup, or one that errors, contributes zero extra index bytes rather than
// failing the claim.
type IndexSizeLookup func(id ChunkID) (map[string]int64, error)

// DiskClaim is the local disk claim for a single chunk: what deleting this
// chunk would actually free on this node. One formula, one currency, shared
// by every consumer that measures local disk against a max-size bound — the
// disk guard's whole-vault footprint (everything the vault holds) and the
// size-drain trigger (SizeRetentionPolicy, the retained chunk store only).
// Scope differs by caller; the per-chunk arithmetic does not.
//
//   - Cloud-backed chunk with no local copy (DiskBytes == 0): 0. There is
//     nothing to reclaim locally by destroying it.
//   - DiskBytes recorded (> 0): DiskBytes. This is also what a cached
//     cloud-backed chunk reports — its cache file takes up exactly as much
//     local disk as an equivalent file-vault chunk.
//   - Otherwise (legacy chunk sealed before DiskBytes tracking existed):
//     logical Bytes plus index file sizes via indexSizes.
func DiskClaim(meta ChunkMeta, indexSizes IndexSizeLookup) int64 {
	if meta.CloudBacked && meta.DiskBytes == 0 {
		return 0
	}
	if meta.DiskBytes > 0 {
		return meta.DiskBytes
	}
	claim := meta.Bytes
	if indexSizes != nil {
		if sizes, err := indexSizes(meta.ID); err == nil {
			for _, sz := range sizes {
				claim += sz
			}
		}
	}
	return claim
}
