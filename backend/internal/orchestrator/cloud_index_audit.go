package orchestrator

import (
	"context"
	"fmt"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// CloudIndexAudit is one node's read-only comparison of three views of a
// cloud-backed vault: what the cluster expects to exist (the FSM manifest, the
// owner), what the blob store actually holds, and what this node's cloud index
// caches about it.
//
// The categories are separated because they call for different responses.
// MissingBlobs is a durability incident. UntrackedBlobs is a cost and hygiene
// problem. The two index categories are node-local cache drift, repairable by
// rebuilding from the store without touching cluster state.
type CloudIndexAudit struct {
	VaultID glid.GLID
	NodeID  string

	ExpectedChunks  int // FSM manifest entries marked cloud-backed
	StoreObjects    int // objects under the vault's prefix
	IndexEntries    int // this node's cached cloud-backed entries
	ArchivedObjects int // store objects in an offline storage class

	// MissingBlobs: the cluster believes these chunks are cloud-backed and the
	// store has no object for them. Nothing else detects this — an object
	// removed by a provider lifecycle rule or an operator emits no event.
	MissingBlobs []chunk.ChunkID

	// SizeMismatches: an object exists but its size is not what the FSM
	// recorded at upload time.
	SizeMismatches []CloudIndexSizeMismatch

	// UntrackedBlobs: objects no manifest entry claims. Bytes being paid for
	// that nothing will read.
	UntrackedBlobs []chunk.ChunkID

	// TombstonedBlobs: objects whose chunk the cluster has deleted. Explainable
	// rather than leaked — the delete may still be propagating — so kept apart
	// from UntrackedBlobs.
	TombstonedBlobs []chunk.ChunkID

	// StaleIndexEntries: cached entries with no object behind them.
	StaleIndexEntries []chunk.ChunkID

	// UnindexedBlobs: objects the cluster knows about that this node's cache
	// has never recorded.
	UnindexedBlobs []chunk.ChunkID
}

// CloudIndexSizeMismatch is one object whose stored size disagrees with the
// size the cluster recorded for it.
type CloudIndexSizeMismatch struct {
	ID            chunk.ChunkID
	ExpectedBytes int64 // CloudBytes from the FSM manifest entry
	StoreBytes    int64 // size the blob store reports
}

// Clean reports whether the audit found no divergence at all.
func (a CloudIndexAudit) Clean() bool {
	return len(a.MissingBlobs) == 0 &&
		len(a.SizeMismatches) == 0 &&
		len(a.UntrackedBlobs) == 0 &&
		len(a.TombstonedBlobs) == 0 &&
		len(a.StaleIndexEntries) == 0 &&
		len(a.UnindexedBlobs) == 0
}

// AuditVaultCloudIndex compares one cloud-backed vault's blob store against the
// FSM manifest and this node's cloud index. Read-only: it never inserts,
// deletes or corrects anything, so it is safe to run against a live vault and
// safe to run repeatedly.
//
// Returns chunk.ErrCloudStoreNotConfigured for a local-only vault. That is not
// the same answer as an empty audit: "no objects, nothing wrong" would tell an
// operator their cloud vault is healthy when it has no cloud store at all.
func (o *Orchestrator) AuditVaultCloudIndex(ctx context.Context, vaultID glid.GLID) (CloudIndexAudit, error) {
	vaultInst := o.FindLocalVaultInstance(vaultID)
	if vaultInst == nil || vaultInst.Chunks == nil {
		return CloudIndexAudit{}, fmt.Errorf("audit cloud index: vault %s is not homed on this node", vaultID)
	}
	auditor, ok := vaultInst.Chunks.(chunk.CloudIndexAuditor)
	if !ok {
		return CloudIndexAudit{}, fmt.Errorf("audit cloud index: vault %s: %w", vaultID, chunk.ErrCloudStoreNotConfigured)
	}
	blobs, err := auditor.ListCloudBlobs(ctx)
	if err != nil {
		return CloudIndexAudit{}, fmt.Errorf("audit cloud index: vault %s: %w", vaultID, err)
	}

	audit := CloudIndexAudit{
		VaultID:      vaultID,
		NodeID:       o.localNodeID,
		StoreObjects: len(blobs),
	}

	byID := make(map[chunk.ChunkID]chunk.CloudBlobInfo, len(blobs))
	for _, b := range blobs {
		byID[b.ID] = b
		if b.Archived {
			audit.ArchivedObjects++
		}
	}

	cached := make(map[chunk.ChunkID]struct{})
	for _, m := range auditor.CloudIndexEntries() {
		cached[m.ID] = struct{}{}
	}
	audit.IndexEntries = len(cached)

	// Expectation side: the FSM manifest owns which chunks are cloud-backed.
	expected := make(map[chunk.ChunkID]struct{})
	for _, e := range vaultManifestEntries(vaultInst) {
		if !e.CloudBacked {
			continue
		}
		expected[e.ID] = struct{}{}
		audit.ExpectedChunks++

		blob, present := byID[e.ID]
		if !present {
			audit.MissingBlobs = append(audit.MissingBlobs, e.ID)
			continue
		}
		// CloudBytes of 0 means the FSM never recorded a transport size for
		// this chunk, which is not a mismatch — there is nothing to compare.
		if e.CloudBytes > 0 && blob.Size != e.CloudBytes {
			audit.SizeMismatches = append(audit.SizeMismatches, CloudIndexSizeMismatch{
				ID: e.ID, ExpectedBytes: e.CloudBytes, StoreBytes: blob.Size,
			})
		}
		if _, indexed := cached[e.ID]; !indexed {
			audit.UnindexedBlobs = append(audit.UnindexedBlobs, e.ID)
		}
	}

	for _, b := range blobs {
		if _, claimed := expected[b.ID]; claimed {
			continue
		}
		if vaultInst.IsTombstoned != nil && vaultInst.IsTombstoned(b.ID) {
			audit.TombstonedBlobs = append(audit.TombstonedBlobs, b.ID)
			continue
		}
		audit.UntrackedBlobs = append(audit.UntrackedBlobs, b.ID)
	}

	for id := range cached {
		if _, present := byID[id]; !present {
			audit.StaleIndexEntries = append(audit.StaleIndexEntries, id)
		}
	}

	return audit, nil
}
