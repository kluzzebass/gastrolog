package orchestrator

import (
	"fmt"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
)

// cloudHealthChecker is an optional interface implemented by chunk managers
// that have a cloud backing store. The orchestrator polls this every 5s
// to raise/clear a "cloud-store:<tierID>" alert.
type cloudHealthChecker interface {
	CloudDegraded() bool
	CloudDegradedError() string
}

// evaluateCloudHealth checks every tier's cloud health and sets/clears
// alerts. When a tier transitions from degraded → healthy, schedules
// post-seal work for sealed chunks that are missing their cloud upload.
// Runs in the rate alert evaluator loop (every 5s).
func (o *Orchestrator) evaluateCloudHealth() {
	if o.alerts == nil {
		return
	}
	o.mu.RLock()
	defer o.mu.RUnlock()

	for _, vault := range o.vaults {
		tier := vault.Instance
		if tier == nil || tier.Type != "cloud" {
			continue
		}
		o.evaluateTierCloudHealth(tier)
	}
}

// evaluateTierCloudHealth checks a single cloud tier's health and runs
// backfill on the tier leader only. Followers skip backfill — they learn
// about cloud-backed chunks via the tier FSM.
func (o *Orchestrator) evaluateTierCloudHealth(tier *VaultInstance) {
	chk, ok := tier.Chunks.(cloudHealthChecker)
	if !ok {
		return
	}
	alertID := fmt.Sprintf("cloud-store:%s", tier.TierID)
	if chk.CloudDegraded() {
		o.alerts.Set(alertID, alert.Error, "cloud",
			fmt.Sprintf("Cloud store unreachable for tier %s: %s",
				tier.TierID.String()[:8], chk.CloudDegradedError()))
	} else {
		o.alerts.Clear(alertID)
	}
	if tier.IsRaftLeader != nil && tier.IsRaftLeader() {
		o.backfillCloudUploads(tier)
	}
}

// backfillCloudUploads reconciles sealed chunks against the tier FSM
// (the single source of truth for CloudBacked). For every sealed chunk
// where the FSM says CloudBacked=false, it schedules an UploadToCloud job.
// UploadToCloud does a Head check — if the blob already exists in S3, it
// adopts and fires AnnounceUpload to update the FSM. If not, it uploads.
//
// The local CloudBacked flag from List() is intentionally ignored — only
// the FSM decides whether a chunk needs work. See gastrolog-68fqk.
func (o *Orchestrator) backfillCloudUploads(tier *VaultInstance) {
	uploader, ok := tier.Chunks.(chunk.ChunkCloudUploader)
	if !ok {
		return
	}

	metas, err := tier.Chunks.List()
	if err != nil {
		return
	}

	var backfilled int
	for _, m := range metas {
		if !m.Sealed || chunkIsCloudBacked(tier, m) {
			continue
		}
		name := fmt.Sprintf("cloud-backfill:%s:%s", tier.TierID, m.ID)
		if o.scheduler.HasPendingPrefix(name) {
			continue
		}
		if err := o.scheduler.RunOnce(name, func(id chunk.ChunkID) error {
			return uploader.UploadToCloud(id)
		}, m.ID); err == nil {
			backfilled++
		}
		o.scheduler.Describe(name, fmt.Sprintf("Cloud backfill upload for chunk %s", m.ID))
	}
	if backfilled > 0 {
		o.logger.Debug("cloud backfill: scheduled uploads",
			"tier", tier.TierID, "count", backfilled)
	}
}

// chunkIsCloudBacked checks the FSM (single source of truth) for CloudBacked.
// Falls back to local state when no FSM exists (single-node / memory mode).
func chunkIsCloudBacked(tier *VaultInstance, m chunk.ChunkMeta) bool {
	if tier.OverlayFromFSM != nil {
		return tier.OverlayFromFSM(m).CloudBacked
	}
	return m.CloudBacked
}
