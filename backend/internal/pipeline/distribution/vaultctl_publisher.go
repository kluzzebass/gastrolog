package distribution

import (
	"context"
	"errors"
	"time"

	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// VaultCtlApplier applies marshaled vaultctlfsm commands for one vault.
// Orchestrator wiring typically uses vaultctlfsm.Applier, which wraps
// vaultraft.MarshalVaultChunkCommand for the vault-instance ID.
type VaultCtlApplier interface {
	Apply(data []byte) error
}

// VaultCtlPublisher publishes completed segment metadata to vault-ctl Raft.
type VaultCtlPublisher struct {
	Applier      VaultCtlApplier
	OriginNodeID string
	Now          func() time.Time
}

var _ Publisher = (*VaultCtlPublisher)(nil)

// Publish commits segment metadata via CmdPublishCompletedSegment.
func (p *VaultCtlPublisher) Publish(ctx context.Context, meta Metadata) error {
	return p.PublishBatch(ctx, []Metadata{meta})
}

// PublishBatch commits one or more segments via a single vault-ctl apply when
// len(metas) > 1.
func (p *VaultCtlPublisher) PublishBatch(_ context.Context, metas []Metadata) error {
	if p.Applier == nil {
		return errors.New("vault-ctl applier required")
	}
	if len(metas) == 0 {
		return nil
	}
	now := p.now()
	entries := make([]vaultctlfsm.CompletedSegmentEntry, len(metas))
	for i, meta := range metas {
		entries[i] = CompletedSegmentEntryFromMetadata(meta, p.OriginNodeID, now)
	}
	if len(entries) == 1 {
		return p.Applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(entries[0]))
	}
	return p.Applier.Apply(vaultctlfsm.MarshalPublishCompletedSegments(entries))
}

func (p *VaultCtlPublisher) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now().UTC()
}

// CompletedSegmentEntryFromMetadata converts pipeline publish metadata to
// vault-ctl registry form.
func CompletedSegmentEntryFromMetadata(meta Metadata, originNodeID string, publishedAt time.Time) vaultctlfsm.CompletedSegmentEntry {
	return vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     meta.SegmentID,
		RecordCount:   meta.RecordCount,
		ByteSize:      meta.ByteSize,
		FirstIngestTS: meta.FirstIngestTS,
		LastIngestTS:  meta.LastIngestTS,
		Checksum:      meta.Checksum,
		OriginNodeID:  originNodeID,
		PublishedAt:   publishedAt,
	}
}
