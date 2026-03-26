package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/analyzer"
)

// IndexInfo describes a single index for a chunk.
type IndexInfo struct {
	Name       string
	Exists     bool
	EntryCount int64
	SizeBytes  int64
}

// ChunkIndexReport aggregates chunk seal status and per-index info.
type ChunkIndexReport struct {
	Sealed  bool
	Indexes []IndexInfo
}

// vaultManagers looks up both managers for a vault under RLock.
// Returns ErrVaultNotFound if the vault doesn't exist.
func (o *Orchestrator) vaultManagers(vaultID uuid.UUID) (chunk.ChunkManager, index.IndexManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	return s.ChunkManager(), s.IndexManager(), nil
}

// chunkManager looks up the chunk manager for a vault under RLock.
func (o *Orchestrator) chunkManager(vaultID uuid.UUID) (chunk.ChunkManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	return s.ChunkManager(), nil
}

// indexManager looks up the index manager for a vault under RLock.
func (o *Orchestrator) indexManager(vaultID uuid.UUID) (index.IndexManager, error) {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	return s.IndexManager(), nil
}

// --- Chunk read ---

// TieredChunkMeta pairs a chunk with the tier it belongs to.
type TieredChunkMeta struct {
	chunk.ChunkMeta
	TierID   uuid.UUID
	TierType string
}

// ListChunkMetas returns all chunk metadata for a vault (active tier only).
// Use ListAllChunkMetas for a multi-tier view.
func (o *Orchestrator) ListChunkMetas(vaultID uuid.UUID) ([]chunk.ChunkMeta, error) {
	cm, err := o.chunkManager(vaultID)
	if err != nil {
		return nil, err
	}
	return cm.List()
}

// ListAllChunkMetas returns chunk metadata from ALL local tiers of a vault,
// each tagged with its tier ID and type.
func (o *Orchestrator) ListAllChunkMetas(vaultID uuid.UUID) ([]TieredChunkMeta, error) {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	var result []TieredChunkMeta
	for _, tier := range vault.Tiers {
		metas, err := tier.Chunks.List()
		if err != nil {
			return nil, fmt.Errorf("list chunks for tier %s: %w", tier.TierID, err)
		}
		for _, m := range metas {
			result = append(result, TieredChunkMeta{
				ChunkMeta: m,
				TierID:    tier.TierID,
				TierType:  tier.Type,
			})
		}
	}
	return result, nil
}

// GetChunkMeta returns metadata for a specific chunk.
func (o *Orchestrator) GetChunkMeta(vaultID uuid.UUID, chunkID chunk.ChunkID) (chunk.ChunkMeta, error) {
	cm, err := o.chunkManager(vaultID)
	if err != nil {
		return chunk.ChunkMeta{}, err
	}
	return cm.Meta(chunkID)
}

// OpenCursor opens a record cursor for the given chunk.
func (o *Orchestrator) OpenCursor(vaultID uuid.UUID, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	cm, err := o.chunkManager(vaultID)
	if err != nil {
		return nil, err
	}
	return cm.OpenCursor(chunkID)
}

// VaultExists returns true if a vault with the given ID is registered.
func (o *Orchestrator) VaultExists(vaultID uuid.UUID) bool {
	o.mu.RLock()
	s := o.vaults[vaultID]
	o.mu.RUnlock()
	return s != nil
}

// VaultType returns the type string for a registered vault, or "" if not found.
func (o *Orchestrator) VaultType(vaultID uuid.UUID) string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if v := o.vaults[vaultID]; v != nil {
		return v.Type()
	}
	return ""
}

// FindLocalTierExported is a public accessor for findLocalTier, used by tests.
func (o *Orchestrator) FindLocalTierExported(vaultID, tierID uuid.UUID) *TierInstance {
	return o.findLocalTier(vaultID, tierID)
}

// findLocalTier returns the TierInstance for a specific tier in a vault,
// or nil if the tier is not local.
func (o *Orchestrator) findLocalTier(vaultID, tierID uuid.UUID) *TierInstance {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return nil
	}
	for _, t := range vault.Tiers {
		if t.TierID == tierID {
			return t
		}
	}
	return nil
}

// AppendToTier appends a record to a specific tier's chunk manager.
// Used by inter-tier transition to target a downstream tier directly,
// bypassing the vault's active tier. Includes seal detection.
func (o *Orchestrator) AppendToTier(vaultID, tierID uuid.UUID, rec chunk.Record) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	vault := o.vaults[vaultID]
	if vault == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	for _, tier := range vault.Tiers {
		if tier.TierID != tierID {
			continue
		}
		cm := tier.Chunks

		activeBefore := cm.Active()
		if _, _, err := cm.Append(rec); err != nil {
			return err
		}

		activeAfter := cm.Active()
		if activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID) {
			o.schedulePostSeal(vaultID, cm, activeBefore.ID)
		}
		return nil
	}
	return fmt.Errorf("tier %s not found in vault %s", tierID, vaultID)
}

// --- Chunk write ---

// Append appends a record to the vault's active chunk.
// If the append triggers a rotation (e.g. age or size policy), post-seal
// work (compression + index builds) is scheduled automatically.
//
// This is the sole write path for all record sources: local ingesters,
// cluster-forwarded records, and the ImportRecords API.
func (o *Orchestrator) Append(vaultID uuid.UUID, rec chunk.Record) (chunk.ChunkID, uint64, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.appendRecord(vaultID, rec)
}

// appendRecord is the unified append-with-seal-detection path.
// Caller MUST hold o.mu.
func (o *Orchestrator) appendRecord(vaultID uuid.UUID, rec chunk.Record) (chunk.ChunkID, uint64, error) {
	vault := o.vaults[vaultID]
	if vault == nil {
		return chunk.ChunkID{}, 0, fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}
	if !vault.Enabled {
		return chunk.ChunkID{}, 0, fmt.Errorf("%w: %s", ErrVaultDisabled, vaultID)
	}

	cm := vault.ChunkManager()
	activeBefore := cm.Active()
	cid, pos, err := cm.Append(rec)
	if err != nil {
		return cid, pos, err
	}

	// Detect seal: if Active() changed, the previous chunk was sealed.
	activeAfter := cm.Active()
	if activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID) {
		o.schedulePostSeal(vaultID, cm, activeBefore.ID)
	}

	return cid, pos, nil
}

// ImportChunkRecords creates a new sealed chunk from the given records in the
// target vault. Used by the ForwardImportRecords handler to receive cross-node
// chunk migrations. Works with any ChunkManager type (file or memory).
// Compression and index builds are scheduled asynchronously via the scheduler.
func (o *Orchestrator) ImportChunkRecords(ctx context.Context, vaultID uuid.UUID, next chunk.RecordIterator) error {
	cm, _, err := o.vaultManagers(vaultID)
	if err != nil {
		return err
	}

	meta, err := cm.ImportRecords(next)
	if err != nil {
		return fmt.Errorf("import records: %w", err)
	}

	if meta.ID != (chunk.ChunkID{}) {
		o.mu.RLock()
		o.schedulePostSeal(vaultID, cm, meta.ID)
		o.mu.RUnlock()
	}

	return nil
}

// ImportToTier imports records as a sealed chunk into a specific tier,
// preserving the given chunk ID. Used by sealed-chunk replication —
// the secondary receives a sealed chunk from the primary with the same ID.
// Schedules postSealWork for local indexing (secondaries need indexes for queries)
// but won't trigger further replication (gated by !IsSecondary in tierReplicationInfo).
func (o *Orchestrator) ImportToTier(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
	// Look up the tier under lock, then release BEFORE the import.
	// ImportRecords reads from a network stream and can block — holding
	// RLock during a network read starves writers (FSM dispatcher) and
	// deadlocks the entire orchestrator.
	cm := func() chunk.ChunkManager {
		o.mu.RLock()
		defer o.mu.RUnlock()
		vault := o.vaults[vaultID]
		if vault == nil {
			return nil
		}
		for _, tier := range vault.Tiers {
			if tier.TierID == tierID {
				return tier.Chunks
			}
		}
		return nil
	}()
	if cm == nil {
		return fmt.Errorf("%w: tier %s in vault %s", ErrVaultNotFound, tierID, vaultID)
	}

	cm.SetNextChunkID(chunkID)
	meta, err := cm.ImportRecords(next)
	if err != nil {
		return fmt.Errorf("import to tier %s: %w", tierID, err)
	}
	o.logger.Info("replication: sealed chunk imported",
		"vault", vaultID, "tier", tierID,
		"chunk", meta.ID.String(), "records", meta.RecordCount)

	if meta.ID != (chunk.ChunkID{}) {
		o.postSealWork(vaultID, cm, meta.ID)
	}
	return nil
}

// SealActive seals the active chunk if it has records. No-op if empty or no active chunk.
// After sealing, schedules compression and index builds (same as ingest-triggered seal).
func (o *Orchestrator) SealActive(vaultID uuid.UUID) error {
	o.mu.RLock()
	vault := o.vaults[vaultID]
	o.mu.RUnlock()
	if vault == nil {
		return fmt.Errorf("%w: %s", ErrVaultNotFound, vaultID)
	}

	cm := vault.ChunkManager()
	active := cm.Active()
	if active == nil || active.RecordCount == 0 {
		return nil
	}
	chunkID := active.ID

	if err := cm.Seal(); err != nil {
		return err
	}

	// Schedule post-seal pipeline (same as ingest onSeal callback).
	o.mu.RLock()
	o.schedulePostSeal(vaultID, cm, chunkID)
	o.mu.RUnlock()

	return nil
}

// --- Index ops ---

// IndexSizes returns the size in bytes for each index of a chunk.
func (o *Orchestrator) IndexSizes(vaultID uuid.UUID, chunkID chunk.ChunkID) (map[string]int64, error) {
	im, err := o.indexManager(vaultID)
	if err != nil {
		return nil, err
	}
	return im.IndexSizes(chunkID), nil
}

// IndexesComplete reports whether all indexes exist for the given chunk.
func (o *Orchestrator) IndexesComplete(vaultID uuid.UUID, chunkID chunk.ChunkID) (bool, error) {
	im, err := o.indexManager(vaultID)
	if err != nil {
		return false, err
	}
	return im.IndexesComplete(chunkID)
}

// BuildIndexes builds all indexes for a sealed chunk.
func (o *Orchestrator) BuildIndexes(ctx context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID) error {
	im, err := o.indexManager(vaultID)
	if err != nil {
		return err
	}
	return im.BuildIndexes(ctx, chunkID)
}

// DeleteIndexes removes all indexes for a chunk.
func (o *Orchestrator) DeleteIndexes(vaultID uuid.UUID, chunkID chunk.ChunkID) error {
	im, err := o.indexManager(vaultID)
	if err != nil {
		return err
	}
	return im.DeleteIndexes(chunkID)
}

// --- Composite ---

// ChunkIndexInfos returns seal status and per-index info for a chunk.
func (o *Orchestrator) ChunkIndexInfos(vaultID uuid.UUID, chunkID chunk.ChunkID) (*ChunkIndexReport, error) {
	cm, im, err := o.vaultManagers(vaultID)
	if err != nil {
		return nil, err
	}

	meta, err := cm.Meta(chunkID)
	if err != nil {
		return nil, err
	}

	sizes := im.IndexSizes(chunkID)

	report := &ChunkIndexReport{
		Sealed:  meta.Sealed,
		Indexes: make([]IndexInfo, 0, 7),
	}

	// Token index
	if idx, err := im.OpenTokenIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "token", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["token"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "token"})
	}

	// Attr key index
	if idx, err := im.OpenAttrKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_key"})
	}

	// Attr value index
	if idx, err := im.OpenAttrValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_val"})
	}

	// Attr kv index
	if idx, err := im.OpenAttrKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "attr_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["attr_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "attr_kv"})
	}

	// KV key index
	if idx, _, err := im.OpenKVKeyIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_key", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_key"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_key"})
	}

	// KV value index
	if idx, _, err := im.OpenKVValueIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_val", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_val"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_val"})
	}

	// KV combined index
	if idx, _, err := im.OpenKVIndex(chunkID); err == nil {
		report.Indexes = append(report.Indexes, IndexInfo{
			Name: "kv_kv", Exists: true, EntryCount: int64(len(idx.Entries())), SizeBytes: sizes["kv_kv"],
		})
	} else {
		report.Indexes = append(report.Indexes, IndexInfo{Name: "kv_kv"})
	}

	return report, nil
}

// NewAnalyzer creates an index analyzer for the given vault.
func (o *Orchestrator) NewAnalyzer(vaultID uuid.UUID) (*analyzer.Analyzer, error) {
	cm, im, err := o.vaultManagers(vaultID)
	if err != nil {
		return nil, err
	}
	return analyzer.New(cm, im), nil
}

// SupportsChunkMove returns true if both vaults support filesystem-level chunk moves.
func (o *Orchestrator) SupportsChunkMove(srcID, dstID uuid.UUID) bool {
	o.mu.RLock()
	srcVault := o.vaults[srcID]
	dstVault := o.vaults[dstID]
	o.mu.RUnlock()
	if srcVault == nil || dstVault == nil {
		return false
	}
	_, srcOK := srcVault.ChunkManager().(chunk.ChunkMover)
	_, dstOK := dstVault.ChunkManager().(chunk.ChunkMover)
	return srcOK && dstOK
}
