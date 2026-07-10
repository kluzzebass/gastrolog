package orchestrator

import (
	"context"
	"os"
	"path/filepath"

	"gastrolog/internal/diskusage"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/pipeline/paths"
)

// LocalStorageBytes returns total on-disk bytes on this node. Counts use
// authoritative metadata from chunk managers, vault-ctl FSM segment registry,
// and system config where available; the filesystem is consulted only for
// pipeline segment files not yet covered by FSM metadata, plus raft WAL/snapshots
// (which have no replicated byte totals).
func (o *Orchestrator) LocalStorageBytes() int64 {
	ctx := context.Background()
	var total int64
	total += o.localChunkStorageBytes()
	for _, vaultID := range o.ListVaults() {
		total += o.localPipelineSegmentStorageBytes(vaultID)
	}
	total += o.localRaftStorageBytes()
	total += o.localManagedFileStorageBytes(ctx)
	return total
}

func (o *Orchestrator) localChunkStorageBytes() int64 {
	var total int64
	for _, vaultID := range o.ListVaults() {
		total += o.localVaultChunkBytes(vaultID)
	}
	return total
}

// localVaultChunkBytes is one vault's local chunk-store claim: sealed GLCB
// bytes where recorded, otherwise record bytes plus index sizes. Cloud-backed
// chunks without a local copy cost nothing here.
func (o *Orchestrator) localVaultChunkBytes(vaultID glid.GLID) int64 {
	metas, err := o.ListLocalChunkMetas(vaultID)
	if err != nil {
		return 0
	}
	var total int64
	for _, meta := range metas {
		if meta.CloudBacked && meta.DiskBytes == 0 {
			continue
		}
		if meta.DiskBytes > 0 {
			total += meta.DiskBytes
			continue
		}
		total += meta.Bytes
		if sizes, err := o.IndexSizes(vaultID, meta.ID); err == nil {
			for _, sz := range sizes {
				total += sz
			}
		}
	}
	return total
}

// localVaultFootprintBytes is the vault's whole local disk claim — chunk
// store plus pipeline segment backlog — measured against the vault's
// max-size budget. Backlog counts by design: the budget bounds everything
// the vault holds on this node, not just its retained data.
func (o *Orchestrator) localVaultFootprintBytes(vaultID glid.GLID) int64 {
	return o.localVaultChunkBytes(vaultID) + o.localPipelineSegmentStorageBytes(vaultID)
}

// vaultRegistryBacklogBytes is the vault's pipeline backlog measured against
// the cluster-global backlog budget: total bytes of unreleased completed
// segments in the vault-ctl registry. Unlike the local measures above this is
// FSM-replicated cluster state — every node computes the same value, so the
// backlog guard needs no peer broadcast. Zero for non-pipeline vaults and
// before the FSM is available.
func (o *Orchestrator) vaultRegistryBacklogBytes(vaultID glid.GLID) int64 {
	fsm, _, _, ok := o.vaultCtlHandle(vaultID)
	if !ok || fsm == nil {
		return 0
	}
	var total int64
	for _, entry := range fsm.ListCompletedSegments() {
		total += int64(entry.ByteSize) //nolint:gosec // segment sizes are bounded
	}
	return total
}

func (o *Orchestrator) localPipelineSegmentStorageBytes(vaultID glid.GLID) int64 {
	o.mu.RLock()
	_, inPipeline := o.pipelineVaults[vaultID]
	o.mu.RUnlock()
	if !inPipeline {
		return 0
	}
	root, err := o.originRoot(vaultID)
	if err != nil {
		return 0
	}

	fsmBytes := make(map[glid.GLID]uint64)
	if fsm, _, _, ok := o.vaultCtlHandle(vaultID); ok && fsm != nil {
		for _, entry := range fsm.ListCompletedSegments() {
			fsmBytes[entry.SegmentID] = entry.ByteSize
		}
	}

	onDisk, err := listLocalSegmentFiles(root)
	if err != nil {
		return 0
	}

	var total int64
	for id, path := range onDisk {
		if sz, ok := fsmBytes[id]; ok {
			total += int64(sz) //nolint:gosec // segment sizes are bounded
			continue
		}
		total += diskusage.FileBytes(path)
	}
	return total
}

func listLocalSegmentFiles(root string) (map[glid.GLID]string, error) {
	out := make(map[glid.GLID]string)
	areas := []string{
		paths.WorkingDir(root),
		paths.CompletedDir(root),
		paths.HeadDir(root),
		paths.PreHeadDir(root),
	}
	for _, dir := range areas {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			id, err := glid.ParseUUID(entry.Name())
			if err != nil {
				continue
			}
			if _, seen := out[id]; seen {
				continue
			}
			out[id] = filepath.Join(dir, entry.Name())
		}
	}
	return out, nil
}

func (o *Orchestrator) localRaftStorageBytes() int64 {
	homeRoot := o.homeDir
	if homeRoot == "" && o.segmentsDir != "" {
		homeRoot = filepath.Dir(o.segmentsDir)
	}
	if homeRoot == "" {
		return 0
	}
	return diskusage.DirBytes(home.New(homeRoot).RaftDir())
}

func (o *Orchestrator) localManagedFileStorageBytes(ctx context.Context) int64 {
	homeRoot := o.homeDir
	if homeRoot == "" && o.segmentsDir != "" {
		homeRoot = filepath.Dir(o.segmentsDir)
	}
	if homeRoot == "" {
		return 0
	}
	sys, err := o.loadSystem(ctx)
	if err != nil || sys == nil {
		return 0
	}
	hd := home.New(homeRoot)
	var total int64
	for _, mf := range sys.Config.ManagedFiles {
		path := hd.ManagedFilePath(mf.ID.String())
		if mf.Size > 0 {
			if _, err := os.Stat(path); err == nil {
				total += mf.Size
			}
			continue
		}
		total += diskusage.FileBytes(path)
	}
	return total
}

func homeDirFromSegments(segmentsDir string) string {
	if segmentsDir == "" {
		return ""
	}
	return filepath.Dir(segmentsDir)
}
