package orchestrator

import (
	"os"
	"path/filepath"

	"gastrolog/internal/diskusage"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// PipelineDiskSegmentCounts holds on-disk segment file counts and byte totals
// for one node.
type PipelineDiskSegmentCounts struct {
	Working               int
	CompletedStaging      int
	Head                  int
	PreHead               int
	WorkingBytes          int64
	CompletedStagingBytes int64
	HeadBytes             int64
	PreHeadBytes          int64
}

// LocalPipelineDiskSegmentCounts counts segment GLID files under the vault's
// pipeline storage areas on this node. Missing directories count as zero.
func (o *Orchestrator) LocalPipelineDiskSegmentCounts(vaultID glid.GLID) (PipelineDiskSegmentCounts, error) {
	var out PipelineDiskSegmentCounts
	root, err := o.originRoot(vaultID)
	if err != nil {
		return out, err
	}
	fsmBytes := o.completedSegmentByteSizes(vaultID)
	if n, b, err := segmentAreaStats(paths.WorkingDir(root), fsmBytes); err != nil {
		return out, err
	} else {
		out.Working, out.WorkingBytes = n, b
	}
	if n, b, err := segmentAreaStats(paths.CompletedDir(root), fsmBytes); err != nil {
		return out, err
	} else {
		out.CompletedStaging, out.CompletedStagingBytes = n, b
	}
	if n, b, err := segmentAreaStats(paths.HeadDir(root), fsmBytes); err != nil {
		return out, err
	} else {
		out.Head, out.HeadBytes = n, b
	}
	if n, b, err := segmentAreaStats(paths.PreHeadDir(root), fsmBytes); err != nil {
		return out, err
	} else {
		out.PreHead, out.PreHeadBytes = n, b
	}
	return out, nil
}

func (o *Orchestrator) completedSegmentByteSizes(vaultID glid.GLID) map[glid.GLID]uint64 {
	out := make(map[glid.GLID]uint64)
	fsm, _, _, ok := o.vaultCtlHandle(vaultID)
	if !ok || fsm == nil {
		return out
	}
	for _, entry := range fsm.ListCompletedSegments() {
		out[entry.SegmentID] = entry.ByteSize
	}
	return out
}

func segmentAreaStats(dir string, fsmBytes map[glid.GLID]uint64) (count int, bytes int64, err error) {
	ids, err := paths.ListSegmentIDs(dir)
	if err != nil {
		return 0, 0, err
	}
	count = len(ids)
	for id := range ids {
		if sz, ok := fsmBytes[id]; ok {
			bytes += int64(sz) //nolint:gosec // segment sizes are bounded
			continue
		}
		bytes += diskusage.FileBytes(filepath.Join(dir, id.String()))
	}
	return count, bytes, nil
}

// AddDiskCounts merges remote disk totals into dst.
func (c *PipelineDiskSegmentCounts) AddDiskCounts(other PipelineDiskSegmentCounts) {
	c.Working += other.Working
	c.CompletedStaging += other.CompletedStaging
	c.Head += other.Head
	c.PreHead += other.PreHead
	c.WorkingBytes += other.WorkingBytes
	c.CompletedStagingBytes += other.CompletedStagingBytes
	c.HeadBytes += other.HeadBytes
	c.PreHeadBytes += other.PreHeadBytes
}

// TouchSegmentFile creates an empty segment file for tests.
func touchSegmentFile(dir string, id glid.GLID) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, id.String()), []byte("x"), 0o600)
}
