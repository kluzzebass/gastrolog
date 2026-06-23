package orchestrator

import (
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// PipelineDiskSegmentCounts holds on-disk segment file counts for one node.
type PipelineDiskSegmentCounts struct {
	Working           int
	CompletedStaging  int
	Head              int
	PreHead           int
}

// LocalPipelineDiskSegmentCounts counts segment GLID files under the vault's
// pipeline storage areas on this node. Missing directories count as zero.
func (o *Orchestrator) LocalPipelineDiskSegmentCounts(vaultID glid.GLID) (PipelineDiskSegmentCounts, error) {
	var out PipelineDiskSegmentCounts
	root, err := o.originRoot(vaultID)
	if err != nil {
		return out, err
	}
	if n, err := countSegmentFiles(paths.WorkingDir(root)); err != nil {
		return out, err
	} else {
		out.Working = n
	}
	if n, err := countSegmentFiles(paths.CompletedDir(root)); err != nil {
		return out, err
	} else {
		out.CompletedStaging = n
	}
	if n, err := countSegmentFiles(paths.HeadDir(root)); err != nil {
		return out, err
	} else {
		out.Head = n
	}
	if n, err := countSegmentFiles(paths.PreHeadDir(root)); err != nil {
		return out, err
	} else {
		out.PreHead = n
	}
	return out, nil
}

func countSegmentFiles(dir string) (int, error) {
	ids, err := paths.ListSegmentIDs(dir)
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

// AddDiskCounts merges remote disk totals into dst.
func (c *PipelineDiskSegmentCounts) AddDiskCounts(other PipelineDiskSegmentCounts) {
	c.Working += other.Working
	c.CompletedStaging += other.CompletedStaging
	c.Head += other.Head
	c.PreHead += other.PreHead
}

// TouchSegmentFile creates an empty segment file for tests.
func touchSegmentFile(dir string, id glid.GLID) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, id.String()), []byte("x"), 0o600)
}
