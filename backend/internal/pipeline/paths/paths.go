package paths

import (
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
)

// On-disk storage area names for the pipeline (design-notes: Storage areas).
const (
	Working   = "working"
	Completed = "completed"
	PreHead   = "pre-head"
	Head      = "head"
)

// Area is a typed storage-area name for segment presence probes. New staging
// areas must be added here so FindSegment callers can probe them.
type Area string

// Typed counterparts of the storage-area name constants.
const (
	AreaWorking   Area = Working
	AreaCompleted Area = Completed
	AreaPreHead   Area = PreHead
	AreaHead      Area = Head
)

// Segment returns the path a segment would occupy in this area under root.
func (a Area) Segment(root string, segmentID glid.GLID) string {
	return filepath.Join(root, string(a), segmentID.String())
}

// FindSegment probes the given storage areas in order and returns the path of
// the first one holding segment bytes. Area order is search preference: each
// caller passes its explicit order, and changing an order changes that
// caller's behavior. This is the single byte-presence probe backing
// publish-without-bytes prevention, re-pull skipping, and build eligibility —
// do not re-implement it with ad-hoc os.Stat loops.
func FindSegment(root string, segmentID glid.GLID, areas ...Area) (string, bool) {
	for _, a := range areas {
		path := a.Segment(root, segmentID)
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

func WorkingDir(root string) string {
	return filepath.Join(root, Working)
}

func CompletedDir(root string) string {
	return filepath.Join(root, Completed)
}

func PreHeadDir(root string) string {
	return filepath.Join(root, PreHead)
}

func HeadDir(root string) string {
	return filepath.Join(root, Head)
}

func WorkingSegment(root string, segmentID glid.GLID) string {
	return filepath.Join(WorkingDir(root), segmentID.String())
}

func CompletedSegment(root string, segmentID glid.GLID) string {
	return filepath.Join(CompletedDir(root), segmentID.String())
}

func PreHeadSegment(root string, segmentID glid.GLID) string {
	return filepath.Join(PreHeadDir(root), segmentID.String())
}

func HeadSegment(root string, segmentID glid.GLID) string {
	return filepath.Join(HeadDir(root), segmentID.String())
}

// EnsureSegmentationDirs creates working/ and completed/ under root.
func EnsureSegmentationDirs(root string) error {
	if err := os.MkdirAll(WorkingDir(root), 0o750); err != nil {
		return err
	}
	return os.MkdirAll(CompletedDir(root), 0o750)
}

// EnsurePreHeadDir creates pre-head/ under root.
func EnsurePreHeadDir(root string) error {
	return os.MkdirAll(PreHeadDir(root), 0o750)
}

// EnsureHeadDir creates head/ under root.
// SyncDir fsyncs a directory so preceding renames into it survive power
// loss. Rename-as-commit is durable only once the parent directory entry is
// flushed; without it, files that cluster-visible Raft state references
// (published segments, holder-receipted pulls, sealed GLCBs) can vanish
// after a crash (gastrolog-4mqy06).
func SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

func EnsureHeadDir(root string) error {
	return os.MkdirAll(HeadDir(root), 0o750)
}

// ListSegmentIDs returns GLID filenames present in dir (non-directories only).
func ListSegmentIDs(dir string) (map[glid.GLID]struct{}, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[glid.GLID]struct{}{}, nil
		}
		return nil, err
	}
	out := make(map[glid.GLID]struct{}, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, err := glid.Parse(e.Name())
		if err != nil {
			continue
		}
		out[id] = struct{}{}
	}
	return out, nil
}
