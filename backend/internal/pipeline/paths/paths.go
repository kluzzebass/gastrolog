package paths

import (
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
)

// On-disk storage area names for the V3 pipeline (design-notes: Storage areas).
const (
	Working   = "working"
	Completed = "completed"
	PreHead   = "pre-head"
	Head      = "head"
)

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
