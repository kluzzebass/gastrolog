package paths

import (
	"errors"
	"os"

	"gastrolog/internal/glid"
)

// PurgeCompleted removes a segment file from completed/ under root. Use on
// origins after ReleaseSegments (holder-gated) so distribution rescan does not
// republish stale bytes.
func PurgeCompleted(root string, segmentID glid.GLID) error {
	path := CompletedSegment(root, segmentID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// PurgeHeadStaging removes a segment file from head/ and pre-head/ under root.
// Use after a home materializes a sealed GLCB so head/ does not grow without
// bound. completed/ is left intact so peer collectors can still pull bytes.
func PurgeHeadStaging(root string, segmentID glid.GLID) error {
	var errs []error
	for _, path := range []string{
		HeadSegment(root, segmentID),
		PreHeadSegment(root, segmentID),
	} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PurgeSegmentStaging removes a segment file from head/, pre-head/, and
// completed/ under root. Missing paths are ignored.
func PurgeSegmentStaging(root string, segmentID glid.GLID) error {
	var errs []error
	for _, path := range []string{
		HeadSegment(root, segmentID),
		PreHeadSegment(root, segmentID),
		CompletedSegment(root, segmentID),
	} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
