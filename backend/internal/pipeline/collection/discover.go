package collection

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

// vaultHeadLayout lists the segment IDs present in head/. Pre-head/ is not
// mirrored: a pre-head file is either owned by an in-flight pull (claimPull)
// or a crash orphan that collectOne promotes in place (gastrolog-5zotim), so
// no planning decision reads it.
func vaultHeadLayout(root string) (map[glid.GLID]struct{}, error) {
	return paths.ListSegmentIDs(paths.HeadDir(root))
}
