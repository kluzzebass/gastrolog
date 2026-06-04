package collection

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

func missingSegments(assigned []AssignedSegment, head, preHead map[glid.GLID]struct{}) []AssignedSegment {
	var out []AssignedSegment
	for _, ref := range assigned {
		if _, ok := head[ref.SegmentID]; ok {
			continue
		}
		if _, ok := preHead[ref.SegmentID]; ok {
			continue
		}
		out = append(out, ref)
	}
	return out
}

func vaultSegmentLayout(root string) (head, preHead map[glid.GLID]struct{}, err error) {
	head, err = paths.ListSegmentIDs(paths.HeadDir(root))
	if err != nil {
		return nil, nil, err
	}
	preHead, err = paths.ListSegmentIDs(paths.PreHeadDir(root))
	if err != nil {
		return nil, nil, err
	}
	return head, preHead, nil
}
