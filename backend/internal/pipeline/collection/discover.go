package collection

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

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
