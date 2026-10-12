package collection

import (
	"context"
	"io"

	"gastrolog/internal/glid"
)

// PullClient pulls segment bytes from a holder.
type PullClient interface {
	// Pull streams the segment into dest from the first reachable source not
	// named in exclude and returns that source's identity. Collection excludes
	// a source whose copy failed verification, so the next attempt for the
	// same segment reads a different copy instead of the same bad bytes.
	Pull(ctx context.Context, vaultID, segmentID glid.GLID, exclude []string, dest io.Writer) (source string, err error)
}
