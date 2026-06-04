package collection

import (
	"context"
	"io"

	"gastrolog/internal/glid"
)

// PullClient pulls segment bytes from a holder.
type PullClient interface {
	Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error
}
