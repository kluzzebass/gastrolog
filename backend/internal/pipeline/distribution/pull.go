package distribution

import (
	"io"

	"gastrolog/internal/glid"
)

// PullRequest is an incoming segment pull on the distribution serve path.
type PullRequest struct {
	VaultID   glid.GLID
	SegmentID glid.GLID
	Dest      io.Writer
}
