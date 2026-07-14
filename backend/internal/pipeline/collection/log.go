package collection

import (
	"context"

	"gastrolog/internal/glid"
)

// AssignedSegment is a segment this home should hold according to the vault-ctl log.
type AssignedSegment struct {
	VaultID   glid.GLID
	SegmentID glid.GLID
	Checksum  uint64
}

// LogReader rolls the vault-ctl log and returns segments assigned to this home.
type LogReader interface {
	Roll(ctx context.Context, vaultID glid.GLID) ([]AssignedSegment, error)
}

// ReceiptCommitter records that this node holds verified segments. One call
// covers a whole collect pass: per-segment vault-ctl applies serialized
// entire passes behind the publish flood and starved leader-home GLCB builds
// (gastrolog-38snf4).
type ReceiptCommitter interface {
	CommitHolderReceipts(ctx context.Context, vaultID glid.GLID, segmentIDs []glid.GLID) error
}
