package collection

import (
	"context"

	"gastrolog/internal/glid"
)

// AssignedSegment is a segment this home should hold according to the vault-ctl log.
type AssignedSegment struct {
	VaultID   glid.GLID
	SegmentID glid.GLID
	Checksum  uint32
}

// LogReader rolls the vault-ctl log and returns segments assigned to this home.
type LogReader interface {
	Roll(ctx context.Context, vaultID glid.GLID) ([]AssignedSegment, error)
}

// ReceiptCommitter records that this node holds a verified segment.
type ReceiptCommitter interface {
	CommitHolderReceipt(ctx context.Context, vaultID, segmentID glid.GLID) error
}
