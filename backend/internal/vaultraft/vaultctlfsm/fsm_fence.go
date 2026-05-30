package vaultctlfsm

import (
	"errors"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// CmdPublishFence durably records one fence cut (F_n) on vault-ctl Raft.
const CmdPublishFence Command = 17

// FenceRecord is one immutable fence boundary for sequenced vault inclusion.
type FenceRecord struct {
	ID             uint64
	UpperBoundSeq  uint64
	PrevBoundSeq   uint64
	CreatedAtNanos int64
}

// FenceSnapshot is a point-in-time copy of published fence history.
type FenceSnapshot struct {
	Records []FenceRecord
}

var (
	ErrFenceRegression   = errors.New("fence: upper bound must exceed previous fence")
	ErrFenceInvalidSeq   = errors.New("fence: upper bound seq must be non-zero")
	ErrFenceEmptyPayload = errors.New("fence: publish payload too short")
)

func (f *FSM) fenceSnapshotLocked() FenceSnapshot {
	out := make([]FenceRecord, len(f.fences))
	copy(out, f.fences)
	return FenceSnapshot{Records: out}
}

// FenceState returns a copy of published fence history.
func (f *FSM) FenceState() FenceSnapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.fenceSnapshotLocked()
}

// LatestFenceUpperBound returns F_n for the most recent fence, or 0 if none.
func (f *FSM) LatestFenceUpperBound() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.fences) == 0 {
		return 0
	}
	return f.fences[len(f.fences)-1].UpperBoundSeq
}

// FenceContainsSeq reports deterministic fence membership: prev < seq <= upper.
// Missing assigned or unassigned seq values inside the range are allowed.
func FenceContainsSeq(prevBound, upperBound, seq uint64) bool {
	return seq > prevBound && seq <= upperBound
}

func (f *FSM) applyPublishFence(c *gastrologv1.PublishFenceCommand) (*FenceRecord, error) {
	upper := c.GetUpperBoundSeq()
	createdNanos := c.GetCreatedAtNanos()
	if upper == 0 {
		return nil, ErrFenceInvalidSeq
	}
	prev := uint64(0)
	if len(f.fences) > 0 {
		prev = f.fences[len(f.fences)-1].UpperBoundSeq
	}
	if upper <= prev {
		return nil, ErrFenceRegression
	}
	rec := FenceRecord{
		ID:             uint64(len(f.fences) + 1),
		UpperBoundSeq:  upper,
		PrevBoundSeq:   prev,
		CreatedAtNanos: createdNanos,
	}
	f.fences = append(f.fences, rec)
	return &rec, nil
}

// NewPublishFence builds a CmdPublishFence command message.
func NewPublishFence(upperBoundSeq uint64, createdAt time.Time) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_PublishFence{PublishFence: &gastrologv1.PublishFenceCommand{
		UpperBoundSeq:  upperBoundSeq,
		CreatedAtNanos: createdAt.UTC().UnixNano(),
	}}}
}

// MarshalPublishFence builds CmdPublishFence wire bytes.
func MarshalPublishFence(upperBoundSeq uint64, createdAt time.Time) []byte {
	return mustMarshalCommand(NewPublishFence(upperBoundSeq, createdAt))
}

// ---------- Snapshot proto converters ----------

// fenceSnapshotToProto converts published fence history to its snapshot proto.
func fenceSnapshotToProto(snap FenceSnapshot) []*gastrologv1.FenceRecord {
	out := make([]*gastrologv1.FenceRecord, 0, len(snap.Records))
	for _, rec := range snap.Records {
		out = append(out, &gastrologv1.FenceRecord{
			Id:             rec.ID,
			UpperBoundSeq:  rec.UpperBoundSeq,
			PrevBoundSeq:   rec.PrevBoundSeq,
			CreatedAtNanos: rec.CreatedAtNanos,
		})
	}
	return out
}

// fenceSnapshotFromProto converts snapshot proto records back to fence history.
func fenceSnapshotFromProto(records []*gastrologv1.FenceRecord) FenceSnapshot {
	out := FenceSnapshot{Records: make([]FenceRecord, 0, len(records))}
	for _, rec := range records {
		out.Records = append(out.Records, FenceRecord{
			ID:             rec.GetId(),
			UpperBoundSeq:  rec.GetUpperBoundSeq(),
			PrevBoundSeq:   rec.GetPrevBoundSeq(),
			CreatedAtNanos: rec.GetCreatedAtNanos(),
		})
	}
	return out
}

func applyFenceSnapshotLocked(f *FSM, snap FenceSnapshot) {
	if len(snap.Records) == 0 {
		f.fences = nil
		return
	}
	f.fences = make([]FenceRecord, len(snap.Records))
	copy(f.fences, snap.Records)
}
