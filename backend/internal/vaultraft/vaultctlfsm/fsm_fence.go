package vaultctlfsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
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
	ErrFenceRegression  = errors.New("fence: upper bound must exceed previous fence")
	ErrFenceInvalidSeq  = errors.New("fence: upper bound seq must be non-zero")
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

func (f *FSM) applyPublishFence(payload []byte) (*FenceRecord, error) {
	if len(payload) < 16 {
		return nil, ErrFenceEmptyPayload
	}
	upper := binary.BigEndian.Uint64(payload[0:8])
	createdNanos := int64(binary.BigEndian.Uint64(payload[8:16])) //nolint:gosec // G115: nanosecond timestamp fits in int64
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

// MarshalPublishFence builds CmdPublishFence wire bytes.
func MarshalPublishFence(upperBoundSeq uint64, createdAt time.Time) []byte {
	var buf [17]byte
	buf[0] = byte(CmdPublishFence)
	binary.BigEndian.PutUint64(buf[1:9], upperBoundSeq)
	binary.BigEndian.PutUint64(buf[9:17], uint64(createdAt.UTC().UnixNano()))
	return buf[:]
}

const sectionFences sectionKind = 5

func encodeFencesSection(w io.Writer, snap FenceSnapshot) error {
	var payload bytes.Buffer
	if err := binary.Write(&payload, binary.BigEndian, uint32(len(snap.Records))); err != nil { //nolint:gosec // G115: fence count bounded
		return err
	}
	for _, rec := range snap.Records {
		if err := binary.Write(&payload, binary.BigEndian, rec.ID); err != nil {
			return err
		}
		if err := binary.Write(&payload, binary.BigEndian, rec.UpperBoundSeq); err != nil {
			return err
		}
		if err := binary.Write(&payload, binary.BigEndian, rec.PrevBoundSeq); err != nil {
			return err
		}
		if err := binary.Write(&payload, binary.BigEndian, uint64(rec.CreatedAtNanos)); err != nil { //nolint:gosec // G115: nanosecond timestamp fits in uint64
			return err
		}
	}
	if err := writeSectionHeader(w, sectionFences, uint32(payload.Len())); err != nil { //nolint:gosec // G115: payload size bounded
		return err
	}
	_, err := payload.WriteTo(w)
	return err
}

func readFencesSection(r io.Reader) (FenceSnapshot, error) {
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return FenceSnapshot{}, fmt.Errorf("read fence count: %w", err)
	}
	records := make([]FenceRecord, 0, count)
	for range count {
		var rec FenceRecord
		var createdNanos uint64
		if err := binary.Read(r, binary.BigEndian, &rec.ID); err != nil {
			return FenceSnapshot{}, fmt.Errorf("read fence id: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &rec.UpperBoundSeq); err != nil {
			return FenceSnapshot{}, fmt.Errorf("read fence upper: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &rec.PrevBoundSeq); err != nil {
			return FenceSnapshot{}, fmt.Errorf("read fence prev: %w", err)
		}
		if err := binary.Read(r, binary.BigEndian, &createdNanos); err != nil {
			return FenceSnapshot{}, fmt.Errorf("read fence created_at: %w", err)
		}
		rec.CreatedAtNanos = int64(createdNanos)
		records = append(records, rec)
	}
	return FenceSnapshot{Records: records}, nil
}

func applyFenceSnapshotLocked(f *FSM, snap FenceSnapshot) {
	if len(snap.Records) == 0 {
		f.fences = nil
		return
	}
	f.fences = make([]FenceRecord, len(snap.Records))
	copy(f.fences, snap.Records)
}
