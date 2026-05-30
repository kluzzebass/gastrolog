package vaultctlfsm

import (
	"errors"
	"slices"
	"strings"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// SeqSentinel is reserved; destination-vault acceptance sequences start at 1.
const SeqSentinel uint64 = 0

const (
	initialSeqNext  = 1
	initialSeqEpoch = 1

	// InitialSeqNext and InitialSeqEpoch are exported for tests/harness.
	InitialSeqNext  = initialSeqNext
	InitialSeqEpoch = initialSeqEpoch
)

const maxSeqHolderIDLen = 256

// Seq allocator command opcodes (vaultctlfsm wire format).
const (
	CmdReserveSeqRange       Command = 14
	CmdBurnSeqLeaseTail      Command = 15
	CmdBumpSeqAllocatorEpoch Command = 16
)

// SeqLeaseGrant is returned from CmdReserveSeqRange on successful apply.
type SeqLeaseGrant struct {
	Start uint64
	End   uint64
	Epoch uint64
}

// SeqActiveLease is one outstanding swath granted to a holder node.
type SeqActiveLease struct {
	HolderID   string
	Epoch      uint64
	RangeStart uint64 // inclusive
	RangeEnd   uint64 // inclusive
}

// SeqBurnedTail records an unassigned gap from an abandoned lease tail.
type SeqBurnedTail struct {
	Start uint64 // inclusive
	End   uint64 // inclusive
	Epoch uint64 // allocator epoch when the tail was burned
}

// SeqAllocatorSnapshot is a point-in-time copy of allocator control state.
type SeqAllocatorSnapshot struct {
	NextSeq      uint64
	Epoch        uint64
	ActiveSwaths []SeqActiveLease
	BurnedTails  []SeqBurnedTail
}

var (
	ErrSeqAllocatorStaleEpoch    = errors.New("seq allocator: stale epoch")
	ErrSeqAllocatorActiveLease   = errors.New("seq allocator: holder already has active swath")
	ErrSeqAllocatorNoActiveLease = errors.New("seq allocator: no matching active swath")
	ErrSeqAllocatorInvalidCount  = errors.New("seq allocator: invalid reservation count")
	ErrSeqAllocatorInvalidHolder = errors.New("seq allocator: invalid holder id")
	ErrSeqAllocatorInvalidRange  = errors.New("seq allocator: consumed end outside lease range")
	ErrSeqAllocatorOverflow      = errors.New("seq allocator: sequence overflow")
)

func (f *FSM) ensureSeqAllocatorDefaultsLocked() {
	if f.seqNextSeq == SeqSentinel {
		f.seqNextSeq = initialSeqNext
	}
	if f.seqEpoch == SeqSentinel {
		f.seqEpoch = initialSeqEpoch
	}
}

// SeqAllocatorState returns a copy of the current allocator control state.
func (f *FSM) SeqAllocatorState() SeqAllocatorSnapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.seqAllocatorSnapshotLocked()
}

func (f *FSM) seqAllocatorSnapshotLocked() SeqAllocatorSnapshot {
	nextSeq := f.seqNextSeq
	if nextSeq == SeqSentinel {
		nextSeq = initialSeqNext
	}
	epoch := f.seqEpoch
	if epoch == SeqSentinel {
		epoch = initialSeqEpoch
	}
	swaths := make([]SeqActiveLease, 0, len(f.seqActiveSwaths))
	for _, lease := range f.seqActiveSwaths {
		if lease == nil {
			continue
		}
		swaths = append(swaths, *lease)
	}
	tails := make([]SeqBurnedTail, len(f.seqBurnedTails))
	copy(tails, f.seqBurnedTails)
	return SeqAllocatorSnapshot{
		NextSeq:      nextSeq,
		Epoch:        epoch,
		ActiveSwaths: swaths,
		BurnedTails:  tails,
	}
}

func (f *FSM) applyReserveSeqRange(c *gastrologv1.ReserveSeqRangeCommand) (any, error) {
	holderID := c.GetHolderId()
	if err := validateSeqHolder(holderID); err != nil {
		return nil, err
	}
	epoch := c.GetEpoch()
	count := c.GetCount()
	if count == 0 {
		return nil, ErrSeqAllocatorInvalidCount
	}

	f.ensureSeqAllocatorDefaultsLocked()
	if epoch != f.seqEpoch {
		return nil, ErrSeqAllocatorStaleEpoch
	}
	if f.seqActiveSwaths == nil {
		f.seqActiveSwaths = make(map[string]*SeqActiveLease)
	}
	if _, ok := f.seqActiveSwaths[holderID]; ok {
		return nil, ErrSeqAllocatorActiveLease
	}

	start := f.seqNextSeq
	end, err := addSeqInclusive(start, count)
	if err != nil {
		return nil, err
	}
	next, err := addSeqExclusive(end, 1)
	if err != nil {
		return nil, err
	}

	f.seqActiveSwaths[holderID] = &SeqActiveLease{
		HolderID:   holderID,
		Epoch:      epoch,
		RangeStart: start,
		RangeEnd:   end,
	}
	f.seqNextSeq = next

	return SeqLeaseGrant{Start: start, End: end, Epoch: epoch}, nil
}

func (f *FSM) applyBurnSeqLeaseTail(c *gastrologv1.BurnSeqLeaseTailCommand) error {
	holderID := c.GetHolderId()
	if err := validateSeqHolder(holderID); err != nil {
		return err
	}
	epoch := c.GetEpoch()
	consumedEnd := c.GetConsumedEnd()

	f.ensureSeqAllocatorDefaultsLocked()
	lease := f.seqActiveSwaths[holderID]
	if lease == nil || lease.Epoch != epoch {
		return ErrSeqAllocatorNoActiveLease
	}
	if consumedEnd < lease.RangeStart || consumedEnd > lease.RangeEnd {
		return ErrSeqAllocatorInvalidRange
	}

	if consumedEnd < lease.RangeEnd {
		f.appendBurnedTailLocked(SeqBurnedTail{
			Start: consumedEnd + 1,
			End:   lease.RangeEnd,
			Epoch: epoch,
		})
	}
	delete(f.seqActiveSwaths, holderID)
	return nil
}

func (f *FSM) applyBumpSeqAllocatorEpoch(_ *gastrologv1.BumpSeqAllocatorEpochCommand) (uint64, error) {
	f.ensureSeqAllocatorDefaultsLocked()
	if f.seqActiveSwaths == nil {
		f.seqActiveSwaths = make(map[string]*SeqActiveLease)
	}
	for _, lease := range f.seqActiveSwaths {
		if lease == nil {
			continue
		}
		f.appendBurnedTailLocked(SeqBurnedTail{
			Start: lease.RangeStart,
			End:   lease.RangeEnd,
			Epoch: f.seqEpoch,
		})
	}
	clear(f.seqActiveSwaths)
	f.seqEpoch++
	return f.seqEpoch, nil
}

func (f *FSM) appendBurnedTailLocked(tail SeqBurnedTail) {
	if tail.Start > tail.End {
		return
	}
	f.seqBurnedTails = append(f.seqBurnedTails, tail)
}

func addSeqInclusive(start, count uint64) (uint64, error) {
	if count == 0 {
		return 0, ErrSeqAllocatorInvalidCount
	}
	end := start + count - 1
	if end < start {
		return 0, ErrSeqAllocatorOverflow
	}
	return end, nil
}

func addSeqExclusive(v, delta uint64) (uint64, error) {
	next := v + delta
	if next < v {
		return 0, ErrSeqAllocatorOverflow
	}
	return next, nil
}

// validateSeqHolder enforces the holder-ID bounds (1..maxSeqHolderIDLen)
// the hand-rolled codec used to enforce via length framing.
func validateSeqHolder(holderID string) error {
	if len(holderID) == 0 || len(holderID) > maxSeqHolderIDLen {
		return ErrSeqAllocatorInvalidHolder
	}
	return nil
}

// NewReserveSeqRange builds a CmdReserveSeqRange command message.
func NewReserveSeqRange(holderID string, epoch, count uint64) (*gastrologv1.VaultCtlCommand, error) {
	if err := validateSeqHolder(holderID); err != nil {
		return nil, err
	}
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_ReserveSeqRange{ReserveSeqRange: &gastrologv1.ReserveSeqRangeCommand{
		HolderId: holderID,
		Epoch:    epoch,
		Count:    count,
	}}}, nil
}

// MarshalReserveSeqRange builds CmdReserveSeqRange wire bytes.
func MarshalReserveSeqRange(holderID string, epoch, count uint64) ([]byte, error) {
	cmd, err := NewReserveSeqRange(holderID, epoch, count)
	if err != nil {
		return nil, err
	}
	return mustMarshalCommand(cmd), nil
}

// NewBurnSeqLeaseTail builds a CmdBurnSeqLeaseTail command message.
func NewBurnSeqLeaseTail(holderID string, epoch, consumedEnd uint64) (*gastrologv1.VaultCtlCommand, error) {
	if err := validateSeqHolder(holderID); err != nil {
		return nil, err
	}
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_BurnSeqLeaseTail{BurnSeqLeaseTail: &gastrologv1.BurnSeqLeaseTailCommand{
		HolderId:    holderID,
		Epoch:       epoch,
		ConsumedEnd: consumedEnd,
	}}}, nil
}

// MarshalBurnSeqLeaseTail builds CmdBurnSeqLeaseTail wire bytes.
func MarshalBurnSeqLeaseTail(holderID string, epoch, consumedEnd uint64) ([]byte, error) {
	cmd, err := NewBurnSeqLeaseTail(holderID, epoch, consumedEnd)
	if err != nil {
		return nil, err
	}
	return mustMarshalCommand(cmd), nil
}

// NewBumpSeqAllocatorEpoch builds a CmdBumpSeqAllocatorEpoch command message.
func NewBumpSeqAllocatorEpoch() *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_BumpSeqAllocatorEpoch{BumpSeqAllocatorEpoch: &gastrologv1.BumpSeqAllocatorEpochCommand{}}}
}

// MarshalBumpSeqAllocatorEpoch builds CmdBumpSeqAllocatorEpoch wire bytes.
func MarshalBumpSeqAllocatorEpoch() []byte {
	return mustMarshalCommand(NewBumpSeqAllocatorEpoch())
}

// ---------- Snapshot proto converters ----------

// seqAllocatorToProto converts allocator control state to its snapshot proto.
// Active swaths are emitted in HolderID order for deterministic encoding.
func seqAllocatorToProto(snap SeqAllocatorSnapshot) *gastrologv1.SeqAllocatorSnapshot {
	out := &gastrologv1.SeqAllocatorSnapshot{
		NextSeq:      snap.NextSeq,
		Epoch:        snap.Epoch,
		ActiveSwaths: make([]*gastrologv1.SeqActiveLease, 0, len(snap.ActiveSwaths)),
		BurnedTails:  make([]*gastrologv1.SeqBurnedTail, 0, len(snap.BurnedTails)),
	}
	swaths := append([]SeqActiveLease(nil), snap.ActiveSwaths...)
	slices.SortFunc(swaths, func(a, b SeqActiveLease) int { return strings.Compare(a.HolderID, b.HolderID) })
	for _, lease := range swaths {
		out.ActiveSwaths = append(out.ActiveSwaths, &gastrologv1.SeqActiveLease{
			HolderId:   lease.HolderID,
			Epoch:      lease.Epoch,
			RangeStart: lease.RangeStart,
			RangeEnd:   lease.RangeEnd,
		})
	}
	for _, tail := range snap.BurnedTails {
		out.BurnedTails = append(out.BurnedTails, &gastrologv1.SeqBurnedTail{
			Start: tail.Start,
			End:   tail.End,
			Epoch: tail.Epoch,
		})
	}
	return out
}

// seqAllocatorFromProto converts a snapshot proto back to allocator state.
func seqAllocatorFromProto(p *gastrologv1.SeqAllocatorSnapshot) SeqAllocatorSnapshot {
	snap := SeqAllocatorSnapshot{
		NextSeq:      p.GetNextSeq(),
		Epoch:        p.GetEpoch(),
		ActiveSwaths: make([]SeqActiveLease, 0, len(p.GetActiveSwaths())),
		BurnedTails:  make([]SeqBurnedTail, 0, len(p.GetBurnedTails())),
	}
	for _, lease := range p.GetActiveSwaths() {
		snap.ActiveSwaths = append(snap.ActiveSwaths, SeqActiveLease{
			HolderID:   lease.GetHolderId(),
			Epoch:      lease.GetEpoch(),
			RangeStart: lease.GetRangeStart(),
			RangeEnd:   lease.GetRangeEnd(),
		})
	}
	for _, tail := range p.GetBurnedTails() {
		snap.BurnedTails = append(snap.BurnedTails, SeqBurnedTail{
			Start: tail.GetStart(),
			End:   tail.GetEnd(),
			Epoch: tail.GetEpoch(),
		})
	}
	return snap
}

func applySeqAllocatorSnapshotLocked(f *FSM, snap SeqAllocatorSnapshot) {
	f.seqNextSeq = snap.NextSeq
	f.seqEpoch = snap.Epoch
	f.seqActiveSwaths = make(map[string]*SeqActiveLease, len(snap.ActiveSwaths))
	for i := range snap.ActiveSwaths {
		cp := snap.ActiveSwaths[i]
		f.seqActiveSwaths[cp.HolderID] = &cp
	}
	f.seqBurnedTails = append([]SeqBurnedTail(nil), snap.BurnedTails...)
	f.ensureSeqAllocatorDefaultsLocked()
}
