package vaultctlfsm

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

var (
	// ErrOpenChunkExists is returned when OpenChunkManifest is applied while
	// a manifest is already open.
	ErrOpenChunkExists = errors.New("open chunk manifest already exists")
	// ErrSealedManifestPending is returned when OpenChunkManifest is applied while
	// a sealed manifest awaiting build already exists.
	ErrSealedManifestPending = errors.New("sealed manifest awaiting build exists")
	// ErrNoOpenChunkManifest is returned when a command requires an open manifest.
	ErrNoOpenChunkManifest = errors.New("no open chunk manifest")
	// ErrOpenChunkChunkIDMismatch is returned when a command chunk_id does not
	// match the open manifest.
	ErrOpenChunkChunkIDMismatch = errors.New("chunk id does not match open manifest")
	// ErrInvalidSegmentRef is returned when record numbers are out of order.
	ErrInvalidSegmentRef = errors.New("invalid open chunk segment ref")
)

// OpenChunkSegmentRef names one segment slice in EventID-sorted record numbers.
type OpenChunkSegmentRef struct {
	SegmentID         glid.GLID
	FirstRecordNumber uint32
	LastRecordNumber  uint32
	SliceBytes        uint64
	RefAddedAt        time.Time
}

// OpenChunkManifest is the replicated manifest of segment refs for one chunk.
type OpenChunkManifest struct {
	ChunkID      chunk.ChunkID
	OpenedAt     time.Time
	Refs         []OpenChunkSegmentRef
	TotalRecords uint64
	TotalBytes   uint64
	SealedAt     time.Time
}

// RecordCount returns the number of records in one ref.
func (r OpenChunkSegmentRef) RecordCount() uint32 {
	if r.LastRecordNumber < r.FirstRecordNumber {
		return 0
	}
	return r.LastRecordNumber - r.FirstRecordNumber + 1
}

func validateSegmentRef(first, last uint32) error {
	if last < first {
		return fmt.Errorf("%w: first=%d last=%d", ErrInvalidSegmentRef, first, last)
	}
	return nil
}

func refRecordCount(first, last uint32) (uint64, error) {
	if err := validateSegmentRef(first, last); err != nil {
		return 0, err
	}
	return uint64(last - first + 1), nil
}

// OpenChunkSummary holds manifest totals without copying all segment refs.
type OpenChunkSummary struct {
	ChunkID      chunk.ChunkID
	OpenedAt     time.Time
	TotalRecords uint64
	TotalBytes   uint64
	RefCount     int
}

// OpenChunkSummary returns running manifest totals for the open chunk, or false
// when no manifest is open.
func (f *FSM) OpenChunkSummary() (OpenChunkSummary, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.openChunk == nil {
		return OpenChunkSummary{}, false
	}
	m := f.openChunk
	return OpenChunkSummary{
		ChunkID:      m.ChunkID,
		OpenedAt:     m.OpenedAt,
		TotalRecords: m.TotalRecords,
		TotalBytes:   m.TotalBytes,
		RefCount:     len(m.Refs),
	}, true
}

// OpenChunk returns a copy of the current open manifest, or nil.
func (f *FSM) OpenChunk() *OpenChunkManifest {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return copyOpenChunkManifest(f.openChunk)
}

// SealedManifest returns a copy of the sealed manifest awaiting local GLCB build,
// or nil.
func (f *FSM) SealedManifest() *OpenChunkManifest {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return copyOpenChunkManifest(f.sealedManifest)
}

// ResumeRecordNumber returns the next record number to chunk from segmentID.
func (f *FSM) ResumeRecordNumber(segmentID glid.GLID) (uint32, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n, ok := f.segmentResume[segmentID]
	return n, ok
}

func copyOpenChunkManifest(m *OpenChunkManifest) *OpenChunkManifest {
	if m == nil {
		return nil
	}
	cp := *m
	if len(m.Refs) > 0 {
		cp.Refs = append([]OpenChunkSegmentRef(nil), m.Refs...)
	}
	return &cp
}

func openChunkSegmentRefEqual(a, b OpenChunkSegmentRef) bool {
	return a.SegmentID == b.SegmentID &&
		a.FirstRecordNumber == b.FirstRecordNumber &&
		a.LastRecordNumber == b.LastRecordNumber &&
		a.SliceBytes == b.SliceBytes &&
		a.RefAddedAt.Equal(b.RefAddedAt)
}

func (f *FSM) applyOpenChunkManifest(c *gastrologv1.OpenChunkManifestCommand) error {
	id := chunkIDFromProto(c.GetChunkId())
	if id == chunk.ChunkID(glid.Nil) {
		return errors.New("chunk id required")
	}
	if f.openChunk != nil {
		if f.openChunk.ChunkID == id {
			return nil
		}
		return ErrOpenChunkExists
	}
	if f.sealedManifest != nil {
		return ErrSealedManifestPending
	}
	if _, dead := f.tombstones[id]; dead {
		return nil
	}
	if existing := f.chunks[id]; existing != nil && existing.State != chunk.ChunkStateActive {
		return fmt.Errorf("chunk %s already exists in state %s", id, existing.State)
	}
	openedAt := time.Unix(0, c.GetOpenedAtNanos())
	f.openChunk = &OpenChunkManifest{
		ChunkID:  id,
		OpenedAt: openedAt,
	}
	if existing := f.chunks[id]; existing == nil {
		f.chunks[id] = &ManifestEntry{
			ID:          id,
			WriteStart:  openedAt,
			IngestStart: openedAt,
			SourceStart: openedAt,
			State:       chunk.ChunkStateActive,
		}
	}
	return nil
}

func (f *FSM) applyAddOpenChunkSegmentRef(c *gastrologv1.AddOpenChunkSegmentRefCommand) error {
	if f.openChunk == nil {
		return ErrNoOpenChunkManifest
	}
	id := chunkIDFromProto(c.GetChunkId())
	if id != f.openChunk.ChunkID {
		return ErrOpenChunkChunkIDMismatch
	}
	count, err := refRecordCount(c.GetFirstRecordNumber(), c.GetLastRecordNumber())
	if err != nil {
		return err
	}
	segID := glid.FromBytes(c.GetSegmentId())
	if segID == glid.Nil {
		return errors.New("segment id required")
	}
	ref := OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: c.GetFirstRecordNumber(),
		LastRecordNumber:  c.GetLastRecordNumber(),
		SliceBytes:        c.GetSliceBytes(),
		RefAddedAt:        time.Unix(0, c.GetRefAddedAtNanos()),
	}
	if n := len(f.openChunk.Refs); n > 0 {
		if openChunkSegmentRefEqual(f.openChunk.Refs[n-1], ref) {
			return nil
		}
	}
	f.openChunk.Refs = append(f.openChunk.Refs, ref)
	f.openChunk.TotalRecords += count
	f.openChunk.TotalBytes += c.GetSliceBytes()
	if f.segmentResume == nil {
		f.segmentResume = make(map[glid.GLID]uint32)
	}
	f.segmentResume[segID] = c.GetLastRecordNumber() + 1
	return nil
}

func (f *FSM) applySealOpenChunkManifest(c *gastrologv1.SealOpenChunkManifestCommand) error {
	id := chunkIDFromProto(c.GetChunkId())
	sealedAt := time.Unix(0, c.GetSealedAtNanos())
	if f.openChunk == nil {
		if pending := f.sealedManifest; pending != nil &&
			pending.ChunkID == id &&
			pending.SealedAt.Equal(sealedAt) {
			return nil
		}
		return ErrNoOpenChunkManifest
	}
	if id != f.openChunk.ChunkID {
		return ErrOpenChunkChunkIDMismatch
	}
	if f.sealedManifest != nil {
		return ErrSealedManifestPending
	}
	f.openChunk.SealedAt = sealedAt
	f.sealedManifest = f.openChunk
	f.openChunk = nil
	if e := f.chunks[id]; e != nil && e.State == chunk.ChunkStateActive {
		e.State = chunk.ChunkStateSealing
	}
	return nil
}

func (f *FSM) applyReleaseSegments(c *gastrologv1.ReleaseSegmentsCommand) error {
	for _, raw := range c.GetSegmentIds() {
		segID := glid.FromBytes(raw)
		if segID == glid.Nil {
			continue
		}
		delete(f.completedSegments, segID)
		f.removeCompletedSegmentOrder(segID)
		delete(f.segmentResume, segID)
	}
	return nil
}

func openChunkManifestToProto(m *OpenChunkManifest) *gastrologv1.OpenChunkManifestState {
	if m == nil {
		return nil
	}
	out := &gastrologv1.OpenChunkManifestState{
		ChunkId:      m.ChunkID[:],
		OpenedAtNanos: m.OpenedAt.UnixNano(),
		Refs:         make([]*gastrologv1.OpenChunkSegmentRef, len(m.Refs)),
		TotalRecords: m.TotalRecords,
		TotalBytes:   m.TotalBytes,
	}
	if !m.SealedAt.IsZero() {
		out.SealedAtNanos = m.SealedAt.UnixNano()
	}
	for i := range m.Refs {
		ref := &m.Refs[i]
		out.Refs[i] = &gastrologv1.OpenChunkSegmentRef{
			SegmentId:          ref.SegmentID[:],
			FirstRecordNumber:  ref.FirstRecordNumber,
			LastRecordNumber:   ref.LastRecordNumber,
			SliceBytes:         ref.SliceBytes,
			RefAddedAtNanos:    ref.RefAddedAt.UnixNano(),
		}
	}
	return out
}

func openChunkManifestFromProto(p *gastrologv1.OpenChunkManifestState) *OpenChunkManifest {
	if p == nil {
		return nil
	}
	m := &OpenChunkManifest{
		ChunkID:      chunkIDFromProto(p.GetChunkId()),
		OpenedAt:     time.Unix(0, p.GetOpenedAtNanos()),
		TotalRecords: p.GetTotalRecords(),
		TotalBytes:   p.GetTotalBytes(),
	}
	if p.GetSealedAtNanos() != 0 {
		m.SealedAt = time.Unix(0, p.GetSealedAtNanos())
	}
	for _, pr := range p.GetRefs() {
		m.Refs = append(m.Refs, OpenChunkSegmentRef{
			SegmentID:         glid.FromBytes(pr.GetSegmentId()),
			FirstRecordNumber: pr.GetFirstRecordNumber(),
			LastRecordNumber:  pr.GetLastRecordNumber(),
			SliceBytes:        pr.GetSliceBytes(),
			RefAddedAt:        time.Unix(0, pr.GetRefAddedAtNanos()),
		})
	}
	return m
}

func (f *FSM) snapshotOpenChunkLocked() *gastrologv1.OpenChunkManifestState {
	return openChunkManifestToProto(f.openChunk)
}

func (f *FSM) snapshotSealedManifestLocked() *gastrologv1.OpenChunkManifestState {
	return openChunkManifestToProto(f.sealedManifest)
}

func (f *FSM) snapshotSegmentResumeLocked() []*gastrologv1.SegmentResumeRecordNumber {
	if len(f.segmentResume) == 0 {
		return nil
	}
	out := make([]*gastrologv1.SegmentResumeRecordNumber, 0, len(f.segmentResume))
	ids := slices.SortedFunc(maps.Keys(f.segmentResume), glid.Compare)
	for _, id := range ids {
		idCopy := id
		out = append(out, &gastrologv1.SegmentResumeRecordNumber{
			SegmentId:         idCopy[:],
			NextRecordNumber: f.segmentResume[id],
		})
	}
	return out
}

func (f *FSM) restoreOpenChunkLocked(snap *gastrologv1.VaultCtlSnapshot) {
	f.openChunk = openChunkManifestFromProto(snap.GetOpenChunk())
	f.sealedManifest = openChunkManifestFromProto(snap.GetSealedManifest())
	f.segmentResume = make(map[glid.GLID]uint32, len(snap.GetSegmentResume()))
	for _, entry := range snap.GetSegmentResume() {
		f.segmentResume[glid.FromBytes(entry.GetSegmentId())] = entry.GetNextRecordNumber()
	}
}

// NewOpenChunkManifest builds an OpenChunkManifest VaultCtlCommand.
func NewOpenChunkManifest(id chunk.ChunkID, openedAt time.Time) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_OpenChunkManifest{
		OpenChunkManifest: &gastrologv1.OpenChunkManifestCommand{
			ChunkId:       id[:],
			OpenedAtNanos: openedAt.UnixNano(),
		},
	}}
}

// MarshalOpenChunkManifest builds Raft log data for OpenChunkManifest.
func MarshalOpenChunkManifest(id chunk.ChunkID, openedAt time.Time) []byte {
	return mustMarshalCommand(NewOpenChunkManifest(id, openedAt))
}

// NewAddOpenChunkSegmentRef builds an AddOpenChunkSegmentRef command.
func NewAddOpenChunkSegmentRef(chunkID chunk.ChunkID, ref OpenChunkSegmentRef) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_AddOpenChunkSegmentRef{
		AddOpenChunkSegmentRef: &gastrologv1.AddOpenChunkSegmentRefCommand{
			ChunkId:            chunkID[:],
			SegmentId:          ref.SegmentID[:],
			FirstRecordNumber:  ref.FirstRecordNumber,
			LastRecordNumber:   ref.LastRecordNumber,
			SliceBytes:         ref.SliceBytes,
			RefAddedAtNanos:    ref.RefAddedAt.UnixNano(),
		},
	}}
}

// MarshalAddOpenChunkSegmentRef builds Raft log data for AddOpenChunkSegmentRef.
func MarshalAddOpenChunkSegmentRef(chunkID chunk.ChunkID, ref OpenChunkSegmentRef) []byte {
	return mustMarshalCommand(NewAddOpenChunkSegmentRef(chunkID, ref))
}

// NewSealOpenChunkManifest builds a SealOpenChunkManifest command.
func NewSealOpenChunkManifest(id chunk.ChunkID, sealedAt time.Time) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_SealOpenChunkManifest{
		SealOpenChunkManifest: &gastrologv1.SealOpenChunkManifestCommand{
			ChunkId:       id[:],
			SealedAtNanos: sealedAt.UnixNano(),
		},
	}}
}

// MarshalSealOpenChunkManifest builds Raft log data for SealOpenChunkManifest.
func MarshalSealOpenChunkManifest(id chunk.ChunkID, sealedAt time.Time) []byte {
	return mustMarshalCommand(NewSealOpenChunkManifest(id, sealedAt))
}

// NewReleaseSegments builds a ReleaseSegments command.
func NewReleaseSegments(segmentIDs []glid.GLID) *gastrologv1.VaultCtlCommand {
	ids := make([][]byte, len(segmentIDs))
	for i, id := range segmentIDs {
		idCopy := id
		ids[i] = idCopy[:]
	}
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_ReleaseSegments{
		ReleaseSegments: &gastrologv1.ReleaseSegmentsCommand{SegmentIds: ids},
	}}
}

// MarshalReleaseSegments builds Raft log data for ReleaseSegments.
func MarshalReleaseSegments(segmentIDs []glid.GLID) []byte {
	return mustMarshalCommand(NewReleaseSegments(segmentIDs))
}
