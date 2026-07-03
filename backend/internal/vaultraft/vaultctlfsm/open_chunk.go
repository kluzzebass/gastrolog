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
	Bounds            ManifestTimeBounds
}

// OpenChunkManifest is the replicated manifest of segment refs for one chunk.
type OpenChunkManifest struct {
	ChunkID      chunk.ChunkID
	OpenedAt     time.Time
	Refs         []OpenChunkSegmentRef
	TotalRecords uint64
	TotalBytes   uint64
	SealedAt     time.Time
	Bounds       ManifestTimeBounds
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

// SealedManifest returns a copy of the oldest sealed manifest awaiting local
// GLCB build, or nil when the queue is empty.
func (f *FSM) SealedManifest() *OpenChunkManifest {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return copyOpenChunkManifest(f.sealedManifestHeadLocked())
}

// SealedManifestCount returns how many manifests are awaiting local GLCB build.
func (f *FSM) SealedManifestCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.sealedManifests)
}

func (f *FSM) sealedManifestHeadLocked() *OpenChunkManifest {
	if len(f.sealedManifests) == 0 {
		return nil
	}
	return f.sealedManifests[0]
}

func (f *FSM) sealedManifestByIDLocked(id chunk.ChunkID) *OpenChunkManifest {
	for _, m := range f.sealedManifests {
		if m != nil && m.ChunkID == id {
			return m
		}
	}
	return nil
}

func (f *FSM) sealedManifestQueuedLocked(id chunk.ChunkID, sealedAt time.Time) bool {
	for _, m := range f.sealedManifests {
		if m != nil && m.ChunkID == id && m.SealedAt.Equal(sealedAt) {
			return true
		}
	}
	return false
}

func (f *FSM) appendSealedManifestLocked(m *OpenChunkManifest) {
	f.sealedManifests = append(f.sealedManifests, m)
}

func (f *FSM) popSealedManifestHeadIfIDLocked(id chunk.ChunkID) bool {
	if len(f.sealedManifests) == 0 || f.sealedManifests[0].ChunkID != id {
		return false
	}
	f.sealedManifests = f.sealedManifests[1:]
	return true
}

func (f *FSM) removeSealedManifestByIDLocked(id chunk.ChunkID) {
	for i, m := range f.sealedManifests {
		if m != nil && m.ChunkID == id {
			f.sealedManifests = append(f.sealedManifests[:i], f.sealedManifests[i+1:]...)
			return
		}
	}
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
		Bounds:            boundsFromAddRefCommand(c),
	}
	if n := len(f.openChunk.Refs); n > 0 {
		if openChunkSegmentRefEqual(f.openChunk.Refs[n-1], ref) {
			return nil
		}
	}
	f.openChunk.Refs = append(f.openChunk.Refs, ref)
	f.openChunk.TotalRecords += count
	f.openChunk.TotalBytes += c.GetSliceBytes()
	sliceBounds := boundsFromAddRefCommand(c)
	mergeManifestTimeBounds(&f.openChunk.Bounds,
		sliceBounds.WriteStart, sliceBounds.WriteEnd,
		sliceBounds.IngestStart, sliceBounds.IngestEnd,
		sliceBounds.SourceStart, sliceBounds.SourceEnd,
	)
	if e := f.chunks[id]; e == nil {
		openedAt := f.openChunk.OpenedAt
		e = &ManifestEntry{
			ID:          id,
			WriteStart:  openedAt,
			IngestStart: openedAt,
			SourceStart: openedAt,
			State:       chunk.ChunkStateActive,
		}
		f.chunks[id] = e
	}
	if e := f.chunks[id]; e != nil {
		e.RecordCount = int64(f.openChunk.TotalRecords) //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
		e.Bytes = int64(f.openChunk.TotalBytes)         //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
		applyManifestBoundsToEntry(e, f.openChunk.Bounds)
	}
	if f.segmentResume == nil {
		f.segmentResume = make(map[glid.GLID]uint32)
	}
	f.segmentResume[segID] = c.GetLastRecordNumber() + 1
	return nil
}

func (f *FSM) applyAddOpenChunkSegmentRefs(c *gastrologv1.AddOpenChunkSegmentRefsCommand) error {
	if c == nil || len(c.GetRefs()) == 0 {
		return errors.New("open chunk segment refs required")
	}
	id := chunkIDFromProto(c.GetChunkId())
	for _, entry := range c.GetRefs() {
		cmd := addOpenChunkSegmentRefCommandFromEntry(id, entry)
		if err := f.applyAddOpenChunkSegmentRef(cmd); err != nil {
			return err
		}
	}
	return nil
}

func (f *FSM) applySealOpenChunkManifest(c *gastrologv1.SealOpenChunkManifestCommand) error {
	id := chunkIDFromProto(c.GetChunkId())
	sealedAt := time.Unix(0, c.GetSealedAtNanos())
	if f.openChunk == nil {
		if f.sealedManifestQueuedLocked(id, sealedAt) {
			return nil
		}
		return ErrNoOpenChunkManifest
	}
	if id != f.openChunk.ChunkID {
		return ErrOpenChunkChunkIDMismatch
	}
	f.openChunk.SealedAt = sealedAt
	f.appendSealedManifestLocked(f.openChunk)
	f.openChunk = nil
	sealed := f.sealedManifests[len(f.sealedManifests)-1]
	f.clearStaleSealTombstoneLocked(sealed.ChunkID)
	f.ensureManifestChunkEntryLocked(sealed, chunk.ChunkStateSealing)
	return nil
}

func (f *FSM) applyDiscardOpenChunkManifest(c *gastrologv1.DiscardOpenChunkManifestCommand) (*chunk.ChunkID, error) {
	id := chunkIDFromProto(c.GetChunkId())
	if id == chunk.ChunkID(glid.Nil) {
		return nil, errors.New("chunk id required")
	}
	var m *OpenChunkManifest
	switch {
	case f.openChunk != nil && f.openChunk.ChunkID == id:
		m = f.openChunk
	default:
		m = f.sealedManifestByIDLocked(id)
	}
	if m == nil {
		return nil, nil
	}
	if len(m.Refs) != 0 || m.TotalRecords != 0 {
		return nil, fmt.Errorf("discard open chunk manifest: %s has content", id)
	}
	f.clearOpenManifestStateIfChunkIDLocked(id)
	if _, existed := f.chunks[id]; !existed {
		return nil, nil
	}
	delete(f.chunks, id)
	return &id, nil
}

// clearStaleSealTombstoneLocked removes a delete-protocol tombstone that blocks
// pipeline seal completion while a sealed manifest is still pending for the same
// chunk ID. Caller MUST hold f.mu.
func (f *FSM) clearStaleSealTombstoneLocked(id chunk.ChunkID) {
	if f.sealedManifestByIDLocked(id) != nil {
		delete(f.tombstones, id)
	}
}

// ensureManifestChunkEntryLocked guarantees f.chunks[id] exists for a chunk
// on the open-manifest seal path. Caller MUST hold f.mu.
func (f *FSM) ensureManifestChunkEntryLocked(m *OpenChunkManifest, state chunk.ChunkState) {
	if m == nil {
		return
	}
	id := m.ChunkID
	f.clearStaleSealTombstoneLocked(id)
	if e := f.chunks[id]; e != nil {
		if e.State != chunk.ChunkStateSealed {
			e.State = state
		}
		e.RecordCount = int64(m.TotalRecords) //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
		e.Bytes = int64(m.TotalBytes)         //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
		applyManifestBoundsToEntry(e, m.Bounds)
		return
	}
	entry := manifestEntryFromOpenChunk(m, state)
	f.chunks[id] = &entry
}

// clearOpenManifestStateIfChunkIDLocked drops open/sealed manifest state for id.
// Caller MUST hold f.mu.
func (f *FSM) clearOpenManifestStateIfChunkIDLocked(id chunk.ChunkID) {
	f.removeSealedManifestByIDLocked(id)
	if f.openChunk != nil && f.openChunk.ChunkID == id {
		f.openChunk = nil
	}
}

// repairSealedManifestChunkEntryLocked recreates a missing manifest entry for a
// pending sealed manifest after snapshot restore or a delete race.
// Caller MUST hold f.mu.
func (f *FSM) repairSealedManifestChunkEntryLocked() {
	for _, sm := range f.sealedManifests {
		if sm == nil {
			continue
		}
		f.clearStaleSealTombstoneLocked(sm.ChunkID)
		f.ensureManifestChunkEntryLocked(sm, chunk.ChunkStateSealing)
	}
}

// applyOpenChunkManifestLocked applies OpenChunkManifest and returns a callback
// effect when a new manifest was created. Caller MUST hold f.mu.
func (f *FSM) applyOpenChunkManifestLocked(c *gastrologv1.OpenChunkManifestCommand) (any, *OpenChunkManifest) {
	hadOpen := f.openChunk != nil
	result := f.applyOpenChunkManifest(c)
	if result == nil && !hadOpen && f.openChunk != nil {
		return result, copyOpenChunkManifest(f.openChunk)
	}
	return result, nil
}

// applyAddOpenChunkSegmentRefLocked applies AddOpenChunkSegmentRef and returns
// a callback effect when a new ref was appended. Caller MUST hold f.mu.
func (f *FSM) applyAddOpenChunkSegmentRefLocked(c *gastrologv1.AddOpenChunkSegmentRefCommand) (any, *OpenChunkManifest) {
	refCount := 0
	if f.openChunk != nil {
		refCount = len(f.openChunk.Refs)
	}
	result := f.applyAddOpenChunkSegmentRef(c)
	if result == nil && f.openChunk != nil && len(f.openChunk.Refs) > refCount {
		return result, copyOpenChunkManifest(f.openChunk)
	}
	return result, nil
}

// applyAddOpenChunkSegmentRefsLocked applies AddOpenChunkSegmentRefs and returns
// a callback effect when any new ref was appended. Caller MUST hold f.mu.
func (f *FSM) applyAddOpenChunkSegmentRefsLocked(c *gastrologv1.AddOpenChunkSegmentRefsCommand) (any, *OpenChunkManifest) {
	refCount := 0
	if f.openChunk != nil {
		refCount = len(f.openChunk.Refs)
	}
	result := f.applyAddOpenChunkSegmentRefs(c)
	if result == nil && f.openChunk != nil && len(f.openChunk.Refs) > refCount {
		return result, copyOpenChunkManifest(f.openChunk)
	}
	return result, nil
}

// applySealOpenChunkManifestLocked applies SealOpenChunkManifest and returns a
// callback effect when the open manifest was sealed. Caller MUST hold f.mu.
func (f *FSM) applySealOpenChunkManifestLocked(c *gastrologv1.SealOpenChunkManifestCommand) (any, *OpenChunkManifest) {
	hadOpen := f.openChunk != nil
	result := f.applySealOpenChunkManifest(c)
	if result == nil && hadOpen && len(f.sealedManifests) > 0 {
		return result, copyOpenChunkManifest(f.sealedManifests[len(f.sealedManifests)-1])
	}
	return result, nil
}

// SegmentReferencedInManifest reports whether segmentID appears in the open
// or sealed (awaiting build) chunk manifest.
func (f *FSM) SegmentReferencedInManifest(segmentID glid.GLID) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, m := range append([]*OpenChunkManifest{f.openChunk}, f.sealedManifests...) {
		if m == nil {
			continue
		}
		for _, ref := range m.Refs {
			if ref.SegmentID == segmentID {
				return true
			}
		}
	}
	return false
}

func (f *FSM) applyReleaseSegments(c *gastrologv1.ReleaseSegmentsCommand) []glid.GLID {
	var released []glid.GLID
	for _, raw := range c.GetSegmentIds() {
		segID := glid.FromBytes(raw)
		if segID == glid.Nil {
			continue
		}
		if f.completedSegments[segID] != nil {
			released = append(released, segID)
		}
		if f.releasedSegments == nil {
			f.releasedSegments = make(map[glid.GLID]struct{})
		}
		f.releasedSegments[segID] = struct{}{}
		delete(f.completedSegments, segID)
		f.removeCompletedSegmentOrder(segID)
		delete(f.segmentResume, segID)
	}
	return released
}

func openChunkManifestToProto(m *OpenChunkManifest) *gastrologv1.OpenChunkManifestState {
	if m == nil {
		return nil
	}
	out := &gastrologv1.OpenChunkManifestState{
		ChunkId:       m.ChunkID[:],
		OpenedAtNanos: m.OpenedAt.UnixNano(),
		Refs:          make([]*gastrologv1.OpenChunkSegmentRef, len(m.Refs)),
		TotalRecords:  m.TotalRecords,
		TotalBytes:    m.TotalBytes,
	}
	if !m.SealedAt.IsZero() {
		out.SealedAtNanos = m.SealedAt.UnixNano()
	}
	ws, we, is, ie, ss, se := manifestBoundsToProto(m.Bounds)
	out.WriteStartNanos = ws
	out.WriteEndNanos = we
	out.IngestStartNanos = is
	out.IngestEndNanos = ie
	out.SourceStartNanos = ss
	out.SourceEndNanos = se
	for i := range m.Refs {
		ref := &m.Refs[i]
		out.Refs[i] = &gastrologv1.OpenChunkSegmentRef{
			SegmentId:         ref.SegmentID[:],
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
			SliceBytes:        ref.SliceBytes,
			RefAddedAtNanos:   ref.RefAddedAt.UnixNano(),
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
	m.Bounds = manifestBoundsFromProto(
		p.GetWriteStartNanos(),
		p.GetWriteEndNanos(),
		p.GetIngestStartNanos(),
		p.GetIngestEndNanos(),
		p.GetSourceStartNanos(),
		p.GetSourceEndNanos(),
	)
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

func (f *FSM) snapshotSealedManifestsLocked() []*gastrologv1.OpenChunkManifestState {
	if len(f.sealedManifests) == 0 {
		return nil
	}
	out := make([]*gastrologv1.OpenChunkManifestState, 0, len(f.sealedManifests))
	for _, m := range f.sealedManifests {
		out = append(out, openChunkManifestToProto(m))
	}
	return out
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
			SegmentId:        idCopy[:],
			NextRecordNumber: f.segmentResume[id],
		})
	}
	return out
}

func (f *FSM) restoreOpenChunkLocked(snap *gastrologv1.VaultCtlSnapshot) {
	f.openChunk = openChunkManifestFromProto(snap.GetOpenChunk())
	f.sealedManifests = nil
	for _, sm := range snap.GetSealedManifests() {
		if m := openChunkManifestFromProto(sm); m != nil {
			f.sealedManifests = append(f.sealedManifests, m)
		}
	}
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
	ws, we, is, ie, ss, se := manifestBoundsToProto(ref.Bounds)
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_AddOpenChunkSegmentRef{
		AddOpenChunkSegmentRef: &gastrologv1.AddOpenChunkSegmentRefCommand{
			ChunkId:           chunkID[:],
			SegmentId:         ref.SegmentID[:],
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
			SliceBytes:        ref.SliceBytes,
			RefAddedAtNanos:   ref.RefAddedAt.UnixNano(),
			WriteStartNanos:   ws,
			WriteEndNanos:     we,
			IngestStartNanos:  is,
			IngestEndNanos:    ie,
			SourceStartNanos:  ss,
			SourceEndNanos:    se,
		},
	}}
}

// MarshalAddOpenChunkSegmentRef builds Raft log data for AddOpenChunkSegmentRef.
func MarshalAddOpenChunkSegmentRef(chunkID chunk.ChunkID, ref OpenChunkSegmentRef) []byte {
	return mustMarshalCommand(NewAddOpenChunkSegmentRef(chunkID, ref))
}

func addOpenChunkSegmentRefCommandFromEntry(chunkID chunk.ChunkID, entry *gastrologv1.AddOpenChunkSegmentRefEntry) *gastrologv1.AddOpenChunkSegmentRefCommand {
	if entry == nil {
		return nil
	}
	return &gastrologv1.AddOpenChunkSegmentRefCommand{
		ChunkId:           chunkID[:],
		SegmentId:         entry.GetSegmentId(),
		FirstRecordNumber: entry.GetFirstRecordNumber(),
		LastRecordNumber:  entry.GetLastRecordNumber(),
		SliceBytes:        entry.GetSliceBytes(),
		RefAddedAtNanos:   entry.GetRefAddedAtNanos(),
		WriteStartNanos:   entry.GetWriteStartNanos(),
		WriteEndNanos:     entry.GetWriteEndNanos(),
		IngestStartNanos:  entry.GetIngestStartNanos(),
		IngestEndNanos:    entry.GetIngestEndNanos(),
		SourceStartNanos:  entry.GetSourceStartNanos(),
		SourceEndNanos:    entry.GetSourceEndNanos(),
	}
}

func openChunkSegmentRefEntryFromRef(ref OpenChunkSegmentRef) *gastrologv1.AddOpenChunkSegmentRefEntry {
	ws, we, is, ie, ss, se := manifestBoundsToProto(ref.Bounds)
	return &gastrologv1.AddOpenChunkSegmentRefEntry{
		SegmentId:         ref.SegmentID[:],
		FirstRecordNumber: ref.FirstRecordNumber,
		LastRecordNumber:  ref.LastRecordNumber,
		SliceBytes:        ref.SliceBytes,
		RefAddedAtNanos:   ref.RefAddedAt.UnixNano(),
		WriteStartNanos:   ws,
		WriteEndNanos:     we,
		IngestStartNanos:  is,
		IngestEndNanos:    ie,
		SourceStartNanos:  ss,
		SourceEndNanos:    se,
	}
}

// NewAddOpenChunkSegmentRefs builds an AddOpenChunkSegmentRefs command.
func NewAddOpenChunkSegmentRefs(chunkID chunk.ChunkID, refs []OpenChunkSegmentRef) *gastrologv1.VaultCtlCommand {
	entries := make([]*gastrologv1.AddOpenChunkSegmentRefEntry, len(refs))
	for i, ref := range refs {
		entries[i] = openChunkSegmentRefEntryFromRef(ref)
	}
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_AddOpenChunkSegmentRefs{
		AddOpenChunkSegmentRefs: &gastrologv1.AddOpenChunkSegmentRefsCommand{
			ChunkId: chunkID[:],
			Refs:    entries,
		},
	}}
}

// MarshalAddOpenChunkSegmentRefs builds Raft log data for AddOpenChunkSegmentRefs.
func MarshalAddOpenChunkSegmentRefs(chunkID chunk.ChunkID, refs []OpenChunkSegmentRef) []byte {
	return mustMarshalCommand(NewAddOpenChunkSegmentRefs(chunkID, refs))
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

// NewDiscardOpenChunkManifest builds a DiscardOpenChunkManifest command.
func NewDiscardOpenChunkManifest(id chunk.ChunkID) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_DiscardOpenChunkManifest{
		DiscardOpenChunkManifest: &gastrologv1.DiscardOpenChunkManifestCommand{
			ChunkId: id[:],
		},
	}}
}

// MarshalDiscardOpenChunkManifest builds Raft log data for DiscardOpenChunkManifest.
func MarshalDiscardOpenChunkManifest(id chunk.ChunkID) []byte {
	return mustMarshalCommand(NewDiscardOpenChunkManifest(id))
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
