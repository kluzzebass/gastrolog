package vaultctlfsm

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// ErrCompletedSegmentConflict is returned when a segment ID is published with
// metadata that disagrees with an existing registry entry.
var ErrCompletedSegmentConflict = errors.New("completed segment metadata conflict")

// CompletedSegmentEntry is replicated registry metadata for one completed
// segment awaiting chunking.
type CompletedSegmentEntry struct {
	SegmentID     glid.GLID
	RecordCount   uint32
	ByteSize      uint64
	FirstIngestTS time.Time
	LastIngestTS  time.Time
	Checksum      uint32
	OriginNodeID  string
	PublishedAt   time.Time
	// Holders are node IDs that have pulled, verified, and committed a holder
	// receipt for this segment (Rubicon C). Grows via CmdAckSegmentHolder
	// toward the vault home set; not part of the publish idempotency check.
	Holders []string
}

// completedSegmentEqual compares the published CONTENT of two entries. It
// deliberately ignores Holders (the publish command never carries them;
// holders grow independently via CmdAckSegmentHolder) and PublishedAt (the
// publisher stamps wall clock per ATTEMPT, so distribution's post-restart
// catch-up re-publish of an already-registered segment carried a fresh
// timestamp and could never compare equal — a permanent conflict/retry
// storm into the vault-ctl leader queue, gastrolog-2usqfx). Content defines
// identity; first write wins on PublishedAt.
func completedSegmentEqual(a, b CompletedSegmentEntry) bool {
	return a.SegmentID == b.SegmentID &&
		a.RecordCount == b.RecordCount &&
		a.ByteSize == b.ByteSize &&
		a.FirstIngestTS.Equal(b.FirstIngestTS) &&
		a.LastIngestTS.Equal(b.LastIngestTS) &&
		a.Checksum == b.Checksum &&
		a.OriginNodeID == b.OriginNodeID
}

func completedSegmentToProto(e *CompletedSegmentEntry) *gastrologv1.CompletedSegmentEntry {
	return &gastrologv1.CompletedSegmentEntry{
		SegmentId:          e.SegmentID[:],
		RecordCount:        e.RecordCount,
		ByteSize:           e.ByteSize,
		FirstIngestTsNanos: e.FirstIngestTS.UnixNano(),
		LastIngestTsNanos:  e.LastIngestTS.UnixNano(),
		Checksum:           e.Checksum,
		OriginNodeId:       e.OriginNodeID,
		PublishedAtNanos:   e.PublishedAt.UnixNano(),
		Holders:            slices.Clone(e.Holders),
	}
}

func completedSegmentFromProto(p *gastrologv1.CompletedSegmentEntry) CompletedSegmentEntry {
	return CompletedSegmentEntry{
		SegmentID:     glid.FromBytes(p.GetSegmentId()),
		RecordCount:   p.GetRecordCount(),
		ByteSize:      p.GetByteSize(),
		FirstIngestTS: time.Unix(0, p.GetFirstIngestTsNanos()),
		LastIngestTS:  time.Unix(0, p.GetLastIngestTsNanos()),
		Checksum:      p.GetChecksum(),
		OriginNodeID:  p.GetOriginNodeId(),
		PublishedAt:   time.Unix(0, p.GetPublishedAtNanos()),
		Holders:       slices.Clone(p.GetHolders()),
	}
}

func completedSegmentFromPublish(c *gastrologv1.PublishCompletedSegmentCommand) (CompletedSegmentEntry, error) {
	if len(c.GetSegmentId()) < glid.Size {
		return CompletedSegmentEntry{}, fmt.Errorf("segment id must be %d bytes", glid.Size)
	}
	if c.GetRecordCount() == 0 {
		return CompletedSegmentEntry{}, errors.New("record count must be positive")
	}
	return CompletedSegmentEntry{
		SegmentID:     glid.FromBytes(c.GetSegmentId()),
		RecordCount:   c.GetRecordCount(),
		ByteSize:      c.GetByteSize(),
		FirstIngestTS: time.Unix(0, c.GetFirstIngestTsNanos()),
		LastIngestTS:  time.Unix(0, c.GetLastIngestTsNanos()),
		Checksum:      c.GetChecksum(),
		OriginNodeID:  c.GetOriginNodeId(),
		PublishedAt:   time.Unix(0, c.GetPublishedAtNanos()),
	}, nil
}

// GetCompletedSegment returns a copy of registry metadata, or nil if absent.
func (f *FSM) GetCompletedSegment(id glid.GLID) *CompletedSegmentEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e := f.completedSegments[id]
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

// ListCompletedSegments returns all completed segment entries sorted by
// FirstIngestTS ascending, then SegmentID.
func (f *FSM) ListCompletedSegments() []CompletedSegmentEntry {
	f.completedListScans.Add(1)
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]CompletedSegmentEntry, 0, len(f.completedSegmentOrder))
	for _, id := range f.completedSegmentOrder {
		if e := f.completedSegments[id]; e != nil {
			out = append(out, *e)
		}
	}
	return out
}

// CompletedListScans returns the cumulative number of ListCompletedSegments
// calls — one full O(N) registry walk each. Used to observe/guard scan
// frequency (gastrolog-36ba70).
func (f *FSM) CompletedListScans() uint64 {
	return f.completedListScans.Load()
}

func compareCompletedSegmentOrder(a, b CompletedSegmentEntry) int {
	if cmp := a.FirstIngestTS.Compare(b.FirstIngestTS); cmp != 0 {
		return cmp
	}
	return a.SegmentID.Compare(b.SegmentID)
}

func (f *FSM) insertCompletedSegmentOrder(entry CompletedSegmentEntry) {
	i, _ := slices.BinarySearchFunc(f.completedSegmentOrder, entry, func(id glid.GLID, target CompletedSegmentEntry) int {
		e := f.completedSegments[id]
		if e == nil {
			return 0
		}
		return compareCompletedSegmentOrder(*e, target)
	})
	f.completedSegmentOrder = slices.Insert(f.completedSegmentOrder, i, entry.SegmentID)
}

func (f *FSM) removeCompletedSegmentOrder(id glid.GLID) {
	for i, existing := range f.completedSegmentOrder {
		if existing == id {
			f.completedSegmentOrder = slices.Delete(f.completedSegmentOrder, i, i+1)
			return
		}
	}
}

func (f *FSM) rebuildCompletedSegmentOrderLocked() {
	f.completedSegmentOrder = f.completedSegmentOrder[:0]
	for id := range f.completedSegments {
		f.completedSegmentOrder = append(f.completedSegmentOrder, id)
	}
	slices.SortFunc(f.completedSegmentOrder, func(a, b glid.GLID) int {
		return compareCompletedSegmentOrder(*f.completedSegments[a], *f.completedSegments[b])
	})
}

func (f *FSM) applyPublishCompletedSegment(c *gastrologv1.PublishCompletedSegmentCommand) error {
	entry, err := completedSegmentFromPublish(c)
	if err != nil {
		return err
	}
	if _, released := f.releasedSegments[entry.SegmentID]; released {
		return nil
	}
	if existing, ok := f.completedSegments[entry.SegmentID]; ok {
		if completedSegmentEqual(entry, *existing) {
			return nil
		}
		return fmt.Errorf("%w: segment %s", ErrCompletedSegmentConflict, entry.SegmentID)
	}
	cp := entry
	f.completedSegments[entry.SegmentID] = &cp
	f.insertCompletedSegmentOrder(entry)
	return nil
}

func (f *FSM) snapshotCompletedSegmentsLocked() []*gastrologv1.CompletedSegmentEntry {
	out := make([]*gastrologv1.CompletedSegmentEntry, 0, len(f.completedSegments))
	ids := slices.SortedFunc(maps.Keys(f.completedSegments), glid.Compare)
	for _, id := range ids {
		out = append(out, completedSegmentToProto(f.completedSegments[id]))
	}
	return out
}

func (f *FSM) restoreCompletedSegmentsLocked(segments []*gastrologv1.CompletedSegmentEntry) {
	f.completedSegments = make(map[glid.GLID]*CompletedSegmentEntry, len(segments))
	for _, pe := range segments {
		e := completedSegmentFromProto(pe)
		f.completedSegments[e.SegmentID] = &e
	}
	f.rebuildCompletedSegmentOrderLocked()
}

// NewPublishCompletedSegment builds a PublishCompletedSegment VaultCtlCommand.
func NewPublishCompletedSegment(entry CompletedSegmentEntry) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_PublishCompletedSegment{
		PublishCompletedSegment: &gastrologv1.PublishCompletedSegmentCommand{
			SegmentId:          entry.SegmentID[:],
			RecordCount:        entry.RecordCount,
			ByteSize:           entry.ByteSize,
			FirstIngestTsNanos: entry.FirstIngestTS.UnixNano(),
			LastIngestTsNanos:  entry.LastIngestTS.UnixNano(),
			Checksum:           entry.Checksum,
			OriginNodeId:       entry.OriginNodeID,
			PublishedAtNanos:   entry.PublishedAt.UnixNano(),
		},
	}}
}

// MarshalPublishCompletedSegment builds Raft log data for PublishCompletedSegment.
func MarshalPublishCompletedSegment(entry CompletedSegmentEntry) []byte {
	return mustMarshalCommand(NewPublishCompletedSegment(entry))
}

// NewPublishCompletedSegments builds a batched PublishCompletedSegments command.
func NewPublishCompletedSegments(entries []CompletedSegmentEntry) *gastrologv1.VaultCtlCommand {
	segs := make([]*gastrologv1.PublishCompletedSegmentCommand, len(entries))
	for i, entry := range entries {
		segs[i] = NewPublishCompletedSegment(entry).GetPublishCompletedSegment()
	}
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_PublishCompletedSegments{
		PublishCompletedSegments: &gastrologv1.PublishCompletedSegmentsCommand{
			Segments: segs,
		},
	}}
}

// MarshalPublishCompletedSegments builds Raft log data for a batched publish.
func MarshalPublishCompletedSegments(entries []CompletedSegmentEntry) []byte {
	return mustMarshalCommand(NewPublishCompletedSegments(entries))
}

// applyAckSegmentHolder records that nodeID now holds the given completed
// segments by appending it to each entry's holder set, and returns the
// segment IDs whose holder sets actually grew (for the ack fan-out).
// Idempotent per segment: a repeated ack for a node already in the set is a
// no-op. Acks for unknown segments are tolerated as no-ops (the publish may
// not have replicated to this node yet, or the entry was already released).
// Batched: a collect pass commits every receipt in one Raft apply
// (gastrolog-38snf4).
func (f *FSM) applyAckSegmentHolder(c *gastrologv1.AckSegmentHolderCommand) ([]glid.GLID, error) {
	nodeID := c.GetNodeId()
	if nodeID == "" {
		return nil, errors.New("ack segment holder: node id required")
	}
	var added []glid.GLID
	for _, raw := range c.GetSegmentIds() {
		segID := glid.FromBytes(raw)
		if segID == glid.Nil {
			continue
		}
		entry, ok := f.completedSegments[segID]
		if !ok {
			continue
		}
		if slices.Contains(entry.Holders, nodeID) {
			continue
		}
		entry.Holders = append(entry.Holders, nodeID)
		added = append(added, segID)
	}
	return added, nil
}

// NewAckSegmentHolders builds an AckSegmentHolder VaultCtlCommand covering
// every given segment.
func NewAckSegmentHolders(segmentIDs []glid.GLID, nodeID string) *gastrologv1.VaultCtlCommand {
	raw := make([][]byte, len(segmentIDs))
	for i, id := range segmentIDs {
		raw[i] = append([]byte(nil), id[:]...)
	}
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_AckSegmentHolder{
		AckSegmentHolder: &gastrologv1.AckSegmentHolderCommand{
			SegmentIds: raw,
			NodeId:     nodeID,
		},
	}}
}

// MarshalAckSegmentHolders builds Raft log data acking every given segment in
// one apply.
func MarshalAckSegmentHolders(segmentIDs []glid.GLID, nodeID string) []byte {
	return mustMarshalCommand(NewAckSegmentHolders(segmentIDs, nodeID))
}

// MarshalAckSegmentHolder builds Raft log data for a single-segment ack.
func MarshalAckSegmentHolder(segmentID glid.GLID, nodeID string) []byte {
	return MarshalAckSegmentHolders([]glid.GLID{segmentID}, nodeID)
}
