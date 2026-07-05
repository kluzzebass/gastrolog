package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	erragg "gastrolog/internal/errs"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// segmentPuller streams a completed segment's bytes from a peer node into dest.
// Production uses *cluster.SegmentPuller (PullSegment RPC); tests substitute a
// fake. The concrete RPC transport is covered in cluster/segment_puller_test.go.
type segmentPuller interface {
	Pull(ctx context.Context, nodeID string, vaultID, segmentID glid.GLID, dest io.Writer) error
}

// segmentLogReader implements collection.LogReader over the local per-vault
// vault-ctl FSM registry. A home self-assigns every completed segment it does
// not yet hold: desired holders = the vault home set (implicit holder model,
// Rubicon C), so the orchestrator only registers a vault as Home on placement
// members and a registered home should end up holding all of the vault's
// segments. Roll therefore returns every registry entry whose holder set does
// not yet include the local node, plus segments already ack'd as holder but
// missing locally while chunking still needs their bytes.
type segmentLogReader struct {
	lookup      func() *vaultctlfsm.FSM
	localNodeID string
	vaultRoot   string
}

var _ collection.LogReader = (*segmentLogReader)(nil)

func (r *segmentLogReader) fsm() *vaultctlfsm.FSM {
	if r.lookup != nil {
		return r.lookup()
	}
	return nil
}

func (r *segmentLogReader) Roll(_ context.Context, vaultID glid.GLID) ([]collection.AssignedSegment, error) {
	fsm := r.fsm()
	if fsm == nil {
		return nil, errors.New("vault-ctl FSM required")
	}
	entries := fsm.ListCompletedSegments()
	var out []collection.AssignedSegment
	for i := range entries {
		e := &entries[i]
		held := slices.Contains(e.Holders, r.localNodeID)
		if held && !segmentNeedsLocalRepull(fsm, *e, r.vaultRoot) {
			continue
		}
		out = append(out, collection.AssignedSegment{
			VaultID:   vaultID,
			SegmentID: e.SegmentID,
			Checksum:  e.Checksum,
		})
	}
	return out, nil
}

// segmentNeedsLocalRepull reports whether this home should pull segment bytes
// even though it already appears in the holder set — for example after head/
// purge left the registry resume cursor mid-segment.
func segmentNeedsLocalRepull(fsm *vaultctlfsm.FSM, entry vaultctlfsm.CompletedSegmentEntry, vaultRoot string) bool {
	if vaultRoot == "" {
		return false
	}
	if collection.LocalSegmentPresent(vaultRoot, entry.SegmentID) {
		return false
	}
	if fsm == nil {
		return true
	}
	// GLCB build still needs segment bytes while the open or sealed-pending
	// manifest references them, even when the planner resume cursor reached
	// RecordCount (segment exhausted for planning).
	if fsm.SegmentReferencedInManifest(entry.SegmentID) {
		return true
	}
	n, ok := fsm.ResumeRecordNumber(entry.SegmentID)
	if !ok {
		return true
	}
	return n < entry.RecordCount
}

// segmentPullClient implements collection.PullClient by resolving a segment's
// source node from the local registry entry (origin first, then any other
// holder for recovery) and streaming bytes over the PullSegment RPC. A failed
// candidate must never leave partial bytes in dest for the next candidate (or
// for the collector's pre-head temp file): when dest can rewind (the
// production pre-head temp file), each candidate streams directly into it and
// a failure truncates back to the starting offset; otherwise each candidate
// is pulled into a private buffer and copied only on success. Streaming
// matters at scale — buffering held every segment fully in RAM and the
// bytes.Buffer doubling growth alone was 24% of all bytes allocated in a soak
// run, garbage that fed the GC sweep stalls behind election churn
// (gastrolog-1xee1s).
type segmentPullClient struct {
	lookup      func() *vaultctlfsm.FSM
	puller      segmentPuller
	localNodeID string
	// vaultRoot is this home's segmentation root (head/, completed/, pre-head/).
	// Segments this node originated land here before holder receipts replicate;
	// read locally instead of looping RPC back to self as origin.
	vaultRoot string
}

var _ collection.PullClient = (*segmentPullClient)(nil)

// rewindableWriter is the subset of *os.File that lets Pull stream a
// candidate directly into dest and discard partial bytes on failure.
type rewindableWriter interface {
	io.Writer
	io.Seeker
	Truncate(size int64) error
}

func (c *segmentPullClient) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	fsm := c.lookup()
	if fsm == nil {
		return errors.New("vault-ctl FSM required")
	}
	entry := fsm.GetCompletedSegment(segmentID)
	if entry == nil {
		return fmt.Errorf("segment %s not in vault-ctl registry", segmentID)
	}

	rw, streaming := dest.(rewindableWriter)
	var start int64
	if streaming {
		pos, err := rw.Seek(0, io.SeekCurrent)
		if err != nil {
			streaming = false
		} else {
			start = pos
		}
	}

	if c.vaultRoot != "" {
		err := copyLocalSegmentFile(c.vaultRoot, segmentID, dest)
		if err == nil {
			return nil
		}
		if streaming {
			if derr := discardPartial(rw, start); derr != nil {
				return derr
			}
		}
	}
	sources := segmentPullSources(entry, c.localNodeID)
	if len(sources) == 0 {
		return fmt.Errorf("no remote holder for segment %s", segmentID)
	}
	if streaming {
		return c.pullStreaming(ctx, sources, vaultID, segmentID, rw, start)
	}
	return c.pullBuffered(ctx, sources, vaultID, segmentID, dest)
}

// discardPartial rewinds dest to where this pull started so the next
// candidate (or a later retry of the whole pull) writes a clean file.
// A rewind failure is terminal: dest holds bytes we cannot retract.
func discardPartial(rw rewindableWriter, start int64) error {
	if err := rw.Truncate(start); err != nil {
		return fmt.Errorf("discard partial segment bytes: %w", err)
	}
	if _, err := rw.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("rewind after partial segment: %w", err)
	}
	return nil
}

// pullStreaming streams each candidate directly into the rewindable dest,
// truncating partial bytes from a failed source before trying the next.
func (c *segmentPullClient) pullStreaming(ctx context.Context, sources []string, vaultID, segmentID glid.GLID, rw rewindableWriter, start int64) error {
	var errs []error
	for _, node := range sources {
		err := c.puller.Pull(ctx, node, vaultID, segmentID, rw)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("pull from %s: %w", node, err))
		if derr := discardPartial(rw, start); derr != nil {
			return errors.Join(derr, erragg.SummaryJoin(errs...))
		}
	}
	return erragg.SummaryJoin(errs...)
}

// pullBuffered pulls each candidate into a private buffer and copies to dest
// only on success — the fallback when dest cannot rewind.
func (c *segmentPullClient) pullBuffered(ctx context.Context, sources []string, vaultID, segmentID glid.GLID, dest io.Writer) error {
	var errs []error
	for _, node := range sources {
		var buf bytes.Buffer
		if err := c.puller.Pull(ctx, node, vaultID, segmentID, &buf); err != nil {
			errs = append(errs, fmt.Errorf("pull from %s: %w", node, err))
			continue
		}
		if _, err := io.Copy(dest, &buf); err != nil {
			return err
		}
		return nil
	}
	return erragg.SummaryJoin(errs...)
}

// copyLocalSegmentFile streams a segment from this home's head/, completed/,
// or pre-head/ when present. The registry publish callback can run before
// distribution promotes a locally-originated segment into head/; without this
// path collection tries to RPC-pull from OriginNodeID (self) and fails with
// "no remote holder".
func copyLocalSegmentFile(vaultRoot string, segmentID glid.GLID, dest io.Writer) error {
	for _, spec := range []struct {
		path     string
		minBytes int
	}{
		{paths.HeadSegment(vaultRoot, segmentID), 0},
		{paths.CompletedSegment(vaultRoot, segmentID), 0},
		{paths.PreHeadSegment(vaultRoot, segmentID), segment.HeaderSizeV1},
	} {
		f, err := os.Open(filepath.Clean(spec.path))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		if info.Size() < int64(spec.minBytes) {
			_ = f.Close()
			continue
		}
		// Stream instead of buffering the whole segment in RAM. Callers
		// discard dest on error (temp-file removal + checksum verification
		// downstream), so a mid-stream failure cannot leak partial bytes.
		_, err = io.Copy(dest, f)
		_ = f.Close()
		return err
	}
	return os.ErrNotExist
}

// segmentPullSources lists candidate source nodes for a segment in preference
// order: the origin first, then any other holder (recovery when the origin is
// unreachable or gone), skipping the local node and duplicates.
func segmentPullSources(entry *vaultctlfsm.CompletedSegmentEntry, localNodeID string) []string {
	var sources []string
	seen := make(map[string]struct{})
	add := func(node string) {
		if node == "" || node == localNodeID {
			return
		}
		if _, ok := seen[node]; ok {
			return
		}
		seen[node] = struct{}{}
		sources = append(sources, node)
	}
	add(entry.OriginNodeID)
	for _, h := range entry.Holders {
		add(h)
	}
	return sources
}

// segmentReceiptCommitter implements collection.ReceiptCommitter by applying a
// CmdAckSegmentHolder for the local node via the per-vault vault-ctl applier
// (leader-forwarding in cluster mode), growing the segment's holder set toward
// the vault home set.
type segmentReceiptCommitter struct {
	applier     vaultctlfsm.Applier
	localNodeID string
}

var _ collection.ReceiptCommitter = (*segmentReceiptCommitter)(nil)

func (c *segmentReceiptCommitter) CommitHolderReceipts(_ context.Context, _ glid.GLID, segmentIDs []glid.GLID) error {
	if c.applier == nil {
		return errors.New("vault-ctl applier required")
	}
	if len(segmentIDs) == 0 {
		return nil
	}
	// One vault-ctl apply for the whole pass (gastrolog-38snf4).
	return c.applier.Apply(vaultctlfsm.MarshalAckSegmentHolders(segmentIDs, c.localNodeID))
}
