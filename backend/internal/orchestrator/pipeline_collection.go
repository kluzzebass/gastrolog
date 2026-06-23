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

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/paths"
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
// not yet include the local node.
type segmentLogReader struct {
	lookup      func() *vaultctlfsm.FSM
	localNodeID string
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
		if slices.Contains(e.Holders, r.localNodeID) {
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

// segmentPullClient implements collection.PullClient by resolving a segment's
// source node from the local registry entry (origin first, then any other
// holder for recovery) and streaming bytes over the PullSegment RPC. Each
// candidate is pulled into a private buffer and copied to dest only on success,
// so a mid-stream failure from one source never leaves partial bytes in dest
// for the next candidate (or for the collector's pre-head temp file).
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

func (c *segmentPullClient) Pull(ctx context.Context, vaultID, segmentID glid.GLID, dest io.Writer) error {
	fsm := c.lookup()
	if fsm == nil {
		return errors.New("vault-ctl FSM required")
	}
	entry := fsm.GetCompletedSegment(segmentID)
	if entry == nil {
		return fmt.Errorf("segment %s not in vault-ctl registry", segmentID)
	}
	if c.vaultRoot != "" {
		if err := copyLocalSegmentFile(c.vaultRoot, segmentID, dest); err == nil {
			return nil
		}
	}
	sources := segmentPullSources(entry, c.localNodeID)
	if len(sources) == 0 {
		return fmt.Errorf("no remote holder for segment %s", segmentID)
	}
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
	return errors.Join(errs...)
}

// copyLocalSegmentFile streams a segment from this home's head/, completed/,
// or pre-head/ when present. The registry publish callback can run before
// distribution promotes a locally-originated segment into head/; without this
// path collection tries to RPC-pull from OriginNodeID (self) and fails with
// "no remote holder".
func copyLocalSegmentFile(vaultRoot string, segmentID glid.GLID, dest io.Writer) error {
	for _, path := range []string{
		paths.HeadSegment(vaultRoot, segmentID),
		paths.CompletedSegment(vaultRoot, segmentID),
		paths.PreHeadSegment(vaultRoot, segmentID),
	} {
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		_, err = dest.Write(data)
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

func (c *segmentReceiptCommitter) CommitHolderReceipt(_ context.Context, _, segmentID glid.GLID) error {
	if c.applier == nil {
		return errors.New("vault-ctl applier required")
	}
	return c.applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segmentID, c.localNodeID))
}
