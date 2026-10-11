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
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// buildSegmentBytes finalizes a one-record segment and returns its bytes and
// the record checksum the origin would publish.
func buildSegmentBytes(t *testing.T, vaultID, segID glid.GLID, raw string) ([]byte, uint64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), segID.String())
	sf, err := segment.Create(path, segment.Meta{ID: segID, VaultID: vaultID})
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC)
	rec := &record.Record{
		SourceTS: ts,
		IngestTS: ts,
		EventID:  record.EventID{IngesterID: glid.New(), NodeID: glid.New(), IngestTS: ts},
		Attrs:    record.Attributes{"k": "v"},
		Raw:      []byte(raw),
	}
	if err := sf.Append(rec, ts); err != nil {
		t.Fatal(err)
	}
	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
	hdr, err := segment.ReadHeader(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data, hdr.SegmentChecksum
}

// perNodePuller serves fixed bytes per source node and records pull order.
type perNodePuller struct {
	mu    sync.Mutex
	serve map[string][]byte
	order []string
}

func (p *perNodePuller) Pull(_ context.Context, nodeID string, _, _ glid.GLID, dest io.Writer) error {
	p.mu.Lock()
	p.order = append(p.order, nodeID)
	data, ok := p.serve[nodeID]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("node %s unreachable", nodeID)
	}
	_, err := dest.Write(data)
	return err
}

func (p *perNodePuller) setServe(nodeID string, data []byte) {
	p.mu.Lock()
	p.serve[nodeID] = data
	p.mu.Unlock()
}

func (p *perNodePuller) pulls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.order...)
}

// corruption is one way a holder's copy can fail verification.
type corruption struct {
	name    string
	corrupt func(t *testing.T, vaultID, segID glid.GLID, good []byte) []byte
}

var corruptions = []corruption{
	{
		// Internally valid segment with the wrong records: fails only the
		// published-checksum comparison.
		name: "wrong-content",
		corrupt: func(t *testing.T, vaultID, segID glid.GLID, _ []byte) []byte {
			bad, _ := buildSegmentBytes(t, vaultID, segID, "stale-or-wrong-bytes")
			return bad
		},
	},
	{
		// Structurally torn copy: fails segment.Open.
		name: "truncated",
		corrupt: func(_ *testing.T, _, _ glid.GLID, good []byte) []byte {
			return append([]byte(nil), good[:len(good)/2]...)
		},
	},
}

const testHealthyHolder = "node-holder"

// corruptSourceFixture is one home collecting a segment published by origin
// and also held by testHealthyHolder, over a real vault-ctl FSM, pull client,
// and collection manager.
type corruptSourceFixture struct {
	fsm      *vaultctlfsm.FSM
	vaultID  glid.GLID
	segID    glid.GLID
	good     []byte
	checksum uint64
	homeRoot string
	puller   *perNodePuller
	mgr      *collection.Manager
}

func newCorruptSourceFixture(t *testing.T, origin string) *corruptSourceFixture {
	t.Helper()
	f := &corruptSourceFixture{
		fsm:      vaultctlfsm.New(),
		vaultID:  glid.New(),
		segID:    glid.New(),
		homeRoot: t.TempDir(),
		puller:   &perNodePuller{serve: map[string][]byte{}},
	}
	f.good, f.checksum = buildSegmentBytes(t, f.vaultID, f.segID, "the-real-segment-record")
	applier := &fsmApplier{fsm: f.fsm}
	if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:    f.segID,
		RecordCount:  1,
		Checksum:     f.checksum,
		OriginNodeID: origin,
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(f.segID, testHealthyHolder)); err != nil {
		t.Fatalf("ack holder: %v", err)
	}
	lookup := func() *vaultctlfsm.FSM { return f.fsm }
	f.mgr = collection.New(collection.Config{})
	if err := f.mgr.RegisterVault(f.vaultID, f.homeRoot, collection.VaultConfig{
		Log:      &segmentLogReader{lookup: lookup, localNodeID: testHomeNode, vaultRoot: f.homeRoot},
		Pull:     &segmentPullClient{lookup: lookup, puller: f.puller, localNodeID: testHomeNode, vaultRoot: f.homeRoot},
		Receipts: &segmentReceiptCommitter{applier: applier, localNodeID: testHomeNode},
	}); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	return f
}

// collectOnce runs one synchronous collect pass (no worker is running).
func (f *corruptSourceFixture) collectOnce(t *testing.T) error {
	t.Helper()
	return f.mgr.CollectOnce(context.Background(), f.vaultID)
}

func (f *corruptSourceFixture) assertLanded(t *testing.T) {
	t.Helper()
	if !headHas(t, f.homeRoot, f.segID) {
		t.Fatalf("segment not in head/; pulls=%v", f.puller.pulls())
	}
	got, err := os.ReadFile(paths.HeadSegment(f.homeRoot, f.segID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, f.good) {
		t.Fatalf("head/ holds %d bytes that differ from the published segment", len(got))
	}
	if !segmentHolds(f.fsm, f.segID, testHomeNode) {
		t.Fatalf("home not recorded as holder after promotion")
	}
}

// TestCollectSkipsSourceServingCorruptSegment: the origin serves bytes that
// fail verification while another holder serves the published segment. One
// collect pass discards the origin's copy, pulls from the holder, and lands
// the segment — the origin is not asked twice.
func TestCollectSkipsSourceServingCorruptSegment(t *testing.T) {
	t.Parallel()
	for _, tc := range corruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newCorruptSourceFixture(t, testOriginNode)
			f.puller.setServe(testOriginNode, tc.corrupt(t, f.vaultID, f.segID, f.good))
			f.puller.setServe(testHealthyHolder, f.good)

			if err := f.collectOnce(t); err != nil {
				t.Fatalf("CollectOnce: %v (pulls=%v)", err, f.puller.pulls())
			}
			f.assertLanded(t)
			if got, want := f.puller.pulls(), []string{testOriginNode, testHealthyHolder}; !slices.Equal(got, want) {
				t.Fatalf("pulls = %v, want %v", got, want)
			}
		})
	}
}

// TestCollectWithEveryCopyCorruptFailsThenRetries: when every source serves
// unverifiable bytes the pass tries each source exactly once, surfaces the
// corruption, and lands nothing. The exclusions do not outlive the pass: once
// a holder serves the published bytes, the next pass offers every source
// again (origin first) and lands the segment.
func TestCollectWithEveryCopyCorruptFailsThenRetries(t *testing.T) {
	t.Parallel()
	for _, tc := range corruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newCorruptSourceFixture(t, testOriginNode)
			bad := tc.corrupt(t, f.vaultID, f.segID, f.good)
			f.puller.setServe(testOriginNode, bad)
			f.puller.setServe(testHealthyHolder, bad)

			err := f.collectOnce(t)
			if !errors.Is(err, collection.ErrCorruptSegment) {
				t.Fatalf("CollectOnce = %v, want ErrCorruptSegment", err)
			}
			if got, want := f.puller.pulls(), []string{testOriginNode, testHealthyHolder}; !slices.Equal(got, want) {
				t.Fatalf("pulls = %v, want each source exactly once %v", got, want)
			}
			if !headEmpty(t, f.homeRoot) {
				t.Fatal("head/ must stay empty when every copy is corrupt")
			}
			if segmentHolds(f.fsm, f.segID, testHomeNode) {
				t.Fatal("home receipted a segment it never verified")
			}
			if _, statErr := os.Stat(paths.PreHeadSegment(f.homeRoot, f.segID)); !os.IsNotExist(statErr) {
				t.Fatalf("corrupt pre-head copy not discarded: %v", statErr)
			}

			f.puller.setServe(testHealthyHolder, f.good)
			if err := f.collectOnce(t); err != nil {
				t.Fatalf("retry CollectOnce: %v (pulls=%v)", err, f.puller.pulls())
			}
			f.assertLanded(t)
			want := []string{testOriginNode, testHealthyHolder, testOriginNode, testHealthyHolder}
			if got := f.puller.pulls(); !slices.Equal(got, want) {
				t.Fatalf("pulls = %v, want %v (retry pass starts from the origin again)", got, want)
			}
		})
	}
}

// TestCollectSkipsCorruptLocalCopy: a home that originated the segment reads
// its own completed/ copy before any remote holder. When that copy fails
// verification the same pass pulls from a remote holder instead, and the
// local completed/ file is left in place (collection never deletes a copy it
// did not write).
func TestCollectSkipsCorruptLocalCopy(t *testing.T) {
	t.Parallel()
	for _, tc := range corruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newCorruptSourceFixture(t, testHomeNode)
			bad := tc.corrupt(t, f.vaultID, f.segID, f.good)
			if err := paths.EnsureSegmentationDirs(f.homeRoot); err != nil {
				t.Fatal(err)
			}
			localPath := paths.CompletedSegment(f.homeRoot, f.segID)
			if err := os.WriteFile(localPath, bad, 0o600); err != nil {
				t.Fatal(err)
			}
			f.puller.setServe(testHealthyHolder, f.good)

			if err := f.collectOnce(t); err != nil {
				t.Fatalf("CollectOnce: %v (pulls=%v)", err, f.puller.pulls())
			}
			f.assertLanded(t)
			if got, want := f.puller.pulls(), []string{testHealthyHolder}; !slices.Equal(got, want) {
				t.Fatalf("pulls = %v, want %v", got, want)
			}
			if got, err := os.ReadFile(localPath); err != nil || !bytes.Equal(got, bad) {
				t.Fatalf("local completed/ copy changed: err=%v", err)
			}
		})
	}
}

// TestCollectPrefersHealthyOrigin: with every copy healthy the origin is the
// only source asked.
func TestCollectPrefersHealthyOrigin(t *testing.T) {
	t.Parallel()
	f := newCorruptSourceFixture(t, testOriginNode)
	f.puller.setServe(testOriginNode, f.good)
	f.puller.setServe(testHealthyHolder, f.good)

	if err := f.collectOnce(t); err != nil {
		t.Fatalf("CollectOnce: %v", err)
	}
	f.assertLanded(t)
	if got, want := f.puller.pulls(), []string{testOriginNode}; !slices.Equal(got, want) {
		t.Fatalf("pulls = %v, want %v", got, want)
	}
}

// fileServingPuller serves each node's copy from that node's own vault root
// (head/, completed/, pre-head/ — the layout a holder serves pulls from), so
// a home that lands a segment becomes a source for the others.
type fileServingPuller struct {
	roots map[string]string
}

func (p *fileServingPuller) Pull(_ context.Context, nodeID string, _, segmentID glid.GLID, dest io.Writer) error {
	root, ok := p.roots[nodeID]
	if !ok {
		return fmt.Errorf("node %s unreachable", nodeID)
	}
	return copyLocalSegmentFile(root, segmentID, dest)
}

// serialApplier serializes applies onto the shared FSM, as the vault-ctl
// leader does for every node's receipts.
type serialApplier struct {
	mu    sync.Mutex
	inner *fsmApplier
}

func (a *serialApplier) Apply(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inner.Apply(data)
}

// countingPuller records the sources each home asked.
type countingPuller struct {
	inner segmentPuller

	mu    sync.Mutex
	asked []string
}

func (p *countingPuller) Pull(ctx context.Context, nodeID string, vaultID, segmentID glid.GLID, dest io.Writer) error {
	p.mu.Lock()
	p.asked = append(p.asked, nodeID)
	p.mu.Unlock()
	return p.inner.Pull(ctx, nodeID, vaultID, segmentID, dest)
}

func (p *countingPuller) askedOf(node string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, a := range p.asked {
		if a == node {
			n++
		}
	}
	return n
}

// TestCollectCorruptOriginAcrossFourHomes: the origin's completed/ copy is
// rotten and one other holder has the published bytes. Four homes collect
// concurrently, each in a single pass; every home lands the published bytes,
// is recorded as holder, and asks the rotten origin at most once.
func TestCollectCorruptOriginAcrossFourHomes(t *testing.T) {
	t.Parallel()
	for _, tc := range corruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fsm := vaultctlfsm.New()
			applier := &serialApplier{inner: &fsmApplier{fsm: fsm}}
			vaultID := glid.New()
			segID := glid.New()
			good, checksum := buildSegmentBytes(t, vaultID, segID, "the-real-segment-record")

			roots := map[string]string{
				testOriginNode:    t.TempDir(),
				testHealthyHolder: t.TempDir(),
			}
			if err := paths.EnsureSegmentationDirs(roots[testOriginNode]); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(paths.CompletedSegment(roots[testOriginNode], segID), tc.corrupt(t, vaultID, segID, good), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := paths.EnsureHeadDir(roots[testHealthyHolder]); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(paths.HeadSegment(roots[testHealthyHolder], segID), good, 0o600); err != nil {
				t.Fatal(err)
			}

			if err := applier.Apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
				SegmentID:    segID,
				RecordCount:  1,
				Checksum:     checksum,
				OriginNodeID: testOriginNode,
			})); err != nil {
				t.Fatalf("publish: %v", err)
			}
			if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, testHealthyHolder)); err != nil {
				t.Fatalf("ack holder: %v", err)
			}

			homes := []string{"node-home-1", "node-home-2", "node-home-3", "node-home-4"}
			for _, h := range homes {
				roots[h] = t.TempDir()
			}
			transport := &fileServingPuller{roots: roots}
			lookup := func() *vaultctlfsm.FSM { return fsm }

			type home struct {
				id     string
				mgr    *collection.Manager
				puller *countingPuller
			}
			var hs []home
			for _, id := range homes {
				p := &countingPuller{inner: transport}
				mgr := collection.New(collection.Config{})
				if err := mgr.RegisterVault(vaultID, roots[id], collection.VaultConfig{
					Log:      &segmentLogReader{lookup: lookup, localNodeID: id, vaultRoot: roots[id]},
					Pull:     &segmentPullClient{lookup: lookup, puller: p, localNodeID: id, vaultRoot: roots[id]},
					Receipts: &segmentReceiptCommitter{applier: applier, localNodeID: id},
				}); err != nil {
					t.Fatalf("RegisterVault %s: %v", id, err)
				}
				hs = append(hs, home{id: id, mgr: mgr, puller: p})
			}

			errs := make([]error, len(hs))
			var wg sync.WaitGroup
			for i, h := range hs {
				wg.Go(func() {
					errs[i] = h.mgr.CollectOnce(context.Background(), vaultID)
				})
			}
			wg.Wait()

			for i, h := range hs {
				if errs[i] != nil {
					t.Errorf("%s CollectOnce: %v", h.id, errs[i])
				}
				got, err := os.ReadFile(paths.HeadSegment(roots[h.id], segID))
				if err != nil {
					t.Errorf("%s: segment not in head/: %v", h.id, err)
					continue
				}
				if !bytes.Equal(got, good) {
					t.Errorf("%s: head/ bytes differ from the published segment", h.id)
				}
				if !segmentHolds(fsm, segID, h.id) {
					t.Errorf("%s not recorded as holder", h.id)
				}
				if n := h.puller.askedOf(testOriginNode); n > 1 {
					t.Errorf("%s asked the rotten origin %d times in one pass", h.id, n)
				}
			}
		})
	}
}
