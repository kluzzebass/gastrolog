package chunking_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"
)

// passGate reports each chunking-worker pass and parks the worker at the end
// of pass parkAt until resume closes, so a test can land an FSM callback's
// Notify after the pass finished its work but before the worker waits again.
type passGate struct {
	parkAt int
	calls  int
	passes chan int
	resume chan struct{}
}

func newPassGate(parkAt int) *passGate {
	return &passGate{parkAt: parkAt, passes: make(chan int, 64), resume: make(chan struct{})}
}

func (g *passGate) hook() {
	g.calls++
	select {
	case g.passes <- g.calls:
	default:
	}
	if g.calls == g.parkAt {
		<-g.resume
	}
}

func (g *passGate) awaitPass(t *testing.T, want int) {
	t.Helper()
	if got := waittest.Recv(t, fmt.Sprintf("worker pass %d", want), g.passes, nil); got != want {
		t.Fatalf("worker pass = %d, want %d", got, want)
	}
}

func runManager(t *testing.T, mgr *chunking.Manager) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = mgr.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// A ReleaseSegments callback that queues a purge after the worker's release
// work but before it waits again must still wake the worker: otherwise the
// released head copy stays on disk until some unrelated release wake.
func TestWorkerKeepsReleaseNotifyLandingAfterPass(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	publishSegForTest(t, fsm, segID, 1, base)

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
	}); err != nil {
		t.Fatal(err)
	}
	gate := newPassGate(1)
	mgr.SetWorkerPassHookForTest(vaultID, gate.hook)
	runManager(t, mgr)

	gate.awaitPass(t, 1)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalReleaseSegments([]glid.GLID{segID}))
	assertHeadPresent(t, home, segID)
	close(gate.resume)

	waitHeadPurged(t, mgr, vaultID, home, segID)
}

// A sealed-manifest callback that lands after a build pass but before the
// worker waits again must still wake the worker: otherwise the GLCB is not
// built until some unrelated wake.
func TestWorkerKeepsBuildWakeLandingAfterPass(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}})

	fsm := vaultctlfsm.New()
	publishSegForTest(t, fsm, segID, 1, base)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 0,
		SliceBytes: 1024, RefAddedAt: base,
	}))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		IsLeader:        func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	gate := newPassGate(2)
	mgr.SetWorkerPassHookForTest(vaultID, gate.hook)
	runManager(t, mgr)

	gate.awaitPass(t, 1)
	mgr.NotifyVault(vaultID)
	gate.awaitPass(t, 2)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, base.Add(time.Minute)))
	close(gate.resume)

	glcbPath := chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID)
	waittest.Progress(t, "GLCB built after a sealed-manifest wake that landed after a pass", func() (string, bool) {
		_, err := os.Stat(glcbPath)
		return fmt.Sprintf("%s glcb-present=%t", stageProgress(mgr, vaultID), err == nil), err == nil
	})
}
