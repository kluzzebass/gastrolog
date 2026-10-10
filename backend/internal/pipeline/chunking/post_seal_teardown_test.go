package chunking_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"
)

// recordingHandler keeps the message of every log record.
type recordingHandler struct {
	mu   *sync.Mutex
	msgs *[]string
}

func newRecordingHandler() recordingHandler {
	return recordingHandler{mu: &sync.Mutex{}, msgs: &[]string{}}
}

func (h recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.msgs = append(*h.msgs, r.Message)
	return nil
}

func (h recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h recordingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(*h.msgs)
}

// heldPostSeal parks the post-seal goroutine at its start until teardown
// cancels its context, then until the test releases it.
type heldPostSeal struct {
	entered   chan struct{}
	sawCancel chan struct{}
	release   chan struct{}
	calls     atomic.Int32
}

func newHeldPostSeal() *heldPostSeal {
	return &heldPostSeal{
		entered:   make(chan struct{}),
		sawCancel: make(chan struct{}),
		release:   make(chan struct{}),
	}
}

func (h *heldPostSeal) hook(ctx context.Context) {
	if h.calls.Add(1) != 1 {
		return
	}
	close(h.entered)
	<-ctx.Done()
	close(h.sawCancel)
	<-h.release
}

// A follower home whose GLCB is built when the leader's seal arrives runs the
// head purge on the post-seal goroutine the sealed-manifest-cleared callback
// starts. Teardown — UnregisterVault, or Run returning — must cancel that
// goroutine and wait for it: once teardown returns, nothing of the vault's
// post-seal work may purge a head copy or log.
func TestPostSealGoroutineEndsWithTeardown(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		run  bool
		// teardown returns once the vault's chunking is torn down.
		teardown func(mgr *chunking.Manager, vaultID glid.GLID, stopRun func())
	}{
		{
			name: "UnregisterVault before Run",
			teardown: func(mgr *chunking.Manager, vaultID glid.GLID, _ func()) {
				mgr.UnregisterVault(vaultID)
			},
		},
		{
			name: "UnregisterVault while Run is active",
			run:  true,
			teardown: func(mgr *chunking.Manager, vaultID glid.GLID, _ func()) {
				mgr.UnregisterVault(vaultID)
			},
		},
		{
			name: "Run returns",
			run:  true,
			teardown: func(_ *chunking.Manager, _ glid.GLID, stopRun func()) {
				stopRun()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

			logs := newRecordingHandler()
			mgr := chunking.New(chunking.Config{Logger: slog.New(logs)})
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
			held := newHeldPostSeal()
			mgr.SetPostSealHookForTest(vaultID, held.hook)

			sealedAt := base.Add(time.Minute)
			applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))
			if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
				t.Fatalf("BuildOnce: %v", err)
			}
			assertHeadPresent(t, home, segID)

			applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkID, sealedAt.Add(time.Second), 1, 100, base, base, base, true, sealedAt.Add(time.Second)))
			waittest.Recv(t, "post-seal goroutine parked", held.entered, nil)

			stopRun := func() {}
			if tc.run {
				firstPass := make(chan struct{})
				var once sync.Once
				mgr.SetWorkerPassHookForTest(vaultID, func() { once.Do(func() { close(firstPass) }) })
				ctx, cancel := context.WithCancel(context.Background())
				runDone := make(chan struct{})
				go func() { _ = mgr.Run(ctx); close(runDone) }()
				stopRun = func() { cancel(); <-runDone }
				t.Cleanup(stopRun)
				waittest.Recv(t, "worker finished its first pass", firstPass, nil)
			}

			var seq, tornDownAt, releasedAt atomic.Int64
			tornDown := make(chan struct{})
			go func() {
				tc.teardown(mgr, vaultID, stopRun)
				tornDownAt.Store(seq.Add(1))
				close(tornDown)
			}()

			waittest.Recv(t, "teardown cancelled the post-seal goroutine", held.sawCancel, nil)
			releasedAt.Store(seq.Add(1))
			close(held.release)
			waittest.Recv(t, "teardown returned", tornDown, nil)

			if tornDownAt.Load() < releasedAt.Load() {
				t.Error("teardown returned while the post-seal goroutine was still running")
			}
			for _, msg := range logs.messages() {
				if msg == "purged head segments after build" || msg == "purging head segment after build" {
					t.Errorf("cancelled post-seal work logged %q", msg)
				}
			}
			assertHeadPresent(t, home, segID)
		})
	}
}
