package orchestrator

// A retention disposition must be evaluated when a chunk is acted on, not when
// the sweep that will act on it began.
//
// retentionRunnerFor captures disposition once per sweep, and a sweep processes
// its entire matched set without returning. The retention cron is registered
// WithSingletonMode(LimitModeReschedule), so ticks landing during a running
// sweep are dropped and never refresh it. The effective refresh interval is one
// whole SWEEP — which, at an observed drain rate of ~1 chunk per 24s, can be
// tens of minutes.
//
// Observed on the dev cluster: a vault whose stored disposition was "delete"
// kept fanning records out to its route target at ~2600 records/sec, because
// the in-flight sweep still held "route".

import (
	"context"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// dispositionLoader serves a vault config the test can mutate between calls,
// standing in for an operator changing disposition while a sweep runs.
type dispositionLoader struct {
	vaultID     glid.GLID
	disposition string
	target      *glid.GLID
	loads       int
}

func (l *dispositionLoader) Load(context.Context) (*system.System, error) {
	l.loads++
	return &system.System{
		Config: system.Config{
			Vaults: []system.VaultConfig{{
				ID:                             l.vaultID,
				Name:                           "v",
				Type:                           system.VaultTypeFile,
				RetentionDisposition:           l.disposition,
				RetentionTransferTargetVaultID: l.target,
			}},
		},
	}, nil
}

func newDispositionRunner(t *testing.T, initial string) (*retentionRunner, *dispositionLoader) {
	t.Helper()
	vaultID := glid.New()
	loader := &dispositionLoader{vaultID: vaultID, disposition: initial}
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	orch.setSystemLoader(loader)
	return &retentionRunner{
		vaultID: vaultID,
		orch:    orch,
		// Captured-at-sweep-start value, deliberately the OPPOSITE of what the
		// store will say, so a stale read is unmistakable.
		disposition: system.RetentionDispositionRoute,
	}, loader
}

// The core regression: the runner's captured field says "route", the config
// store says "delete", and the chunk must be deleted.
func TestDispositionResolvesFromConfigNotTheSweepCapture(t *testing.T) {
	t.Parallel()
	r, _ := newDispositionRunner(t, system.RetentionDispositionDelete)

	got, _ := r.currentDisposition()
	if got != system.RetentionDispositionDelete {
		t.Fatalf("disposition = %q, want %q — the sweep-captured value must not win over current config",
			got, system.RetentionDispositionDelete)
	}
}

// Flipping disposition must take effect on the NEXT chunk, without the sweep
// ending. This is the operator-visible promise: change it, and the very next
// chunk honours the change.
func TestDispositionChangeTakesEffectOnTheNextChunkMidSweep(t *testing.T) {
	t.Parallel()
	r, loader := newDispositionRunner(t, system.RetentionDispositionRoute)

	if got, _ := r.currentDisposition(); got != system.RetentionDispositionRoute {
		t.Fatalf("before the change: %q, want route", got)
	}

	// Operator switches to delete. No sweep boundary, no restart.
	loader.disposition = system.RetentionDispositionDelete

	if got, _ := r.currentDisposition(); got != system.RetentionDispositionDelete {
		t.Fatalf("after the change: %q, want delete — a change mid-sweep must be honoured "+
			"by the next chunk, not the next sweep", got)
	}
}

// applyRetentionDispositionToChunk is the dispatch that was reading the stale
// field. With the store saying delete it must be a no-op returning true (the
// caller then destroys the local chunk) and must NOT reach the routing path.
func TestApplyDispositionHonoursCurrentConfig(t *testing.T) {
	t.Parallel()
	r, loader := newDispositionRunner(t, system.RetentionDispositionDelete)

	// r.orch has no pipeline wired, so if this took the route branch it would
	// hit SubmitRetentionRecord and fail rather than return true.
	if !r.applyRetentionDispositionToChunk(chunk.NewChunkID()) {
		t.Fatal("delete disposition must be a no-op returning true")
	}
	if loader.loads == 0 {
		t.Error("the dispatch never consulted the config store; it is still reading the captured field")
	}
}

// The transfer target travels with the disposition, resolved at the same
// moment. A target read from a stale capture could re-home a chunk into a vault
// the operator has since pointed away from.
func TestTransferTargetResolvesWithTheDisposition(t *testing.T) {
	t.Parallel()
	r, loader := newDispositionRunner(t, system.RetentionDispositionTransfer)
	first := glid.New()
	loader.target = &first

	_, target := r.currentDisposition()
	if target == nil || *target != first {
		t.Fatalf("target = %v, want the configured %s", target, first)
	}

	second := glid.New()
	loader.target = &second
	_, target = r.currentDisposition()
	if target == nil || *target != second {
		t.Fatalf("target after retarget = %v, want %s", target, second)
	}
}

// A config load that fails must not silently change behaviour: fall back to the
// value captured at sweep start rather than defaulting to delete, which would
// destroy records the operator asked to be routed.
func TestUnreadableConfigFallsBackToTheCapturedDisposition(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	// No system loader installed: loadSystem returns ErrNoSystemLoader.
	r := &retentionRunner{
		vaultID:     glid.New(),
		orch:        orch,
		disposition: system.RetentionDispositionRoute,
	}

	got, _ := r.currentDisposition()
	if got != system.RetentionDispositionRoute {
		t.Fatalf("with an unreadable config: %q, want the captured %q — an unreadable config "+
			"is not a reason to start deleting", got, system.RetentionDispositionRoute)
	}
}

// A vault absent from config (deleted mid-sweep) also falls back rather than
// silently becoming delete.
func TestVaultMissingFromConfigFallsBackToTheCapturedDisposition(t *testing.T) {
	t.Parallel()
	r, loader := newDispositionRunner(t, system.RetentionDispositionDelete)
	loader.vaultID = glid.New() // config now describes a DIFFERENT vault

	got, _ := r.currentDisposition()
	if got != system.RetentionDispositionRoute {
		t.Fatalf("vault absent from config: %q, want the captured %q", got, system.RetentionDispositionRoute)
	}
}

// flipAfterLoader flips disposition once a given number of resolutions have
// happened, simulating an operator changing it partway through a sweep that is
// already processing chunks.
type flipAfterLoader struct {
	vaultID  glid.GLID
	before   string
	after    string
	flipAt   int
	resolved int
}

func (l *flipAfterLoader) Load(context.Context) (*system.System, error) {
	l.resolved++
	d := l.before
	if l.resolved > l.flipAt {
		d = l.after
	}
	return &system.System{
		Config: system.Config{
			Vaults: []system.VaultConfig{{
				ID:                   l.vaultID,
				Name:                 "v",
				Type:                 system.VaultTypeFile,
				RetentionDisposition: d,
			}},
		},
	}, nil
}

// The definition-of-done case: a disposition changed WHILE a sweep is walking
// its chunk set must be honoured by the chunks that come after the change, not
// deferred to the next sweep.
//
// Each chunk resolves independently, so flipping after the 3rd resolution means
// chunks 1-3 route and 4-8 delete. Under the old per-sweep capture the split
// would be 8-0: every chunk in the batch would carry the value the sweep began
// with.
func TestDispositionFlipMidSweepSplitsTheBatch(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	loader := &flipAfterLoader{
		vaultID: vaultID,
		before:  system.RetentionDispositionRoute,
		after:   system.RetentionDispositionDelete,
		flipAt:  3,
	}
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	orch.setSystemLoader(loader)
	r := &retentionRunner{
		vaultID:     vaultID,
		orch:        orch,
		disposition: system.RetentionDispositionRoute,
	}

	const chunks = 8
	var routed, deleted int
	for range chunks {
		switch d, _ := r.currentDisposition(); d {
		case system.RetentionDispositionRoute:
			routed++
		case system.RetentionDispositionDelete:
			deleted++
		}
	}

	if routed != 3 || deleted != chunks-3 {
		t.Fatalf("routed=%d deleted=%d, want 3 and %d — the batch must split at the change, "+
			"not carry the sweep's opening value to every chunk", routed, deleted, chunks-3)
	}
}
