package chunking_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// These tests assert the UNIFIED corrupt-GLCB recovery story (gastrolog-687m11,
// replacing the gastrolog-5do8sh gap 4 divergence pins). Both the build pass
// and restart recovery now share one story for an existing-but-unreadable
// sealed GLCB: detect → quarantine the file to a .corrupt sibling → raise the
// per-vault operator alert → heal (rebuild from source segments when they are
// still available; otherwise the quarantine's stat-miss on the canonical path
// hands the chunk to the missing-GLCB machinery — the orchestrator's GLCB
// catch-up sweep re-pulls from a peer home) → clear the alert and remove the
// quarantine once the canonical GLCB is readable again.

// glcbCorruptions are the on-disk shapes a damaged data.glcb can take. Each
// mutation keeps the file present (os.Stat succeeds) but makes
// glcb.OpenMappedBlob fail: truncation trips the minimum-size check,
// a garbaged header fails preamble validation, and a zero-length file is
// rejected as "empty GLCB" before parsing.
var glcbCorruptions = []struct {
	name    string
	corrupt func(t *testing.T, glcbPath string)
}{
	{
		name: "truncated",
		corrupt: func(t *testing.T, glcbPath string) {
			t.Helper()
			if err := os.Truncate(glcbPath, 4); err != nil {
				t.Fatalf("truncate GLCB: %v", err)
			}
		},
	},
	{
		name: "garbage header",
		corrupt: func(t *testing.T, glcbPath string) {
			t.Helper()
			f, err := os.OpenFile(glcbPath, os.O_WRONLY, 0)
			if err != nil {
				t.Fatalf("open GLCB for corruption: %v", err)
			}
			defer func() { _ = f.Close() }()
			garbage := make([]byte, 32)
			for i := range garbage {
				garbage[i] = 0xFF
			}
			if _, err := f.WriteAt(garbage, 0); err != nil {
				t.Fatalf("overwrite GLCB header: %v", err)
			}
		},
	},
	{
		name: "empty",
		corrupt: func(t *testing.T, glcbPath string) {
			t.Helper()
			if err := os.Truncate(glcbPath, 0); err != nil {
				t.Fatalf("truncate GLCB to zero: %v", err)
			}
		},
	},
}

// alertEvent is one Set/Clear observed by historyAlertSink, in order.
type alertEvent struct {
	kind    string // "set" | "clear"
	id      string
	message string
}

// historyAlertSink records the full ordered Set/Clear history so a single
// pass that raises AND clears an alert (corrupt detected → rebuilt) is
// observable, unlike the last-state-only recordingAlertSink.
type historyAlertSink struct {
	mu     sync.Mutex
	events []alertEvent
}

func (s *historyAlertSink) Raise(typeID, instanceKey, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, alertEvent{kind: "set", id: sinkAlarmID(typeID, instanceKey), message: detail})
}

func (s *historyAlertSink) Clear(typeID, instanceKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, alertEvent{kind: "clear", id: sinkAlarmID(typeID, instanceKey)})
}

func (s *historyAlertSink) history(id string) []alertEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []alertEvent
	for _, e := range s.events {
		if e.id == id {
			out = append(out, e)
		}
	}
	return out
}

// requireSetThenCleared asserts the alert was raised, with a message naming
// the chunk, and ended cleared (healed chunks must not leave the alert up).
func requireSetThenCleared(t *testing.T, sink *historyAlertSink, id string, chunkID chunk.ChunkID) {
	t.Helper()
	events := sink.history(id)
	if len(events) == 0 {
		t.Fatalf("alert %s never raised", id)
	}
	if events[0].kind != "set" {
		t.Fatalf("alert %s first event = %q, want set", id, events[0].kind)
	}
	if !strings.Contains(events[0].message, chunkID.String()) {
		t.Fatalf("alert message must name the corrupt chunk %s, got: %q", chunkID, events[0].message)
	}
	last := events[len(events)-1]
	if last.kind != "clear" {
		t.Fatalf("alert %s final event = %+v, want clear", id, last)
	}
}

type builtGLCBFixture struct {
	home     string
	fsm      *vaultctlfsm.FSM
	vaultID  glid.GLID
	segID    glid.GLID
	chunkID  chunk.ChunkID
	glcbPath string
}

func (fx builtGLCBFixture) quarantinePath() string {
	return fx.glcbPath + chunking.GLCBCorruptSuffix
}

func (fx builtGLCBFixture) alertID() string {
	return "chunking-glcb-corrupt:" + fx.vaultID.String()
}

// setupSealingChunkWithBuiltGLCB reproduces the TestRecoverOnceSealsFromExistingGLCB
// fixture: head segments on disk, a sealed manifest pending in the FSM
// (chunk in Sealing), and a valid GLCB already materialized at
// ChunkGLCBPath — the state right before CmdSealChunk would commit.
func setupSealingChunkWithBuiltGLCB(t *testing.T) builtGLCBFixture {
	t.Helper()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	home := t.TempDir()
	writeHeadSegment(t, home, segID, vaultID, []recordForSeg{{0, base, "one"}, {1, base.Add(time.Second), "two"}})

	fsm := vaultctlfsm.New()
	openedAt := base
	sealedAt := base.Add(time.Minute)
	publishSegForTest(t, fsm, segID, 1, openedAt)
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, openedAt))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID:         segID,
		FirstRecordNumber: 0,
		LastRecordNumber:  1,
		SliceBytes:        4096,
		RefAddedAt:        openedAt,
	}))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, sealedAt))

	if _, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest: chunking.SealedManifest{
			ChunkID:  chunkID,
			OpenedAt: openedAt,
			SealedAt: sealedAt,
			Refs: []chunking.ManifestRef{{
				SegmentID:         segID,
				FirstRecordNumber: 0,
				LastRecordNumber:  1,
			}},
		},
		VaultID:   vaultID,
		ChunkRoot: filepath.Join(home, "chunks"),
		Locate:    chunking.HeadSegmentLocator{Root: home},
	}); err != nil {
		t.Fatalf("BuildSealedChunk: %v", err)
	}
	return builtGLCBFixture{
		home:     home,
		fsm:      fsm,
		vaultID:  vaultID,
		segID:    segID,
		chunkID:  chunkID,
		glcbPath: chunking.ChunkGLCBPath(filepath.Join(home, "chunks"), chunkID),
	}
}

func registerFixtureVault(t *testing.T, fx builtGLCBFixture, sink alert.Sink) *chunking.Manager {
	t.Helper()
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(fx.vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       fx.home,
		ChunkRoot:       filepath.Join(fx.home, "chunks"),
		FSM:             fx.fsm,
		Locate:          chunking.HeadSegmentLocator{Root: fx.home},
		Applier:         &flakyFSMApplier{fsm: fx.fsm},
		IsLeader:        func() bool { return true },
		Alerts:          sink,
	}); err != nil {
		t.Fatal(err)
	}
	return mgr
}

// requireHealed asserts the post-heal state shared by every corruption path:
// the canonical GLCB opens and carries every manifest record, the chunk is
// Sealed with the manifest cleared, the quarantine file is gone, and the
// corrupt-GLCB alert ended cleared.
func requireHealed(t *testing.T, fx builtGLCBFixture, sink *historyAlertSink) {
	t.Helper()
	blob, err := glcb.OpenMappedBlob(fx.glcbPath)
	if err != nil {
		t.Fatalf("open healed GLCB: %v", err)
	}
	defer func() { _ = blob.Close() }()
	if blob.Meta().RecordCount != 2 {
		t.Fatalf("healed GLCB records = %d, want 2", blob.Meta().RecordCount)
	}

	entry := fx.fsm.Get(fx.chunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed after heal", entry)
	}
	if entry.RecordCount != 2 {
		t.Fatalf("RecordCount = %d, want 2", entry.RecordCount)
	}
	if fx.fsm.SealedManifest() != nil {
		t.Fatal("sealed manifest must clear after the healed chunk seals")
	}

	if _, err := os.Stat(fx.quarantinePath()); !os.IsNotExist(err) {
		t.Fatalf("quarantine file must be removed once the chunk heals; stat = %v", err)
	}
	requireSetThenCleared(t, sink, fx.alertID(), fx.chunkID)
}

// TestBuildOnceQuarantinesAndRebuildsCorruptGLCB: build-path half of the
// unified story. BuildOnce on a corrupt existing GLCB quarantines the file,
// raises the corrupt-GLCB alert, rebuilds from the still-present source
// segments, seals, clears the alert, and removes the quarantine — all in the
// same pass. The pre-687m11 behavior rebuilt too, but silently: the
// corruption signal was swallowed (nolint:nilerr) with no alert and no
// quarantine.
func TestBuildOnceQuarantinesAndRebuildsCorruptGLCB(t *testing.T) {
	t.Parallel()
	for _, tc := range glcbCorruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := setupSealingChunkWithBuiltGLCB(t)
			tc.corrupt(t, fx.glcbPath)

			sink := &historyAlertSink{}
			mgr := registerFixtureVault(t, fx, sink)
			if err := mgr.BuildOnce(t.Context(), fx.vaultID); err != nil {
				t.Fatalf("BuildOnce on a corrupt existing GLCB must rebuild, got: %v", err)
			}
			requireHealed(t, fx, sink)
		})
	}
}

// TestRecoverOnceQuarantinesCorruptGLCBAndBuildHeals: recover-path half of
// the unified story. RecoverOnce on a corrupt existing GLCB no longer
// propagates the read error (pre-687m11 it errored and NEVER rebuilt — the
// chunk starved until operator action): it quarantines the file, raises the
// alert, and degrades to the missing-GLCB case, which the worker's normal
// build pass then heals by rebuilding from source segments.
func TestRecoverOnceQuarantinesCorruptGLCBAndBuildHeals(t *testing.T) {
	t.Parallel()
	for _, tc := range glcbCorruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := setupSealingChunkWithBuiltGLCB(t)
			tc.corrupt(t, fx.glcbPath)

			sink := &historyAlertSink{}
			mgr := registerFixtureVault(t, fx, sink)
			if err := mgr.RecoverOnce(context.Background(), fx.vaultID); err != nil {
				t.Fatalf("RecoverOnce on a corrupt GLCB must quarantine and degrade to missing, got: %v", err)
			}

			// Quarantined, not sealed from bad bytes: canonical path reads
			// as missing, damaged bytes preserved beside it, alert up, chunk
			// still Sealing with the manifest pending for the build pass.
			if _, err := os.Stat(fx.glcbPath); !os.IsNotExist(err) {
				t.Fatalf("canonical GLCB path must stat-miss after quarantine; stat = %v", err)
			}
			if _, err := os.Stat(fx.quarantinePath()); err != nil {
				t.Fatalf("quarantine file must exist after detection: %v", err)
			}
			events := sink.history(fx.alertID())
			if len(events) != 1 || events[0].kind != "set" {
				t.Fatalf("alert history after RecoverOnce = %+v, want exactly one set", events)
			}
			entry := fx.fsm.Get(fx.chunkID)
			if entry == nil || entry.State != chunk.ChunkStateSealing {
				t.Fatalf("chunk entry = %+v, want Sealing (recovery must not seal from a corrupt GLCB)", entry)
			}
			if fx.fsm.SealedManifest() == nil {
				t.Fatal("sealed manifest must remain pending so the build pass can rebuild")
			}

			// The worker's build pass (BuildOnce here) heals: rebuild from
			// the still-present head segments, seal, clear alert + quarantine.
			if err := mgr.BuildOnce(t.Context(), fx.vaultID); err != nil {
				t.Fatalf("BuildOnce after quarantine must rebuild from segments, got: %v", err)
			}
			requireHealed(t, fx, sink)
		})
	}
}

// TestCorruptGLCBSegmentsGoneDegradesToMissing: the segments-released case.
// When the source segment bytes are gone, a local rebuild is impossible —
// the corrupt case must degrade to EXACTLY the missing-GLCB state: after
// quarantine the canonical path stat-misses, which is the precise trigger
// condition of the orchestrator's GLCB catch-up sweep (pullMissingGLCB →
// peer re-pull; cross-node end-to-end coverage in
// TestOrchPipeline_GLCBReplicaCatchup). The pull's success side then clears
// the corrupt state via Manager.NoteGLCBRestored, exercised here directly.
func TestCorruptGLCBSegmentsGoneDegradesToMissing(t *testing.T) {
	t.Parallel()
	fx := setupSealingChunkWithBuiltGLCB(t)

	// Keep a pristine copy of the GLCB to play the peer's verified re-pull.
	pristine, err := os.ReadFile(fx.glcbPath)
	if err != nil {
		t.Fatalf("read pristine GLCB: %v", err)
	}
	glcbCorruptions[0].corrupt(t, fx.glcbPath)
	// Source segment bytes are gone from this home (no Collector is wired,
	// so nothing can bring them back — the released-segments reality).
	if err := os.Remove(paths.HeadSegment(fx.home, fx.segID)); err != nil {
		t.Fatalf("remove head segment: %v", err)
	}

	sink := &historyAlertSink{}
	mgr := registerFixtureVault(t, fx, sink)
	err = mgr.BuildOnce(t.Context(), fx.vaultID)
	var missing *chunking.MissingSegmentsError
	if !errors.As(err, &missing) {
		t.Fatalf("BuildOnce = %v, want MissingSegmentsError (rebuild impossible without segments)", err)
	}

	// Degraded to missing: canonical path stat-misses (the catch-up sweep's
	// pull trigger), damaged bytes quarantined, alert up.
	if _, statErr := os.Stat(fx.glcbPath); !os.IsNotExist(statErr) {
		t.Fatalf("canonical GLCB path must stat-miss so the peer re-pull triggers; stat = %v", statErr)
	}
	if _, statErr := os.Stat(fx.quarantinePath()); statErr != nil {
		t.Fatalf("quarantine file must exist: %v", statErr)
	}
	events := sink.history(fx.alertID())
	if len(events) != 1 || events[0].kind != "set" {
		t.Fatalf("alert history = %+v, want exactly one set while unhealed", events)
	}

	// Peer re-pull lands a verified copy on the canonical path (what
	// verifyAndPromoteGLCB's rename does) and the orchestrator notifies
	// chunking. Alert clears, quarantine removed.
	if err := os.WriteFile(fx.glcbPath, pristine, 0o600); err != nil {
		t.Fatalf("restore pristine GLCB: %v", err)
	}
	mgr.NoteGLCBRestored(fx.vaultID, fx.chunkID)
	if _, statErr := os.Stat(fx.quarantinePath()); !os.IsNotExist(statErr) {
		t.Fatalf("quarantine file must be removed on restore; stat = %v", statErr)
	}

	// The next build pass adopts the restored GLCB and seals.
	if err := mgr.BuildOnce(t.Context(), fx.vaultID); err != nil {
		t.Fatalf("BuildOnce after peer restore must adopt and seal, got: %v", err)
	}
	requireHealed(t, fx, sink)
}
