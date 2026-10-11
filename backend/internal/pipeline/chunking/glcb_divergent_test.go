package chunking_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// twoHomes is a vault homed on a leader and a follower that share one
// vault-ctl FSM, standing in for the replicated state both apply. The leader
// builds the chunk and commits its seal; the follower builds afterwards from
// the segment it holds.
type twoHomes struct {
	vaultID      glid.GLID
	fsm          *vaultctlfsm.FSM
	chunk        chunkFixture
	leaderHome   string
	followerHome string
	leader       *chunking.Manager
	follower     *chunking.Manager
	sink         *historyAlertSink
}

func (h twoHomes) leaderGLCB() string {
	return chunking.ChunkGLCBPath(filepath.Join(h.leaderHome, "chunks"), h.chunk.id)
}

func (h twoHomes) followerGLCB() string {
	return chunking.ChunkGLCBPath(filepath.Join(h.followerHome, "chunks"), h.chunk.id)
}

func (h twoHomes) divergedAlarmID() string {
	return "chunking-build-diverged:" + h.vaultID.String()
}

func (h twoHomes) sealedEntry(t *testing.T) vaultctlfsm.ManifestEntry {
	t.Helper()
	e := h.fsm.Get(h.chunk.id)
	if e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed", e)
	}
	return *e
}

func (h twoHomes) buildOn(t *testing.T, mgr *chunking.Manager, who string) {
	t.Helper()
	if err := mgr.BuildOnce(t.Context(), h.vaultID); err != nil {
		t.Fatalf("%s BuildOnce: %v", who, err)
	}
}

// newTwoHomes seals one chunk built on the leader from a segment holding
// "agreed record", and gives the follower the same segment ID holding
// followerRecord. A different followerRecord is a well-formed segment whose
// bytes differ from the leader's — the follower's build then diverges.
func newTwoHomes(t *testing.T, followerRecord string) twoHomes {
	t.Helper()
	h := twoHomes{
		vaultID:      glid.New(),
		fsm:          vaultctlfsm.New(),
		chunk:        newChunkFixtures(1, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))[0],
		leaderHome:   t.TempDir(),
		followerHome: t.TempDir(),
		sink:         &historyAlertSink{},
	}
	writeHeadSegment(t, h.leaderHome, h.chunk.seg, h.vaultID, []recordForSeg{{0, h.chunk.at, "agreed record"}})
	writeHeadSegment(t, h.followerHome, h.chunk.seg, h.vaultID, []recordForSeg{{0, h.chunk.at, followerRecord}})
	planAndSealManifest(t, fsmApply(t, h.fsm), h.chunk)

	register := func(home string, leader bool) *chunking.Manager {
		var applied [][]byte
		var applier chunking.VaultCtlApplier = &recordingApplier{out: &applied}
		if leader {
			applier = &fsmApplier{fsm: h.fsm}
		}
		mgr := chunking.New(chunking.Config{})
		if err := mgr.RegisterVault(h.vaultID, chunking.VaultConfig{
			// No segment holder receipts are committed here, so the leader
			// never releases the chunk's segment and the sealed manifest
			// stays for the follower to build from.
			RequiredHolders: func() ([]string, bool) { return []string{"leader", "follower"}, true },
			VaultRoot:       home,
			ChunkRoot:       filepath.Join(home, "chunks"),
			FSM:             h.fsm,
			Locate:          chunking.HeadSegmentLocator{Root: home},
			Applier:         applier,
			IsLeader:        func() bool { return leader },
			Alerts:          h.sink,
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { mgr.UnregisterVault(h.vaultID) })
		return mgr
	}
	h.leader = register(h.leaderHome, true)
	h.follower = register(h.followerHome, false)

	h.buildOn(t, h.leader, "leader")
	h.sealedEntry(t)
	h.buildOn(t, h.follower, "follower")
	if _, err := os.Stat(h.followerGLCB()); err != nil {
		t.Fatalf("premise: the follower built its copy: %v", err)
	}
	return h
}

func verifiedDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	d, err := glcb.VerifyBlobDigest(path)
	if err != nil {
		t.Fatalf("verify %s: %v", path, err)
	}
	return d
}

func TestSealRecordsTheLeaderBuildDigest(t *testing.T) {
	t.Parallel()
	h := newTwoHomes(t, "agreed record")

	e := h.sealedEntry(t)
	if e.Hash == ([32]byte{}) {
		t.Fatal("the committed seal recorded no digest")
	}
	if want := verifiedDigest(t, h.leaderGLCB()); e.Hash != want {
		t.Fatalf("sealed digest %x, want the leader build's %x", e.Hash[:8], want[:8])
	}
}

func TestFollowerBuildCarryingTheSealedDigestIsAgreed(t *testing.T) {
	t.Parallel()
	h := newTwoHomes(t, "agreed record")

	leaderBytes, err := os.ReadFile(h.leaderGLCB())
	if err != nil {
		t.Fatal(err)
	}
	followerBytes, err := os.ReadFile(h.followerGLCB())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leaderBytes, followerBytes) {
		t.Fatal("premise: homes building one manifest over identical segments produce identical bytes")
	}

	if !h.follower.CheckLandedGLCB(h.vaultID, h.sealedEntry(t)) {
		t.Fatal("a follower copy carrying the sealed digest was not agreed")
	}
	if _, err := os.Stat(h.followerGLCB()); err != nil {
		t.Fatalf("the agreed copy left the canonical path: %v", err)
	}
	if _, err := os.Stat(h.followerGLCB() + chunking.GLCBDivergentSuffix); !os.IsNotExist(err) {
		t.Fatalf("an agreed copy was quarantined (stat=%v)", err)
	}
	if events := h.sink.history(h.divergedAlarmID()); len(events) != 0 {
		t.Fatalf("an agreed copy touched the divergence alarm: %+v", events)
	}
}

func TestFollowerBuildDivergingFromTheSealedDigestIsQuarantinedAndReplaced(t *testing.T) {
	t.Parallel()
	h := newTwoHomes(t, "different record")
	e := h.sealedEntry(t)
	if verifiedDigest(t, h.followerGLCB()) == e.Hash {
		t.Fatal("premise: the follower's build differs from the leader's")
	}
	divergent, err := os.ReadFile(h.followerGLCB())
	if err != nil {
		t.Fatal(err)
	}

	if h.follower.CheckLandedGLCB(h.vaultID, e) {
		t.Fatal("a follower copy whose digest differs from the sealed one was agreed")
	}
	if _, err := os.Stat(h.followerGLCB()); !os.IsNotExist(err) {
		t.Fatalf("the divergent copy is still on the canonical path, where it is served and claimed (stat=%v)", err)
	}
	quarantined, err := os.ReadFile(h.followerGLCB() + chunking.GLCBDivergentSuffix)
	if err != nil {
		t.Fatalf("the divergent copy was not kept aside: %v", err)
	}
	if !bytes.Equal(quarantined, divergent) {
		t.Fatal("the quarantined file is not the divergent copy")
	}
	events := h.sink.history(h.divergedAlarmID())
	if len(events) != 1 || events[0].kind != "set" || !strings.Contains(events[0].message, h.chunk.id.String()) {
		t.Fatalf("divergence alarm events = %+v, want one raise naming chunk %s", events, h.chunk.id)
	}

	// The same manifest over the same local segment builds the same
	// divergent bytes, so later passes leave the chunk to the peer pull.
	before := chunksBuilt(h.follower, h.vaultID)
	h.buildOn(t, h.follower, "follower after quarantine")
	h.buildOn(t, h.follower, "follower again")
	if _, err := os.Stat(h.followerGLCB()); !os.IsNotExist(err) {
		t.Fatalf("the follower rebuilt the divergent chunk (stat=%v)", err)
	}
	if after := chunksBuilt(h.follower, h.vaultID); after != before {
		t.Fatalf("follower built %d more chunks after the quarantine, want 0", after-before)
	}
	if h.follower.CheckLandedGLCB(h.vaultID, e) {
		t.Fatal("a home with no copy on its canonical path reported an agreed copy")
	}
	if events := h.sink.history(h.divergedAlarmID()); len(events) != 1 {
		t.Fatalf("a repeat check re-raised the alarm: %+v", events)
	}

	// The agreed copy lands from a peer home, the way the catch-up pull
	// promotes it after verifying it against the sealed digest.
	agreed, err := os.ReadFile(h.leaderGLCB())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.followerGLCB(), agreed, 0o600); err != nil {
		t.Fatal(err)
	}
	h.follower.NoteGLCBRestored(h.vaultID, h.chunk.id)
	if !h.follower.CheckLandedGLCB(h.vaultID, e) {
		t.Fatal("the agreed copy was not agreed after it landed")
	}
	if _, err := os.Stat(h.followerGLCB() + chunking.GLCBDivergentSuffix); !os.IsNotExist(err) {
		t.Fatalf("the replaced divergent copy was left behind (stat=%v)", err)
	}
	events = h.sink.history(h.divergedAlarmID())
	if last := events[len(events)-1]; last.kind != "clear" {
		t.Fatalf("divergence alarm final event = %+v, want clear once no chunk diverges", last)
	}
	h.buildOn(t, h.follower, "follower after recovery")
	got, err := os.ReadFile(h.followerGLCB())
	if err != nil || !bytes.Equal(got, agreed) {
		t.Fatalf("the follower's copy after recovery is not the agreed one (err=%v)", err)
	}
}

// One standing alarm per vault, however many of its chunks diverge.
func TestDivergenceAlarmIsOnePerVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	fsm := vaultctlfsm.New()
	home := t.TempDir()
	chunkRoot := filepath.Join(home, "chunks")
	fixtures := newChunkFixtures(3, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))
	writeFixtureSegments(t, home, vaultID, fixtures)
	apply := fsmApply(t, fsm)
	for _, f := range fixtures {
		planAndSealManifest(t, apply, f)
		ts := f.at.Add(time.Second)
		wrong := [32]byte{0xde, 0xad}
		apply(vaultctlfsm.MarshalSealChunk(f.id, ts, 1, 100, f.at, f.at, f.at, true, ts, wrong))
	}
	sink := &recordingAlertSink{}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       chunkRoot,
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &recordingApplier{out: new([][]byte)},
		IsLeader:        func() bool { return false },
		Alerts:          sink,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })
	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		if mgr.CheckLandedGLCB(vaultID, *fsm.Get(f.id)) {
			t.Fatalf("chunk %s agreed with a digest no build produces", f.id)
		}
	}
	active, _ := sink.snapshot()
	var diverged []string
	for id, detail := range active {
		if strings.HasPrefix(id, "chunking-build-diverged") {
			diverged = append(diverged, id)
			if !strings.Contains(detail, "3 sealed chunk(s)") {
				t.Errorf("alarm detail does not count every divergent chunk: %q", detail)
			}
		}
	}
	if len(diverged) != 1 || diverged[0] != "chunking-build-diverged:"+vaultID.String() {
		t.Fatalf("divergence alarms = %v, want exactly one for vault %s", diverged, vaultID)
	}
}

func TestCheckLandedGLCBEdgeCases(t *testing.T) {
	t.Parallel()
	h := newTwoHomes(t, "agreed record")
	e := h.sealedEntry(t)

	t.Run("a chunk not yet sealed has no digest to agree with", func(t *testing.T) {
		sealing := e
		sealing.State = chunk.ChunkStateSealing
		if h.follower.CheckLandedGLCB(h.vaultID, sealing) {
			t.Fatal("a copy of an unsealed chunk was agreed")
		}
		if _, err := os.Stat(h.followerGLCB()); err != nil {
			t.Fatalf("checking an unsealed chunk moved its copy: %v", err)
		}
	})

	t.Run("a chunk sealed without a digest agrees with any readable copy", func(t *testing.T) {
		undigested := e
		undigested.Hash = [32]byte{}
		if !h.follower.CheckLandedGLCB(h.vaultID, undigested) {
			t.Fatal("a readable copy of a chunk sealed without a digest was refused")
		}
	})

	t.Run("an unregistered vault is never agreed", func(t *testing.T) {
		if h.follower.CheckLandedGLCB(glid.New(), e) {
			t.Fatal("a vault this manager does not run reported an agreed copy")
		}
	})

	t.Run("an unreadable copy takes the corrupt path, not the divergent one", func(t *testing.T) {
		if err := os.Truncate(h.followerGLCB(), 4); err != nil {
			t.Fatal(err)
		}
		if h.follower.CheckLandedGLCB(h.vaultID, e) {
			t.Fatal("an unreadable copy was agreed")
		}
		if _, err := os.Stat(h.followerGLCB() + chunking.GLCBCorruptSuffix); err != nil {
			t.Fatalf("the unreadable copy was not quarantined as corrupt: %v", err)
		}
		if _, err := os.Stat(h.followerGLCB() + chunking.GLCBDivergentSuffix); !os.IsNotExist(err) {
			t.Fatalf("an unreadable copy was quarantined as divergent (stat=%v)", err)
		}
	})
}

// Restart recovery seals a chunk whose GLCB finished building before the
// node went down. The seal it proposes records that blob's digest.
func TestRecoveredSealRecordsTheDigestOfTheExistingGLCB(t *testing.T) {
	t.Parallel()
	fx := setupSealingChunkWithBuiltGLCB(t)
	mgr := registerFixtureVault(t, fx, &historyAlertSink{})
	t.Cleanup(func() { mgr.UnregisterVault(fx.vaultID) })

	if err := mgr.RecoverOnce(t.Context(), fx.vaultID); err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	e := fx.fsm.Get(fx.chunkID)
	if e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed by recovery", e)
	}
	if want := verifiedDigest(t, fx.glcbPath); e.Hash != want {
		t.Fatalf("recovered seal recorded digest %x, want the existing GLCB's %x", e.Hash[:8], want[:8])
	}
}
