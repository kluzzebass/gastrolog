package chunking_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// stalePlan is one chunk over a two-record segment, its manifest ref carrying
// the record bounds the planner records.
type stalePlan struct {
	vaultID glid.GLID
	chunkID chunk.ChunkID
	segID   glid.GLID
	at      time.Time
	home    string
	ref     vaultctlfsm.OpenChunkSegmentRef
}

func newStalePlan(t *testing.T) stalePlan {
	t.Helper()
	p := stalePlan{
		vaultID: glid.New(),
		chunkID: chunk.NewChunkID(),
		segID:   glid.New(),
		at:      time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC),
		home:    t.TempDir(),
	}
	writeHeadSegment(t, p.home, p.segID, p.vaultID, []recordForSeg{{0, p.at, "one"}, {1, p.at.Add(time.Second), "two"}})
	p.ref = boundedRef(t, p.home, p.segID, 0, 1, p.at)
	return p
}

// boundedRef is the ref the planner adds for records first..last of a
// segment under home: its bounds are those records' timestamps.
func boundedRef(t *testing.T, home string, segID glid.GLID, first, last uint32, at time.Time) vaultctlfsm.OpenChunkSegmentRef {
	t.Helper()
	ref := vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: segID, FirstRecordNumber: first, LastRecordNumber: last, SliceBytes: 4096, RefAddedAt: at,
	}
	bounds, err := chunking.BoundsFromManifestRefs([]vaultctlfsm.OpenChunkSegmentRef{ref}, chunking.HeadSegmentLocator{Root: home})
	if err != nil {
		t.Fatal(err)
	}
	if bounds.IsZero() {
		t.Fatal("premise: the manifest ref records its records' bounds")
	}
	ref.Bounds = bounds
	return ref
}

// planManifest applies the planner's commands for the chunk: publish its
// segment, open the manifest, add the ref and, when seal is set, seal the
// manifest into the seal queue.
func planManifest(apply func([]byte), chunkID chunk.ChunkID, ref vaultctlfsm.OpenChunkSegmentRef, records uint32, at time.Time, seal bool) {
	apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: ref.SegmentID, RecordCount: records, ByteSize: 1, FirstIngestTS: at, LastIngestTS: at, Checksum: 1, PublishedAt: at,
	}))
	apply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, at))
	apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, ref))
	if seal {
		apply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, at.Add(time.Minute)))
	}
}

// glcbBytes builds chunkID from the given refs over segments under segHome
// into a scratch chunk root and returns the blob and its digest.
func glcbBytes(t *testing.T, segHome string, vaultID glid.GLID, chunkID chunk.ChunkID, refs ...vaultctlfsm.OpenChunkSegmentRef) ([]byte, [32]byte) {
	t.Helper()
	manifest := chunking.SealedManifest{ChunkID: chunkID}
	for _, r := range refs {
		manifest.Refs = append(manifest.Refs, chunking.ManifestRef{
			SegmentID: r.SegmentID, FirstRecordNumber: r.FirstRecordNumber, LastRecordNumber: r.LastRecordNumber,
		})
	}
	res, err := chunking.BuildSealedChunk(chunking.BuildInput{
		Manifest:  manifest,
		VaultID:   vaultID,
		ChunkRoot: t.TempDir(),
		Locate:    chunking.HeadSegmentLocator{Root: segHome},
	})
	if err != nil {
		t.Fatalf("build %s: %v", chunkID, err)
	}
	data, err := os.ReadFile(res.GLCBPath)
	if err != nil {
		t.Fatal(err)
	}
	return data, res.BlobDigest
}

// mismatchedGLCBs are GLCBs that are not builds of p's manifest, each
// differing from it in one fact the manifest records.
func mismatchedGLCBs(t *testing.T, p stalePlan) map[string][]byte {
	t.Helper()
	other := t.TempDir()
	countSeg, spanSeg := glid.New(), glid.New()
	writeHeadSegment(t, other, countSeg, p.vaultID, []recordForSeg{
		{0, p.at, "one"}, {1, p.at.Add(500 * time.Millisecond), "between"}, {2, p.at.Add(time.Second), "two"},
	})
	writeHeadSegment(t, other, spanSeg, p.vaultID, []recordForSeg{
		{0, p.at.Add(time.Hour), "one"}, {1, p.at.Add(time.Hour + time.Second), "two"},
	})
	otherCount, _ := glcbBytes(t, other, p.vaultID, p.chunkID, vaultctlfsm.OpenChunkSegmentRef{SegmentID: countSeg, LastRecordNumber: 2})
	otherSpan, _ := glcbBytes(t, other, p.vaultID, p.chunkID, vaultctlfsm.OpenChunkSegmentRef{SegmentID: spanSeg, LastRecordNumber: 1})
	otherChunk, _ := glcbBytes(t, p.home, p.vaultID, chunk.NewChunkID(), p.ref)
	return map[string][]byte{
		"another record count": otherCount,
		"another record span":  otherSpan,
		"another chunk":        otherChunk,
	}
}

func plantGLCB(t *testing.T, chunkRoot string, chunkID chunk.ChunkID, data []byte) string {
	t.Helper()
	path := chunking.ChunkGLCBPath(chunkRoot, chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// registerLeader registers the vault on a single leader home. Its one
// required holder never commits a receipt, so no segment is released.
func registerLeader(t *testing.T, vaultID glid.GLID, home string, fsm *vaultctlfsm.FSM) *chunking.Manager {
	t.Helper()
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: func() ([]string, bool) { return []string{"leader"}, true },
		VaultRoot:       home,
		ChunkRoot:       filepath.Join(home, "chunks"),
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &flakyFSMApplier{fsm: fsm},
		IsLeader:        func() bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })
	return mgr
}

func requireSealedWithDigest(t *testing.T, fsm *vaultctlfsm.FSM, id chunk.ChunkID, glcbPath string, want [32]byte) {
	t.Helper()
	e := fsm.Get(id)
	if e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("chunk entry = %+v, want Sealed", e)
	}
	if e.Hash != want {
		t.Fatalf("sealed digest %x, want a build of the manifest's %x", e.Hash[:8], want[:8])
	}
	if got := verifiedDigest(t, glcbPath); got != want {
		t.Fatalf("canonical GLCB digest %x, want a build of the manifest's %x", got[:8], want[:8])
	}
}

func requireQuarantinedMismatched(t *testing.T, glcbPath string, stale []byte) {
	t.Helper()
	kept, err := os.ReadFile(glcbPath + chunking.GLCBMismatchedSuffix)
	if err != nil {
		t.Fatalf("the mismatched GLCB was not kept aside: %v", err)
	}
	if !bytes.Equal(kept, stale) {
		t.Fatal("the quarantined file is not the mismatched GLCB")
	}
}

func TestLeaderBuildsTheChunkInsteadOfSealingAMismatchedGLCB(t *testing.T) {
	t.Parallel()
	plan := newStalePlan(t)
	for name, stale := range mismatchedGLCBs(t, plan) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := plan
			p.home = t.TempDir()
			writeHeadSegment(t, p.home, p.segID, p.vaultID, []recordForSeg{{0, p.at, "one"}, {1, p.at.Add(time.Second), "two"}})
			_, fresh := glcbBytes(t, p.home, p.vaultID, p.chunkID, p.ref)
			fsm := vaultctlfsm.New()
			planManifest(fsmApply(t, fsm), p.chunkID, p.ref, 2, p.at, true)
			mgr := registerLeader(t, p.vaultID, p.home, fsm)
			glcbPath := plantGLCB(t, filepath.Join(p.home, "chunks"), p.chunkID, stale)

			if err := mgr.BuildOnce(t.Context(), p.vaultID); err != nil {
				t.Fatalf("BuildOnce: %v", err)
			}
			requireSealedWithDigest(t, fsm, p.chunkID, glcbPath, fresh)
			if e := fsm.Get(p.chunkID); e.RecordCount != 2 {
				t.Fatalf("sealed RecordCount = %d, want the manifest's 2", e.RecordCount)
			}
			requireQuarantinedMismatched(t, glcbPath, stale)
			if n := chunksBuilt(mgr, p.vaultID); n != 1 {
				t.Fatalf("leader built %d chunks, want 1 (the mismatched GLCB is not a build)", n)
			}
		})
	}
}

func TestLeaderAdoptsAGLCBBuiltFromTheManifest(t *testing.T) {
	t.Parallel()
	p := newStalePlan(t)
	built, digest := glcbBytes(t, p.home, p.vaultID, p.chunkID, p.ref)
	fsm := vaultctlfsm.New()
	planManifest(fsmApply(t, fsm), p.chunkID, p.ref, 2, p.at, true)
	mgr := registerLeader(t, p.vaultID, p.home, fsm)
	glcbPath := plantGLCB(t, filepath.Join(p.home, "chunks"), p.chunkID, built)

	if err := mgr.BuildOnce(t.Context(), p.vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	requireSealedWithDigest(t, fsm, p.chunkID, glcbPath, digest)
	if n := chunksBuilt(mgr, p.vaultID); n != 0 {
		t.Fatalf("leader built %d chunks, want 0: a GLCB built from the manifest is adopted", n)
	}
	if _, err := os.Stat(glcbPath + chunking.GLCBMismatchedSuffix); !os.IsNotExist(err) {
		t.Fatalf("a GLCB built from the manifest was quarantined (stat=%v)", err)
	}
}

// A chunk already sealed out of seal order reaches the seal-queue head with
// its digest recorded. A GLCB holding the manifest's record count and span
// but other bytes is not that build.
func TestLeaderBuildsTheChunkInsteadOfSealingAGLCBWithAnotherDigest(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	home := t.TempDir()
	at := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	first, second := chunk.NewChunkID(), chunk.NewChunkID()
	firstSeg, secondSeg := glid.New(), glid.New()
	writeHeadSegment(t, home, firstSeg, vaultID, []recordForSeg{{0, at, "first"}})
	writeHeadSegment(t, home, secondSeg, vaultID, []recordForSeg{{0, at.Add(time.Minute), "second"}})
	firstRef := boundedRef(t, home, firstSeg, 0, 0, at)
	secondRef := boundedRef(t, home, secondSeg, 0, 0, at.Add(time.Minute))
	_, fresh := glcbBytes(t, home, vaultID, second, secondRef)

	other := t.TempDir()
	writeHeadSegment(t, other, secondSeg, vaultID, []recordForSeg{{0, at.Add(time.Minute), "other bytes"}})
	stale, staleDigest := glcbBytes(t, other, vaultID, second, secondRef)
	if staleDigest == fresh {
		t.Fatal("premise: the stale GLCB's bytes differ from a build of the manifest")
	}

	fsm := vaultctlfsm.New()
	apply := fsmApply(t, fsm)
	planManifest(apply, first, firstRef, 1, at, true)
	planManifest(apply, second, secondRef, 1, at.Add(time.Minute), true)
	ts := at.Add(2 * time.Minute)
	apply(vaultctlfsm.MarshalSealChunk(second, ts, 1, 100, at.Add(time.Minute), at.Add(time.Minute), at.Add(time.Minute), true, ts, fresh))
	mgr := registerLeader(t, vaultID, home, fsm)
	glcbPath := plantGLCB(t, filepath.Join(home, "chunks"), second, stale)

	for pass := range 2 {
		if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
			t.Fatalf("BuildOnce %d: %v", pass, err)
		}
	}
	if m := fsm.SealedManifest(); m != nil {
		t.Fatalf("seal queue still holds %s", m.ChunkID)
	}
	requireSealedWithDigest(t, fsm, second, glcbPath, fresh)
	requireQuarantinedMismatched(t, glcbPath, stale)
}

func TestRestartRecoveryAdoptsOnlyAGLCBBuiltFromTheManifest(t *testing.T) {
	t.Parallel()
	plan := newStalePlan(t)

	t.Run("a GLCB built from the manifest is sealed", func(t *testing.T) {
		t.Parallel()
		p := plan
		built, digest := glcbBytes(t, p.home, p.vaultID, p.chunkID, p.ref)
		fsm := vaultctlfsm.New()
		planManifest(fsmApply(t, fsm), p.chunkID, p.ref, 2, p.at, true)
		home := t.TempDir()
		glcbPath := plantGLCB(t, filepath.Join(home, "chunks"), p.chunkID, built)
		mgr := registerLeader(t, p.vaultID, home, fsm)

		if err := mgr.RecoverOnce(t.Context(), p.vaultID); err != nil {
			t.Fatalf("RecoverOnce: %v", err)
		}
		requireSealedWithDigest(t, fsm, p.chunkID, glcbPath, digest)
	})

	for name, stale := range mismatchedGLCBs(t, plan) {
		t.Run(name+" is not sealed; the build pass builds the chunk", func(t *testing.T) {
			t.Parallel()
			p := plan
			p.home = t.TempDir()
			writeHeadSegment(t, p.home, p.segID, p.vaultID, []recordForSeg{{0, p.at, "one"}, {1, p.at.Add(time.Second), "two"}})
			_, fresh := glcbBytes(t, p.home, p.vaultID, p.chunkID, p.ref)
			fsm := vaultctlfsm.New()
			planManifest(fsmApply(t, fsm), p.chunkID, p.ref, 2, p.at, true)
			glcbPath := plantGLCB(t, filepath.Join(p.home, "chunks"), p.chunkID, stale)
			mgr := registerLeader(t, p.vaultID, p.home, fsm)

			if err := mgr.RecoverOnce(t.Context(), p.vaultID); err != nil {
				t.Fatalf("RecoverOnce: %v", err)
			}
			if e := fsm.Get(p.chunkID); e != nil && e.IsSealed() {
				t.Fatalf("recovery sealed a mismatched GLCB: %+v", e)
			}
			if _, err := os.Stat(glcbPath); !os.IsNotExist(err) {
				t.Fatalf("the mismatched GLCB is still on the canonical path (stat=%v)", err)
			}
			requireQuarantinedMismatched(t, glcbPath, stale)

			if err := mgr.BuildOnce(t.Context(), p.vaultID); err != nil {
				t.Fatalf("BuildOnce: %v", err)
			}
			requireSealedWithDigest(t, fsm, p.chunkID, glcbPath, fresh)
		})
	}
}

// Recovery also finds GLCBs of chunks that are not the seal-queue head. A chunk
// queued behind the head is checked against its own manifest. The open
// chunk's GLCB is left where it is: the open manifest still grows, and right
// after a restart this node's FSM can lag the build it made before stopping.
// The build pass judges that GLCB once the chunk's seal applies.
func TestRestartRecoveryChecksChunksBehindTheHeadAgainstTheirManifests(t *testing.T) {
	t.Parallel()
	plan := newStalePlan(t)
	stale := mismatchedGLCBs(t, plan)["another record count"]

	t.Run("the open chunk's build, ahead of this FSM, is adopted once it seals", func(t *testing.T) {
		t.Parallel()
		p := plan
		home := t.TempDir()
		segA, segB := glid.New(), glid.New()
		writeHeadSegment(t, home, segA, p.vaultID, []recordForSeg{{0, p.at, "first"}})
		writeHeadSegment(t, home, segB, p.vaultID, []recordForSeg{{0, p.at.Add(time.Second), "second"}})
		refA := boundedRef(t, home, segA, 0, 0, p.at)
		refB := boundedRef(t, home, segB, 0, 0, p.at)
		built, digest := glcbBytes(t, home, p.vaultID, p.chunkID, refA, refB)

		// This FSM has applied the manifest's first ref only; the GLCB is a
		// build of the whole manifest.
		fsm := vaultctlfsm.New()
		apply := fsmApply(t, fsm)
		publish := func(segID glid.GLID) {
			apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
				SegmentID: segID, RecordCount: 1, ByteSize: 1, FirstIngestTS: p.at, LastIngestTS: p.at, Checksum: 1, PublishedAt: p.at,
			}))
		}
		publish(segA)
		apply(vaultctlfsm.MarshalOpenChunkManifest(p.chunkID, p.at))
		apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(p.chunkID, refA))
		glcbPath := plantGLCB(t, filepath.Join(home, "chunks"), p.chunkID, built)
		mgr := registerLeader(t, p.vaultID, home, fsm)

		if err := mgr.RecoverOnce(t.Context(), p.vaultID); err != nil {
			t.Fatalf("RecoverOnce: %v", err)
		}
		if _, err := os.Stat(glcbPath + chunking.GLCBMismatchedSuffix); !os.IsNotExist(err) {
			t.Fatalf("recovery quarantined the open chunk's build against a manifest this FSM has only partly applied (stat=%v)", err)
		}
		if got := verifiedDigest(t, glcbPath); got != digest {
			t.Fatal("recovery replaced the open chunk's GLCB")
		}
		if e := fsm.Get(p.chunkID); e != nil && e.IsSealed() {
			t.Fatalf("recovery sealed the open chunk: %+v", e)
		}

		publish(segB)
		apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(p.chunkID, refB))
		apply(vaultctlfsm.MarshalSealOpenChunkManifest(p.chunkID, p.at.Add(time.Minute)))
		if err := mgr.BuildOnce(t.Context(), p.vaultID); err != nil {
			t.Fatalf("BuildOnce: %v", err)
		}
		requireSealedWithDigest(t, fsm, p.chunkID, glcbPath, digest)
		if _, err := os.Stat(glcbPath + chunking.GLCBMismatchedSuffix); !os.IsNotExist(err) {
			t.Fatalf("the build of the manifest was quarantined (stat=%v)", err)
		}
	})

	t.Run("a mismatched GLCB of the open chunk is quarantined once it seals", func(t *testing.T) {
		t.Parallel()
		p := plan
		p.home = t.TempDir()
		writeHeadSegment(t, p.home, p.segID, p.vaultID, []recordForSeg{{0, p.at, "one"}, {1, p.at.Add(time.Second), "two"}})
		_, fresh := glcbBytes(t, p.home, p.vaultID, p.chunkID, p.ref)
		fsm := vaultctlfsm.New()
		planManifest(fsmApply(t, fsm), p.chunkID, p.ref, 2, p.at, false)
		glcbPath := plantGLCB(t, filepath.Join(p.home, "chunks"), p.chunkID, stale)
		mgr := registerLeader(t, p.vaultID, p.home, fsm)

		if err := mgr.RecoverOnce(t.Context(), p.vaultID); err != nil {
			t.Fatalf("RecoverOnce: %v", err)
		}
		if e := fsm.Get(p.chunkID); e != nil && e.IsSealed() {
			t.Fatalf("recovery sealed a mismatched GLCB of the open chunk: %+v", e)
		}

		fsmApply(t, fsm)(vaultctlfsm.MarshalSealOpenChunkManifest(p.chunkID, p.at.Add(time.Minute)))
		if err := mgr.BuildOnce(t.Context(), p.vaultID); err != nil {
			t.Fatalf("BuildOnce: %v", err)
		}
		requireQuarantinedMismatched(t, glcbPath, stale)
		requireSealedWithDigest(t, fsm, p.chunkID, glcbPath, fresh)
	})

	t.Run("a chunk queued behind the head", func(t *testing.T) {
		t.Parallel()
		p := plan
		head := chunk.NewChunkID()
		headSeg := glid.New()
		home := t.TempDir()
		writeHeadSegment(t, home, headSeg, p.vaultID, []recordForSeg{{0, p.at, "head"}})
		fsm := vaultctlfsm.New()
		apply := fsmApply(t, fsm)
		planManifest(apply, head, boundedRef(t, home, headSeg, 0, 0, p.at), 1, p.at, true)
		planManifest(apply, p.chunkID, p.ref, 2, p.at, true)
		glcbPath := plantGLCB(t, filepath.Join(home, "chunks"), p.chunkID, stale)
		mgr := registerLeader(t, p.vaultID, home, fsm)

		if err := mgr.RecoverOnce(t.Context(), p.vaultID); err != nil {
			t.Fatalf("RecoverOnce: %v", err)
		}
		if e := fsm.Get(p.chunkID); e != nil && e.IsSealed() {
			t.Fatalf("recovery sealed a mismatched GLCB of a queued chunk: %+v", e)
		}
		requireQuarantinedMismatched(t, glcbPath, stale)
	})
}

// Four homes, each with its own FSM replica. The leader holds a GLCB for the
// chunk that is not a build of its manifest; the seal records a build of the
// manifest, so every home's own build is the agreed copy.
func TestAMismatchedGLCBOnTheLeaderDoesNotMakeOtherHomesDivergent(t *testing.T) {
	t.Parallel()
	const homes = 4
	const leader = 0
	p := newStalePlan(t)
	stale := mismatchedGLCBs(t, p)["another record count"]
	_, fresh := glcbBytes(t, p.home, p.vaultID, p.chunkID, p.ref)

	homeNodes := []string{"home-0", "home-1", "home-2", "home-3"}
	log := &replicatedLog{proposed: make(map[int][][]byte)}
	roots := make([]string, homes)
	mgrs := make([]*chunking.Manager, homes)
	for h := range homes {
		log.replicas = append(log.replicas, vaultctlfsm.New())
		roots[h] = t.TempDir()
		deliverSegment(t, p.home, roots[h], p.segID)
		mgrs[h] = chunking.New(chunking.Config{})
		isLeader := h == leader
		if err := mgrs[h].RegisterVault(p.vaultID, chunking.VaultConfig{
			RequiredHolders: func() ([]string, bool) { return homeNodes, true },
			VaultRoot:       roots[h],
			ChunkRoot:       filepath.Join(roots[h], "chunks"),
			FSM:             log.replicas[h],
			Locate:          chunking.HeadSegmentLocator{Root: roots[h]},
			Applier:         proposer{log: log, home: h},
			IsLeader:        func() bool { return isLeader },
		}); err != nil {
			t.Fatal(err)
		}
		mgr := mgrs[h]
		t.Cleanup(func() { mgr.UnregisterVault(p.vaultID) })
	}
	leaderGLCB := plantGLCB(t, filepath.Join(roots[leader], "chunks"), p.chunkID, stale)
	planManifest(func(data []byte) {
		if err := log.commit(leader, data); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}, p.chunkID, p.ref, 2, p.at, true)

	for h := range homes {
		if err := mgrs[h].BuildOnce(t.Context(), p.vaultID); err != nil {
			t.Fatalf("home %d BuildOnce: %v", h, err)
		}
	}
	requireSealedWithDigest(t, log.replicas[leader], p.chunkID, leaderGLCB, fresh)
	for h := range homes {
		e := log.replicas[h].Get(p.chunkID)
		if e == nil || !e.IsSealed() {
			t.Fatalf("home %d: chunk entry = %+v, want Sealed", h, e)
		}
		if !mgrs[h].CheckLandedGLCB(p.vaultID, *e) {
			t.Fatalf("home %d's build of the manifest is not the agreed copy", h)
		}
		glcbPath := chunking.ChunkGLCBPath(filepath.Join(roots[h], "chunks"), p.chunkID)
		if _, err := os.Stat(glcbPath + chunking.GLCBDivergentSuffix); !os.IsNotExist(err) {
			t.Fatalf("home %d quarantined its copy as divergent (stat=%v)", h, err)
		}
		if got := verifiedDigest(t, glcbPath); got != fresh {
			t.Fatalf("home %d copy digest %x, want %x", h, got[:8], fresh[:8])
		}
	}
}
