package chunking_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type fsmApplier struct {
	fsm *vaultctlfsm.FSM
	log *[][]byte
}

func (a *fsmApplier) Apply(data []byte) error {
	cp := append([]byte(nil), data...)
	if a.log != nil {
		*a.log = append(*a.log, cp)
	}
	if err := a.fsm.Apply(&hraft.Log{Data: cp}); err != nil {
		return fmt.Errorf("apply: %v", err)
	}
	return nil
}

func writeCompletedSegment(t *testing.T, vaultRoot string, segID, vaultID glid.GLID, recs []recordForSeg) {
	t.Helper()
	if err := os.MkdirAll(paths.CompletedDir(vaultRoot), 0o750); err != nil {
		t.Fatal(err)
	}
	records := make([]record.Record, len(recs))
	for i, r := range recs {
		records[i] = makeRecordForSeg(segID, r.seq, r.ts, r.raw)
	}
	src := writeSegment(t, segID, vaultID, records)
	dest := paths.CompletedSegment(vaultRoot, segID)
	if err := os.Rename(src, dest); err != nil {
		t.Fatal(err)
	}
}

func publishSegment(t *testing.T, fsm *vaultctlfsm.FSM, segID glid.GLID, pubAt time.Time, recordCount uint32, firstTS, lastTS time.Time) {
	t.Helper()
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID:     segID,
		RecordCount:   recordCount,
		ByteSize:      1024,
		FirstIngestTS: firstTS,
		LastIngestTS:  lastTS,
		Checksum:      1,
		OriginNodeID:  "origin",
		PublishedAt:   pubAt,
	}))
}

func TestLeaderPlannerOpensAndAddsRefOnPublish(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		publishSegment(t, fsm, segID, pubAt, 2, base, base.Add(time.Second))
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	if err := mgr.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}

	open := fsm.OpenChunk()
	if open == nil {
		t.Fatal("expected open manifest")
	}
	if open.ChunkID != chunkID {
		t.Fatalf("chunk ID = %s, want %s", open.ChunkID, chunkID)
	}
	if len(open.Refs) != 1 {
		t.Fatalf("refs = %d, want 1", len(open.Refs))
	}
	if open.Refs[0].FirstRecordNumber != 0 || open.Refs[0].LastRecordNumber != 1 {
		t.Fatalf("ref = [%d,%d], want [0,1]", open.Refs[0].FirstRecordNumber, open.Refs[0].LastRecordNumber)
	}
}

func planUntilSealed(t *testing.T, mgr *chunking.Manager, vaultID glid.GLID, fsm *vaultctlfsm.FSM) *vaultctlfsm.OpenChunkManifest {
	t.Helper()
	ctx := t.Context()
	for step := 0; step < 16; step++ {
		if sealed := fsm.SealedManifest(); sealed != nil {
			return sealed
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce step %d: %v", step, err)
		}
	}
	t.Fatal("expected sealed manifest after planner catch-up")
	return nil
}

func TestLeaderPlannerFillsOpenWhileSealedManifestPending(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})

	chunkA := chunk.NewChunkID()
	chunkB := chunk.NewChunkID()
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkA, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkA, base.Add(30*time.Second)))
	if pending := fsm.SealedManifest(); pending == nil || pending.ChunkID != chunkA {
		t.Fatalf("sealed pending = %+v, want %s", pending, chunkA)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkB },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, pubAt, 2, base, base.Add(time.Second))
	ctx := t.Context()
	for step := range 8 {
		if open := fsm.OpenChunk(); open != nil && len(open.Refs) > 0 {
			if pending := fsm.SealedManifest(); pending == nil || pending.ChunkID != chunkA {
				t.Fatalf("step %d: sealed pending = %+v, want %s", step, pending, chunkA)
			}
			if open.ChunkID != chunkB {
				t.Fatalf("open chunk = %s, want %s", open.ChunkID, chunkB)
			}
			return
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce step %d: %v", step, err)
		}
	}
	t.Fatal("expected open manifest refs while sealed manifest pending")
}

func TestLeaderPlannerRotatesWhileSealedManifestPending(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
		{2, base.Add(2 * time.Second), "c"},
		{3, base.Add(3 * time.Second), "d"},
	})

	chunkA := chunk.NewChunkID()
	chunkB := chunk.NewChunkID()
	chunkC := chunk.NewChunkID()
	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkA, base))
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkA, base.Add(30*time.Second)))

	mgr := chunking.New(chunking.Config{})
	nextChunk := chunkB
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID: func() chunk.ChunkID {
			id := nextChunk
			if id == chunkB {
				nextChunk = chunkC
			}
			return id
		},
		Policy: chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, pubAt, 4, base, base.Add(3*time.Second))
	ctx := t.Context()
	for step := range 16 {
		if fsm.SealedManifestCount() >= 2 {
			open := fsm.OpenChunk()
			if open != nil && open.ChunkID == chunkC {
				return
			}
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce step %d: %v", step, err)
		}
	}
	t.Fatalf("sealed queue=%d open=%+v, want chunk B sealed and chunk C open",
		fsm.SealedManifestCount(), fsm.OpenChunk())
}

func TestLeaderPlannerRotatesAtMaxRecords(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
		{2, base.Add(2 * time.Second), "c"},
		{3, base.Add(3 * time.Second), "d"},
	})

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, pubAt, 4, base, base.Add(3*time.Second))
	sealed := planUntilSealed(t, mgr, vaultID, fsm)
	if sealed.TotalRecords != 2 {
		t.Fatalf("sealed records = %d, want 2", sealed.TotalRecords)
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("open manifest must clear on rotate")
	}
}

// TestLeaderPlannerRotatesAtMaxAgeWhenCaughtUp: MaxAge is a bound on how
// long a manifest stays OPEN, anchored at open-time wall clock — never at
// segment PublishedAt. A manifest opened over a lagging backlog (published
// hours ago) must NOT rotate at its first evaluation; it rotates once the
// clock advances MaxAge past the open (a PublishedAt anchor makes every
// backlog manifest born expired, flooding the seal queue with ~30K-record
// chunks).
func TestLeaderPlannerRotatesAtMaxAgeWhenCaughtUp(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	firstWrite := base.Add(10 * time.Minute)
	openNow := firstWrite.Add(2 * time.Hour) // planner runs 2h behind the publish
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, firstWrite, "a"},
		{1, firstWrite.Add(time.Second), "b"},
	})

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	now := openNow
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxAge: time.Hour},
		Now:             func() time.Time { return now },
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, pubAt, 2, firstWrite, firstWrite.Add(time.Second))
	ctx := t.Context()

	// Phase 1 — backlog: the segment published 2h ago must not make the
	// fresh manifest rotate. Plan to a fixpoint at the open-time clock.
	for step := 0; step < 8; step++ {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce step %d: %v", step, err)
		}
	}
	if sealed := fsm.SealedManifest(); sealed != nil {
		t.Fatalf("manifest sealed at open-time eval (SealedAt %v) — max-age anchored on PublishedAt, not open time", sealed.SealedAt)
	}
	open := fsm.OpenChunk()
	if open == nil {
		t.Fatal("expected open manifest holding the backlog refs")
	}
	if !open.OpenedAt.Equal(openNow) {
		t.Fatalf("OpenedAt = %v, want open-time wall clock %v (not segment PublishedAt %v)", open.OpenedAt, openNow, pubAt)
	}

	// Phase 2 — the clock passes MaxAge since open: now it rotates.
	now = openNow.Add(time.Hour + time.Second)
	for step := 0; step < 8; step++ {
		if fsm.SealedManifest() != nil {
			break
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce (aged) step %d: %v", step, err)
		}
	}
	sealed := fsm.SealedManifest()
	if sealed == nil {
		t.Fatal("expected sealed manifest once MaxAge elapsed since open")
	}
	if !sealed.SealedAt.Equal(now) {
		t.Fatalf("SealedAt = %v, want wall-clock %v", sealed.SealedAt, now)
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("open manifest must clear on age rotate")
	}
}

func TestLeaderPlannerDoesNotOpenWithoutLocalSegment(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	// Registry entry without a local completed segment file — planner must not
	// open an empty manifest while waiting for collection.
	publishSegment(t, fsm, segID, base, 2, base, base.Add(time.Second))
	if err := mgr.PlanOnce(t.Context(), vaultID); err != nil {
		t.Fatal(err)
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("must not open manifest before a segment is plannable locally")
	}
	if fsm.Get(chunkID) != nil {
		t.Fatal("must not create phantom chunk entry")
	}
}

func TestLeaderPlannerDiscardsStalledEmptyOpen(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	evalNow := base.Add(2 * time.Minute)
	chunkID := chunk.NewChunkID()
	vaultID := glid.New()
	vaultRoot := t.TempDir()

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkID, base))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxAge: time.Minute},
		Now:             func() time.Time { return evalNow },
	}); err != nil {
		t.Fatal(err)
	}

	if err := mgr.PlanOnce(t.Context(), vaultID); err != nil {
		t.Fatal(err)
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("stalled empty open manifest must be discarded, not left open")
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("empty manifest must not enter sealed pending state")
	}
	if fsm.Get(chunkID) != nil {
		t.Fatal("phantom chunk entry must be removed")
	}
}

func TestLeaderPlannerFollowerDoesNotPropose(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{{0, base, "a"}})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return false },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 10},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, base, 1, base, base)
	if err := mgr.PlanOnce(t.Context(), vaultID); err != nil {
		t.Fatal(err)
	}
	if fsm.OpenChunk() != nil {
		t.Fatal("follower must not open manifest")
	}
}

func TestLeaderPlannerReplicatedManifestSequence(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segA := glid.New()
	segB := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segA, vaultID, []recordForSeg{{0, base, "a"}})
	writeCompletedSegment(t, vaultRoot, segB, vaultID, []recordForSeg{{0, base.Add(time.Second), "b"}})

	fsmLeader := vaultctlfsm.New()
	fsmFollower := vaultctlfsm.New()
	var wireLog [][]byte
	chunkID := chunk.NewChunkID()

	applier := &fsmApplier{fsm: fsmLeader, log: &wireLog}

	publishSegment(t, fsmLeader, segA, pubAt, 1, base, base)
	publishSegment(t, fsmLeader, segB, pubAt.Add(time.Second), 1, base.Add(time.Second), base.Add(time.Second))
	applyChunkCmd(t, fsmFollower, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segA, RecordCount: 1, ByteSize: 1024, FirstIngestTS: base, LastIngestTS: base,
		PublishedAt: pubAt, OriginNodeID: "origin", Checksum: 1,
	}))
	applyChunkCmd(t, fsmFollower, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segB, RecordCount: 1, ByteSize: 1024,
		FirstIngestTS: base.Add(time.Second), LastIngestTS: base.Add(time.Second),
		PublishedAt: pubAt.Add(time.Second), OriginNodeID: "origin", Checksum: 1,
	}))

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsmLeader,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := mgr.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}

	for _, data := range wireLog {
		if err := fsmFollower.Apply(&hraft.Log{Data: data}); err != nil {
			t.Fatalf("follower apply: %v", err)
		}
	}

	leaderOpen := fsmLeader.OpenChunk()
	followerOpen := fsmFollower.OpenChunk()
	if leaderOpen == nil || followerOpen == nil {
		t.Fatal("expected open manifest on both replicas")
	}
	if len(leaderOpen.Refs) != len(followerOpen.Refs) {
		t.Fatalf("leader refs = %d follower refs = %d", len(leaderOpen.Refs), len(followerOpen.Refs))
	}
	for i := range leaderOpen.Refs {
		l := leaderOpen.Refs[i]
		f := followerOpen.Refs[i]
		if l.SegmentID != f.SegmentID || l.FirstRecordNumber != f.FirstRecordNumber || l.LastRecordNumber != f.LastRecordNumber {
			t.Fatalf("ref %d diverged: leader %+v follower %+v", i, l, f)
		}
	}
}

func planUntilOpenRecords(t *testing.T, mgr *chunking.Manager, vaultID glid.GLID, fsm *vaultctlfsm.FSM, wantRecords uint64) {
	t.Helper()
	ctx := t.Context()
	for step := 0; step < 16; step++ {
		open := fsm.OpenChunk()
		if open != nil && open.TotalRecords == wantRecords {
			return
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce step %d: %v", step, err)
		}
	}
	t.Fatalf("expected open manifest with %d records", wantRecords)
}

func TestLeaderPlannerFailoverContinuesManifest(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
		{2, base.Add(2 * time.Second), "c"},
		{3, base.Add(3 * time.Second), "d"},
	})

	fsmA := vaultctlfsm.New()
	fsmB := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	var leader atomic.Bool
	leader.Store(true)

	applierA := &fsmApplier{fsm: fsmA}
	mgrA := chunking.New(chunking.Config{})
	if err := mgrA.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsmA,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applierA,
		IsLeader:        leader.Load,
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 100},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsmA, segID, pubAt, 4, base, base.Add(3*time.Second))
	applyChunkCmd(t, fsmB, vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: segID, RecordCount: 4, ByteSize: 1024,
		FirstIngestTS: base, LastIngestTS: base.Add(3 * time.Second),
		PublishedAt: pubAt, OriginNodeID: "origin", Checksum: 1,
	}))
	planUntilOpenRecords(t, mgrA, vaultID, fsmA, 4)

	fsmB.RestoreProto(fsmA.SnapshotProto())
	leader.Store(false)

	applierB := &fsmApplier{fsm: fsmB}
	mgrB := chunking.New(chunking.Config{})
	if err := mgrB.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsmB,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applierB,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}

	sealed := planUntilSealed(t, mgrB, vaultID, fsmB)
	if sealed.TotalRecords != 4 {
		t.Fatalf("sealed records = %d, want 4", sealed.TotalRecords)
	}
}

// TestLeaderPlannerSecondManifestSkipsConsumedSegments is the regression
// test for the duplicate-chunk bug: after a manifest seals and its chunk
// builds (CmdSealChunk clears the pending manifest), the next manifest must
// resume from the persisted segmentResume positions — NOT re-add records the
// sealed chunk already consumed. Before the fix the planner only consulted
// resume positions for refs in the current open manifest, so manifest 2
// restarted every fully-consumed segment from record 0.
func TestLeaderPlannerSecondManifestSkipsConsumedSegments(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segA := glid.New()
	segB := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segA, vaultID, []recordForSeg{
		{0, base, "a0"},
		{1, base.Add(time.Second), "a1"},
	})
	writeCompletedSegment(t, vaultRoot, segB, vaultID, []recordForSeg{
		{0, base.Add(2 * time.Second), "b0"},
		{1, base.Add(3 * time.Second), "b1"},
	})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segA, pubAt, 2, base, base.Add(time.Second))
	publishSegment(t, fsm, segB, pubAt.Add(time.Second), 2, base.Add(2*time.Second), base.Add(3*time.Second))

	first := planUntilSealed(t, mgr, vaultID, fsm)
	if len(first.Refs) != 1 || first.Refs[0].SegmentID != segA {
		t.Fatalf("first manifest refs = %+v, want one ref to segA", first.Refs)
	}

	// Build completes: CmdSealChunk clears the pending sealed manifest.
	applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(
		first.ChunkID, base.Add(time.Minute), 2, 1024,
		base, base.Add(time.Second), base.Add(time.Second), true,
		base.Add(time.Minute),
	))

	second := planUntilSealed(t, mgr, vaultID, fsm)
	if second.ChunkID == first.ChunkID {
		t.Fatal("second manifest reused first chunk ID")
	}
	if len(second.Refs) != 1 || second.Refs[0].SegmentID != segB {
		t.Fatalf("second manifest refs = %+v, want one ref to segB only", second.Refs)
	}
	if second.Refs[0].FirstRecordNumber != 0 || second.Refs[0].LastRecordNumber != 1 {
		t.Fatalf("second manifest ref = [%d,%d], want [0,1]",
			second.Refs[0].FirstRecordNumber, second.Refs[0].LastRecordNumber)
	}
}

// TestLazyPlannerPartialPathIndexesOneSegment ensures continuing a partial
// manifest ref opens only that segment's index, not the full registry.
func TestLazyPlannerPartialPathIndexesOneSegment(t *testing.T) {
	t.Parallel()
	const segmentCount = 50
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segA := glid.New()
	segIDs := make([]glid.GLID, 0, segmentCount)
	segIDs = append(segIDs, segA)
	for i := 1; i < segmentCount; i++ {
		segIDs = append(segIDs, glid.New())
	}
	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
			{0, ts, "a"},
			{1, ts.Add(time.Millisecond), "b"},
		})
	}

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	var indexOpens atomic.Int64
	indexOpener := func(path string) (*chunking.OrderedIndex, error) {
		indexOpens.Add(1)
		return chunking.BuildOrderedIndex(path)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 1000},
		IndexOpener:     indexOpener,
	}); err != nil {
		t.Fatal(err)
	}

	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, pubAt.Add(time.Duration(i)*time.Millisecond), 2, ts, ts.Add(time.Millisecond))
	}

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	open := fsm.OpenChunk()
	if open == nil || len(open.Refs) != 1 || open.Refs[0].SegmentID != segA {
		t.Fatalf("open refs = %+v, want one partial ref on segA", open)
	}

	indexOpens.Store(0)
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if got := indexOpens.Load(); got > 1 {
		t.Fatalf("partial planOnce index opens = %d, want at most 1", got)
	}
}

// TestPlannerOpensManifestAfterPickingSegment verifies the leader only opens a
// manifest once it has picked a plannable segment (one index open for k-way).
func TestPlannerOpensManifestAfterPickingSegment(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{{0, base, "a"}})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	var indexOpens atomic.Int64
	indexOpener := func(path string) (*chunking.OrderedIndex, error) {
		indexOpens.Add(1)
		return chunking.BuildOrderedIndex(path)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 1000},
		IndexOpener:     indexOpener,
	}); err != nil {
		t.Fatal(err)
	}
	publishSegment(t, fsm, segID, pubAt, 1, base, base)

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if got := indexOpens.Load(); got != 1 {
		t.Fatalf("first PlanOnce index opens = %d, want 1 (pick before open)", got)
	}
	if fsm.OpenChunk() == nil {
		t.Fatal("manifest must open once a local segment is plannable")
	}
}

// TestLoadSegmentViewsCachesIndexesAcrossPlanSteps guards the planner's
// segment index cache: once a segment index is warm, later partial-manifest
// steps must not reopen it.
func TestLoadSegmentViewsCachesIndexesAcrossPlanSteps(t *testing.T) {
	t.Parallel()
	const segmentCount = 20
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segIDs := make([]glid.GLID, segmentCount)
	for i := range segIDs {
		segIDs[i] = glid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segIDs[i], vaultID, []recordForSeg{{0, ts, fmt.Sprintf("s%d", i)}})
	}

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	var indexOpens atomic.Int64
	indexOpener := func(path string) (*chunking.OrderedIndex, error) {
		indexOpens.Add(1)
		return chunking.BuildOrderedIndex(path)
	}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 1000},
		IndexOpener:     indexOpener,
	}); err != nil {
		t.Fatal(err)
	}

	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, pubAt.Add(time.Duration(i)*time.Millisecond), 1, ts, ts)
	}

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	open := fsm.OpenChunk()
	if open == nil || len(open.Refs) != 1 {
		t.Fatalf("open refs = %+v, want one ref", open)
	}

	indexOpens.Store(0)
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if got := indexOpens.Load(); got != 0 {
		t.Fatalf("warm-cache partial PlanOnce index opens = %d, want 0", got)
	}
}

// TestLoadSegmentViewsIndexesAllActiveSegments ensures the planner sees every
// non-exhausted registry segment, not an arbitrary prefix cap.
func TestLoadSegmentViewsIndexesAllActiveSegments(t *testing.T) {
	t.Parallel()
	const segmentCount = 40
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segIDs := make([]glid.GLID, segmentCount)
	for i := range segIDs {
		segIDs[i] = glid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segIDs[i], vaultID, []recordForSeg{{0, ts, fmt.Sprintf("s%d", i)}})
	}

	fsm := vaultctlfsm.New()
	chunkID := chunk.NewChunkID()
	applier := &fsmApplier{fsm: fsm}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		NewChunkID:      func() chunk.ChunkID { return chunkID },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 1000},
	}); err != nil {
		t.Fatal(err)
	}

	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, pubAt.Add(time.Duration(i)*time.Millisecond), 1, ts, ts)
	}

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	open := fsm.OpenChunk()
	if open == nil {
		t.Fatal("expected open manifest")
	}
	if len(open.Refs) != 1 {
		t.Fatalf("refs = %d, want 1", len(open.Refs))
	}
	if open.Refs[0].SegmentID != segIDs[0] {
		t.Fatalf("first ref segment = %s, want earliest EventID segment %s", open.Refs[0].SegmentID, segIDs[0])
	}
}

// TestPlannerCatchUpBatchesRefsInOneApply verifies planCatchUp amortizes many
// segment refs into a single vault-ctl apply.
func TestPlannerCatchUpBatchesRefsInOneApply(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	const segmentCount = 12
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segIDs := make([]glid.GLID, segmentCount)
	for i := range segIDs {
		segIDs[i] = glid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segIDs[i], vaultID, []recordForSeg{{0, ts, "x"}})
	}

	fsm := vaultctlfsm.New()
	var applyLog [][]byte
	applier := &fsmApplier{fsm: fsm, log: &applyLog}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 10_000},
	}); err != nil {
		t.Fatal(err)
	}
	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, pubAt.Add(time.Duration(i)*time.Millisecond), 1, ts, ts)
	}

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	nBefore := len(applyLog)
	if err := mgr.PlanCatchUp(ctx, vaultID); err != nil {
		t.Fatalf("PlanCatchUp: %v", err)
	}
	catchUpApplies := applyLog[nBefore:]
	if len(catchUpApplies) != 1 {
		t.Fatalf("catch-up applies = %d, want 1 batched apply", len(catchUpApplies))
	}
	var cmd gastrologv1.VaultCtlCommand
	if err := proto.Unmarshal(catchUpApplies[0], &cmd); err != nil {
		t.Fatalf("decode apply: %v", err)
	}
	batch := cmd.GetAddOpenChunkSegmentRefs()
	if batch == nil {
		t.Fatalf("apply command = %T, want AddOpenChunkSegmentRefs", cmd.GetCommand())
	}
	if got := len(batch.GetRefs()); got != segmentCount {
		t.Fatalf("batched refs = %d, want %d", got, segmentCount)
	}
	open := fsm.OpenChunk()
	if open == nil || len(open.Refs) != segmentCount {
		t.Fatalf("open refs = %d, want %d", len(open.Refs), segmentCount)
	}
}

// TestPlannerCatchUpAppliesRefsBeforeSealAtMaxRecords guards against sealing an
// empty manifest when a batched catch-up step fills MaxRecords in simulation.
func TestPlannerCatchUpAppliesRefsBeforeSealAtMaxRecords(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
		{2, base.Add(2 * time.Second), "c"},
		{3, base.Add(3 * time.Second), "d"},
	})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 2},
	}); err != nil {
		t.Fatal(err)
	}
	publishSegment(t, fsm, segID, pubAt, 4, base, base.Add(3*time.Second))

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if err := mgr.PlanCatchUp(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	sealed := fsm.SealedManifest()
	if sealed == nil {
		t.Fatal("expected sealed manifest")
	}
	if sealed.TotalRecords != 2 {
		t.Fatalf("sealed records = %d, want 2", sealed.TotalRecords)
	}
}

// TestLeaderPlannerGatesSingleCopySegments: the planner never references a
// segment with fewer than min(2, placement) holders — a single-copy segment
// in a manifest wedges the vault's serial seal queue if that copy's node
// dies. Holders accrue via receipts; planning follows.
func TestLeaderPlannerGatesSingleCopySegments(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	chunkID := chunk.NewChunkID()
	now := base.Add(10 * time.Minute)

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsm,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applier,
		IsLeader:   func() bool { return true },
		NewChunkID: func() chunk.ChunkID { return chunkID },
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 100},
		Now:        func() time.Time { return now },
		RequiredHolders: func() ([]string, bool) {
			return []string{"node-origin", "node-home", "node-3", "node-4"}, true
		},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, base.Add(time.Minute), 2, base, base.Add(time.Second))
	ctx := t.Context()

	// Zero holders: publish precedes replication; nothing plannable.
	for range 4 {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
	}
	if open := fsm.OpenChunk(); open != nil {
		t.Fatalf("planner referenced a zero-holder segment: %+v", open.Refs)
	}

	// One holder (the origin's own receipt): still inside the window.
	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, "node-origin")); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
	}
	if open := fsm.OpenChunk(); open != nil {
		t.Fatalf("planner referenced a single-copy segment: %+v", open.Refs)
	}

	// Second holder receipt: replication window closed — plan it.
	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, "node-home")); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if open := fsm.OpenChunk(); open != nil && len(open.Refs) > 0 {
			break
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
	}
	open := fsm.OpenChunk()
	if open == nil || len(open.Refs) != 1 || open.Refs[0].SegmentID != segID {
		t.Fatalf("segment not planned after reaching 2 holders: %+v", open)
	}
}

// TestLeaderPlannerUnderReplicatedAlert: segments gated past the grace
// period raise the under-replicated alert; reaching the holder minimum
// clears it (a stuck replication window is a visible registry condition, not
// a silent planning stall).
func TestLeaderPlannerUnderReplicatedAlert(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{{0, base, "a"}})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	sink := &recordingAlertSink{}
	now := base

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsm,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applier,
		IsLeader:   func() bool { return true },
		NewChunkID: chunk.NewChunkID,
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 100},
		Now:        func() time.Time { return now },
		Alerts:     sink,
		RequiredHolders: func() ([]string, bool) {
			return []string{"node-origin", "node-home", "node-3"}, true
		},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, base, 1, base, base)
	ctx := t.Context()

	// Freshly published: gated but inside the grace period — no alert.
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	if active, _ := sink.snapshot(); len(active) != 0 {
		t.Fatalf("alert raised inside grace period: %v", active)
	}

	// Still zero holders past the grace period: alert.
	now = base.Add(5 * time.Minute)
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	active, _ := sink.snapshot()
	if len(active) != 1 {
		t.Fatalf("expected under-replicated alert, got %v", active)
	}

	// Holder minimum reached: cleared.
	for _, n := range []string{"node-origin", "node-home"} {
		if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, n)); err != nil {
			t.Fatal(err)
		}
	}
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatal(err)
	}
	active, cleared := sink.snapshot()
	if len(active) != 0 || cleared == 0 {
		t.Fatalf("alert not cleared after holders reached minimum: active=%v cleared=%d", active, cleared)
	}
}
