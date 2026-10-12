package chunking_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
	"gastrolog/internal/waittest"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// chunkFixture is one chunk over one single-record segment.
type chunkFixture struct {
	id  chunk.ChunkID
	seg glid.GLID
	at  time.Time
}

func newChunkFixtures(n int, base time.Time) []chunkFixture {
	out := make([]chunkFixture, n)
	for i := range out {
		out[i] = chunkFixture{id: chunk.NewChunkID(), seg: glid.New(), at: base.Add(time.Duration(i) * time.Minute)}
	}
	return out
}

// writeFixtureSegments writes every fixture's segment into root's head/.
func writeFixtureSegments(t *testing.T, root string, vaultID glid.GLID, fixtures []chunkFixture) {
	t.Helper()
	for _, f := range fixtures {
		writeHeadSegment(t, root, f.seg, vaultID, []recordForSeg{{0, f.at, "record at " + f.at.Format(time.TimeOnly)}})
	}
}

// deliverSegment copies a segment from one home's head/ into another's, the
// way collection lands a pulled segment.
func deliverSegment(t *testing.T, from, to string, segID glid.GLID) {
	t.Helper()
	data, err := os.ReadFile(paths.HeadSegment(from, segID))
	if err != nil {
		t.Fatalf("read segment %s: %v", segID, err)
	}
	if err := paths.EnsureHeadDir(to); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.HeadSegment(to, segID), data, 0o600); err != nil {
		t.Fatalf("write segment %s: %v", segID, err)
	}
}

// planAndSealManifest applies the leader planner's commands for one chunk:
// publish its segment, open the manifest, add the ref, seal the manifest.
func planAndSealManifest(t *testing.T, apply func([]byte), f chunkFixture) {
	t.Helper()
	apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
		SegmentID: f.seg, RecordCount: 1, ByteSize: 1, FirstIngestTS: f.at, LastIngestTS: f.at, Checksum: 1, PublishedAt: f.at,
	}))
	apply(vaultctlfsm.MarshalOpenChunkManifest(f.id, f.at))
	apply(vaultctlfsm.MarshalAddOpenChunkSegmentRef(f.id, vaultctlfsm.OpenChunkSegmentRef{
		SegmentID: f.seg, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1024, RefAddedAt: f.at,
	}))
	apply(vaultctlfsm.MarshalSealOpenChunkManifest(f.id, f.at.Add(time.Second)))
}

// commitSeal applies the CmdSealChunk a leader that built the chunk on its
// own home commits.
func commitSeal(apply func([]byte), f chunkFixture) {
	ts := f.at.Add(time.Second)
	apply(vaultctlfsm.MarshalSealChunk(f.id, ts, 1, 100, f.at, f.at, f.at, true, ts, [32]byte{}))
}

func fsmApply(t *testing.T, fsm *vaultctlfsm.FSM) func([]byte) {
	return func(data []byte) { applyChunkCmd(t, fsm, data) }
}

// builtLog records OnBuilt in firing order.
type builtLog struct {
	mu  sync.Mutex
	ids []chunk.ChunkID
}

func (b *builtLog) add(id chunk.ChunkID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ids = append(b.ids, id)
}

func (b *builtLog) snapshot() []chunk.ChunkID {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.ids)
}

func fixtureIDs(fixtures ...chunkFixture) []chunk.ChunkID {
	out := make([]chunk.ChunkID, len(fixtures))
	for i, f := range fixtures {
		out[i] = f.id
	}
	return out
}

func sealChunkCommands(t *testing.T, applied [][]byte) int {
	t.Helper()
	n := 0
	for _, data := range applied {
		var cmd gastrologv1.VaultCtlCommand
		if err := proto.Unmarshal(data, &cmd); err != nil {
			t.Fatalf("decode applied command: %v", err)
		}
		if cmd.GetSealChunk() != nil {
			n++
		}
	}
	return n
}

func chunksBuilt(mgr *chunking.Manager, vaultID glid.GLID) uint64 {
	for _, st := range mgr.StageStats() {
		if st.VaultID == vaultID {
			return st.ChunksBuilt
		}
	}
	return 0
}

// A follower home must build a GLCB for every sealed chunk, even when the
// vault-ctl leader seals and commits CmdSealChunk for chunk N, and seals
// chunk N+1, before the follower has the segments N references.
func TestFollowerBuildsEverySealedChunkWhenLeaderRunsAhead(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// followerPassBetween runs a follower build pass after CmdSealChunk(N)
		// commits but before N+1 is sealed, so N has already left the seal
		// queue when the follower last looked.
		followerPassBetween bool
	}{
		{name: "two manifests ahead in the FSM queue", followerPassBetween: false},
		{name: "build pass between N's seal and N+1's", followerPassBetween: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
			vaultID := glid.New()
			segA := glid.New()
			segB := glid.New()
			chunkN := chunk.NewChunkID()
			chunkN1 := chunk.NewChunkID()
			home := t.TempDir()
			chunkRoot := filepath.Join(home, "chunks")

			// The follower already holds N+1's segment; N's segment has not
			// been collected yet.
			writeHeadSegment(t, home, segB, vaultID, []recordForSeg{{0, base.Add(2 * time.Minute), "b"}})

			// One FSM stands in for the replicated vault-ctl state; commands
			// applied directly here are the leader's committed log entries.
			fsm := vaultctlfsm.New()
			publishSegForTest(t, fsm, segA, 1, base)
			publishSegForTest(t, fsm, segB, 1, base.Add(2*time.Minute))

			applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkN, base))
			applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkN, vaultctlfsm.OpenChunkSegmentRef{
				SegmentID: segA, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1024, RefAddedAt: base,
			}))

			var applied [][]byte
			mgr := chunking.New(chunking.Config{})
			if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
				RequiredHolders: chunking.NoRequiredHolders,
				VaultRoot:       home,
				ChunkRoot:       chunkRoot,
				FSM:             fsm,
				Locate:          chunking.HeadSegmentLocator{Root: home},
				Applier:         &recordingApplier{out: &applied},
				IsLeader:        func() bool { return false },
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

			// Leader seals N. The follower's build pass cannot build it yet.
			applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkN, base.Add(time.Minute)))
			if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
				t.Fatalf("follower BuildOnce while N's segment is missing: %v", err)
			}
			glcbN := chunking.ChunkGLCBPath(chunkRoot, chunkN)
			if _, err := os.Stat(glcbN); err == nil {
				t.Fatal("precondition: follower must not have built N before its segment arrives")
			}

			sealN := func() {
				// Leader built N on its own home and commits CmdSealChunk(N),
				// popping N from the sealed-manifest queue.
				ts := base.Add(time.Minute)
				applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealChunk(chunkN, ts, 1, 100, base, base, base, true, ts, [32]byte{}))
			}
			sealN1Manifest := func() {
				applyChunkCmd(t, fsm, vaultctlfsm.MarshalOpenChunkManifest(chunkN1, base.Add(2*time.Minute)))
				applyChunkCmd(t, fsm, vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkN1, vaultctlfsm.OpenChunkSegmentRef{
					SegmentID: segB, FirstRecordNumber: 0, LastRecordNumber: 0, SliceBytes: 1024, RefAddedAt: base.Add(2 * time.Minute),
				}))
				applyChunkCmd(t, fsm, vaultctlfsm.MarshalSealOpenChunkManifest(chunkN1, base.Add(3*time.Minute)))
			}

			if tc.followerPassBetween {
				sealN()
				if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
					t.Fatalf("follower BuildOnce after N sealed: %v", err)
				}
				sealN1Manifest()
			} else {
				// Rotation is not blocked by an unbuilt head: N+1 queues behind N.
				sealN1Manifest()
				sealN()
			}

			if e := fsm.Get(chunkN); e == nil || e.State != chunk.ChunkStateSealed {
				t.Fatalf("precondition: chunk N entry = %+v, want Sealed", e)
			}
			if head := fsm.SealedManifest(); head == nil || head.ChunkID != chunkN1 {
				t.Fatalf("precondition: sealed-manifest head = %+v, want N+1", head)
			}

			// N's segment now reaches the follower. Every subsequent build
			// pass has what it needs to build both N and N+1.
			writeHeadSegment(t, home, segA, vaultID, []recordForSeg{{0, base, "a"}})
			for i := range 3 {
				if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
					t.Fatalf("follower BuildOnce #%d: %v", i+1, err)
				}
			}

			if _, err := os.Stat(chunking.ChunkGLCBPath(chunkRoot, chunkN1)); err != nil {
				t.Fatalf("follower GLCB for N+1: %v", err)
			}
			if _, err := os.Stat(glcbN); err != nil {
				t.Fatalf("follower never built GLCB for sealed chunk N (%s): %v", chunkN, err)
			}
		})
	}
}

// A follower several sealed chunks behind builds each one as its segments
// arrive, in seal order, without one chunk's missing segment holding back
// another chunk or the seal-queue head.
func TestFollowerBuildsEverySealedChunkItFellBehindOn(t *testing.T) {
	t.Parallel()
	const behind = 4
	vaultID := glid.New()
	source := t.TempDir()
	home := t.TempDir()
	chunkRoot := filepath.Join(home, "chunks")
	fixtures := newChunkFixtures(behind+1, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))
	sealed, head := fixtures[:behind], fixtures[behind]
	writeFixtureSegments(t, source, vaultID, fixtures)

	fsm := vaultctlfsm.New()
	apply := fsmApply(t, fsm)
	for _, f := range sealed {
		planAndSealManifest(t, apply, f)
		commitSeal(apply, f)
	}
	planAndSealManifest(t, apply, head)
	deliverSegment(t, source, home, head.seg)

	var applied [][]byte
	built := &builtLog{}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       chunkRoot,
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &recordingApplier{out: &applied},
		IsLeader:        func() bool { return false },
		OnBuilt:         built.add,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	buildOnce := func(when string) {
		t.Helper()
		if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
			t.Fatalf("BuildOnce %s: %v", when, err)
		}
	}

	buildOnce("with only the seal-queue head's segment")
	if got, want := built.snapshot(), fixtureIDs(head); !slices.Equal(got, want) {
		t.Fatalf("built %v, want only the seal-queue head %v", got, want)
	}

	deliverSegment(t, source, home, sealed[2].seg)
	buildOnce("with one sealed chunk's segment")
	if got, want := built.snapshot(), fixtureIDs(head, sealed[2]); !slices.Equal(got, want) {
		t.Fatalf("built %v, want %v", got, want)
	}

	for _, f := range sealed {
		deliverSegment(t, source, home, f.seg)
	}
	buildOnce("with every segment")
	buildOnce("after every chunk is built")
	want := fixtureIDs(head, sealed[2], sealed[0], sealed[1], sealed[3])
	if got := built.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("built %v, want %v (each once, sealed chunks in seal order)", got, want)
	}
	for _, f := range fixtures {
		if _, err := os.Stat(chunking.ChunkGLCBPath(chunkRoot, f.id)); err != nil {
			t.Fatalf("GLCB for %s: %v", f.id, err)
		}
	}
	if n := chunksBuilt(mgr, vaultID); n != uint64(len(fixtures)) {
		t.Fatalf("chunks built = %d, want %d", n, len(fixtures))
	}
	if n := sealChunkCommands(t, applied); n != 0 {
		t.Fatalf("follower proposed %d CmdSealChunk, want 0", n)
	}
}

// The leader's flow is unchanged: it builds the seal-queue head, commits
// CmdSealChunk, and never rebuilds a chunk it already sealed even though the
// FSM keeps that chunk's manifest for other homes.
func TestLeaderSealsTheQueueHeadAndDoesNotRebuildSealedChunks(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	home := t.TempDir()
	chunkRoot := filepath.Join(home, "chunks")
	fixtures := newChunkFixtures(2, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))
	first, second := fixtures[0], fixtures[1]
	writeFixtureSegments(t, home, vaultID, fixtures)

	fsm := vaultctlfsm.New()
	apply := fsmApply(t, fsm)
	planAndSealManifest(t, apply, first)
	planAndSealManifest(t, apply, second)

	var applied [][]byte
	built := &builtLog{}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		// A second home that holds no segment yet keeps them unreleased, so
		// the sealed manifests stay in the FSM for it.
		RequiredHolders: func() ([]string, bool) { return []string{"leader", "follower"}, true },
		VaultRoot:       home,
		ChunkRoot:       chunkRoot,
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Applier:         &recordingApplier{out: &applied, fsm: fsm},
		IsLeader:        func() bool { return true },
		OnBuilt:         built.add,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce first head: %v", err)
	}
	if e := fsm.Get(first.id); e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("first chunk entry = %+v, want Sealed", e)
	}
	if fsm.SealedChunkManifest(first.id) == nil {
		t.Fatal("the FSM dropped the sealed chunk's manifest at seal; other homes cannot build it")
	}
	if headID, ok := fsm.SealedManifestHeadChunkID(); !ok || headID != second.id {
		t.Fatalf("seal-queue head = %s ok=%v, want %s", headID, ok, second.id)
	}
	firstInfo, err := os.Stat(chunking.ChunkGLCBPath(chunkRoot, first.id))
	if err != nil {
		t.Fatalf("first GLCB: %v", err)
	}

	for i := range 2 {
		if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
			t.Fatalf("BuildOnce #%d after the first seal: %v", i+2, err)
		}
	}
	if e := fsm.Get(second.id); e == nil || e.State != chunk.ChunkStateSealed {
		t.Fatalf("second chunk entry = %+v, want Sealed", e)
	}
	again, err := os.Stat(chunking.ChunkGLCBPath(chunkRoot, first.id))
	if err != nil {
		t.Fatalf("first GLCB after later passes: %v", err)
	}
	if !os.SameFile(firstInfo, again) {
		t.Fatal("the leader rebuilt a chunk it had already built and sealed")
	}
	if got, want := built.snapshot(), fixtureIDs(first, second); !slices.Equal(got, want) {
		t.Fatalf("built %v, want %v", got, want)
	}
	if n := sealChunkCommands(t, applied); n != 2 {
		t.Fatalf("leader committed %d CmdSealChunk, want 2", n)
	}
	if n := chunksBuilt(mgr, vaultID); n != 2 {
		t.Fatalf("chunks built = %d, want 2", n)
	}
}

// A home that restarts with sealed chunks it never built builds them from the
// FSM it restores — the manifests ride the vault-ctl snapshot.
func TestHomeRestartBuildsSealedChunksItHadNotBuilt(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	home := t.TempDir()
	chunkRoot := filepath.Join(home, "chunks")
	fixtures := newChunkFixtures(2, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))
	writeFixtureSegments(t, home, vaultID, fixtures)

	live := vaultctlfsm.New()
	apply := fsmApply(t, live)
	for _, f := range fixtures {
		planAndSealManifest(t, apply, f)
		commitSeal(apply, f)
	}

	snap, err := proto.Marshal(live.SnapshotProto())
	if err != nil {
		t.Fatal(err)
	}
	restored := vaultctlfsm.New()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	builtCh := make(chan chunk.ChunkID, len(fixtures))
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       chunkRoot,
		FSM:             restored,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		IsLeader:        func() bool { return false },
		OnBuilt:         func(id chunk.ChunkID) { builtCh <- id },
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run(ctx) }()

	var got []chunk.ChunkID
	for range fixtures {
		got = append(got, waittest.Recv(t, "restarted home builds a sealed chunk it lacks", builtCh,
			func() string { return stageProgress(mgr, vaultID) }))
	}
	cancel()
	if err := <-runErr; err != nil && err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}
	if want := fixtureIDs(fixtures...); !slices.Equal(got, want) {
		t.Fatalf("built %v, want %v in seal order", got, want)
	}
	for _, f := range fixtures {
		if _, err := os.Stat(chunking.ChunkGLCBPath(chunkRoot, f.id)); err != nil {
			t.Fatalf("GLCB for %s: %v", f.id, err)
		}
	}
}

// Once a sealed chunk's segment is released the chunk can no longer be built
// from segments; the home stops trying (the GLCB catch-up pull supplies it)
// and keeps building the chunks it can.
func TestReleasedSegmentStopsTheHomeWaitingToBuildTheChunk(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	source := t.TempDir()
	home := t.TempDir()
	chunkRoot := filepath.Join(home, "chunks")
	fixtures := newChunkFixtures(2, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))
	lost, next := fixtures[0], fixtures[1]
	writeFixtureSegments(t, source, vaultID, fixtures)

	fsm := vaultctlfsm.New()
	apply := fsmApply(t, fsm)
	planAndSealManifest(t, apply, lost)
	commitSeal(apply, lost)

	collector := &segmentCollectorRecorder{}
	built := &builtLog{}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       chunkRoot,
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		Collector:       collector,
		IsLeader:        func() bool { return false },
		OnBuilt:         built.add,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce while the segment is uncollected: %v", err)
	}
	if n := collector.calls.Load(); n != 1 || !slices.Equal(collector.ids, []glid.GLID{lost.seg}) {
		t.Fatalf("collection asked %d times for %v, want once for [%s]", n, collector.ids, lost.seg)
	}

	apply(vaultctlfsm.MarshalReleaseSegments([]glid.GLID{lost.seg}))
	planAndSealManifest(t, apply, next)
	deliverSegment(t, source, home, next.seg)

	for i := range 2 {
		if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
			t.Fatalf("BuildOnce #%d after the release: %v", i+1, err)
		}
	}
	if n := collector.calls.Load(); n != 1 {
		t.Fatalf("collection asked %d times, want no request after the segment was released", n)
	}
	if got, want := built.snapshot(), fixtureIDs(next); !slices.Equal(got, want) {
		t.Fatalf("built %v, want %v", got, want)
	}
	if _, err := os.Stat(chunking.ChunkGLCBPath(chunkRoot, lost.id)); !os.IsNotExist(err) {
		t.Fatalf("GLCB for the chunk whose segment was released: err=%v, want not built", err)
	}
}

// A cloud-backed chunk's bytes live in the blob store; a home without a local
// GLCB serves it from there and does not rebuild it from segments.
func TestHomeDoesNotRebuildACloudBackedSealedChunk(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	home := t.TempDir()
	chunkRoot := filepath.Join(home, "chunks")
	fixtures := newChunkFixtures(1, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))
	f := fixtures[0]
	writeFixtureSegments(t, home, vaultID, fixtures)

	fsm := vaultctlfsm.New()
	apply := fsmApply(t, fsm)
	planAndSealManifest(t, apply, f)
	commitSeal(apply, f)
	apply(vaultctlfsm.MarshalUploadChunk(f.id, 100, 0, 0, 0, 0, [32]byte{1}, glid.New(), 0))
	if e := fsm.Get(f.id); e == nil || !e.CloudBacked {
		t.Fatalf("precondition: chunk entry = %+v, want cloud-backed", e)
	}

	built := &builtLog{}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       home,
		ChunkRoot:       chunkRoot,
		FSM:             fsm,
		Locate:          chunking.HeadSegmentLocator{Root: home},
		IsLeader:        func() bool { return false },
		OnBuilt:         built.add,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.UnregisterVault(vaultID) })

	if err := mgr.BuildOnce(t.Context(), vaultID); err != nil {
		t.Fatalf("BuildOnce: %v", err)
	}
	if got := built.snapshot(); len(got) != 0 {
		t.Fatalf("built %v, want nothing for a cloud-backed chunk", got)
	}
	if _, err := os.Stat(chunking.ChunkGLCBPath(chunkRoot, f.id)); !os.IsNotExist(err) {
		t.Fatalf("local GLCB for a cloud-backed chunk: err=%v, want none", err)
	}
}

// replicatedLog stands in for vault-ctl Raft: every committed command applies
// to every home's FSM replica in the same order.
type replicatedLog struct {
	mu       sync.Mutex
	replicas []*vaultctlfsm.FSM
	proposed map[int][][]byte
}

func (l *replicatedLog) commit(proposer int, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.proposed[proposer] = append(l.proposed[proposer], append([]byte(nil), data...))
	for _, fsm := range l.replicas {
		if result := fsm.Apply(&hraft.Log{Data: data}); result != nil {
			if err, ok := result.(error); ok {
				return err
			}
		}
	}
	return nil
}

func (l *replicatedLog) proposedBy(home int) [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.proposed[home])
}

// proposer is one home's vault-ctl applier: followers forward to the leader,
// so every proposal lands in the one replicated log.
type proposer struct {
	log  *replicatedLog
	home int
}

func (p proposer) Apply(data []byte) error { return p.log.commit(p.home, data) }

// Four homes, each with its own FSM replica: the leader builds and seals
// every chunk as soon as it is planned, while one follower lacks every
// segment, one never runs a build pass until the end, and one holds only some
// segments. Every home ends with every chunk, built locally and byte-identical
// to the leader's.
func TestEveryHomeBuildsEverySealedChunkWhileTheLeaderRunsAhead(t *testing.T) {
	t.Parallel()
	const homes = 4
	const leader = 0
	const lacksSegments, idle, partial = 1, 2, 3
	vaultID := glid.New()
	source := t.TempDir()
	fixtures := newChunkFixtures(4, time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC))
	writeFixtureSegments(t, source, vaultID, fixtures)

	// Segments release once every home holds them; no home commits a holder
	// receipt here, so every segment stays available to collection.
	homeNodes := []string{"home-0", "home-1", "home-2", "home-3"}
	log := &replicatedLog{proposed: make(map[int][][]byte)}
	roots := make([]string, homes)
	mgrs := make([]*chunking.Manager, homes)
	built := make([]*builtLog, homes)
	for h := range homes {
		log.replicas = append(log.replicas, vaultctlfsm.New())
		roots[h] = t.TempDir()
		built[h] = &builtLog{}
		mgrs[h] = chunking.New(chunking.Config{})
		isLeader := h == leader
		if err := mgrs[h].RegisterVault(vaultID, chunking.VaultConfig{
			RequiredHolders: func() ([]string, bool) { return homeNodes, true },
			VaultRoot:       roots[h],
			ChunkRoot:       filepath.Join(roots[h], "chunks"),
			FSM:             log.replicas[h],
			Locate:          chunking.HeadSegmentLocator{Root: roots[h]},
			Applier:         proposer{log: log, home: h},
			IsLeader:        func() bool { return isLeader },
			OnBuilt:         built[h].add,
		}); err != nil {
			t.Fatal(err)
		}
		mgr := mgrs[h]
		t.Cleanup(func() { mgr.UnregisterVault(vaultID) })
	}
	for _, f := range fixtures {
		deliverSegment(t, source, roots[leader], f.seg)
	}
	deliverSegment(t, source, roots[partial], fixtures[1].seg)
	deliverSegment(t, source, roots[partial], fixtures[3].seg)

	buildOn := func(h int, when string) {
		t.Helper()
		if err := mgrs[h].BuildOnce(t.Context(), vaultID); err != nil {
			t.Fatalf("home %d BuildOnce %s: %v", h, when, err)
		}
	}
	plannerApply := func(data []byte) {
		if err := log.commit(leader, data); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	for _, f := range fixtures {
		planAndSealManifest(t, plannerApply, f)
		buildOn(leader, "after the manifest sealed")
		if e := log.replicas[leader].Get(f.id); e == nil || e.State != chunk.ChunkStateSealed {
			t.Fatalf("leader did not seal %s: %+v", f.id, e)
		}
		buildOn(lacksSegments, "while it lacks every segment")
		buildOn(partial, "while it holds some segments")
	}
	if got, want := built[partial].snapshot(), fixtureIDs(fixtures[1], fixtures[3]); !slices.Equal(got, want) {
		t.Fatalf("partial home built %v before the rest arrived, want %v", got, want)
	}

	for _, h := range []int{lacksSegments, idle, partial} {
		for _, f := range fixtures {
			deliverSegment(t, source, roots[h], f.seg)
		}
		buildOn(h, "with every segment")
	}

	want := fixtureIDs(fixtures...)
	slices.SortFunc(want, chunk.ChunkID.Compare)
	for h := range homes {
		got := built[h].snapshot()
		slices.SortFunc(got, chunk.ChunkID.Compare)
		if !slices.Equal(got, want) {
			t.Fatalf("home %d built %v, want every chunk exactly once", h, built[h].snapshot())
		}
		for _, f := range fixtures {
			leaderGLCB, err := os.ReadFile(chunking.ChunkGLCBPath(filepath.Join(roots[leader], "chunks"), f.id))
			if err != nil {
				t.Fatalf("leader GLCB %s: %v", f.id, err)
			}
			homeGLCB, err := os.ReadFile(chunking.ChunkGLCBPath(filepath.Join(roots[h], "chunks"), f.id))
			if err != nil {
				t.Fatalf("home %d GLCB %s: %v", h, f.id, err)
			}
			if !bytes.Equal(leaderGLCB, homeGLCB) {
				t.Fatalf("home %d GLCB %s differs from the leader's", h, f.id)
			}
		}
	}
	if n := sealChunkCommands(t, log.proposedBy(leader)); n != len(fixtures) {
		t.Fatalf("leader committed %d CmdSealChunk, want %d", n, len(fixtures))
	}
	for _, h := range []int{lacksSegments, idle, partial} {
		if n := sealChunkCommands(t, log.proposedBy(h)); n != 0 {
			t.Fatalf("follower home %d proposed %d CmdSealChunk, want 0", h, n)
		}
	}
}
