package orchestrator

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// A node outside the vault's placement may delete its copy of a sealed chunk
// only once it is no longer a recorded holder, no delete is pending, and the
// placement holds the chunk without it: at its holder quorum, or in the cloud.
func TestFormerHomeCopyReleased(t *testing.T) {
	t.Parallel()
	placement := []string{"home-a", "home-b"}
	sealed := func(holders ...string) vaultctlfsm.ManifestEntry {
		return vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed, Holders: holders}
	}
	cloud := sealed()
	cloud.CloudBacked = true
	active := sealed("home-a", "home-b")
	active.State = chunk.ChunkStateActive
	cases := []struct {
		name    string
		e       vaultctlfsm.ManifestEntry
		pending *vaultctlfsm.PendingDelete
		want    bool
	}{
		{name: "still a recorded holder", e: sealed("former", "home-a", "home-b")},
		{name: "delete pending", e: sealed("home-a", "home-b"), pending: &vaultctlfsm.PendingDelete{}},
		{name: "placement below quorum", e: sealed("home-a")},
		{name: "held only outside the placement", e: sealed("elsewhere")},
		{name: "not sealed", e: active},
		{name: "placement holds the quorum", e: sealed("home-a", "home-b"), want: true},
		{name: "cloud-backed", e: cloud, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formerHomeCopyReleased(tc.e, tc.pending, "former", placement); got != tc.want {
				t.Fatalf("formerHomeCopyReleased = %v, want %v", got, tc.want)
			}
		})
	}
}

// The sweep deletes exactly the released copies under the former home's
// storage root, leaves every other copy and every chunk outside the vault
// chunk manifest alone, and deletes nothing on a node that is placed on the
// vault.
func TestPurgeFormerHomeChunkCopiesKeepsWhatThePlacementStillNeeds(t *testing.T) {
	t.Parallel()
	n := newFormerHome(t)
	fsm := vaultctlfsm.New()
	apply := func(data []byte) {
		t.Helper()
		applyToFSM(t, fsm, data)
	}
	sealed := func(holders ...string) chunk.ChunkID {
		id := chunk.NewChunkID()
		now := time.Now()
		apply(vaultctlfsm.MarshalCreateChunk(id, now, now, now))
		apply(vaultctlfsm.MarshalSealChunk(id, now, 1, 1, now, now, now, true, now, [32]byte{}))
		for _, h := range holders {
			data, err := vaultctlfsm.MarshalAckChunkHolders([]chunk.ChunkID{id}, h)
			if err != nil {
				t.Fatal(err)
			}
			apply(data)
		}
		return id
	}
	handedOff := sealed(n.home)
	stillClaimed := sealed(n.home, n.self)
	notYetHeld := sealed()
	cloud := sealed()
	apply(vaultctlfsm.MarshalUploadChunk(cloud, 64, 0, 0, 0, 0, [32]byte{1}, glid.New(), 0))
	deleting := sealed(n.home)
	apply(vaultctlfsm.MarshalRequestDelete(deleting, time.Now(), "retention-ttl", []string{n.home}))
	unknown := chunk.NewChunkID()
	copies := map[chunk.ChunkID]string{}
	for _, id := range []chunk.ChunkID{handedOff, stillClaimed, notYetHeld, cloud, deleting, unknown} {
		copies[id] = chunking.ChunkGLCBPath(pipelineChunkRoot(n.root), id)
		writeFormerHomeFile(t, copies[id], []byte("copy"))
	}

	ctx := context.Background()
	sys, err := n.store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := n.o.purgeFormerHomeChunkCopies(sys, n.vault, fsm)
	if err != nil {
		t.Fatalf("purgeFormerHomeChunkCopies: %v", err)
	}
	want := []chunk.ChunkID{handedOff, cloud}
	slices.SortFunc(dropped, chunk.ChunkID.Compare)
	slices.SortFunc(want, chunk.ChunkID.Compare)
	if !slices.Equal(dropped, want) {
		t.Fatalf("dropped %v, want %v", dropped, want)
	}
	for id, path := range copies {
		_, err := os.Stat(path)
		if gone := os.IsNotExist(err); gone != slices.Contains(want, id) {
			t.Errorf("chunk %s: copy gone=%v (stat %v)", id, gone, err)
		}
	}

	// Placed on the vault again, the node keeps everything, even a chunk it
	// no longer claims.
	if err := n.store.SetVaultPlacements(ctx, n.vault, []system.VaultPlacement{
		{StorageID: n.storages[n.home], Leader: true},
		{StorageID: n.storages[n.self]},
	}); err != nil {
		t.Fatal(err)
	}
	if sys, err = n.store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	apply(vaultctlfsm.MarshalPruneNode(n.self))
	if dropped, err := n.o.purgeFormerHomeChunkCopies(sys, n.vault, fsm); err != nil || len(dropped) != 0 {
		t.Fatalf("a placed node purged %v (err %v); it deletes nothing here", dropped, err)
	}
}
