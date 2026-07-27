package server_test

// gastrolog-3cobq4 review fix: allStorageStates took the pointer
// PeerStorageStats.FindStorageState returns straight into the shared
// per-peer stats cache and mutated PlacedVaultIds on it outside any lock.
// cluster.PeerState.FindStorageState returns the exact *StorageState living
// inside its cached NodeStats entry — the same object every concurrent
// WatchSystemStatus subscriber / ListStorages caller / GetClusterStatus
// caller reads and marshals. This test drives the REAL cluster.PeerState
// (not a harness fake — the fakes elsewhere in this package build a fresh
// proto per call and can't reproduce this aliasing) with concurrent
// ListStorages calls plus a concurrent stats-broadcast tick, so `go test
// -race` catches the shared-mutation race directly.

import (
	"context"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/system"
)

func TestListStorages_ConcurrentAccessDoesNotRaceSharedPeerState(t *testing.T) {
	ctx := context.Background()
	cfgStore := sysmem.NewStore()
	storageID := glid.New()

	// storageID is hosted on "peer-1" — this SystemServer has no local
	// orchestrator (nil), so allStorageStates can ONLY resolve its live
	// state through PeerStorageStats, guaranteeing every call hits the
	// aliasing-prone peer branch.
	if err := cfgStore.SetNodeStorageConfig(ctx, system.NodeStorageConfig{
		NodeID: "peer-1",
		FileStorages: []system.FileStorage{{
			ID:   storageID,
			Name: "guarded-storage",
			Path: "/data",
		}},
	}); err != nil {
		t.Fatalf("SetNodeStorageConfig: %v", err)
	}

	peerState := cluster.NewPeerState(time.Minute, 0)
	peerState.Update("peer-1", &gastrologv1.NodeStats{
		Storages: []*gastrologv1.StorageState{{
			Id:   storageID.ToProto(),
			Name: "guarded-storage",
		}},
	}, time.Now())

	sysServer := server.NewSystemServer(server.SystemServerConfig{
		CfgStore:         cfgStore,
		PeerStorageStats: peerState,
	})

	var wg sync.WaitGroup
	// Readers: concurrent ListStorages calls. Each one that hits the peer
	// branch gets the SAME *StorageState pointer back from
	// peerState.FindStorageState and (pre-fix) writes PlacedVaultIds on it
	// in place — a write/write race across goroutines.
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if _, err := sysServer.ListStorages(ctx, connect.NewRequest(&gastrologv1.ListStoragesRequest{})); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	// Writer: simulates the periodic stats-broadcast tick replacing the
	// peer's cached NodeStats concurrently with readers — the production
	// shape this race actually occurs under.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			peerState.Update("peer-1", &gastrologv1.NodeStats{
				Storages: []*gastrologv1.StorageState{{
					Id:   storageID.ToProto(),
					Name: "guarded-storage",
				}},
			}, time.Now())
		}
	}()
	wg.Wait()
}
