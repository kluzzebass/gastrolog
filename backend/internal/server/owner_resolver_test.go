package server

import (
	"context"
	"errors"
	"slices"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/server/routing"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Owner resolvers (gastrolog-51ge9) answer "which node(s) own this
// resource" for the routing interceptor. They read only the replicated
// cluster-ctl store — they carry no node identity at all — which is what
// makes the answer identical on every node.

func TestOwnerResolvers_CoverEveryDeclaredResourceKind(t *testing.T) {
	resolvers := ownerResolvers(sysmem.NewStore())
	for proc, route := range routing.DefaultRoutes() {
		if route.Strategy != routing.RouteToResourceOwner || route.Resource == nil {
			continue
		}
		if resolvers[route.Resource.Kind] == nil {
			t.Errorf("procedure %s targets resource kind %q with no registered resolver", proc, route.Resource.Kind)
		}
	}
}

func TestIngesterOwner_ResolvesAliveNodes(t *testing.T) {
	ctx := context.Background()
	store := sysmem.NewStore()
	id := glid.New()

	if err := store.PutIngester(ctx, system.IngesterConfig{ID: id, Name: "burst", Type: "scatterbox", Enabled: true, AllNodes: true}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}
	// Alive on two nodes, explicitly dead on a third.
	for nodeID, alive := range map[string]bool{"node-c": true, "node-a": true, "node-b": false} {
		if err := store.SetIngesterAlive(ctx, id, nodeID, alive); err != nil {
			t.Fatalf("SetIngesterAlive(%s): %v", nodeID, err)
		}
	}

	// Resolvers built independently (as each node builds its own) must
	// agree, and the order must be deterministic so every non-owner node
	// forwards to the same place.
	want := []string{"node-a", "node-c"}
	for _, asNode := range []string{"node-a", "node-b", "node-c", "node-d"} {
		r := ownerResolvers(store)[routing.ResourceIngester]
		owners, err := r.ResolveOwners(ctx, id.String())
		if err != nil {
			t.Fatalf("ResolveOwners on %s: %v", asNode, err)
		}
		if !slices.Equal(owners, want) {
			t.Errorf("ResolveOwners on %s = %v, want %v", asNode, owners, want)
		}
	}
}

func TestIngesterOwner_UnknownIngesterIsNotFound(t *testing.T) {
	r := ownerResolvers(sysmem.NewStore())[routing.ResourceIngester]
	owners, err := r.ResolveOwners(context.Background(), glid.New().String())
	if !errors.Is(err, routing.ErrResourceNotFound) {
		t.Fatalf("got (%v, %v), want ErrResourceNotFound", owners, err)
	}
}

func TestIngesterOwner_ConfiguredButNotAlive(t *testing.T) {
	ctx := context.Background()
	store := sysmem.NewStore()
	id := glid.New()
	if err := store.PutIngester(ctx, system.IngesterConfig{ID: id, Name: "burst", Type: "scatterbox", Enabled: true}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	owners, err := ownerResolvers(store)[routing.ResourceIngester].ResolveOwners(ctx, id.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(owners) != 0 {
		t.Errorf("got owners %v, want none — nothing is running the ingester", owners)
	}
}

func TestIngesterOwner_MalformedIDDoesNotRoute(t *testing.T) {
	owners, err := ownerResolvers(sysmem.NewStore())[routing.ResourceIngester].ResolveOwners(context.Background(), "not-a-glid")
	if err != nil || len(owners) != 0 {
		t.Fatalf("got (%v, %v), want (nil, nil) so the handler reports the bad ID", owners, err)
	}
}

func TestVaultOwner_ResolvesLeaderPlacement(t *testing.T) {
	ctx := context.Background()
	store := sysmem.NewStore()

	vaultID := glid.New()
	placements := []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID("node-2"), Leader: true},
	}
	if err := store.PutVault(ctx, system.VaultConfig{
		ID:   vaultID,
		Name: "v",
		Type: system.VaultTypeMemory,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	// Placements are seeded through their owner. PutVault deliberately ignores
	// them now, so a fixture that attached them to the config would silently
	// resolve no owners (gastrolog-kl8c3s).
	if err := store.SetVaultPlacements(ctx, vaultID, placements); err != nil {
		t.Fatalf("SetVaultPlacements: %v", err)
	}

	owners, err := ownerResolvers(store)[routing.ResourceVault].ResolveOwners(ctx, vaultID.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(owners, []string{"node-2"}) {
		t.Errorf("got %v, want [node-2]", owners)
	}
}

func TestVaultOwner_UnknownVaultDefersToHandler(t *testing.T) {
	// A vault the store does not know about resolves to no owner rather
	// than an error: the vault handlers produce the domain-accurate error
	// (and accept vault references this resolver cannot interpret).
	owners, err := ownerResolvers(sysmem.NewStore())[routing.ResourceVault].ResolveOwners(context.Background(), glid.New().String())
	if err != nil || len(owners) != 0 {
		t.Fatalf("got (%v, %v), want (nil, nil)", owners, err)
	}
}
