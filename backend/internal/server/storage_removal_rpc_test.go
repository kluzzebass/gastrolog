package server_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// Through the RPC: a node storage config that drops the storage a vault is
// placed on is refused before anything is written.
func TestSetNodeStorageConfigRefusesDroppingAPlacedStorage(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	const nodeID = "storage-node"
	placedID, spareID := glid.New(), glid.New()
	if err := store.SetNodeStorageConfig(ctx, system.NodeStorageConfig{NodeID: nodeID, FileStorages: []system.FileStorage{
		{ID: placedID, Name: "placed", Path: "storage/placed", StorageClass: 1},
		{ID: spareID, Name: "spare", Path: "storage/spare", StorageClass: 2},
	}}); err != nil {
		t.Fatal(err)
	}
	v := system.VaultConfig{ID: glid.New(), Name: "lives-here", Type: system.VaultTypeFile, StorageClass: 1, Enabled: true}
	if err := store.PutVault(ctx, v); err != nil {
		t.Fatal(err)
	}
	if err := store.SetVaultPlacements(ctx, v.ID, []system.VaultPlacement{{StorageID: placedID.String(), Leader: true}}); err != nil {
		t.Fatal(err)
	}

	only := func(id glid.GLID, name string, class uint32) *gastrologv1.SetNodeStorageConfigRequest {
		return &gastrologv1.SetNodeStorageConfigRequest{Config: &gastrologv1.NodeStorageConfig{
			NodeId:       []byte(nodeID),
			FileStorages: []*gastrologv1.FileStorage{{Id: id.ToProto(), Name: name, Path: "storage/" + name, StorageClass: class}},
		}}
	}

	_, err := client.SetNodeStorageConfig(ctx, connect.NewRequest(only(spareID, "spare", 2)))
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("dropping the placed storage must be FailedPrecondition, got %v", err)
	}
	stored, err := store.GetNodeStorageConfig(ctx, nodeID)
	if err != nil || stored == nil || len(stored.FileStorages) != 2 {
		t.Fatalf("a refused config must not be written: stored=%+v err=%v", stored, err)
	}

	if _, err := client.SetNodeStorageConfig(ctx, connect.NewRequest(only(placedID, "placed", 1))); err != nil {
		t.Fatalf("dropping the unplaced storage must pass: %v", err)
	}
}
