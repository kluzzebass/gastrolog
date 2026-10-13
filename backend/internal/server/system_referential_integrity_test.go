package server_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"

	"connectrpc.com/connect"
)

// requireRejectionNames asserts err is a FailedPrecondition whose message
// carries every wanted fragment and none of the IDs' raw wire bytes.
func requireRejectionNames(t *testing.T, err error, ids []glid.GLID, want ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected FailedPrecondition, got nil")
	}
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	msg := ce.Message()
	for _, w := range want {
		if !strings.Contains(msg, w) {
			t.Errorf("message %q does not contain %q", msg, w)
		}
	}
	for _, id := range ids {
		if strings.Contains(msg, string(id.Bytes())) {
			t.Errorf("message %q carries the raw bytes of ID %s", msg, id)
		}
	}
	if strings.Contains(msg, `\x`) {
		t.Errorf("message %q carries escaped bytes", msg)
	}
}

func TestDeleteVaultReferencedByRouteNamesVaultAndRoute(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	vaultID, routeID := glid.New(), glid.New()
	if err := cfgStore.PutVault(ctx, system.VaultConfig{
		ID: vaultID, Name: "orders", Type: system.VaultTypeMemory, Enabled: true,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := cfgStore.PutRoute(ctx, system.RouteConfig{
		ID: routeID, Name: "to-orders", Destinations: []glid.GLID{vaultID}, Enabled: true,
	}); err != nil {
		t.Fatalf("PutRoute: %v", err)
	}

	_, err := client.DeleteVault(ctx, connect.NewRequest(&gastrologv1.DeleteVaultRequest{Id: vaultID.ToProto()}))
	requireRejectionNames(t, err, []glid.GLID{vaultID, routeID},
		`vault "orders" (`+vaultID.String()+`)`,
		`route "to-orders" (`+routeID.String()+`)`)

	if v, err := cfgStore.GetVault(ctx, vaultID); err != nil || v == nil {
		t.Fatalf("rejected delete removed the vault: %v, %v", v, err)
	}
}

func TestDeleteCloudServiceReferencedByVaultNamesServiceAndVault(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	csID, vaultID := glid.New(), glid.New()
	if err := cfgStore.PutCloudService(ctx, system.CloudService{
		ID: csID, Name: "archive", Provider: "s3", Bucket: "chunks", Region: "us-east-1",
	}); err != nil {
		t.Fatalf("PutCloudService: %v", err)
	}
	if err := cfgStore.PutVault(ctx, system.VaultConfig{
		ID: vaultID, Name: "cold", Type: system.VaultTypeFile, CloudServiceID: &csID, Enabled: true,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	_, err := client.DeleteCloudService(ctx, connect.NewRequest(&gastrologv1.DeleteCloudServiceRequest{Id: csID.ToProto()}))
	requireRejectionNames(t, err, []glid.GLID{csID, vaultID},
		`cloud service "archive" (`+csID.String()+`)`,
		`vault "cold" (`+vaultID.String()+`)`)

	if cs, err := cfgStore.GetCloudService(ctx, csID); err != nil || cs == nil {
		t.Fatalf("rejected delete removed the cloud service: %v, %v", cs, err)
	}
}

// TestMultiNode_DeleteVaultReferencedByRouteNamesVaultAndRoute deletes a vault
// another node hosts through a node that hosts no vault: the rejection names
// both entities from the replicated config, not from local state.
func TestMultiNode_DeleteVaultReferencedByRouteNamesVaultAndRoute(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"}, WithoutVault("coord"))
	ctx := context.Background()

	data := h.Node(t, "data-3")
	vaultCfg, err := h.cfgStore.GetVault(ctx, data.vaultID)
	if err != nil || vaultCfg == nil {
		t.Fatalf("GetVault(%s): %v, %v", data.vaultID, vaultCfg, err)
	}
	routeID := glid.New()
	if err := h.cfgStore.PutRoute(ctx, system.RouteConfig{
		ID: routeID, Name: "to-data-3", Destinations: []glid.GLID{data.vaultID}, Enabled: true,
	}); err != nil {
		t.Fatalf("PutRoute: %v", err)
	}

	_, err = h.configClient.DeleteVault(ctx, connect.NewRequest(&gastrologv1.DeleteVaultRequest{Id: data.vaultID.ToProto()}))
	requireRejectionNames(t, err, []glid.GLID{data.vaultID, routeID},
		`vault "`+vaultCfg.Name+`" (`+data.vaultID.String()+`)`,
		`route "to-data-3" (`+routeID.String()+`)`)
}
