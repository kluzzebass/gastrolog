package server_test

import (
	"context"
	"net/http"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
	"github.com/google/uuid"
)

// newConfigTestSetup creates an orchestrator, config vault, and Connect client
// for testing config RPCs.
func newConfigTestSetup(t *testing.T) (gastrologv1connect.ConfigServiceClient, config.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := cfgmem.NewStore()
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
			"file":   chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
			"file":   indexfile.NewFactory(),
		},
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	client := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")

	return client, cfgStore, orch
}

func TestDeleteVaultForce(t *testing.T) {
	client, cfgStore, orch := newConfigTestSetup(t)
	ctx := context.Background()

	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	// Create a filter first, then a vault that uses it.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     vaultID.String(),
			Type:   "memory",
			Filter: filterID.String(),
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Ingest data so the vault is non-empty.
	if err := orch.Ingest(chunk.Record{
		Raw: []byte("test data"),
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Non-force delete should fail.
	_, err = client.DeleteVault(ctx, connect.NewRequest(&gastrologv1.DeleteVaultRequest{
		Id: vaultID.String(),
	}))
	if err == nil {
		t.Fatal("expected error for non-force delete of non-empty vault")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", connect.CodeOf(err))
	}

	// Force delete should succeed.
	_, err = client.DeleteVault(ctx, connect.NewRequest(&gastrologv1.DeleteVaultRequest{
		Id:    vaultID.String(),
		Force: true,
	}))
	if err != nil {
		t.Fatalf("DeleteVault(force=true): %v", err)
	}

	// Verify vault is gone from runtime.
	if cm := orch.ChunkManager(vaultID); cm != nil {
		t.Error("ChunkManager should be nil after force delete")
	}

	// Verify vault is gone from config.
	stored, _ := cfgStore.GetVault(ctx, vaultID)
	if stored != nil {
		t.Error("vault should be removed from config vault")
	}
}

func TestDeleteVaultNotFound(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	nonexistentID := uuid.Must(uuid.NewV7())
	_, err := client.DeleteVault(ctx, connect.NewRequest(&gastrologv1.DeleteVaultRequest{
		Id: nonexistentID.String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestPutVaultNestedDirPrevention(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	baseDir := t.TempDir()

	vault1ID := uuid.Must(uuid.NewV7())
	nestedChildID := uuid.Must(uuid.NewV7())
	nestedParentID := uuid.Must(uuid.NewV7())
	siblingID := uuid.Must(uuid.NewV7())
	memVaultID := uuid.Must(uuid.NewV7())
	duplicateDirID := uuid.Must(uuid.NewV7())

	// Seed a file vault at baseDir/vault1 directly in config (not via RPC,
	// to avoid actually creating the directory and orchestrator entry).
	err := cfgStore.PutVault(ctx, config.VaultConfig{
		ID:     vault1ID,
		Type:   "file",
		Params: map[string]string{"dir": baseDir + "/vault1"},
	})
	if err != nil {
		t.Fatalf("seed vault1: %v", err)
	}

	// Attempt to create a file vault nested inside vault1.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     nestedChildID.String(),
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/vault1/archive"},
		},
	}))
	if err == nil {
		t.Fatal("expected error for nested child directory")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}

	// Attempt to create a file vault that is a parent of vault1.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     nestedParentID.String(),
			Type:   "file",
			Params: map[string]string{"dir": baseDir},
		},
	}))
	if err == nil {
		t.Fatal("expected error for parent directory")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}

	// Sibling directory should be OK.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     siblingID.String(),
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/vault2"},
		},
	}))
	if err != nil {
		t.Fatalf("sibling directory should be allowed: %v", err)
	}

	// Memory vaults should not be checked.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:   memVaultID.String(),
			Type: "memory",
		},
	}))
	if err != nil {
		t.Fatalf("memory vault should always be allowed: %v", err)
	}

	// Updating a file vault's own dir to itself should be OK
	// (seeded directly to avoid orchestrator conflicts).
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     vault1ID.String(),
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/vault1"},
		},
	}))
	if err != nil {
		t.Fatalf("updating self should be allowed: %v", err)
	}

	// Same exact dir as another vault should fail.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     duplicateDirID.String(),
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/vault1"},
		},
	}))
	if err == nil {
		t.Fatal("expected error for duplicate directory")
	}
}

func TestPauseResumeVaultRPC(t *testing.T) {
	client, _, orch := newConfigTestSetup(t)
	ctx := context.Background()

	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	// Create a filter and a vault.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     vaultID.String(),
			Type:   "memory",
			Filter: filterID.String(),
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Pause the vault via RPC.
	_, err = client.PauseVault(ctx, connect.NewRequest(&gastrologv1.PauseVaultRequest{
		Id: vaultID.String(),
	}))
	if err != nil {
		t.Fatalf("PauseVault: %v", err)
	}

	// Verify runtime state.
	if orch.IsVaultEnabled(vaultID) {
		t.Error("vault should be disabled in runtime")
	}

	// Verify disabled in VaultInfo via VaultService.
	handler := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{}).Handler()
	vaultClient := gastrologv1connect.NewVaultServiceClient(
		&http.Client{Transport: &embeddedTransport{handler: handler}},
		"http://embedded",
	)
	vaultResp, err := vaultClient.GetVault(ctx, connect.NewRequest(&gastrologv1.GetVaultRequest{Id: vaultID.String()}))
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if vaultResp.Msg.Vault.Enabled {
		t.Error("VaultInfo.Enabled should be false after pause")
	}

	// Resume via RPC.
	_, err = client.ResumeVault(ctx, connect.NewRequest(&gastrologv1.ResumeVaultRequest{
		Id: vaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ResumeVault: %v", err)
	}

	if !orch.IsVaultEnabled(vaultID) {
		t.Error("vault should be enabled after resume")
	}

	// Ingest should work after resume.
	if err := orch.Ingest(chunk.Record{Raw: []byte("after resume")}); err != nil {
		t.Fatalf("Ingest after resume: %v", err)
	}
}

func TestPauseVaultNotFoundRPC(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	nonexistentID := uuid.Must(uuid.NewV7())
	_, err := client.PauseVault(ctx, connect.NewRequest(&gastrologv1.PauseVaultRequest{
		Id: nonexistentID.String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestResumeVaultNotFoundRPC(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	nonexistentID := uuid.Must(uuid.NewV7())
	_, err := client.ResumeVault(ctx, connect.NewRequest(&gastrologv1.ResumeVaultRequest{
		Id: nonexistentID.String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestPauseVaultPersistsToConfig(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	// Create a filter and vault.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:     vaultID.String(),
			Type:   "memory",
			Filter: filterID.String(),
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Pause and check config persistence.
	_, err = client.PauseVault(ctx, connect.NewRequest(&gastrologv1.PauseVaultRequest{
		Id: vaultID.String(),
	}))
	if err != nil {
		t.Fatalf("PauseVault: %v", err)
	}

	stored, err := cfgStore.GetVault(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetVault from config: %v", err)
	}
	if stored.Enabled {
		t.Error("config vault should have Enabled=false after pause")
	}

	// Resume and check config persistence.
	_, err = client.ResumeVault(ctx, connect.NewRequest(&gastrologv1.ResumeVaultRequest{
		Id: vaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ResumeVault: %v", err)
	}

	stored, err = cfgStore.GetVault(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetVault from config: %v", err)
	}
	if !stored.Enabled {
		t.Error("config vault should have Enabled=true after resume")
	}
}
