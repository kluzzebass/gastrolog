package server_test

import (
	"context"
	"errors"
	"maps"
	"net"
	"net/http"
	"slices"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/ingester/syslog"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
	"github.com/google/uuid"
)

// testAfterConfigApply creates a dispatch callback for non-raft test stores.
// It mirrors the production configDispatcher but lives in the test package.
func testAfterConfigApply(orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories) func(raftfsm.Notification) {
	return func(n raftfsm.Notification) {
		ctx := context.Background()
		switch n.Kind {
		case raftfsm.NotifyVaultPut:
			cfg, err := cfgStore.GetVault(ctx, n.ID)
			if err != nil || cfg == nil {
				return
			}
			if slices.Contains(orch.ListVaults(), n.ID) {
				_ = orch.ReloadFilters(ctx)
				_ = orch.ReloadRotationPolicies(ctx)
				_ = orch.ReloadRetentionPolicies(ctx)
				if !cfg.Enabled {
					_ = orch.DisableVault(n.ID)
				} else {
					_ = orch.EnableVault(n.ID)
				}
			} else {
				_ = orch.AddVault(ctx, *cfg, factories)
			}
		case raftfsm.NotifyVaultDeleted:
			_ = orch.ForceRemoveVault(n.ID)
		case raftfsm.NotifyFilterPut, raftfsm.NotifyFilterDeleted:
			_ = orch.ReloadFilters(ctx)
		case raftfsm.NotifyRotationPolicyPut, raftfsm.NotifyRotationPolicyDeleted:
			_ = orch.ReloadRotationPolicies(ctx)
		case raftfsm.NotifyRetentionPolicyPut, raftfsm.NotifyRetentionPolicyDeleted:
			_ = orch.ReloadRetentionPolicies(ctx)
		case raftfsm.NotifyIngesterPut:
			cfg, err := cfgStore.GetIngester(ctx, n.ID)
			if err != nil || cfg == nil {
				return
			}
			if slices.Contains(orch.ListIngesters(), n.ID) {
				if err := orch.RemoveIngester(n.ID); err != nil && !errors.Is(err, orchestrator.ErrIngesterNotFound) {
					return
				}
			}
			if !cfg.Enabled {
				return
			}
			reg, ok := factories.IngesterTypes[cfg.Type]
			if !ok {
				return
			}
			params := cfg.Params
			if factories.HomeDir != "" {
				params = make(map[string]string, len(cfg.Params)+1)
				maps.Copy(params, cfg.Params)
				params["_state_dir"] = factories.HomeDir
			}
			ing, err := reg.Factory(cfg.ID, params, factories.Logger)
			if err != nil {
				return
			}
			_ = orch.AddIngester(cfg.ID, cfg.Name, cfg.Type, ing)
		case raftfsm.NotifyIngesterDeleted:
			_ = orch.RemoveIngester(n.ID)
		}
	}
}

// newConfigTestSetup creates an orchestrator, config vault, and Connect client
// for testing config RPCs.
func newConfigTestSetup(t *testing.T) (gastrologv1connect.ConfigServiceClient, config.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := cfgmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore})
	if err != nil {
		t.Fatal(err)
	}

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

	srv := server.New(orch, cfgStore, factories, nil, server.Config{
		AfterConfigApply: testAfterConfigApply(orch, cfgStore, factories),
	})
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
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Name: "catch-all", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:   vaultID.String(),
			Name: "test-vault",
			Type: "memory",
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Set a catch-all filter directly on the orchestrator (not via route,
	// since the test also needs to force-delete the vault without
	// hitting route referential integrity checks).
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{
		{VaultID: vaultID, Kind: orchestrator.FilterCatchAll, Expr: "*"},
	}))

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
			Name:   "nested-child",
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
			Name:   "nested-parent",
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
			Name:   "sibling",
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
			Name: "mem-vault",
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
			Name:   "vault1-self-update",
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
			Name:   "duplicate-dir",
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
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Name: "catch-all-2", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:   vaultID.String(),
			Name: "pause-vault",
			Type: "memory",
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Route the catch-all filter to the vault so Ingest delivers records.
	_, err = client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
		Config: &gastrologv1.RouteConfig{
			Id:       uuid.Must(uuid.NewV7()).String(),
			Name:     "test-route",
			FilterId: filterID.String(),
			Destinations: []*gastrologv1.RouteDestination{
				{VaultId: vaultID.String()},
			},
			Enabled: true,
		},
	}))
	if err != nil {
		t.Fatalf("PutRoute: %v", err)
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
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Name: "catch-all-3", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:   vaultID.String(),
			Name: "persist-vault",
			Type: "memory",
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

// newConfigTestSetupWithIngesters is like newConfigTestSetup but wires in
// ingester type registrations so PutIngester validation works end-to-end.
func newConfigTestSetupWithIngesters(t *testing.T) (gastrologv1connect.ConfigServiceClient, config.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := cfgmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
			"file":   chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
			"file":   indexfile.NewFactory(),
		},
		IngesterTypes: map[string]orchestrator.IngesterRegistration{
			"syslog": {Factory: syslog.NewFactory(), Defaults: syslog.ParamDefaults, ListenAddrs: syslog.ListenAddrs},
		},
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{
		AfterConfigApply: testAfterConfigApply(orch, cfgStore, factories),
	})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	client := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")

	return client, cfgStore, orch
}

func TestDuplicateEntityNames(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	t.Run("filter", func(t *testing.T) {
		_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
			Config: &gastrologv1.FilterConfig{Name: "my-filter", Expression: "*"},
		}))
		if err != nil {
			t.Fatalf("first PutFilter: %v", err)
		}
		_, err = client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
			Config: &gastrologv1.FilterConfig{Name: "my-filter", Expression: "level=error"},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate filter name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("vault", func(t *testing.T) {
		_, err := client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
			Config: &gastrologv1.VaultConfig{Name: "my-vault", Type: "memory"},
		}))
		if err != nil {
			t.Fatalf("first PutVault: %v", err)
		}
		_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
			Config: &gastrologv1.VaultConfig{Name: "my-vault", Type: "memory"},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate vault name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("route", func(t *testing.T) {
		_, err := client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
			Config: &gastrologv1.RouteConfig{Name: "my-route"},
		}))
		if err != nil {
			t.Fatalf("first PutRoute: %v", err)
		}
		_, err = client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
			Config: &gastrologv1.RouteConfig{Name: "my-route"},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate route name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("rotation_policy", func(t *testing.T) {
		_, err := client.PutRotationPolicy(ctx, connect.NewRequest(&gastrologv1.PutRotationPolicyRequest{
			Config: &gastrologv1.RotationPolicyConfig{Name: "my-rotation", MaxRecords: 1000},
		}))
		if err != nil {
			t.Fatalf("first PutRotationPolicy: %v", err)
		}
		_, err = client.PutRotationPolicy(ctx, connect.NewRequest(&gastrologv1.PutRotationPolicyRequest{
			Config: &gastrologv1.RotationPolicyConfig{Name: "my-rotation", MaxRecords: 2000},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate rotation policy name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("retention_policy", func(t *testing.T) {
		_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
			Config: &gastrologv1.RetentionPolicyConfig{Name: "my-retention", MaxChunks: 10},
		}))
		if err != nil {
			t.Fatalf("first PutRetentionPolicy: %v", err)
		}
		_, err = client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
			Config: &gastrologv1.RetentionPolicyConfig{Name: "my-retention", MaxChunks: 20},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate retention policy name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("ingester", func(t *testing.T) {
		// Ingesters use disabled=true to skip factory validation.
		_, err := client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
			Config: &gastrologv1.IngesterConfig{Name: "my-ingester", Type: "syslog"},
		}))
		if err != nil {
			t.Fatalf("first PutIngester: %v", err)
		}
		_, err = client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
			Config: &gastrologv1.IngesterConfig{Name: "my-ingester", Type: "syslog"},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate ingester name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("node", func(t *testing.T) {
		nodeA := uuid.Must(uuid.NewV7())
		nodeB := uuid.Must(uuid.NewV7())
		_, err := client.PutNodeConfig(ctx, connect.NewRequest(&gastrologv1.PutNodeConfigRequest{
			Config: &gastrologv1.NodeConfig{Id: nodeA.String(), Name: "alpha"},
		}))
		if err != nil {
			t.Fatalf("first PutNodeConfig: %v", err)
		}
		_, err = client.PutNodeConfig(ctx, connect.NewRequest(&gastrologv1.PutNodeConfigRequest{
			Config: &gastrologv1.NodeConfig{Id: nodeB.String(), Name: "alpha"},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate node name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("update_self_allowed", func(t *testing.T) {
		// Creating with an explicit ID, then updating with the same ID and name should work.
		id := uuid.Must(uuid.NewV7())
		_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
			Config: &gastrologv1.FilterConfig{Id: id.String(), Name: "self-update", Expression: "*"},
		}))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		_, err = client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
			Config: &gastrologv1.FilterConfig{Id: id.String(), Name: "self-update", Expression: "level=error"},
		}))
		if err != nil {
			t.Fatalf("update self should be allowed: %v", err)
		}
	})

	t.Run("lookup_names_across_types", func(t *testing.T) {
		// Two lookups with the same name across different types should conflict.
		_, err := client.PutSettings(ctx, connect.NewRequest(&gastrologv1.PutSettingsRequest{
			Lookup: &gastrologv1.PutLookupSettings{
				HttpLookups: []*gastrologv1.HTTPLookupEntry{
					{Name: "duped", UrlTemplate: "http://example.com/{ip}"},
				},
				CsvLookups: []*gastrologv1.CSVLookupEntry{
					{Name: "duped", FileId: uuid.Must(uuid.NewV7()).String()},
				},
			},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate lookup name across types")
		}
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
		}
	})

	t.Run("lookup_names_within_type", func(t *testing.T) {
		// Two lookups with the same name within the same type should also conflict.
		_, err := client.PutSettings(ctx, connect.NewRequest(&gastrologv1.PutSettingsRequest{
			Lookup: &gastrologv1.PutLookupSettings{
				HttpLookups: []*gastrologv1.HTTPLookupEntry{
					{Name: "api-a", UrlTemplate: "http://a.example.com/{ip}"},
					{Name: "api-a", UrlTemplate: "http://b.example.com/{ip}"},
				},
			},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate lookup name within type")
		}
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
		}
	})

	t.Run("empty_name_rejected", func(t *testing.T) {
		_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
			Config: &gastrologv1.FilterConfig{Name: "", Expression: "*"},
		}))
		if err == nil {
			t.Fatal("expected error for empty name")
		}
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
		}
	})
}

func TestPutIngesterUnknownType(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	_, err := client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
		Config: &gastrologv1.IngesterConfig{
			Name:    "bad-ingester",
			Type:    "nonexistent",
			Enabled: true,
		},
	}))
	if err == nil {
		t.Fatal("expected error for unknown ingester type")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

func TestDeleteIngesterNotFound(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	nonexistentID := uuid.Must(uuid.NewV7())
	_, err := client.DeleteIngester(ctx, connect.NewRequest(&gastrologv1.DeleteIngesterRequest{
		Id: nonexistentID.String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent ingester")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected CodeNotFound, got %v: %v", connect.CodeOf(err), err)
	}
}

// noopIngester is a dummy ingester that blocks until cancelled.
type noopIngester struct{}

func (n *noopIngester) Run(ctx context.Context, _ chan<- orchestrator.IngestMessage) error {
	<-ctx.Done()
	return nil
}

// mockPeerIngesterStats implements server.PeerIngesterStatsProvider for tests.
type mockPeerIngesterStats struct {
	stats map[string]*gastrologv1.IngesterNodeStats
}

func (m *mockPeerIngesterStats) FindIngesterStats(id string) *gastrologv1.IngesterNodeStats {
	return m.stats[id]
}

func TestListIngestersRemoteRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgStore := cfgmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	remoteIngID := uuid.Must(uuid.NewV7())
	_ = cfgStore.PutIngester(ctx, config.IngesterConfig{
		ID: remoteIngID, Name: "remote-syslog", Type: "syslog", NodeID: "node-B", Enabled: true,
	})

	peerStats := &mockPeerIngesterStats{stats: map[string]*gastrologv1.IngesterNodeStats{
		remoteIngID.String(): {
			Id: remoteIngID.String(), Running: true,
			MessagesIngested: 42, BytesIngested: 1024, Errors: 1,
		},
	}}

	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:            "node-A",
		PeerIngesterStats: peerStats,
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	client := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")

	resp, err := client.ListIngesters(ctx, connect.NewRequest(&gastrologv1.ListIngestersRequest{}))
	if err != nil {
		t.Fatal(err)
	}

	var found *gastrologv1.IngesterInfo
	for _, ing := range resp.Msg.Ingesters {
		if ing.Id == remoteIngID.String() {
			found = ing
		}
	}
	if found == nil {
		t.Fatal("remote ingester not found in list")
	}
	if !found.Running {
		t.Error("expected Running=true for remote ingester")
	}
}

func TestGetIngesterStatusRemote(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgStore := cfgmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	remoteIngID := uuid.Must(uuid.NewV7())
	_ = cfgStore.PutIngester(ctx, config.IngesterConfig{
		ID: remoteIngID, Name: "remote-syslog", Type: "syslog", NodeID: "node-B", Enabled: true,
	})

	peerStats := &mockPeerIngesterStats{stats: map[string]*gastrologv1.IngesterNodeStats{
		remoteIngID.String(): {
			Id: remoteIngID.String(), Running: true,
			MessagesIngested: 42, BytesIngested: 1024, Errors: 1,
		},
	}}

	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:            "node-A",
		PeerIngesterStats: peerStats,
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	client := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")

	resp, err := client.GetIngesterStatus(ctx, connect.NewRequest(&gastrologv1.GetIngesterStatusRequest{
		Id: remoteIngID.String(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Msg.Running {
		t.Error("expected Running=true")
	}
	if resp.Msg.MessagesIngested != 42 {
		t.Errorf("MessagesIngested = %d, want 42", resp.Msg.MessagesIngested)
	}
	if resp.Msg.BytesIngested != 1024 {
		t.Errorf("BytesIngested = %d, want 1024", resp.Msg.BytesIngested)
	}
	if resp.Msg.Errors != 1 {
		t.Errorf("Errors = %d, want 1", resp.Msg.Errors)
	}
}

func TestGetIngesterStatusLocal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgStore := cfgmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	ingID := uuid.Must(uuid.NewV7())
	_ = cfgStore.PutIngester(ctx, config.IngesterConfig{
		ID: ingID, Name: "local-syslog", Type: "syslog", NodeID: "node-A", Enabled: true,
	})

	// Register in orchestrator so GetIngesterStats returns non-nil.
	orch.RegisterIngester(ingID, "local-syslog", "syslog", &noopIngester{})

	// Simulate some stats.
	stats := orch.GetIngesterStats(ingID)
	stats.MessagesIngested.Store(99)
	stats.BytesIngested.Store(2048)
	stats.Errors.Store(3)

	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID: "node-A",
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	client := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")

	resp, err := client.GetIngesterStatus(ctx, connect.NewRequest(&gastrologv1.GetIngesterStatusRequest{
		Id: ingID.String(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.MessagesIngested != 99 {
		t.Errorf("MessagesIngested = %d, want 99", resp.Msg.MessagesIngested)
	}
	if resp.Msg.BytesIngested != 2048 {
		t.Errorf("BytesIngested = %d, want 2048", resp.Msg.BytesIngested)
	}
	if resp.Msg.Errors != 3 {
		t.Errorf("Errors = %d, want 3", resp.Msg.Errors)
	}
}

func TestListIngestersNoPeerStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgStore := cfgmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	remoteIngID := uuid.Must(uuid.NewV7())
	_ = cfgStore.PutIngester(ctx, config.IngesterConfig{
		ID: remoteIngID, Name: "remote-syslog", Type: "syslog", NodeID: "node-B", Enabled: true,
	})

	// No PeerIngesterStats provided (single-node mode).
	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID: "node-A",
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	client := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")

	resp, err := client.ListIngesters(ctx, connect.NewRequest(&gastrologv1.ListIngestersRequest{}))
	if err != nil {
		t.Fatal(err)
	}

	var found *gastrologv1.IngesterInfo
	for _, ing := range resp.Msg.Ingesters {
		if ing.Id == remoteIngID.String() {
			found = ing
		}
	}
	if found == nil {
		t.Fatal("remote ingester not found in list")
	}
	if found.Running {
		t.Error("expected Running=false when no peer stats available")
	}

	// GetIngesterStatus should also work without error.
	statusResp, err := client.GetIngesterStatus(ctx, connect.NewRequest(&gastrologv1.GetIngesterStatusRequest{
		Id: remoteIngID.String(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if statusResp.Msg.Running {
		t.Error("expected Running=false")
	}
	if statusResp.Msg.MessagesIngested != 0 {
		t.Errorf("MessagesIngested = %d, want 0", statusResp.Msg.MessagesIngested)
	}
}

func TestPutIngesterMissingRequiredParam(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	// Syslog requires at least one of udp_addr or tcp_addr.
	_, err := client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
		Config: &gastrologv1.IngesterConfig{
			Name:    "syslog-test",
			Type:    "syslog",
			Enabled: true,
			Params:  map[string]string{},
		},
	}))
	if err == nil {
		t.Fatal("expected error for syslog without addr params")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

func TestPutIngesterListenAddrConflict(t *testing.T) {
	t.Parallel()
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	// Create a syslog ingester on :15140.
	_, err := client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
		Config: &gastrologv1.IngesterConfig{
			Name:    "syslog-a",
			Type:    "syslog",
			Enabled: true,
			Params:  map[string]string{"udp_addr": ":15140"},
		},
	}))
	if err != nil {
		t.Fatalf("first ingester: %v", err)
	}

	// Second ingester on same address should fail.
	_, err = client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
		Config: &gastrologv1.IngesterConfig{
			Name:    "syslog-b",
			Type:    "syslog",
			Enabled: true,
			Params:  map[string]string{"udp_addr": ":15140"},
		},
	}))
	if err == nil {
		t.Fatal("expected error for conflicting listen address")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

func TestPutIngesterListenAddrUpdateSelf(t *testing.T) {
	t.Parallel()
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	// Create a syslog ingester.
	resp, err := client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
		Config: &gastrologv1.IngesterConfig{
			Name:    "syslog-self",
			Type:    "syslog",
			Enabled: true,
			Params:  map[string]string{"udp_addr": ":15141"},
		},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Find the created ingester's ID.
	var ingID string
	for _, ing := range resp.Msg.Config.Ingesters {
		if ing.Name == "syslog-self" {
			ingID = ing.Id
			break
		}
	}
	if ingID == "" {
		t.Fatal("ingester not found in config response")
	}

	// Re-save the same ingester with the same address — should succeed.
	_, err = client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
		Config: &gastrologv1.IngesterConfig{
			Id:      ingID,
			Name:    "syslog-self",
			Type:    "syslog",
			Enabled: true,
			Params:  map[string]string{"udp_addr": ":15141"},
		},
	}))
	if err != nil {
		t.Fatalf("re-save same address should succeed: %v", err)
	}
}

func TestPutIngesterExternalPortConflict(t *testing.T) {
	t.Parallel()
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	// Occupy a UDP port to simulate an external process.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	occupiedAddr := pc.LocalAddr().String()

	// Creating an ingester on that address should fail at trial bind.
	_, err = client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
		Config: &gastrologv1.IngesterConfig{
			Name:    "syslog-external-conflict",
			Type:    "syslog",
			Enabled: true,
			Params:  map[string]string{"udp_addr": occupiedAddr},
		},
	}))
	if err == nil {
		t.Fatal("expected error when external process holds the port")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

func TestGetRouteStats(t *testing.T) {
	t.Parallel()
	client, cfgStore, orch := newConfigTestSetup(t)
	ctx := context.Background()

	// Before any ingestion, stats should be zero.
	resp, err := client.GetRouteStats(ctx, connect.NewRequest(&gastrologv1.GetRouteStatsRequest{}))
	if err != nil {
		t.Fatalf("GetRouteStats: %v", err)
	}
	if resp.Msg.TotalIngested != 0 {
		t.Errorf("expected 0 ingested, got %d", resp.Msg.TotalIngested)
	}
	// No filter set configured yet.
	if resp.Msg.FilterSetActive {
		t.Error("expected filterSetActive=false before routes configured")
	}

	// Configure a vault, filter, and route.
	vaultID := uuid.Must(uuid.NewV7())
	filterID := uuid.Must(uuid.NewV7())
	routeID := uuid.Must(uuid.NewV7())

	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100000),
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVault(vaultID, cm, nil, nil))

	_ = cfgStore.PutVault(ctx, config.VaultConfig{ID: vaultID, Name: "test-vault", Type: "memory", Enabled: true})
	_ = cfgStore.PutFilter(ctx, config.FilterConfig{ID: filterID, Expression: "*"})
	_ = cfgStore.PutRoute(ctx, config.RouteConfig{
		ID: routeID, FilterID: &filterID,
		Destinations: []uuid.UUID{vaultID}, Enabled: true,
	})

	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}

	// Ingest some records.
	for range 5 {
		if err := orch.Ingest(chunk.Record{Raw: []byte("test")}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	resp, err = client.GetRouteStats(ctx, connect.NewRequest(&gastrologv1.GetRouteStatsRequest{}))
	if err != nil {
		t.Fatalf("GetRouteStats: %v", err)
	}
	if resp.Msg.TotalIngested != 5 {
		t.Errorf("expected 5 ingested, got %d", resp.Msg.TotalIngested)
	}
	if resp.Msg.TotalRouted != 5 {
		t.Errorf("expected 5 routed, got %d", resp.Msg.TotalRouted)
	}
	if resp.Msg.TotalDropped != 0 {
		t.Errorf("expected 0 dropped, got %d", resp.Msg.TotalDropped)
	}
	if !resp.Msg.FilterSetActive {
		t.Error("expected filterSetActive=true")
	}
	if len(resp.Msg.VaultStats) != 1 {
		t.Fatalf("expected 1 vault stat, got %d", len(resp.Msg.VaultStats))
	}
	vs := resp.Msg.VaultStats[0]
	if vs.VaultId != vaultID.String() {
		t.Errorf("expected vault %s, got %s", vaultID, vs.VaultId)
	}
	if vs.RecordsMatched != 5 {
		t.Errorf("expected 5 matched, got %d", vs.RecordsMatched)
	}
}

// ---------- Eject route & retention rule validation ----------

func TestPutRouteEjectOnly(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	resp, err := client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
		Config: &gastrologv1.RouteConfig{
			Name:      "eject-route",
			EjectOnly: true,
			Enabled:   true,
		},
	}))
	if err != nil {
		t.Fatalf("PutRoute: %v", err)
	}

	// Verify route is in config and marked eject_only.
	var found bool
	for _, r := range resp.Msg.Config.Routes {
		if r.Name == "eject-route" {
			found = true
			if !r.EjectOnly {
				t.Error("route should be eject_only=true")
			}
		}
	}
	if !found {
		t.Fatal("route not found in config response")
	}
}

func TestPutVaultEjectRetentionRule(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create a retention policy.
	rpResp, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{Name: "eject-retention", MaxChunks: 5},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}
	rpID := rpResp.Msg.Config.RetentionPolicies[0].Id

	// Create an eject-only route.
	routeResp, err := client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
		Config: &gastrologv1.RouteConfig{
			Name:      "eject-target",
			EjectOnly: true,
			Enabled:   true,
		},
	}))
	if err != nil {
		t.Fatalf("PutRoute: %v", err)
	}
	var routeID string
	for _, r := range routeResp.Msg.Config.Routes {
		if r.Name == "eject-target" {
			routeID = r.Id
		}
	}
	if routeID == "" {
		t.Fatal("eject route not found in config")
	}

	// Create vault with eject retention rule.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Name: "eject-vault",
			Type: "memory",
			RetentionRules: []*gastrologv1.RetentionRule{
				{
					RetentionPolicyId: rpID,
					Action:            "eject",
					EjectRouteIds:     []string{routeID},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("PutVault with eject rule: %v", err)
	}
}

func TestPutVaultEjectRuleRequiresEjectOnlyRoute(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create retention policy.
	rpResp, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{Name: "rp-eject-only", MaxChunks: 5},
	}))
	if err != nil {
		t.Fatal(err)
	}
	rpID := rpResp.Msg.Config.RetentionPolicies[0].Id

	// Create a normal route (NOT eject-only).
	routeResp, err := client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
		Config: &gastrologv1.RouteConfig{
			Name:    "normal-route",
			Enabled: true,
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var routeID string
	for _, r := range routeResp.Msg.Config.Routes {
		if r.Name == "normal-route" {
			routeID = r.Id
		}
	}

	// Vault with eject rule referencing a non-eject-only route → error.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Name: "bad-eject-vault",
			Type: "memory",
			RetentionRules: []*gastrologv1.RetentionRule{
				{
					RetentionPolicyId: rpID,
					Action:            "eject",
					EjectRouteIds:     []string{routeID},
				},
			},
		},
	}))
	if err == nil {
		t.Fatal("expected error for eject rule referencing non-eject-only route")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestPutVaultEjectRuleMissingRouteIDs(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	rpResp, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{Name: "rp-missing", MaxChunks: 5},
	}))
	if err != nil {
		t.Fatal(err)
	}
	rpID := rpResp.Msg.Config.RetentionPolicies[0].Id

	// Eject rule with no route IDs → error.
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Name: "missing-routes-vault",
			Type: "memory",
			RetentionRules: []*gastrologv1.RetentionRule{
				{
					RetentionPolicyId: rpID,
					Action:            "eject",
					EjectRouteIds:     []string{},
				},
			},
		},
	}))
	if err == nil {
		t.Fatal("expected error for eject rule with no route IDs")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestDeleteRouteReferencedByEjectVault(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create retention policy + eject-only route + vault referencing it.
	rpResp, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{Name: "rp-ref-integrity", MaxChunks: 5},
	}))
	if err != nil {
		t.Fatal(err)
	}
	rpID := rpResp.Msg.Config.RetentionPolicies[0].Id

	routeResp, err := client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
		Config: &gastrologv1.RouteConfig{Name: "eject-route-ref", EjectOnly: true, Enabled: true},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var routeID string
	for _, r := range routeResp.Msg.Config.Routes {
		if r.Name == "eject-route-ref" {
			routeID = r.Id
		}
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Name: "vault-with-eject",
			Type: "memory",
			RetentionRules: []*gastrologv1.RetentionRule{
				{
					RetentionPolicyId: rpID,
					Action:            "eject",
					EjectRouteIds:     []string{routeID},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Try to delete the route → should fail with FailedPrecondition.
	_, err = client.DeleteRoute(ctx, connect.NewRequest(&gastrologv1.DeleteRouteRequest{
		Id: routeID,
	}))
	if err == nil {
		t.Fatal("expected error deleting route referenced by eject vault")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", connect.CodeOf(err))
	}
}

func TestPutVaultEjectRuleNonexistentRoute(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	rpResp, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{Name: "rp-noexist", MaxChunks: 5},
	}))
	if err != nil {
		t.Fatal(err)
	}
	rpID := rpResp.Msg.Config.RetentionPolicies[0].Id

	// Reference a route that doesn't exist.
	fakeRouteID := uuid.Must(uuid.NewV7()).String()
	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Name: "vault-no-route",
			Type: "memory",
			RetentionRules: []*gastrologv1.RetentionRule{
				{
					RetentionPolicyId: rpID,
					Action:            "eject",
					EjectRouteIds:     []string{fakeRouteID},
				},
			},
		},
	}))
	if err == nil {
		t.Fatal("expected error for eject rule referencing non-existent route")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}
