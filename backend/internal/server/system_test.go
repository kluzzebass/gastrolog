package server_test

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
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
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/ingester/syslog"
	"gastrolog/internal/ingester/tail"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/system/raftfsm"

	"connectrpc.com/connect"
)

// testAfterConfigApply creates a dispatch callback for non-raft test stores.
// It mirrors the production configDispatcher but lives in the test package.
func testAfterConfigApply(orch *orchestrator.Orchestrator, cfgStore system.Store, factories orchestrator.Factories) func(raftfsm.Notification) {
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
			_ = orch.AddIngester(cfg.ID, cfg.Name, cfg.Type, false, ing)
		case raftfsm.NotifyIngesterDeleted:
			_ = orch.RemoveIngester(n.ID)
		}
	}
}

// ensureMemoryTier creates a memory tier in the config store linked to the
// given vault, and returns the tier ID as a string.
func ensureMemoryTier(t *testing.T, cfgStore system.Store, vaultID glid.GLID) string {
	t.Helper()
	tierID := glid.New()
	if err := cfgStore.PutTier(context.Background(), system.TierConfig{
		ID: tierID, Name: "test-tier-" + tierID.String()[:8], Type: system.VaultTypeMemory,
		VaultID: vaultID, Position: 0,
	}); err != nil {
		t.Fatalf("ensureMemoryTier: %v", err)
	}
	return tierID.String()
}

// newConfigTestSetup creates an orchestrator, config vault, and Connect client
// for testing config RPCs.
func newConfigTestSetup(t *testing.T) (gastrologv1connect.SystemServiceClient, system.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore})
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
	client := gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")

	return client, cfgStore, orch
}

func TestDeleteVaultForce(t *testing.T) {
	client, cfgStore, orch := newConfigTestSetup(t)
	ctx := context.Background()

	filterID := glid.New()
	vaultID := glid.New()
	ensureMemoryTier(t, cfgStore, vaultID)

	// Create a filter first, then a vault that uses it.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.Bytes(), Name: "catch-all", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      vaultID.Bytes(),
			Name:    "test-vault",
			Enabled: true,
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
		Id: vaultID.Bytes(),
	}))
	if err == nil {
		t.Fatal("expected error for non-force delete of non-empty vault")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", connect.CodeOf(err))
	}

	// Force delete should succeed.
	_, err = client.DeleteVault(ctx, connect.NewRequest(&gastrologv1.DeleteVaultRequest{
		Id:    vaultID.Bytes(),
		Force: true,
	}))
	if err != nil {
		t.Fatalf("DeleteVault(force=true): %v", err)
	}

	// Verify vault is gone from runtime.
	if cm := orch.ChunkManager(vaultID); cm != nil {
		t.Error("ChunkManager should be nil after force delete")
	}

	// Verify vault is gone from system.
	stored, _ := cfgStore.GetVault(ctx, vaultID)
	if stored != nil {
		t.Error("vault should be removed from config vault")
	}
}

func TestDeleteVaultNotFound(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	nonexistentID := glid.New()
	_, err := client.DeleteVault(ctx, connect.NewRequest(&gastrologv1.DeleteVaultRequest{
		Id: nonexistentID.Bytes(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

// TestPutVaultNestedDirPrevention was removed: directory overlap validation
// has moved from VaultConfig to TierConfig (tiered storage refactor).

func TestPauseResumeVaultRPC(t *testing.T) {
	client, cfgStore, orch := newConfigTestSetup(t)
	ctx := context.Background()

	filterID := glid.New()
	vaultID := glid.New()
	ensureMemoryTier(t, cfgStore, vaultID)

	// Create a filter and a vault.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.Bytes(), Name: "catch-all-2", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      vaultID.Bytes(),
			Name:    "pause-vault",
			Enabled: true,
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Route the catch-all filter to the vault so Ingest delivers records.
	_, err = client.PutRoute(ctx, connect.NewRequest(&gastrologv1.PutRouteRequest{
		Config: &gastrologv1.RouteConfig{
			Id:       glid.New().Bytes(),
			Name:     "test-route",
			FilterId: filterID.Bytes(),
			Destinations: []*gastrologv1.RouteDestination{
				{VaultId: vaultID.Bytes()},
			},
			Enabled: true,
		},
	}))
	if err != nil {
		t.Fatalf("PutRoute: %v", err)
	}

	// Pause the vault via RPC.
	_, err = client.PauseVault(ctx, connect.NewRequest(&gastrologv1.PauseVaultRequest{
		Id: vaultID.Bytes(),
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
	vaultResp, err := vaultClient.GetVault(ctx, connect.NewRequest(&gastrologv1.GetVaultRequest{Id: vaultID.Bytes()}))
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if vaultResp.Msg.Vault.Enabled {
		t.Error("VaultInfo.Enabled should be false after pause")
	}

	// Resume via RPC.
	_, err = client.ResumeVault(ctx, connect.NewRequest(&gastrologv1.ResumeVaultRequest{
		Id: vaultID.Bytes(),
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

	nonexistentID := glid.New()
	_, err := client.PauseVault(ctx, connect.NewRequest(&gastrologv1.PauseVaultRequest{
		Id: nonexistentID.Bytes(),
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

	nonexistentID := glid.New()
	_, err := client.ResumeVault(ctx, connect.NewRequest(&gastrologv1.ResumeVaultRequest{
		Id: nonexistentID.Bytes(),
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

	filterID := glid.New()
	vaultID := glid.New()
	ensureMemoryTier(t, cfgStore, vaultID)

	// Create a filter and vault.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.Bytes(), Name: "catch-all-3", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      vaultID.Bytes(),
			Name:    "persist-vault",
			Enabled: true,
		},
	}))
	if err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Pause and check config persistence.
	_, err = client.PauseVault(ctx, connect.NewRequest(&gastrologv1.PauseVaultRequest{
		Id: vaultID.Bytes(),
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
		Id: vaultID.Bytes(),
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
func newConfigTestSetupWithIngesters(t *testing.T) (gastrologv1connect.SystemServiceClient, system.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore})
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
	client := gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")

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
			Config: &gastrologv1.VaultConfig{Name: "my-vault"},
		}))
		if err != nil {
			t.Fatalf("first PutVault: %v", err)
		}
		_, err = client.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
			Config: &gastrologv1.VaultConfig{Name: "my-vault"},
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
			Config: &gastrologv1.IngesterConfig{Name: "my-ingester", Type: "chatterbox"},
		}))
		if err != nil {
			t.Fatalf("first PutIngester: %v", err)
		}
		_, err = client.PutIngester(ctx, connect.NewRequest(&gastrologv1.PutIngesterRequest{
			Config: &gastrologv1.IngesterConfig{Name: "my-ingester", Type: "chatterbox"},
		}))
		if err == nil {
			t.Fatal("expected error for duplicate ingester name")
		}
		if connect.CodeOf(err) != connect.CodeAlreadyExists {
			t.Fatalf("expected AlreadyExists, got %v", connect.CodeOf(err))
		}
	})

	t.Run("node", func(t *testing.T) {
		nodeA := glid.New()
		nodeB := glid.New()
		_, err := client.PutNodeConfig(ctx, connect.NewRequest(&gastrologv1.PutNodeConfigRequest{
			Config: &gastrologv1.NodeConfig{Id: nodeA.Bytes(), Name: "alpha"},
		}))
		if err != nil {
			t.Fatalf("first PutNodeConfig: %v", err)
		}
		_, err = client.PutNodeConfig(ctx, connect.NewRequest(&gastrologv1.PutNodeConfigRequest{
			Config: &gastrologv1.NodeConfig{Id: nodeB.Bytes(), Name: "alpha"},
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
		id := glid.New()
		_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
			Config: &gastrologv1.FilterConfig{Id: id.Bytes(), Name: "self-update", Expression: "*"},
		}))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		_, err = client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
			Config: &gastrologv1.FilterConfig{Id: id.Bytes(), Name: "self-update", Expression: "level=error"},
		}))
		if err != nil {
			t.Fatalf("update self should be allowed: %v", err)
		}
	})

	t.Run("lookup_names_across_types", func(t *testing.T) {
		// Two lookups with the same name across different types should conflict.
		_, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
			Lookup: &gastrologv1.PutLookupSettings{
				HttpLookups: []*gastrologv1.HTTPLookupEntry{
					{Name: "duped", UrlTemplate: "http://example.com/{ip}"},
				},
				CsvLookups: []*gastrologv1.CSVLookupEntry{
					{Name: "duped", FileId: glid.New().Bytes()},
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
		_, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
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

	nonexistentID := glid.New()
	_, err := client.DeleteIngester(ctx, connect.NewRequest(&gastrologv1.DeleteIngesterRequest{
		Id: nonexistentID.Bytes(),
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

func (m *mockPeerIngesterStats) CollectIngesterAlive(id string) map[string]bool {
	if s := m.stats[id]; s != nil {
		return map[string]bool{"peer": s.Running}
	}
	return nil
}

func TestListIngestersRemoteRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	remoteIngID := glid.New()
	_ = cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: remoteIngID, Name: "remote-syslog", Enabled: true,
	})
	// Simulate remote node reporting alive via Raft store.
	_ = cfgStore.SetIngesterAlive(ctx, remoteIngID, "node-B", true)

	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID: "node-A",
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	client := gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")

	resp, err := client.ListIngesters(ctx, connect.NewRequest(&gastrologv1.ListIngestersRequest{}))
	if err != nil {
		t.Fatal(err)
	}

	var found *gastrologv1.IngesterInfo
	for _, ing := range resp.Msg.Ingesters {
		if glid.FromBytes(ing.Id) == remoteIngID {
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

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	remoteIngID := glid.New()
	_ = cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: remoteIngID, Name: "remote-syslog", Enabled: true,
	})
	// Simulate remote node reporting alive via Raft store.
	_ = cfgStore.SetIngesterAlive(ctx, remoteIngID, "node-B", true)

	peerStats := &mockPeerIngesterStats{stats: map[string]*gastrologv1.IngesterNodeStats{
		remoteIngID.String(): {
			Id: remoteIngID.Bytes(), Running: true,
			MessagesIngested: 42, BytesIngested: 1024, Errors: 1,
		},
	}}

	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID:            "node-A",
		PeerIngesterStats: peerStats,
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	client := gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")

	resp, err := client.GetIngesterStatus(ctx, connect.NewRequest(&gastrologv1.GetIngesterStatusRequest{
		Id: remoteIngID.Bytes(),
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

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	ingID := glid.New()
	_ = cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: ingID, Name: "local-syslog", Enabled: true,
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
	client := gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")

	resp, err := client.GetIngesterStatus(ctx, connect.NewRequest(&gastrologv1.GetIngesterStatusRequest{
		Id: ingID.Bytes(),
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

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}

	remoteIngID := glid.New()
	_ = cfgStore.PutIngester(ctx, system.IngesterConfig{
		ID: remoteIngID, Name: "remote-syslog", Enabled: true,
	})

	// No PeerIngesterStats provided (single-node mode).
	srv := server.New(orch, cfgStore, orchestrator.Factories{}, nil, server.Config{
		NodeID: "node-A",
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	client := gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")

	resp, err := client.ListIngesters(ctx, connect.NewRequest(&gastrologv1.ListIngestersRequest{}))
	if err != nil {
		t.Fatal(err)
	}

	var found *gastrologv1.IngesterInfo
	for _, ing := range resp.Msg.Ingesters {
		if glid.FromBytes(ing.Id) == remoteIngID {
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
		Id: remoteIngID.Bytes(),
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
	var ingID []byte
	for _, ing := range resp.Msg.System.Ingesters {
		if ing.Name == "syslog-self" {
			ingID = ing.Id
			break
		}
	}
	if len(ingID) == 0 {
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

func TestGetIngesterDefaultsModes(t *testing.T) {
	t.Parallel()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
		IngesterTypes: map[string]orchestrator.IngesterRegistration{
			"syslog": {Factory: syslog.NewFactory(), Defaults: syslog.ParamDefaults, ListenAddrs: syslog.ListenAddrs},
			"tail":   {Factory: tail.NewFactory(), Defaults: tail.ParamDefaults},
			"kafka":  {Factory: nil, Defaults: nil, SingletonSupported: true}, // non-listener with singleton support
		},
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{})
	handler := srv.Handler()
	httpClient := &http.Client{Transport: &embeddedTransport{handler: handler}}
	client := gastrologv1connect.NewSystemServiceClient(httpClient, "http://test")
	ctx := context.Background()

	resp, err := client.GetIngesterDefaults(ctx, connect.NewRequest(&gastrologv1.GetIngesterDefaultsRequest{}))
	if err != nil {
		t.Fatalf("GetIngesterDefaults: %v", err)
	}

	// syslog has ListenAddrs → passive.
	syslogDef := resp.Msg.Types["syslog"]
	if syslogDef == nil {
		t.Fatal("expected syslog in types")
	}
	if syslogDef.Mode != gastrologv1.IngesterMode_INGESTER_MODE_PASSIVE {
		t.Errorf("syslog: expected PASSIVE, got %v", syslogDef.Mode)
	}
	if syslogDef.SingletonSupported {
		t.Errorf("syslog: expected SingletonSupported=false, got true")
	}

	// tail has no ListenAddrs → active. Not singleton-supported.
	tailDef := resp.Msg.Types["tail"]
	if tailDef == nil {
		t.Fatal("expected tail in types")
	}
	if tailDef.Mode != gastrologv1.IngesterMode_INGESTER_MODE_ACTIVE {
		t.Errorf("tail: expected ACTIVE, got %v", tailDef.Mode)
	}
	if tailDef.SingletonSupported {
		t.Errorf("tail: expected SingletonSupported=false, got true")
	}

	// kafka: non-listener with singleton support.
	kafkaDef := resp.Msg.Types["kafka"]
	if kafkaDef == nil {
		t.Fatal("expected kafka in types")
	}
	if !kafkaDef.SingletonSupported {
		t.Errorf("kafka: expected SingletonSupported=true, got false")
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
	vaultID := glid.New()
	filterID := glid.New()
	routeID := glid.New()

	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100000),
	})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, cm, nil, nil))

	_ = cfgStore.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "test-vault", Enabled: true})
	_ = cfgStore.PutFilter(ctx, system.FilterConfig{ID: filterID, Expression: "*"})
	_ = cfgStore.PutRoute(ctx, system.RouteConfig{
		ID: routeID, FilterID: &filterID,
		Destinations: []glid.GLID{vaultID}, Enabled: true,
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
	if glid.FromBytes(vs.VaultId) != vaultID {
		t.Errorf("expected vault %s, got %s", vaultID, glid.FromBytes(vs.VaultId))
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
	for _, r := range resp.Msg.System.Routes {
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

// TestPutVaultEjectRetentionRule, TestPutVaultEjectRuleRequiresEjectOnlyRoute,
// TestPutVaultEjectRuleMissingRouteIDs, TestDeleteRouteReferencedByEjectVault,
// and TestPutVaultEjectRuleNonexistentRoute were removed: retention rules
// (including eject rules) have moved from VaultConfig to TierConfig as part of
// the tiered storage refactor. These validations will be tested when the
// PutTier RPC is implemented.

// Remaining eject tests removed — see comment above.

// ---------------------------------------------------------------------------
// DeleteLookup tests
// ---------------------------------------------------------------------------

func TestDeleteLookupHTTP(t *testing.T) {
	t.Parallel()
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create an HTTP lookup via PutLookupSettings.
	_, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
		Lookup: &gastrologv1.PutLookupSettings{
			HttpLookups: []*gastrologv1.HTTPLookupEntry{
				{Name: "weather-api", UrlTemplate: "http://weather.example.com/{city}"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("PutLookupSettings (create lookup): %v", err)
	}

	// Verify the lookup exists in the config store.
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("LoadServerSettings: %v", err)
	}
	if len(ss.Lookup.HTTPLookups) != 1 {
		t.Fatalf("expected 1 HTTP lookup, got %d", len(ss.Lookup.HTTPLookups))
	}
	if ss.Lookup.HTTPLookups[0].Name != "weather-api" {
		t.Fatalf("lookup name = %q, want %q", ss.Lookup.HTTPLookups[0].Name, "weather-api")
	}

	// Delete the lookup by name.
	_, err = client.DeleteLookup(ctx, connect.NewRequest(&gastrologv1.DeleteLookupRequest{
		Name: "weather-api",
	}))
	if err != nil {
		t.Fatalf("DeleteLookup: %v", err)
	}

	// Verify the lookup is gone.
	ss, err = cfgStore.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("LoadServerSettings after delete: %v", err)
	}
	if len(ss.Lookup.HTTPLookups) != 0 {
		t.Fatalf("expected 0 HTTP lookups after delete, got %d", len(ss.Lookup.HTTPLookups))
	}
}

func TestDeleteLookupNotFound(t *testing.T) {
	t.Parallel()
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.DeleteLookup(ctx, connect.NewRequest(&gastrologv1.DeleteLookupRequest{
		Name: "nonexistent-lookup",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent lookup")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestDeleteLookupEmptyName(t *testing.T) {
	t.Parallel()
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.DeleteLookup(ctx, connect.NewRequest(&gastrologv1.DeleteLookupRequest{
		Name: "",
	}))
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestDeleteLookupCSV(t *testing.T) {
	t.Parallel()
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create a CSV lookup.
	_, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
		Lookup: &gastrologv1.PutLookupSettings{
			CsvLookups: []*gastrologv1.CSVLookupEntry{
				{Name: "hosts-csv", FileId: glid.New().Bytes()},
			},
		},
	}))
	if err != nil {
		t.Fatalf("PutLookupSettings (create CSV lookup): %v", err)
	}

	// Delete it.
	_, err = client.DeleteLookup(ctx, connect.NewRequest(&gastrologv1.DeleteLookupRequest{
		Name: "hosts-csv",
	}))
	if err != nil {
		t.Fatalf("DeleteLookup CSV: %v", err)
	}

	// Verify gone.
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("LoadServerSettings after CSV delete: %v", err)
	}
	if len(ss.Lookup.CSVLookups) != 0 {
		t.Fatalf("expected 0 CSV lookups after delete, got %d", len(ss.Lookup.CSVLookups))
	}
}

func TestDeleteLookupOnlyRemovesTarget(t *testing.T) {
	t.Parallel()
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create two HTTP lookups.
	_, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
		Lookup: &gastrologv1.PutLookupSettings{
			HttpLookups: []*gastrologv1.HTTPLookupEntry{
				{Name: "api-alpha", UrlTemplate: "http://alpha.example.com/{id}"},
				{Name: "api-beta", UrlTemplate: "http://beta.example.com/{id}"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("PutLookupSettings (create two lookups): %v", err)
	}

	// Delete only one.
	_, err = client.DeleteLookup(ctx, connect.NewRequest(&gastrologv1.DeleteLookupRequest{
		Name: "api-alpha",
	}))
	if err != nil {
		t.Fatalf("DeleteLookup: %v", err)
	}

	// Verify api-beta survives.
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("LoadServerSettings after partial delete: %v", err)
	}
	if len(ss.Lookup.HTTPLookups) != 1 {
		t.Fatalf("expected 1 HTTP lookup remaining, got %d", len(ss.Lookup.HTTPLookups))
	}
	if ss.Lookup.HTTPLookups[0].Name != "api-beta" {
		t.Fatalf("surviving lookup = %q, want %q", ss.Lookup.HTTPLookups[0].Name, "api-beta")
	}
}

func TestDeleteLookupIdempotentSecondCallFails(t *testing.T) {
	t.Parallel()
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create an HTTP lookup.
	_, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
		Lookup: &gastrologv1.PutLookupSettings{
			HttpLookups: []*gastrologv1.HTTPLookupEntry{
				{Name: "once-only", UrlTemplate: "http://once.example.com/{x}"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("PutLookupSettings: %v", err)
	}

	// First delete succeeds.
	_, err = client.DeleteLookup(ctx, connect.NewRequest(&gastrologv1.DeleteLookupRequest{
		Name: "once-only",
	}))
	if err != nil {
		t.Fatalf("first DeleteLookup: %v", err)
	}

	// Second delete returns NotFound.
	_, err = client.DeleteLookup(ctx, connect.NewRequest(&gastrologv1.DeleteLookupRequest{
		Name: "once-only",
	}))
	if err == nil {
		t.Fatal("expected error on second delete")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound on second delete, got %v", connect.CodeOf(err))
	}
}

// TestPutTierRejectsCloudServiceIDChange pins the tier-shape immutability
// rule: cloud_service_id is fixed at tier creation. Mutating it would
// either orphan cloud blobs (cloud→local) or trigger an implicit
// mass-upload (local→cloud); neither is safe to do silently. To migrate,
// users must create a new tier and route data via retention rules. See
// gastrolog-4k5mg.
func TestPutTierRejectsCloudServiceIDChange(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	vaultID := glid.New()
	if err := cfgStore.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "v", Enabled: true}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	// Create a local-only file tier first.
	tierID := glid.New()
	if _, err := client.PutTier(ctx, connect.NewRequest(&gastrologv1.PutTierRequest{
		Config: &gastrologv1.TierConfig{
			Id:                tierID.ToProto(),
			Name:              "tier",
			Type:              gastrologv1.TierType_TIER_TYPE_FILE,
			VaultId:           vaultID.ToProto(),
			Position:          0,
			StorageClass:      1,
			ReplicationFactor: 1,
		},
	})); err != nil {
		t.Fatalf("PutTier (initial local-only): %v", err)
	}

	// Attempt to flip the same tier to cloud-backed by setting
	// cloud_service_id. Must reject — even though the cloud service may
	// be valid, the binding cannot be added in-place.
	csID := glid.New()
	_, err := client.PutTier(ctx, connect.NewRequest(&gastrologv1.PutTierRequest{
		Config: &gastrologv1.TierConfig{
			Id:                tierID.ToProto(),
			Name:              "tier",
			Type:              gastrologv1.TierType_TIER_TYPE_FILE,
			VaultId:           vaultID.ToProto(),
			Position:          0,
			CloudServiceId:    csID.ToProto(),
			StorageClass:      1,
			ReplicationFactor: 1,
		},
	}))
	if err == nil {
		t.Fatal("expected PutTier to reject cloud_service_id change on existing tier")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v: %v", connect.CodeOf(err), err)
	}
}

// TestPutTierAcceptsUnchangedCloudServiceID verifies the immutability
// guard doesn't fire on no-op updates: re-putting a tier with the same
// (or no) cloud_service_id must succeed, so users can edit other tier
// fields without triggering false positives.
func TestPutTierAcceptsUnchangedCloudServiceID(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	vaultID := glid.New()
	if err := cfgStore.PutVault(ctx, system.VaultConfig{ID: vaultID, Name: "v", Enabled: true}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}

	tierID := glid.New()
	mk := func(name string) *gastrologv1.PutTierRequest {
		return &gastrologv1.PutTierRequest{
			Config: &gastrologv1.TierConfig{
				Id:                tierID.ToProto(),
				Name:              name,
				Type:              gastrologv1.TierType_TIER_TYPE_FILE,
				VaultId:           vaultID.ToProto(),
				Position:          0,
				StorageClass:      1,
				ReplicationFactor: 1,
			},
		}
	}
	if _, err := client.PutTier(ctx, connect.NewRequest(mk("first"))); err != nil {
		t.Fatalf("PutTier (initial): %v", err)
	}
	// Re-put with a different name but unchanged cloud_service_id (still
	// empty). Must succeed.
	if _, err := client.PutTier(ctx, connect.NewRequest(mk("renamed"))); err != nil {
		t.Fatalf("PutTier (rename, no cloud_service_id change): %v", err)
	}
}
