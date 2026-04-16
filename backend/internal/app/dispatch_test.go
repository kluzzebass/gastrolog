package app

import (
	"gastrolog/internal/glid"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"


	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// captureHandler is an slog.Handler that records every log record.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler       { return h }

func (h *captureHandler) hasMessage(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *captureHandler) reset() {
	h.mu.Lock()
	h.records = h.records[:0]
	h.mu.Unlock()
}

// mockOrch implements orchActions with configurable error returns.
type mockOrch struct {
	vaults     []glid.GLID
	vaultTypes map[glid.GLID]string
	ingesters  []glid.GLID

	addVaultErr        error
	forceRemoveErr     error
	reloadFiltersErr   error
	reloadRotationErr  error
	reloadRetentionErr error
	disableVaultErr    error
	enableVaultErr     error
	drainVaultErr      error
	cancelDrainErr     error
	isDraining         bool
	addIngesterErr     error
	removeIngesterErr  error
	updateMaxJobsErr   error

	drainCalls          []glid.GLID // IDs passed to DrainVault
	cancelDrainIDs      []glid.GLID // IDs passed to CancelDrain
	forceRemoveIDs      []glid.GLID // IDs passed to ForceRemoveVault
	unregisterIDs       []glid.GLID // IDs passed to UnregisterVault
	unregisterErr       error
	removeIngesterIDs   []glid.GLID // IDs passed to RemoveIngester
	reloadFiltersCalls  int         // number of ReloadFilters calls

	// Tier drain tracking.
	tierDrainCalls      []glid.GLID                                                  // tier IDs passed to DrainTier
	removeTierCalls     [][2]glid.GLID                                               // [vaultID, tierID] pairs passed to RemoveTierFromVault
	localTierExported   func(vaultID, tierID glid.GLID) *orchestrator.TierInstance   // configurable return
}

func (m *mockOrch) ListVaults() []glid.GLID    { return m.vaults }
func (m *mockOrch) ListIngesters() []glid.GLID { return m.ingesters }
func (m *mockOrch) VaultType(id glid.GLID) string {
	if m.vaultTypes != nil {
		return m.vaultTypes[id]
	}
	return ""
}

func (m *mockOrch) AddVault(context.Context, system.VaultConfig, orchestrator.Factories) error {
	return m.addVaultErr
}
func (m *mockOrch) ReloadFilters(context.Context) error {
	m.reloadFiltersCalls++
	return m.reloadFiltersErr
}
func (m *mockOrch) ReloadRotationPolicies(context.Context) error { return m.reloadRotationErr }
func (m *mockOrch) ReloadRetentionPolicies(context.Context) error { return m.reloadRetentionErr }
func (m *mockOrch) DisableVault(glid.GLID) error                 { return m.disableVaultErr }
func (m *mockOrch) EnableVault(glid.GLID) error                  { return m.enableVaultErr }
func (m *mockOrch) ForceRemoveVault(id glid.GLID) error {
	m.forceRemoveIDs = append(m.forceRemoveIDs, id)
	return m.forceRemoveErr
}
func (m *mockOrch) RemoveTierFromVault(vaultID, tierID glid.GLID) bool {
	m.removeTierCalls = append(m.removeTierCalls, [2]glid.GLID{vaultID, tierID})
	return true
}
func (m *mockOrch) DrainTier(_ context.Context, _, tierID glid.GLID, _ orchestrator.TierDrainMode, _ string) error {
	m.tierDrainCalls = append(m.tierDrainCalls, tierID)
	return nil
}
func (m *mockOrch) UnregisterVault(id glid.GLID) error {
	m.unregisterIDs = append(m.unregisterIDs, id)
	return m.unregisterErr
}
func (m *mockOrch) HasMissingTiers(_ glid.GLID, _ []glid.GLID) bool { return false }
func (m *mockOrch) LocalTierIDs(_ glid.GLID) []glid.GLID             { return nil }
func (m *mockOrch) AddTierToVault(_ context.Context, _, _ glid.GLID, _ orchestrator.Factories) error {
	return nil
}
func (m *mockOrch) DrainVault(_ context.Context, id glid.GLID, _ string) error {
	m.drainCalls = append(m.drainCalls, id)
	return m.drainVaultErr
}
func (m *mockOrch) IsDraining(glid.GLID) bool                    { return m.isDraining }
func (m *mockOrch) CancelDrain(_ context.Context, id glid.GLID) error {
	m.cancelDrainIDs = append(m.cancelDrainIDs, id)
	return m.cancelDrainErr
}
func (m *mockOrch) RemoveIngester(id glid.GLID) error {
	m.removeIngesterIDs = append(m.removeIngesterIDs, id)
	return m.removeIngesterErr
}
func (m *mockOrch) UpdateMaxConcurrentJobs(int) error            { return m.updateMaxJobsErr }

func (m *mockOrch) AddIngester(glid.GLID, string, string, bool, orchestrator.Ingester) error {
	return m.addIngesterErr
}

// stubCfgStore implements system.Store with configurable returns for the
// methods the dispatcher reads. The nil-embedded interface panics on
// any other method call — a deliberate test safety net.
type stubCfgStore struct {
	system.Store // nil embed — panics on uncalled methods

	vault       *system.VaultConfig
	vaultErr    error
	vaultList   []system.VaultConfig
	vaultListErr error
	tiers       []system.TierConfig
	ingester    *system.IngesterConfig
	ingesterErr error
	settings    system.ServerSettings
	settingsErr error
	cfg         *system.Config
	loadErr     error

	ingesterAssignments map[glid.GLID]string // ingester ID → assigned node
}

func (s *stubCfgStore) GetVault(context.Context, glid.GLID) (*system.VaultConfig, error) {
	return s.vault, s.vaultErr
}
func (s *stubCfgStore) ListVaults(context.Context) ([]system.VaultConfig, error) {
	return s.vaultList, s.vaultListErr
}
func (s *stubCfgStore) GetIngester(context.Context, glid.GLID) (*system.IngesterConfig, error) {
	return s.ingester, s.ingesterErr
}
func (s *stubCfgStore) LoadServerSettings(context.Context) (system.ServerSettings, error) {
	return s.settings, s.settingsErr
}
func (s *stubCfgStore) Load(context.Context) (*system.System, error) {
	if s.cfg == nil { return nil, s.loadErr }; return &system.System{Config: *s.cfg}, s.loadErr
}
func (s *stubCfgStore) ListTiers(context.Context) ([]system.TierConfig, error) {
	if len(s.tiers) > 0 {
		return s.tiers, nil
	}
	if s.cfg != nil {
		return s.cfg.Tiers, nil
	}
	return nil, nil
}

func (s *stubCfgStore) GetIngesterAssignment(_ context.Context, id glid.GLID) (string, error) {
	if s.ingesterAssignments != nil {
		return s.ingesterAssignments[id], nil
	}
	return "", nil
}
func (s *stubCfgStore) GetIngesterCheckpoint(context.Context, glid.GLID) ([]byte, error) {
	return nil, nil
}

// noopIngester satisfies orchestrator.Ingester.
type noopIngester struct{}

func (noopIngester) Run(context.Context, chan<- orchestrator.IngestMessage) error { return nil }

// newTestDispatcher creates a configDispatcher wired to the given mocks.
func newTestDispatcher(orch orchActions, store system.Store, h *captureHandler) *configDispatcher {
	return &configDispatcher{
		orch:        orch,
		cfgStore:    store,
		factories:   orchestrator.Factories{IngesterTypes: map[string]orchestrator.IngesterRegistration{}},
		localNodeID: "local",
		logger:      slog.New(h),
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandle_NilOrch(t *testing.T) {
	h := &captureHandler{}
	d := newTestDispatcher(nil, &stubCfgStore{}, h)

	// Should not panic; bootstrap phase silently returns.
	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: glid.New()})

	if h.count() != 0 {
		t.Fatal("expected no log output when orch is nil")
	}
}

func TestHandle_VaultPut(t *testing.T) {
	id := glid.New()

	t.Run("store_error", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			vaultErr: errors.New("db down"),
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if !h.hasMessage("dispatch: read vault config") {
			t.Fatal("expected error log for store read failure")
		}
	})

	t.Run("store_returns_nil", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			vault: nil, // nil config, no error
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if !h.hasMessage("dispatch: read vault config") {
			t.Fatal("expected error log for nil vault config")
		}
	})

	// remote_node_reloads_filters and cloud_vault_reassignment_skips_drain
	// were removed: they tested the concept of NodeID-based remote vault
	// assignment which no longer exists. Remote vault routing will be
	// reintroduced via tier primary election in a future issue.

	t.Run("unscoped_node_not_skipped", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{} // no error → AddVault succeeds
		tierID := glid.New()
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &system.VaultConfig{ID: id, Enabled: true},
			tiers: []system.TierConfig{{ID: tierID, VaultID: id, Position: 0}},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if h.count() != 0 {
			t.Fatal("unexpected error logs for unscoped vault add")
		}
	})

	t.Run("add_vault_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{addVaultErr: errors.New("factory boom")}
		tierID := glid.New()
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &system.VaultConfig{ID: id, Enabled: true},
			tiers: []system.TierConfig{{ID: tierID, VaultID: id, Position: 0}},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if !h.hasMessage("dispatch: add vault") {
			t.Fatal("expected error log for AddVault failure")
		}
	})

	t.Run("existing_vault_reload_errors", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults:             []glid.GLID{id}, // vault already registered
			reloadFiltersErr:   errors.New("f"),
			reloadRotationErr:  errors.New("r"),
			reloadRetentionErr: errors.New("ret"),
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &system.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if !h.hasMessage("dispatch: reload filters") {
			t.Error("expected reload filters error")
		}
		if !h.hasMessage("dispatch: reload rotation policies") {
			t.Error("expected reload rotation policies error")
		}
		if !h.hasMessage("dispatch: reload retention policies") {
			t.Error("expected reload retention policies error")
		}
	})
}

func TestHandle_VaultDeleted(t *testing.T) {
	id := glid.New()

	t.Run("force_remove_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{forceRemoveErr: errors.New("storage busy")}
		d := newTestDispatcher(mo, &stubCfgStore{}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultDeleted, ID: id})

		if !h.hasMessage("dispatch: force remove vault") {
			t.Fatal("expected error log for ForceRemoveVault failure")
		}
	})

	t.Run("not_found_suppressed", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{forceRemoveErr: orchestrator.ErrVaultNotFound}
		d := newTestDispatcher(mo, &stubCfgStore{}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultDeleted, ID: id})

		if h.hasMessage("dispatch: force remove vault") {
			t.Fatal("ErrVaultNotFound should be silently ignored")
		}
	})

	t.Run("remote_node_dir_skipped", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)

		d.Handle(raftfsm.Notification{
			Kind:   raftfsm.NotifyVaultDeleted,
			ID:     id,
			NodeID: "other-node",
			Dir:    "/tmp/should-not-be-removed",
		})

		// No os.RemoveAll should be called for a remote node's directory.
		if h.hasMessage("dispatch: remove vault directory") {
			t.Fatal("should not attempt to remove remote node's directory")
		}
	})
}

func TestHandle_IngesterPut(t *testing.T) {
	id := glid.New()

	t.Run("store_error", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			ingesterErr: errors.New("db down"),
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: read ingester config") {
			t.Fatal("expected error log for store read failure")
		}
	})

	t.Run("remote_node_not_registered_skipped", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{} // ingester not locally registered
		d := newTestDispatcher(mo, &stubCfgStore{
			ingester: &system.IngesterConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if len(mo.removeIngesterIDs) != 0 {
			t.Fatal("should not call RemoveIngester when not locally registered")
		}
	})

	t.Run("reassignment_stops_local_ingester", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			ingesters: []glid.GLID{id}, // locally registered
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingester: &system.IngesterConfig{ID: id, Name: "chatterbox", Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if len(mo.removeIngesterIDs) != 1 || mo.removeIngesterIDs[0] != id {
			t.Fatalf("expected RemoveIngester(%s) for reassigned ingester, got %v", id, mo.removeIngesterIDs)
		}
		if h.hasMessage("dispatch: add ingester") {
			t.Fatal("should not add ingester on a node it's leaving")
		}
	})

	t.Run("unknown_type", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			ingester: &system.IngesterConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: unknown ingester type") {
			t.Fatal("expected error log for unknown ingester type")
		}
	})

	t.Run("factory_error", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			ingester: &system.IngesterConfig{ID: id, Type: "test", Enabled: true},
		}, h)
		d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{Factory: func(glid.GLID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
			return nil, errors.New("bad params")
		}}

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: create ingester") {
			t.Fatal("expected error log for factory failure")
		}
	})

	t.Run("add_ingester_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{addIngesterErr: errors.New("duplicate")}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingester: &system.IngesterConfig{ID: id, Type: "test", Enabled: true},
		}, h)
		d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{Factory: func(glid.GLID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
			return noopIngester{}, nil
		}}

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: add ingester") {
			t.Fatal("expected error log for AddIngester failure")
		}
	})

	t.Run("existing_ingester_remove_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			ingesters:         []glid.GLID{id},
			removeIngesterErr: errors.New("stuck"),
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingester: &system.IngesterConfig{ID: id, Enabled: true},
		}, h)
		d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{Factory: func(glid.GLID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
			return noopIngester{}, nil
		}}

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: remove existing ingester") {
			t.Fatal("expected error log for RemoveIngester failure during update")
		}
	})

	t.Run("disabled_ingester_skips_add", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{addIngesterErr: errors.New("should not be called")}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingester: &system.IngesterConfig{ID: id, Enabled: false},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if h.hasMessage("dispatch: add ingester") {
			t.Fatal("disabled ingester should not be added")
		}
	})
}

func TestHandle_IngesterDeleted(t *testing.T) {
	id := glid.New()

	t.Run("remove_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{removeIngesterErr: errors.New("stuck")}
		d := newTestDispatcher(mo, &stubCfgStore{}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterDeleted, ID: id})

		if !h.hasMessage("dispatch: remove ingester") {
			t.Fatal("expected error log for RemoveIngester failure")
		}
	})

	t.Run("not_found_suppressed", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{removeIngesterErr: orchestrator.ErrIngesterNotFound}
		d := newTestDispatcher(mo, &stubCfgStore{}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterDeleted, ID: id})

		if h.hasMessage("dispatch: remove ingester") {
			t.Fatal("ErrIngesterNotFound should be silently ignored")
		}
	})
}

func TestHandle_ReloadErrors(t *testing.T) {
	tests := []struct {
		name    string
		kind    raftfsm.NotifyKind
		orch    *mockOrch
		wantMsg string
	}{
		{
			name:    "filter_put",
			kind:    raftfsm.NotifyFilterPut,
			orch:    &mockOrch{reloadFiltersErr: errors.New("f")},
			wantMsg: "dispatch: reload filters",
		},
		{
			name:    "filter_deleted",
			kind:    raftfsm.NotifyFilterDeleted,
			orch:    &mockOrch{reloadFiltersErr: errors.New("f")},
			wantMsg: "dispatch: reload filters",
		},
		{
			name:    "route_put",
			kind:    raftfsm.NotifyRoutePut,
			orch:    &mockOrch{reloadFiltersErr: errors.New("f")},
			wantMsg: "dispatch: reload filters",
		},
		{
			name:    "rotation_policy_put",
			kind:    raftfsm.NotifyRotationPolicyPut,
			orch:    &mockOrch{reloadRotationErr: errors.New("r")},
			wantMsg: "dispatch: reload rotation policies",
		},
		{
			name:    "retention_policy_deleted",
			kind:    raftfsm.NotifyRetentionPolicyDeleted,
			orch:    &mockOrch{reloadRetentionErr: errors.New("r")},
			wantMsg: "dispatch: reload retention policies",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &captureHandler{}
			d := newTestDispatcher(tc.orch, &stubCfgStore{}, h)

			d.Handle(raftfsm.Notification{Kind: tc.kind})

			if !h.hasMessage(tc.wantMsg) {
				t.Fatalf("expected %q in logs", tc.wantMsg)
			}
		})
	}
}

func TestHandle_SettingPut(t *testing.T) {
	t.Run("non_server_key_ignored", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			settingsErr: errors.New("should not be called"),
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "auth"})

		if h.count() != 0 {
			t.Fatal("non-server key should be ignored")
		}
	})

	t.Run("load_settings_error", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			settingsErr: errors.New("corrupt"),
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

		if !h.hasMessage("dispatch: load server settings") {
			t.Fatal("expected error log for LoadServerSettings failure")
		}
	})

	t.Run("update_max_jobs_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{updateMaxJobsErr: errors.New("invalid")}
		d := newTestDispatcher(mo, &stubCfgStore{
			settings: system.ServerSettings{
				Scheduler: system.SchedulerConfig{MaxConcurrentJobs: 8},
			},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

		if !h.hasMessage("dispatch: update max concurrent jobs") {
			t.Fatal("expected error log for UpdateMaxConcurrentJobs failure")
		}
	})

	t.Run("zero_max_jobs_skipped", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{updateMaxJobsErr: errors.New("should not be called")}
		d := newTestDispatcher(mo, &stubCfgStore{
			settings: system.ServerSettings{}, // MaxConcurrentJobs = 0
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

		if h.count() != 0 {
			t.Fatal("zero MaxConcurrentJobs should not trigger update")
		}
	})
}

func TestHandle_ClusterTLSPut(t *testing.T) {
	t.Run("nil_cluster_tls_skipped", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			loadErr: errors.New("should not be called"),
		}, h)
		d.clusterTLS = nil

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyClusterTLSPut})

		if h.count() != 0 {
			t.Fatal("nil clusterTLS should skip TLS reload")
		}
	})

	t.Run("load_config_error", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			loadErr: errors.New("corrupt"),
		}, h)
		// Non-nil clusterTLS to enter the handler.
		d.clusterTLS = &cluster.ClusterTLS{}

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyClusterTLSPut})

		if !h.hasMessage("dispatch: read cluster TLS for reload") {
			t.Fatal("expected error log for Load failure")
		}
	})

	t.Run("nil_cluster_tls_in_config", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			cfg: &system.Config{}, // ClusterTLS is nil
		}, h)
		d.clusterTLS = &cluster.ClusterTLS{}

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyClusterTLSPut})

		if !h.hasMessage("dispatch: read cluster TLS for reload") {
			t.Fatal("expected error log when ClusterTLS is nil in config")
		}
	})
}

func TestHandle_ConfigSignal(t *testing.T) {
	t.Run("fires_on_vault_put", func(t *testing.T) {
		h := &captureHandler{}
		sig := notify.NewSignal()
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
		d.configSignal = sig

		ch := sig.C()
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: glid.New()})

		select {
		case <-ch:
			// expected
		default:
			t.Fatal("configSignal should fire on vault put")
		}
	})

	t.Run("fires_on_ingester_put", func(t *testing.T) {
		h := &captureHandler{}
		sig := notify.NewSignal()
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
		d.configSignal = sig

		ch := sig.C()
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: glid.New()})

		select {
		case <-ch:
		default:
			t.Fatal("configSignal should fire on ingester put")
		}
	})

	t.Run("suppressed_on_cluster_tls", func(t *testing.T) {
		h := &captureHandler{}
		sig := notify.NewSignal()
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
		d.configSignal = sig
		d.clusterTLS = nil // so handleClusterTLSPut returns early

		ch := sig.C()
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyClusterTLSPut})

		select {
		case <-ch:
			t.Fatal("configSignal should NOT fire on ClusterTLS mutation")
		default:
			// expected
		}
	})

	t.Run("fires_on_setting_put", func(t *testing.T) {
		h := &captureHandler{}
		sig := notify.NewSignal()
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
		d.configSignal = sig

		ch := sig.C()
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "other"})

		select {
		case <-ch:
		default:
			t.Fatal("configSignal should fire on setting put")
		}
	})

	t.Run("fires_on_node_config", func(t *testing.T) {
		h := &captureHandler{}
		sig := notify.NewSignal()
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
		d.configSignal = sig

		ch := sig.C()
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyNodeConfigPut})

		select {
		case <-ch:
		default:
			t.Fatal("configSignal should fire on node config put")
		}
	})
}

func TestHandle_VaultDrain(t *testing.T) {
	id := glid.New()

	// reassign_triggers_drain and drain_error_logged were removed:
	// they tested NodeID-based vault reassignment which no longer exists.
	// With tiered storage, handleVaultPut no longer calls maybeStartDrain.

	t.Run("already_draining_cancels", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults:     []glid.GLID{id},
			isDraining: true,
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &system.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		// Draining vault on put → cancel drain and apply changes.
		if len(mo.cancelDrainIDs) != 1 || mo.cancelDrainIDs[0] != id {
			t.Fatalf("expected CancelDrain(%s), got %v", id, mo.cancelDrainIDs)
		}
	})

	t.Run("reassign_back_cancels_drain", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults:     []glid.GLID{id},
			isDraining: true,
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &system.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if len(mo.cancelDrainIDs) != 1 || mo.cancelDrainIDs[0] != id {
			t.Fatalf("expected CancelDrain(%s), got %v", id, mo.cancelDrainIDs)
		}
	})

	t.Run("cancel_drain_error_logged", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults:         []glid.GLID{id},
			isDraining:     true,
			cancelDrainErr: errors.New("boom"),
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &system.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if !h.hasMessage("dispatch: cancel drain") {
			t.Fatal("expected error log for CancelDrain failure")
		}
	})
}

func (m *mockOrch) FindLocalTierExported(vaultID, tierID glid.GLID) *orchestrator.TierInstance {
	if m.localTierExported != nil {
		return m.localTierExported(vaultID, tierID)
	}
	return nil
}

func (m *mockOrch) StopTierLeaderLoop(glid.GLID)            {}
func (m *mockOrch) SetDesiredTierLeader(glid.GLID, string) {}

// TestHandleTierDeleted_DrainOnlyOnLeader verifies that when a tier is deleted
// with drain=true, only the config leader for that tier initiates a drain.
// Follower nodes should immediately remove their local tier instance.
func TestHandleTierDeleted_DrainOnlyOnLeader(t *testing.T) {
	vaultID := glid.New()
	tierID := glid.New()

	t.Run("leader_drains", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults: []glid.GLID{vaultID},
			localTierExported: func(_, _ glid.GLID) *orchestrator.TierInstance {
				return &orchestrator.TierInstance{
					TierID:     tierID,
					IsFollower: false, // this node is the leader
				}
			},
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vaultList: []system.VaultConfig{{ID: vaultID}},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyTierDeleted, ID: tierID, Drain: true})

		if len(mo.tierDrainCalls) != 1 {
			t.Fatalf("expected 1 DrainTier call, got %d", len(mo.tierDrainCalls))
		}
		if mo.tierDrainCalls[0] != tierID {
			t.Fatalf("DrainTier called with wrong tier: %s", mo.tierDrainCalls[0])
		}
		if len(mo.removeTierCalls) != 0 {
			t.Fatalf("leader should not call RemoveTierFromVault, got %d calls", len(mo.removeTierCalls))
		}
	})

	t.Run("follower_removes_immediately", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults: []glid.GLID{vaultID},
			localTierExported: func(_, _ glid.GLID) *orchestrator.TierInstance {
				return &orchestrator.TierInstance{
					TierID:       tierID,
					IsFollower:   true,
					LeaderNodeID: "other-node",
				}
			},
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vaultList: []system.VaultConfig{{ID: vaultID}},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyTierDeleted, ID: tierID, Drain: true})

		if len(mo.tierDrainCalls) != 0 {
			t.Fatalf("follower should not drain, got %d DrainTier calls", len(mo.tierDrainCalls))
		}
		if len(mo.removeTierCalls) != 1 {
			t.Fatalf("expected 1 RemoveTierFromVault call, got %d", len(mo.removeTierCalls))
		}
	})

	t.Run("no_local_tier_removes_nothing", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults: []glid.GLID{vaultID},
			localTierExported: func(_, _ glid.GLID) *orchestrator.TierInstance {
				return nil // this node doesn't host the tier
			},
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vaultList: []system.VaultConfig{{ID: vaultID}},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyTierDeleted, ID: tierID, Drain: true})

		if len(mo.tierDrainCalls) != 0 {
			t.Fatalf("node without tier should not drain, got %d calls", len(mo.tierDrainCalls))
		}
		if len(mo.removeTierCalls) != 0 {
			t.Fatalf("node without tier should not remove, got %d calls", len(mo.removeTierCalls))
		}
	})

	t.Run("non_drain_always_removes", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults: []glid.GLID{vaultID},
			localTierExported: func(_, _ glid.GLID) *orchestrator.TierInstance {
				return &orchestrator.TierInstance{
					TierID:     tierID,
					IsFollower: false,
				}
			},
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vaultList: []system.VaultConfig{{ID: vaultID}},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyTierDeleted, ID: tierID, Drain: false})

		if len(mo.tierDrainCalls) != 0 {
			t.Fatalf("non-drain delete should not call DrainTier, got %d calls", len(mo.tierDrainCalls))
		}
		if len(mo.removeTierCalls) != 1 {
			t.Fatalf("expected 1 RemoveTierFromVault call, got %d", len(mo.removeTierCalls))
		}
	})
}

// ---------- shouldRunIngester ----------

func TestShouldRunIngesterPassiveOnSelectedNode(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:      glid.New(),
		Enabled: true,
		NodeIDs: []string{"node-1", "node-2"},
	}

	if !d.shouldRunIngester(context.Background(), cfg, true) {
		t.Fatal("passive ingester on selected node should return true")
	}
}

func TestShouldRunIngesterPassiveNotOnSelectedNode(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-3"

	cfg := system.IngesterConfig{
		ID:      glid.New(),
		Enabled: true,
		NodeIDs: []string{"node-1", "node-2"},
	}

	if d.shouldRunIngester(context.Background(), cfg, true) {
		t.Fatal("passive ingester NOT on selected node should return false")
	}
}

func TestShouldRunIngesterActiveAssignedHere(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
		ingesterAssignments: map[glid.GLID]string{ingID: "node-1"},
	}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:      ingID,
		Enabled: true,
		NodeIDs: []string{"node-1", "node-2"},
	}

	if !d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("active ingester assigned to this node should return true")
	}
}

func TestShouldRunIngesterActiveAssignedElsewhere(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
		ingesterAssignments: map[glid.GLID]string{ingID: "node-2"},
	}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:      ingID,
		Enabled: true,
		NodeIDs: []string{"node-1", "node-2"},
	}

	if d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("active ingester assigned elsewhere should return false")
	}
}

func TestShouldRunIngesterActiveNoAssignment(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:      glid.New(),
		Enabled: true,
		NodeIDs: []string{"node-1"},
	}

	// Empty assignment = backwards compat: allow local start.
	if !d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("active ingester with no assignment should return true (backwards compat)")
	}
}

func TestShouldRunIngesterPassiveEmptyNodeIDs(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:      glid.New(),
		Enabled: true,
		NodeIDs: nil, // empty
	}

	// Empty NodeIDs means "all nodes", so passive should run.
	if !d.shouldRunIngester(context.Background(), cfg, true) {
		t.Fatal("passive ingester with empty NodeIDs should return true")
	}
}

// ---------- handleIngesterAssignment ----------

func TestHandleIngesterAssignmentStartsLocally(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	mo := &mockOrch{} // not locally running yet
	d := newTestDispatcher(mo, &stubCfgStore{
		ingester:            &system.IngesterConfig{ID: ingID, Type: "test", Enabled: true, NodeIDs: []string{"local"}},
		ingesterAssignments: map[glid.GLID]string{ingID: "local"},
	}, h)
	d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{Factory: func(glid.GLID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
		return noopIngester{}, nil
	}}

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterAssignmentSet, ID: ingID})

	// The ingester should have been added (handleIngesterPut is called internally).
	// No error logs expected for a successful add.
	if h.hasMessage("dispatch: remove reassigned ingester") {
		t.Fatal("should not remove when assigned to this node")
	}
}

func TestHandleIngesterAssignmentStopsLocally(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	mo := &mockOrch{
		ingesters: []glid.GLID{ingID}, // running locally
	}
	d := newTestDispatcher(mo, &stubCfgStore{
		ingesterAssignments: map[glid.GLID]string{ingID: "other-node"},
	}, h)

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterAssignmentSet, ID: ingID})

	if len(mo.removeIngesterIDs) != 1 || mo.removeIngesterIDs[0] != ingID {
		t.Fatalf("expected RemoveIngester(%s), got %v", ingID, mo.removeIngesterIDs)
	}
}

func TestHandleIngesterAssignmentAlreadyRunning(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	mo := &mockOrch{
		ingesters: []glid.GLID{ingID}, // already running locally
	}
	d := newTestDispatcher(mo, &stubCfgStore{
		ingesterAssignments: map[glid.GLID]string{ingID: "local"},
	}, h)

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterAssignmentSet, ID: ingID})

	// Already running locally, assigned here — no action needed.
	if len(mo.removeIngesterIDs) != 0 {
		t.Fatal("should not remove an ingester that is already running on the assigned node")
	}
}
