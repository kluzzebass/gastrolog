package app

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
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
	vaults     []uuid.UUID
	vaultTypes map[uuid.UUID]string
	ingesters  []uuid.UUID

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

	drainCalls          []uuid.UUID // IDs passed to DrainVault
	cancelDrainIDs      []uuid.UUID // IDs passed to CancelDrain
	forceRemoveIDs      []uuid.UUID // IDs passed to ForceRemoveVault
	unregisterIDs       []uuid.UUID // IDs passed to UnregisterVault
	unregisterErr       error
	removeIngesterIDs   []uuid.UUID // IDs passed to RemoveIngester
	reloadFiltersCalls  int         // number of ReloadFilters calls
}

func (m *mockOrch) ListVaults() []uuid.UUID    { return m.vaults }
func (m *mockOrch) ListIngesters() []uuid.UUID { return m.ingesters }
func (m *mockOrch) VaultType(id uuid.UUID) string {
	if m.vaultTypes != nil {
		return m.vaultTypes[id]
	}
	return ""
}

func (m *mockOrch) AddVault(context.Context, config.VaultConfig, orchestrator.Factories) error {
	return m.addVaultErr
}
func (m *mockOrch) ReloadFilters(context.Context) error {
	m.reloadFiltersCalls++
	return m.reloadFiltersErr
}
func (m *mockOrch) ReloadRotationPolicies(context.Context) error { return m.reloadRotationErr }
func (m *mockOrch) ReloadRetentionPolicies(context.Context) error { return m.reloadRetentionErr }
func (m *mockOrch) DisableVault(uuid.UUID) error                 { return m.disableVaultErr }
func (m *mockOrch) EnableVault(uuid.UUID) error                  { return m.enableVaultErr }
func (m *mockOrch) ForceRemoveVault(id uuid.UUID) error {
	m.forceRemoveIDs = append(m.forceRemoveIDs, id)
	return m.forceRemoveErr
}
func (m *mockOrch) RemoveTierFromVault(_, _ uuid.UUID) bool { return false }
func (m *mockOrch) UnregisterVault(id uuid.UUID) error {
	m.unregisterIDs = append(m.unregisterIDs, id)
	return m.unregisterErr
}
func (m *mockOrch) HasMissingTiers(_ uuid.UUID, _ []uuid.UUID) bool { return false }
func (m *mockOrch) DrainVault(_ context.Context, id uuid.UUID, _ string) error {
	m.drainCalls = append(m.drainCalls, id)
	return m.drainVaultErr
}
func (m *mockOrch) IsDraining(uuid.UUID) bool                    { return m.isDraining }
func (m *mockOrch) CancelDrain(_ context.Context, id uuid.UUID) error {
	m.cancelDrainIDs = append(m.cancelDrainIDs, id)
	return m.cancelDrainErr
}
func (m *mockOrch) RemoveIngester(id uuid.UUID) error {
	m.removeIngesterIDs = append(m.removeIngesterIDs, id)
	return m.removeIngesterErr
}
func (m *mockOrch) UpdateMaxConcurrentJobs(int) error            { return m.updateMaxJobsErr }

func (m *mockOrch) AddIngester(uuid.UUID, string, string, orchestrator.Ingester) error {
	return m.addIngesterErr
}

// stubCfgStore implements config.Store with configurable returns for the
// four methods the dispatcher reads. The nil-embedded interface panics on
// any other method call — a deliberate test safety net.
type stubCfgStore struct {
	config.Store // nil embed — panics on uncalled methods

	vault       *config.VaultConfig
	vaultErr    error
	ingester    *config.IngesterConfig
	ingesterErr error
	settings    config.ServerSettings
	settingsErr error
	cfg         *config.Config
	loadErr     error
}

func (s *stubCfgStore) GetVault(context.Context, uuid.UUID) (*config.VaultConfig, error) {
	return s.vault, s.vaultErr
}
func (s *stubCfgStore) GetIngester(context.Context, uuid.UUID) (*config.IngesterConfig, error) {
	return s.ingester, s.ingesterErr
}
func (s *stubCfgStore) LoadServerSettings(context.Context) (config.ServerSettings, error) {
	return s.settings, s.settingsErr
}
func (s *stubCfgStore) Load(context.Context) (*config.Config, error) {
	return s.cfg, s.loadErr
}

// noopIngester satisfies orchestrator.Ingester.
type noopIngester struct{}

func (noopIngester) Run(context.Context, chan<- orchestrator.IngestMessage) error { return nil }

// newTestDispatcher creates a configDispatcher wired to the given mocks.
func newTestDispatcher(orch orchActions, store config.Store, h *captureHandler) *configDispatcher {
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
	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: uuid.New()})

	if h.count() != 0 {
		t.Fatal("expected no log output when orch is nil")
	}
}

func TestHandle_VaultPut(t *testing.T) {
	id := uuid.Must(uuid.NewV7())

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
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &config.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if h.count() != 0 {
			t.Fatal("unexpected error logs for unscoped vault add")
		}
	})

	t.Run("add_vault_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{addVaultErr: errors.New("factory boom")}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &config.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if !h.hasMessage("dispatch: add vault") {
			t.Fatal("expected error log for AddVault failure")
		}
	})

	t.Run("existing_vault_reload_errors", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults:             []uuid.UUID{id}, // vault already registered
			reloadFiltersErr:   errors.New("f"),
			reloadRotationErr:  errors.New("r"),
			reloadRetentionErr: errors.New("ret"),
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &config.VaultConfig{ID: id, Enabled: true},
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
	id := uuid.Must(uuid.NewV7())

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
	id := uuid.Must(uuid.NewV7())

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
			ingester: &config.IngesterConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if len(mo.removeIngesterIDs) != 0 {
			t.Fatal("should not call RemoveIngester when not locally registered")
		}
	})

	t.Run("reassignment_stops_local_ingester", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			ingesters: []uuid.UUID{id}, // locally registered
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingester: &config.IngesterConfig{ID: id, Name: "chatterbox", Enabled: true},
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
			ingester: &config.IngesterConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: unknown ingester type") {
			t.Fatal("expected error log for unknown ingester type")
		}
	})

	t.Run("factory_error", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
			ingester: &config.IngesterConfig{ID: id, Type: "test", Enabled: true},
		}, h)
		d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{Factory: func(uuid.UUID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
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
			ingester: &config.IngesterConfig{ID: id, Type: "test", Enabled: true},
		}, h)
		d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{Factory: func(uuid.UUID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
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
			ingesters:         []uuid.UUID{id},
			removeIngesterErr: errors.New("stuck"),
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingester: &config.IngesterConfig{ID: id, Enabled: true},
		}, h)
		d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{Factory: func(uuid.UUID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
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
			ingester: &config.IngesterConfig{ID: id, Enabled: false},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if h.hasMessage("dispatch: add ingester") {
			t.Fatal("disabled ingester should not be added")
		}
	})
}

func TestHandle_IngesterDeleted(t *testing.T) {
	id := uuid.Must(uuid.NewV7())

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
			settings: config.ServerSettings{
				Scheduler: config.SchedulerConfig{MaxConcurrentJobs: 8},
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
			settings: config.ServerSettings{}, // MaxConcurrentJobs = 0
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
			cfg: &config.Config{}, // ClusterTLS is nil
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
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: uuid.Must(uuid.NewV7())})

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
		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: uuid.Must(uuid.NewV7())})

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
	id := uuid.Must(uuid.NewV7())

	// reassign_triggers_drain and drain_error_logged were removed:
	// they tested NodeID-based vault reassignment which no longer exists.
	// With tiered storage, handleVaultPut no longer calls maybeStartDrain.

	t.Run("already_draining_cancels", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults:     []uuid.UUID{id},
			isDraining: true,
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &config.VaultConfig{ID: id, Enabled: true},
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
			vaults:     []uuid.UUID{id},
			isDraining: true,
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &config.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if len(mo.cancelDrainIDs) != 1 || mo.cancelDrainIDs[0] != id {
			t.Fatalf("expected CancelDrain(%s), got %v", id, mo.cancelDrainIDs)
		}
	})

	t.Run("cancel_drain_error_logged", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{
			vaults:         []uuid.UUID{id},
			isDraining:     true,
			cancelDrainErr: errors.New("boom"),
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &config.VaultConfig{ID: id, Enabled: true},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

		if !h.hasMessage("dispatch: cancel drain") {
			t.Fatal("expected error log for CancelDrain failure")
		}
	})
}

func (m *mockOrch) FindLocalTierExported(_ uuid.UUID, _ uuid.UUID) *orchestrator.TierInstance {
	return nil
}
