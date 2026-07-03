package app

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"gastrolog/internal/cluster"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
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
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

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
	reconcileErr       error
	updateMaxJobsErr   error
	currentMaxJobs     int

	drainCalls         []glid.GLID // IDs passed to DrainVault
	cancelDrainIDs     []glid.GLID // IDs passed to CancelDrain
	forceRemoveIDs     []glid.GLID // IDs passed to ForceRemoveVault
	unregisterIDs      []glid.GLID // IDs passed to UnregisterVault
	unregisterErr      error
	reconcileCalls     [][]glid.GLID // desired ID sets passed to ReconcileIngesters
	addVaultCalls      []glid.GLID   // IDs passed to AddVault
	reloadFiltersCalls int           // number of ReloadFilters calls

	// Vault drain tracking.
	vaultDrainCalls       []glid.GLID                                         // vault IDs passed to DrainInstance
	removeInstanceCalls   []glid.GLID                                         // vault IDs passed to RemoveVaultInstance
	localInstanceExported func(vaultID glid.GLID) *orchestrator.VaultInstance // configurable return

	refreshVaultCtlCalls [][]system.NodeConfig // node lists passed to RefreshVaultCtlMembers
}

func (m *mockOrch) ListVaults() []glid.GLID    { return m.vaults }
func (m *mockOrch) ListIngesters() []glid.GLID { return m.ingesters }
func (m *mockOrch) VaultType(id glid.GLID) string {
	if m.vaultTypes != nil {
		return m.vaultTypes[id]
	}
	return ""
}

func (m *mockOrch) AddVault(_ context.Context, cfg system.VaultConfig, _ orchestrator.Factories) error {
	m.addVaultCalls = append(m.addVaultCalls, cfg.ID)
	if m.addVaultErr == nil {
		// Mirror real-orchestrator behavior: register the vault so subsequent
		// ListVaults() reflects it and the dispatcher's "is it registered?"
		// check succeeds.
		m.vaults = append(m.vaults, cfg.ID)
	}
	return m.addVaultErr
}
func (m *mockOrch) ReloadFilters(context.Context) error {
	m.reloadFiltersCalls++
	return m.reloadFiltersErr
}
func (m *mockOrch) ReloadRotationPolicies(context.Context) error  { return m.reloadRotationErr }
func (m *mockOrch) ReloadRetentionPolicies(context.Context) error { return m.reloadRetentionErr }
func (m *mockOrch) ApplyRotationPolicyForRole(context.Context, glid.GLID) error {
	return nil
}
func (m *mockOrch) DisableVault(glid.GLID) error { return m.disableVaultErr }
func (m *mockOrch) EnableVault(glid.GLID) error  { return m.enableVaultErr }
func (m *mockOrch) ForceRemoveVault(id glid.GLID) error {
	m.forceRemoveIDs = append(m.forceRemoveIDs, id)
	return m.forceRemoveErr
}
func (m *mockOrch) RemoveVaultInstance(vaultID glid.GLID) bool {
	m.removeInstanceCalls = append(m.removeInstanceCalls, vaultID)
	return true
}
func (m *mockOrch) DeleteVaultInstance(vaultID glid.GLID) bool {
	m.removeInstanceCalls = append(m.removeInstanceCalls, vaultID)
	return true
}
func (m *mockOrch) DrainInstance(_ context.Context, vaultID glid.GLID, _ orchestrator.DrainMode, _ string) error {
	m.vaultDrainCalls = append(m.vaultDrainCalls, vaultID)
	return nil
}
func (m *mockOrch) UnregisterVault(id glid.GLID) error {
	m.unregisterIDs = append(m.unregisterIDs, id)
	return m.unregisterErr
}
func (m *mockOrch) MissingVaultInstance(_ glid.GLID, _ []glid.GLID) bool { return false }
func (m *mockOrch) LocalInstanceIDs(_ glid.GLID) []glid.GLID             { return nil }
func (m *mockOrch) AddVaultInstance(_ context.Context, _ glid.GLID, _ orchestrator.Factories) error {
	return nil
}
func (m *mockOrch) DrainVault(_ context.Context, id glid.GLID, _ string) error {
	m.drainCalls = append(m.drainCalls, id)
	return m.drainVaultErr
}
func (m *mockOrch) IsDraining(glid.GLID) bool { return m.isDraining }
func (m *mockOrch) CancelDrain(_ context.Context, id glid.GLID) error {
	m.cancelDrainIDs = append(m.cancelDrainIDs, id)
	return m.cancelDrainErr
}
func (m *mockOrch) UpdateMaxConcurrentJobs(n int) error {
	if m.updateMaxJobsErr == nil {
		m.currentMaxJobs = n
	}
	return m.updateMaxJobsErr
}
func (m *mockOrch) MaxConcurrentJobs() int { return m.currentMaxJobs }

// ReconcileIngesters records the desired ID set and, on success, reflects it
// into m.ingesters so ListIngesters mirrors the orchestrator's converged state.
func (m *mockOrch) ReconcileIngesters(desired []orchestrator.IngesterDesired) error {
	ids := make([]glid.GLID, 0, len(desired))
	for _, d := range desired {
		ids = append(ids, d.ID)
	}
	m.reconcileCalls = append(m.reconcileCalls, ids)
	if m.reconcileErr != nil {
		return m.reconcileErr
	}
	m.ingesters = ids
	return nil
}

// lastReconcile returns the desired ID set from the most recent
// ReconcileIngesters call, or nil if it was never called.
func (m *mockOrch) lastReconcile() []glid.GLID {
	if len(m.reconcileCalls) == 0 {
		return nil
	}
	return m.reconcileCalls[len(m.reconcileCalls)-1]
}

// reconciledContains reports whether the most recent reconcile included id.
func (m *mockOrch) reconciledContains(id glid.GLID) bool {
	return slices.Contains(m.lastReconcile(), id)
}

// stubCfgStore implements system.Store with configurable returns for the
// methods the dispatcher reads. The nil-embedded interface panics on
// any other method call — a deliberate test safety net.
type stubCfgStore struct {
	system.Store // nil embed — panics on uncalled methods

	vault           *system.VaultConfig
	vaultErr        error
	vaultList       []system.VaultConfig
	vaultListErr    error
	ingester        *system.IngesterConfig
	ingesterErr     error
	ingesterList    []system.IngesterConfig
	ingesterListErr error
	// ingestersByID lets ReplayConfigFromStore-style tests register the
	// per-ingester `GetIngester` lookups that handleIngesterPut performs
	// after iterating the list. Falls back to `ingester` when nil.
	ingestersByID map[glid.GLID]system.IngesterConfig
	settings      system.ServerSettings
	settingsErr   error
	cfg           *system.Config
	loadErr       error

	ingesterAssignments map[glid.GLID]string // ingester ID → assigned node

	nodes    []system.NodeConfig
	nodesErr error

	placements map[glid.GLID][]system.VaultPlacement
	nscs       []system.NodeStorageConfig
}

func (s *stubCfgStore) GetVault(context.Context, glid.GLID) (*system.VaultConfig, error) {
	return s.vault, s.vaultErr
}
func (s *stubCfgStore) ListVaults(context.Context) ([]system.VaultConfig, error) {
	return s.vaultList, s.vaultListErr
}
func (s *stubCfgStore) GetIngester(_ context.Context, id glid.GLID) (*system.IngesterConfig, error) {
	if s.ingestersByID != nil {
		if cfg, ok := s.ingestersByID[id]; ok {
			c := cfg
			return &c, s.ingesterErr
		}
	}
	return s.ingester, s.ingesterErr
}
func (s *stubCfgStore) ListIngesters(context.Context) ([]system.IngesterConfig, error) {
	return s.ingesterList, s.ingesterListErr
}
func (s *stubCfgStore) LoadServerSettings(context.Context) (system.ServerSettings, error) {
	return s.settings, s.settingsErr
}
func (s *stubCfgStore) Load(context.Context) (*system.System, error) {
	if s.cfg == nil {
		return nil, s.loadErr
	}
	return &system.System{Config: *s.cfg}, s.loadErr
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
func (s *stubCfgStore) ListNodes(context.Context) ([]system.NodeConfig, error) {
	return s.nodes, s.nodesErr
}
func (s *stubCfgStore) GetVaultPlacements(_ context.Context, vaultID glid.GLID) ([]system.VaultPlacement, error) {
	return s.placements[vaultID], nil
}
func (s *stubCfgStore) ListNodeStorageConfigs(context.Context) ([]system.NodeStorageConfig, error) {
	return s.nscs, nil
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
	// reintroduced via vault leader election in a future issue.

	t.Run("unscoped_node_not_skipped", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{} // no error → AddVault succeeds
		d := newTestDispatcher(mo, &stubCfgStore{
			vault: &system.VaultConfig{ID: id, Enabled: true},
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
			vault: &system.VaultConfig{ID: id, Enabled: true},
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

// reg is the test ingester registration used across ingester dispatch tests.
func testIngesterReg() orchestrator.IngesterRegistration {
	return orchestrator.IngesterRegistration{
		Factory: func(glid.GLID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
			return noopIngester{}, nil
		},
	}
}

// Every ingester FSM notification now recomputes the full desired set from the
// config store and drives it through ReconcileIngesters; the orchestrator diffs
// and only (re)builds changed ingesters. These tests assert which configs land
// in the reconciled desired set.
func TestHandle_IngesterPut(t *testing.T) {
	id := glid.New()

	t.Run("list_store_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingesterListErr: errors.New("db down"),
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: list ingesters") {
			t.Fatal("expected error log for store list failure")
		}
		if len(mo.reconcileCalls) != 0 {
			t.Fatal("should not reconcile when the config list cannot be read")
		}
	})

	t.Run("eligible_included", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingesterList: []system.IngesterConfig{{ID: id, Type: "test", Enabled: true}},
		}, h)
		d.factories.IngesterTypes["test"] = testIngesterReg()

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !mo.reconciledContains(id) {
			t.Fatalf("expected eligible ingester %s in desired set, got %v", id, mo.lastReconcile())
		}
	})

	t.Run("unknown_type_excluded", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingesterList: []system.IngesterConfig{{ID: id, Type: "missing", Enabled: true}},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: unknown ingester type") {
			t.Fatal("expected error log for unknown ingester type")
		}
		if mo.reconciledContains(id) {
			t.Fatal("unknown-type ingester must not be in the desired set")
		}
	})

	t.Run("disabled_excluded", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingesterList: []system.IngesterConfig{{ID: id, Type: "test", Enabled: false}},
		}, h)
		d.factories.IngesterTypes["test"] = testIngesterReg()

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if mo.reconciledContains(id) {
			t.Fatal("disabled ingester must not be in the desired set")
		}
	})

	t.Run("pinned_to_other_node_excluded", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingesterList: []system.IngesterConfig{{ID: id, Type: "test", Enabled: true, NodeIDs: []string{"other-node"}}},
		}, h)
		d.factories.IngesterTypes["test"] = testIngesterReg()

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if mo.reconciledContains(id) {
			t.Fatal("ingester pinned to another node must not be in this node's desired set")
		}
	})

	t.Run("reconcile_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{reconcileErr: errors.New("build failed")}
		d := newTestDispatcher(mo, &stubCfgStore{
			ingesterList: []system.IngesterConfig{{ID: id, Type: "test", Enabled: true}},
		}, h)
		d.factories.IngesterTypes["test"] = testIngesterReg()

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterPut, ID: id})

		if !h.hasMessage("dispatch: reconcile ingesters") {
			t.Fatal("expected error log when ReconcileIngesters fails")
		}
	})
}

func TestHandle_IngesterDeleted(t *testing.T) {
	id := glid.New()

	t.Run("reconciles_without_deleted_ingester", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{ingesters: []glid.GLID{id}}
		// Store no longer lists the deleted ingester.
		d := newTestDispatcher(mo, &stubCfgStore{}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterDeleted, ID: id})

		if mo.reconciledContains(id) {
			t.Fatal("deleted ingester must not appear in the reconciled desired set")
		}
	})

	t.Run("reconcile_error", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{reconcileErr: errors.New("stuck")}
		d := newTestDispatcher(mo, &stubCfgStore{}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterDeleted, ID: id})

		if !h.hasMessage("dispatch: reconcile ingesters") {
			t.Fatal("expected error log when ReconcileIngesters fails")
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
		// gastrolog-4kkoo (Phase 5): NotifyFilterPut/Deleted removed —
		// expressions are inline on routes, so route_put / route_deleted
		// already cover the reload-filters dispatch case.
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

	t.Run("lookup_settings_key_skipped", func(t *testing.T) {
		h := &captureHandler{}
		d := newTestDispatcher(&mockOrch{updateMaxJobsErr: errors.New("should not be called")}, &stubCfgStore{
			settingsErr: errors.New("should not be called"),
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: system.NotifyKeyLookupSettings})

		if h.count() != 0 {
			t.Fatal("lookup_settings should not load settings or update scheduler")
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

	t.Run("unchanged_max_jobs_skipped", func(t *testing.T) {
		// Legacy Raft entries and service saves use NotifySettingPut with
		// keys that may touch the scheduler. Lookup-only saves use a
		// different key so this path is not hit. When the desired
		// concurrency equals the current value, no rebuild.
		h := &captureHandler{}
		mo := &mockOrch{
			currentMaxJobs:   8,
			updateMaxJobsErr: errors.New("should not be called"),
		}
		d := newTestDispatcher(mo, &stubCfgStore{
			settings: system.ServerSettings{
				Scheduler: system.SchedulerConfig{MaxConcurrentJobs: 8},
			},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

		if h.count() != 0 {
			t.Fatalf("unchanged MaxConcurrentJobs should not trigger update, got logs: %d", h.count())
		}
	})

	t.Run("changed_max_jobs_rebuilds", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{currentMaxJobs: 4}
		d := newTestDispatcher(mo, &stubCfgStore{
			settings: system.ServerSettings{
				Scheduler: system.SchedulerConfig{MaxConcurrentJobs: 8},
			},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: "server"})

		if mo.currentMaxJobs != 8 {
			t.Fatalf("expected MaxConcurrentJobs to update to 8, got %d", mo.currentMaxJobs)
		}
	})

	t.Run("changed_max_jobs_rebuilds_service_key", func(t *testing.T) {
		h := &captureHandler{}
		mo := &mockOrch{currentMaxJobs: 4}
		d := newTestDispatcher(mo, &stubCfgStore{
			settings: system.ServerSettings{
				Scheduler: system.SchedulerConfig{MaxConcurrentJobs: 8},
			},
		}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifySettingPut, Key: system.NotifyKeyServiceSettings})

		if mo.currentMaxJobs != 8 {
			t.Fatalf("expected MaxConcurrentJobs to update to 8, got %d", mo.currentMaxJobs)
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
	// With vault storage, handleVaultPut no longer calls maybeStartDrain.

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

func (m *mockOrch) FindLocalVaultInstance(vaultID glid.GLID) *orchestrator.VaultInstance {
	if m.localInstanceExported != nil {
		return m.localInstanceExported(vaultID)
	}
	return nil
}

func (m *mockOrch) RefreshVaultCtlMembers(nodes []system.NodeConfig, _ orchestrator.Factories) {
	m.refreshVaultCtlCalls = append(m.refreshVaultCtlCalls, nodes)
}

// gastrolog-4zy8a: every cluster membership change (NodeConfig add/remove)
// must propagate into per-vault vault-ctl Raft groups via RefreshVaultCtlMembers.
// Without this, vault-ctl groups stay frozen at bootstrap membership and
// scaled-in nodes loop forever in pre-vote campaigns.
func TestHandle_NodeConfigChange_RefreshesVaultCtlMembers(t *testing.T) {
	t.Parallel()

	nodes := []system.NodeConfig{
		{ID: glid.New(), Name: "node-a"},
		{ID: glid.New(), Name: "node-b"},
		{ID: glid.New(), Name: "node-c"},
	}

	t.Run("put_refreshes_with_current_node_list", func(t *testing.T) {
		t.Parallel()
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{nodes: nodes}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyNodeConfigPut, ID: nodes[0].ID})

		if len(mo.refreshVaultCtlCalls) != 1 {
			t.Fatalf("expected 1 RefreshVaultCtlMembers call, got %d", len(mo.refreshVaultCtlCalls))
		}
		if got := len(mo.refreshVaultCtlCalls[0]); got != 3 {
			t.Fatalf("expected 3 nodes in refresh payload, got %d", got)
		}
	})

	t.Run("delete_also_refreshes", func(t *testing.T) {
		t.Parallel()
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{nodes: nodes[:2]}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyNodeConfigDeleted, ID: nodes[2].ID})

		if len(mo.refreshVaultCtlCalls) != 1 {
			t.Fatalf("expected 1 RefreshVaultCtlMembers call on delete, got %d", len(mo.refreshVaultCtlCalls))
		}
		if got := len(mo.refreshVaultCtlCalls[0]); got != 2 {
			t.Fatalf("expected 2 nodes after delete, got %d", got)
		}
	})

	t.Run("list_error_logs_and_skips", func(t *testing.T) {
		t.Parallel()
		h := &captureHandler{}
		mo := &mockOrch{}
		d := newTestDispatcher(mo, &stubCfgStore{nodesErr: errors.New("store down")}, h)

		d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyNodeConfigPut, ID: nodes[0].ID})

		if len(mo.refreshVaultCtlCalls) != 0 {
			t.Fatalf("expected no refresh on list error, got %d", len(mo.refreshVaultCtlCalls))
		}
		if !h.hasMessage("dispatch: list nodes for vault-ctl membership refresh") {
			t.Fatal("expected error log for ListNodes failure")
		}
	})
}

// gastrolog-4zy8a: when the placement leader transfers but this node stays
// a follower, the local VaultInstance.LeaderNodeID must be refreshed so the
// lifecycle reconciler's RequestReplicaCatchup targets the new leader
// instead of looping forever against the old (stale) one.
func TestHandle_PlacementsSet_RefreshesLeaderPointerWhenRoleUnchanged(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	oldLeaderID := glid.New().String()
	newLeaderID := glid.New().String()
	localID := glid.New().String()

	// Storage IDs as real GLIDs (placements store them as base32 strings).
	oldLeaderStorage := glid.New()
	newLeaderStorage := glid.New()
	localStorage := glid.New()

	nscs := []system.NodeStorageConfig{
		{NodeID: oldLeaderID, FileStorages: []system.FileStorage{{ID: oldLeaderStorage, StorageClass: 1}}},
		{NodeID: newLeaderID, FileStorages: []system.FileStorage{{ID: newLeaderStorage, StorageClass: 1}}},
		{NodeID: localID, FileStorages: []system.FileStorage{{ID: localStorage, StorageClass: 1}}},
	}

	// New placement: new leader is on newLeaderID; local stays a follower.
	placements := []system.VaultPlacement{
		{StorageID: newLeaderStorage.String(), Leader: true},
		{StorageID: localStorage.String(), Leader: false},
	}

	// Existing in-memory vault instance: still pointing at the OLD leader.
	existing := &orchestrator.VaultInstance{
		VaultID:      vaultID,
		IsFollower:   true,
		LeaderNodeID: oldLeaderID,
	}

	h := &captureHandler{}
	mo := &mockOrch{
		localInstanceExported: func(id glid.GLID) *orchestrator.VaultInstance {
			if id == vaultID {
				return existing
			}
			return nil
		},
	}
	d := newTestDispatcher(mo, &stubCfgStore{
		vault:      &system.VaultConfig{ID: vaultID, Enabled: true, Type: system.VaultTypeFile, StorageClass: 1},
		nscs:       nscs,
		placements: map[glid.GLID][]system.VaultPlacement{vaultID: placements},
	}, h)
	d.localNodeID = localID

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPlacementsSet, ID: vaultID})

	if existing.LeaderNodeID != newLeaderID {
		t.Fatalf("LeaderNodeID not refreshed: want %q, got %q", newLeaderID, existing.LeaderNodeID)
	}
	if !existing.IsFollower {
		t.Fatal("IsFollower should still be true (role didn't change)")
	}
}

// gastrolog-3idjc: when a fresh joiner replays the cluster's post-snapshot
// log, NotifyVaultPlacementsSet for a vault can arrive before the dispatcher
// has ever seen a NotifyVaultPut for that vault — because the vault-put
// landed inside the snapshot and snapshot restore does NOT fire onApply
// notifications. The orchestrator's vault list is therefore empty for this
// vault, and a naive AddVaultInstance fails with ErrVaultNotFound, leaving
// the joiner permanently without the vault.
//
// rebuildVaultIfInstanceMissing must detect this and call AddVault first,
// so the placement notification self-heals the missing registration instead
// of erroring out forever.
func TestHandle_PlacementsSet_RegistersVaultWhenMissing(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	localID := glid.New().String()
	localStorage := glid.New()

	nscs := []system.NodeStorageConfig{
		{NodeID: localID, FileStorages: []system.FileStorage{{ID: localStorage, StorageClass: 1}}},
	}
	// Local node IS the leader → vaultBelongsHere true → rebuild path runs.
	placements := []system.VaultPlacement{
		{StorageID: localStorage.String(), Leader: true},
	}

	h := &captureHandler{}
	mo := &mockOrch{} // empty vaults slice — simulates the missing-after-snapshot state
	d := newTestDispatcher(mo, &stubCfgStore{
		vault:      &system.VaultConfig{ID: vaultID, Enabled: true, Type: system.VaultTypeFile, StorageClass: 1},
		nscs:       nscs,
		placements: map[glid.GLID][]system.VaultPlacement{vaultID: placements},
	}, h)
	d.localNodeID = localID

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPlacementsSet, ID: vaultID})

	if len(mo.addVaultCalls) != 1 || mo.addVaultCalls[0] != vaultID {
		t.Fatalf("expected defensive AddVault(%s), got %v", vaultID, mo.addVaultCalls)
	}
	if h.hasMessage("dispatch: add vault instance") {
		t.Fatal("AddVaultInstance should not error: AddVault must run first when the vault is missing from the orchestrator")
	}
	if h.hasMessage("dispatch: add vault before instance") {
		t.Fatal("defensive AddVault should succeed; got an error log")
	}
}

// gastrolog-3idjc: when the vault IS already registered (steady state),
// rebuildVaultIfInstanceMissing must not redundantly call AddVault — only
// AddVaultInstance.
func TestHandle_PlacementsSet_DoesNotReregisterExistingVault(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	localID := glid.New().String()
	localStorage := glid.New()

	nscs := []system.NodeStorageConfig{
		{NodeID: localID, FileStorages: []system.FileStorage{{ID: localStorage, StorageClass: 1}}},
	}
	placements := []system.VaultPlacement{
		{StorageID: localStorage.String(), Leader: true},
	}

	h := &captureHandler{}
	mo := &mockOrch{
		vaults: []glid.GLID{vaultID}, // vault already in orchestrator's list
	}
	d := newTestDispatcher(mo, &stubCfgStore{
		vault:      &system.VaultConfig{ID: vaultID, Enabled: true, Type: system.VaultTypeFile, StorageClass: 1},
		nscs:       nscs,
		placements: map[glid.GLID][]system.VaultPlacement{vaultID: placements},
	}, h)
	d.localNodeID = localID

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyVaultPlacementsSet, ID: vaultID})

	if len(mo.addVaultCalls) != 0 {
		t.Fatalf("AddVault should NOT be called when vault is already registered, got %v", mo.addVaultCalls)
	}
}

func TestShouldRunIngesterParallelOnSelectedNode(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:      glid.New(),
		Enabled: true,
		NodeIDs: []string{"node-1", "node-2"},
	}

	if !d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("parallel ingester on selected node should return true")
	}
}

func TestShouldRunIngesterParallelNotOnSelectedNode(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-3"

	cfg := system.IngesterConfig{
		ID:      glid.New(),
		Enabled: true,
		NodeIDs: []string{"node-1", "node-2"},
	}

	if d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("parallel ingester NOT on selected node should return false")
	}
}

// AllNodes=true must short-circuit any NodeIDs check — a brand-new node
// that was never in NodeIDs still runs the ingester. That's the whole
// point of the flag (gastrolog-2g7lr).
func TestShouldRunIngesterAllNodesIncludesNewJoiner(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-99-just-joined"

	cfg := system.IngesterConfig{
		ID:       glid.New(),
		Enabled:  true,
		AllNodes: true,
		// NodeIDs intentionally a stale snapshot from before this node existed.
		NodeIDs: []string{"node-1", "node-2", "node-3"},
	}

	if !d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("AllNodes=true must run on every node, including those not in NodeIDs (gastrolog-2g7lr)")
	}
}

// AllNodes=true with NodeIDs empty (clean state) — runs on every node.
func TestShouldRunIngesterAllNodesEmptyNodeIDs(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:       glid.New(),
		Enabled:  true,
		AllNodes: true,
		NodeIDs:  nil,
	}

	if !d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("AllNodes=true with empty NodeIDs must run on every node")
	}
}

// Legacy backwards compat: empty NodeIDs without AllNodes flag falls
// through the old "empty list = match all" semantic. Existing FSM data
// pre-AllNodes still works.
func TestShouldRunIngesterLegacyEmptyNodeIDs(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:       glid.New(),
		Enabled:  true,
		AllNodes: false, // legacy config never had this flag set
		NodeIDs:  nil,
	}

	if !d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("legacy empty NodeIDs (no AllNodes flag) must keep running everywhere for backwards compat")
	}
}

// Singleton + AllNodes — placement assignment still narrows to the one
// node the placement manager picked, even though every node is eligible.
func TestShouldRunIngesterSingletonAllNodesNotAssignedHere(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
		ingesterAssignments: map[glid.GLID]string{ingID: "node-7"},
	}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:        ingID,
		Enabled:   true,
		AllNodes:  true,
		Singleton: true,
	}

	if d.shouldRunIngester(context.Background(), cfg, true) {
		t.Fatal("singleton ingester must respect placement assignment even when AllNodes is on")
	}
}

func TestShouldRunIngesterSingletonAssignedHere(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
		ingesterAssignments: map[glid.GLID]string{ingID: "node-1"},
	}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:        ingID,
		Enabled:   true,
		NodeIDs:   []string{"node-1", "node-2"},
		Singleton: true,
	}

	if !d.shouldRunIngester(context.Background(), cfg, true) {
		t.Fatal("singleton ingester assigned to this node should return true")
	}
}

func TestShouldRunIngesterSingletonAssignedElsewhere(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{
		ingesterAssignments: map[glid.GLID]string{ingID: "node-2"},
	}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:        ingID,
		Enabled:   true,
		NodeIDs:   []string{"node-1", "node-2"},
		Singleton: true,
	}

	if d.shouldRunIngester(context.Background(), cfg, true) {
		t.Fatal("singleton ingester assigned elsewhere should return false")
	}
}

func TestShouldRunIngesterSingletonNoAssignment(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:        glid.New(),
		Enabled:   true,
		NodeIDs:   []string{"node-1"},
		Singleton: true,
	}

	// Empty assignment = placement manager hasn't run yet. Allow local start;
	// placement will narrow it down on the next reconcile cycle.
	if !d.shouldRunIngester(context.Background(), cfg, true) {
		t.Fatal("singleton ingester with no assignment should return true (pre-placement)")
	}
}

func TestShouldRunIngesterParallelEmptyNodeIDs(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	d := newTestDispatcher(&mockOrch{}, &stubCfgStore{}, h)
	d.localNodeID = "node-1"

	cfg := system.IngesterConfig{
		ID:      glid.New(),
		Enabled: true,
		NodeIDs: nil, // empty
	}

	// Empty NodeIDs means "all nodes", so parallel should run.
	if !d.shouldRunIngester(context.Background(), cfg, false) {
		t.Fatal("parallel ingester with empty NodeIDs should return true")
	}
}

// ---------- handleIngesterAssignment ----------

// singletonTestIngester builds an IngesterConfig + registration for
// exercising handleIngesterAssignment. Instance has Singleton=true, the
// registered type has SingletonSupported=true.
func singletonTestIngester(ingID glid.GLID, nodeIDs ...string) (*system.IngesterConfig, orchestrator.IngesterRegistration) {
	return &system.IngesterConfig{
			ID: ingID, Type: "test", Enabled: true, NodeIDs: nodeIDs, Singleton: true,
		}, orchestrator.IngesterRegistration{
			Factory: func(glid.GLID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
				return noopIngester{}, nil
			},
			SingletonSupported: true,
		}
}

func TestHandleIngesterAssignmentStartsLocally(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	ing, reg := singletonTestIngester(ingID, "local")
	mo := &mockOrch{} // not locally running yet
	d := newTestDispatcher(mo, &stubCfgStore{
		ingesterList:        []system.IngesterConfig{*ing},
		ingesterAssignments: map[glid.GLID]string{ingID: "local"},
	}, h)
	d.factories.IngesterTypes["test"] = reg

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterAssignmentSet, ID: ingID})

	// Singleton assigned to this node — included in the desired set.
	if !mo.reconciledContains(ingID) {
		t.Fatalf("expected singleton assigned locally in desired set, got %v", mo.lastReconcile())
	}
}

func TestHandleIngesterAssignmentStopsLocally(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	ing, reg := singletonTestIngester(ingID, "local", "other-node")
	mo := &mockOrch{
		ingesters: []glid.GLID{ingID}, // running locally
	}
	d := newTestDispatcher(mo, &stubCfgStore{
		ingesterList:        []system.IngesterConfig{*ing},
		ingesterAssignments: map[glid.GLID]string{ingID: "other-node"},
	}, h)
	d.factories.IngesterTypes["test"] = reg

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterAssignmentSet, ID: ingID})

	// Singleton reassigned to another node — dropped from the desired set so
	// the orchestrator stops it locally.
	if mo.reconciledContains(ingID) {
		t.Fatalf("singleton assigned elsewhere must be excluded, got %v", mo.lastReconcile())
	}
}

func TestHandleIngesterAssignmentAlreadyRunning(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	ing, reg := singletonTestIngester(ingID, "local")
	mo := &mockOrch{
		ingesters: []glid.GLID{ingID}, // already running locally
	}
	d := newTestDispatcher(mo, &stubCfgStore{
		ingesterList:        []system.IngesterConfig{*ing},
		ingesterAssignments: map[glid.GLID]string{ingID: "local"},
	}, h)
	d.factories.IngesterTypes["test"] = reg

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterAssignmentSet, ID: ingID})

	// Assigned here and already running — still in the desired set; the
	// orchestrator's no-flap reconcile keeps the running instance untouched.
	if !mo.reconciledContains(ingID) {
		t.Fatalf("ingester assigned and running locally must stay in desired set, got %v", mo.lastReconcile())
	}
}

func TestHandleIngesterAssignmentIgnoresParallel(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	ingID := glid.New()
	// Parallel ingester (Singleton=false) with a stale assignment pointing elsewhere.
	mo := &mockOrch{ingesters: []glid.GLID{ingID}}
	d := newTestDispatcher(mo, &stubCfgStore{
		ingesterList:        []system.IngesterConfig{{ID: ingID, Type: "test", Enabled: true, NodeIDs: []string{"local"}, Singleton: false}},
		ingesterAssignments: map[glid.GLID]string{ingID: "other-node"},
	}, h)
	d.factories.IngesterTypes["test"] = orchestrator.IngesterRegistration{
		Factory: func(glid.GLID, map[string]string, *slog.Logger) (orchestrator.Ingester, error) {
			return noopIngester{}, nil
		},
		SingletonSupported: true,
	}

	d.Handle(raftfsm.Notification{Kind: raftfsm.NotifyIngesterAssignmentSet, ID: ingID})

	// Parallel ingester eligible on this node (NodeIDs contains local); the
	// stale singleton assignment is irrelevant, so it stays in the desired set.
	if !mo.reconciledContains(ingID) {
		t.Fatalf("parallel ingester eligible locally must stay in desired set, got %v", mo.lastReconcile())
	}
}

// ---------------------------------------------------------------------------
// ReplayConfigFromStore — gastrolog-3hcfm: post-snapshot-replication catchup
// ---------------------------------------------------------------------------

// dispatcherForReplay creates a configDispatcher with the chatterbox-style
// ingester type registered, so handleIngesterPut can build an ingester from
// the snapshot-deposited config during a replay run.
func dispatcherForReplay(orch orchActions, store system.Store, h *captureHandler) *configDispatcher {
	d := newTestDispatcher(orch, store, h)
	d.factories.IngesterTypes = map[string]orchestrator.IngesterRegistration{
		"chatterbox-test": {
			Factory: func(_ glid.GLID, _ map[string]string, _ *slog.Logger) (orchestrator.Ingester, error) {
				return noopIngester{}, nil
			},
		},
	}
	return d
}

func TestReplayConfigFromStore_RegistersIngestersFromSnapshot(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	orch := &mockOrch{}

	ingID := glid.New()
	cfg := system.IngesterConfig{
		ID: ingID, Name: "chatterbox", Type: "chatterbox-test",
		AllNodes: true, Enabled: true,
	}
	store := &stubCfgStore{
		ingesterList:  []system.IngesterConfig{cfg},
		ingestersByID: map[glid.GLID]system.IngesterConfig{ingID: cfg},
	}

	d := dispatcherForReplay(orch, store, h)
	d.ReplayConfigFromStore(context.Background())

	if !orch.reconciledContains(ingID) {
		t.Fatalf("expected snapshot-deposited ingester %s in reconciled desired set, got %v",
			ingID, orch.lastReconcile())
	}
}

func TestReplayConfigFromStore_SkipsIngesterPinnedToOtherNode(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	orch := &mockOrch{}

	otherNode := glid.New().String()
	ingID := glid.New()
	cfg := system.IngesterConfig{
		ID: ingID, Name: "pinned", Type: "chatterbox-test",
		AllNodes: false, NodeIDs: []string{otherNode}, Enabled: true,
	}
	store := &stubCfgStore{
		ingesterList:  []system.IngesterConfig{cfg},
		ingestersByID: map[glid.GLID]system.IngesterConfig{ingID: cfg},
	}

	d := dispatcherForReplay(orch, store, h)
	d.ReplayConfigFromStore(context.Background())

	if orch.reconciledContains(ingID) {
		t.Fatalf("expected ingester pinned to %s (local=%s) excluded from desired set, got %v",
			otherNode, d.localNodeID, orch.lastReconcile())
	}
}

func TestReplayConfigFromStore_SkipsDisabledIngester(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	orch := &mockOrch{}

	ingID := glid.New()
	cfg := system.IngesterConfig{
		ID: ingID, Name: "off", Type: "chatterbox-test",
		AllNodes: true, Enabled: false,
	}
	store := &stubCfgStore{
		ingesterList:  []system.IngesterConfig{cfg},
		ingestersByID: map[glid.GLID]system.IngesterConfig{ingID: cfg},
	}

	d := dispatcherForReplay(orch, store, h)
	d.ReplayConfigFromStore(context.Background())

	if orch.reconciledContains(ingID) {
		t.Fatalf("expected disabled ingester excluded from desired set, got %v",
			orch.lastReconcile())
	}
}

func TestReplayConfigFromStore_RegistersVaultsFromSnapshot(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	orch := &mockOrch{}

	vaultID := glid.New()
	v := system.VaultConfig{ID: vaultID, Name: "v", Type: system.VaultTypeMemory}
	store := &stubCfgStore{
		vault:     &v,
		vaultList: []system.VaultConfig{v},
	}

	d := dispatcherForReplay(orch, store, h)
	d.ReplayConfigFromStore(context.Background())

	if !slices.Contains(orch.addVaultCalls, vaultID) {
		t.Fatalf("expected AddVault for snapshot-deposited vault %s, got %v",
			vaultID, orch.addVaultCalls)
	}
}

func TestReplayConfigFromStore_ReloadsRoutesAndPolicies(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	orch := &mockOrch{}
	store := &stubCfgStore{}

	d := dispatcherForReplay(orch, store, h)
	d.ReplayConfigFromStore(context.Background())

	if orch.reloadFiltersCalls == 0 {
		t.Fatal("expected ReloadFilters to be called at least once during replay")
	}
}

// TestReplayConfigFromStore_ReconcilesAlreadyRegisteredIdempotently verifies
// that replay reconciles the full desired set even for ingesters the
// orchestrator already runs, listing each exactly once. The no-flap guarantee
// (not tearing down and re-adding an unchanged ingester, which would race
// setIngesterAlive(true) against a stale setIngesterAlive(false) — observed in
// k8s as "7/10 alive" after a scale-up) now lives in the orchestrator's
// ReconcileIngesters diff, exercised by ingestion.Manager's no-op reconcile
// test. Dispatch's job is simply to hand over the correct desired set.
func TestReplayConfigFromStore_ReconcilesAlreadyRegisteredIdempotently(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	orch := &mockOrch{}

	ingID := glid.New()
	cfg := system.IngesterConfig{
		ID: ingID, Name: "chatterbox", Type: "chatterbox-test",
		AllNodes: true, Enabled: true,
	}
	store := &stubCfgStore{
		ingesterList:  []system.IngesterConfig{cfg},
		ingestersByID: map[glid.GLID]system.IngesterConfig{ingID: cfg},
	}

	// Pre-register the ingester to simulate a joiner that already got
	// the PutIngester Apply notification via the live dispatcher.
	orch.ingesters = []glid.GLID{ingID}

	d := dispatcherForReplay(orch, store, h)
	d.ReplayConfigFromStore(context.Background())

	desired := orch.lastReconcile()
	if len(desired) != 1 || desired[0] != ingID {
		t.Fatalf("expected desired set [%s] exactly once, got %v", ingID, desired)
	}
}

// TestReplayConfigFromStore_SkipsAlreadyRegisteredVaults mirrors the
// ingester check for vaults — replay must not call AddVault on a
// vault the orchestrator already holds.
func TestReplayConfigFromStore_SkipsAlreadyRegisteredVaults(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	orch := &mockOrch{}

	vaultID := glid.New()
	v := system.VaultConfig{ID: vaultID, Name: "v", Type: system.VaultTypeMemory}
	store := &stubCfgStore{
		vault:     &v,
		vaultList: []system.VaultConfig{v},
	}

	orch.vaults = []glid.GLID{vaultID}

	d := dispatcherForReplay(orch, store, h)
	d.ReplayConfigFromStore(context.Background())

	if len(orch.addVaultCalls) != 0 {
		t.Fatalf("expected zero AddVault calls for pre-registered vault, got %v",
			orch.addVaultCalls)
	}
}

// TestReplayConfigFromStore_NoOpWhenOrchUnwired verifies the guard at the
// top of ReplayConfigFromStore: calling it on a dispatcher whose orch
// hasn't been wired yet is silent (no panic, no calls). This matters
// because the dispatcher is constructed before orch in app.go.
func TestReplayConfigFromStore_NoOpWhenOrchUnwired(t *testing.T) {
	t.Parallel()
	d := &configDispatcher{logger: slog.Default()}
	// Should not panic and should not call into any store.
	d.ReplayConfigFromStore(context.Background())
}
