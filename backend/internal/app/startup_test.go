package app

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/config"
	"gastrolog/internal/config/memory"
)

// ---------------------------------------------------------------------------
// Extended stubs for startup tests
// ---------------------------------------------------------------------------

// startupStub extends stubCfgStore with node and server-settings methods
// needed by the startup functions under test.
type startupStub struct {
	config.Store // nil embed — panics on uncalled methods

	cfg         *config.Config
	loadErr     error
	settings    config.ServerSettings
	settingsErr error
	node        *config.NodeConfig
	nodeErr     error
	putNodeErr  error
	putNodes    []config.NodeConfig // records PutNode calls
}

func (s *startupStub) Load(context.Context) (*config.Config, error) {
	return s.cfg, s.loadErr
}
func (s *startupStub) LoadServerSettings(context.Context) (config.ServerSettings, error) {
	return s.settings, s.settingsErr
}
func (s *startupStub) GetNode(_ context.Context, _ uuid.UUID) (*config.NodeConfig, error) {
	return s.node, s.nodeErr
}
func (s *startupStub) PutNode(_ context.Context, n config.NodeConfig) error {
	s.putNodes = append(s.putNodes, n)
	return s.putNodeErr
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// ---------------------------------------------------------------------------
// ensureNodeConfig
// ---------------------------------------------------------------------------

func TestEnsureNodeConfig_ExistingNode(t *testing.T) {
	t.Parallel()
	nodeID := uuid.Must(uuid.NewV7()).String()
	store := &startupStub{node: &config.NodeConfig{ID: uuid.MustParse(nodeID), Name: "old-panda"}}

	name, err := ensureNodeConfig(context.Background(), store, nodeID, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "old-panda" {
		t.Fatalf("expected existing name, got %q", name)
	}
	if len(store.putNodes) != 0 {
		t.Fatal("PutNode should not be called for existing node")
	}
}

func TestEnsureNodeConfig_NewNode(t *testing.T) {
	t.Parallel()
	nodeID := uuid.Must(uuid.NewV7()).String()
	store := &startupStub{node: nil}

	name, err := ensureNodeConfig(context.Background(), store, nodeID, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name == "" {
		t.Fatal("expected petname, got empty string")
	}
	if len(store.putNodes) != 1 {
		t.Fatalf("expected 1 PutNode call, got %d", len(store.putNodes))
	}
	if store.putNodes[0].Name != name {
		t.Fatalf("PutNode name %q != returned name %q", store.putNodes[0].Name, name)
	}
}

func TestEnsureNodeConfig_InvalidID(t *testing.T) {
	t.Parallel()
	_, err := ensureNodeConfig(context.Background(), &startupStub{}, "not-a-uuid", "")
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

func TestEnsureNodeConfig_GetNodeError(t *testing.T) {
	t.Parallel()
	nodeID := uuid.Must(uuid.NewV7()).String()
	store := &startupStub{nodeErr: errors.New("db down")}

	_, err := ensureNodeConfig(context.Background(), store, nodeID, "")
	if err == nil || !errors.Is(err, store.nodeErr) {
		t.Fatalf("expected wrapped db error, got %v", err)
	}
}

func TestEnsureNodeConfig_PutNodeError(t *testing.T) {
	t.Parallel()
	nodeID := uuid.Must(uuid.NewV7()).String()
	store := &startupStub{node: nil, putNodeErr: errors.New("write failed")}

	_, err := ensureNodeConfig(context.Background(), store, nodeID, "")
	if err == nil || !errors.Is(err, store.putNodeErr) {
		t.Fatalf("expected wrapped write error, got %v", err)
	}
}

func TestEnsureNodeConfig_PreferredName(t *testing.T) {
	t.Parallel()
	nodeID := uuid.Must(uuid.NewV7()).String()
	store := &startupStub{node: nil}

	name, err := ensureNodeConfig(context.Background(), store, nodeID, "coord")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "coord" {
		t.Fatalf("expected %q, got %q", "coord", name)
	}
	if len(store.putNodes) != 1 || store.putNodes[0].Name != "coord" {
		t.Fatal("PutNode should store the preferred name")
	}
}

func TestEnsureNodeConfig_PreferredNameOverridesExisting(t *testing.T) {
	t.Parallel()
	nodeID := uuid.Must(uuid.NewV7()).String()
	store := &startupStub{node: &config.NodeConfig{ID: uuid.MustParse(nodeID), Name: "old-panda"}}

	name, err := ensureNodeConfig(context.Background(), store, nodeID, "data-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "data-1" {
		t.Fatalf("expected %q, got %q", "data-1", name)
	}
	if len(store.putNodes) != 1 || store.putNodes[0].Name != "data-1" {
		t.Fatal("PutNode should update the name")
	}
}

func TestEnsureNodeConfig_PreferredNameMatchesExisting(t *testing.T) {
	t.Parallel()
	nodeID := uuid.Must(uuid.NewV7()).String()
	store := &startupStub{node: &config.NodeConfig{ID: uuid.MustParse(nodeID), Name: "coord"}}

	name, err := ensureNodeConfig(context.Background(), store, nodeID, "coord")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "coord" {
		t.Fatalf("expected %q, got %q", "coord", name)
	}
	if len(store.putNodes) != 0 {
		t.Fatal("PutNode should not be called when name already matches")
	}
}

// ---------------------------------------------------------------------------
// ensureConfig
// ---------------------------------------------------------------------------

func TestEnsureConfig_ExistingConfigWithSecret(t *testing.T) {
	t.Parallel()
	store := memory.NewStore()
	ctx := context.Background()
	// Bootstrap first to get a valid config + secret.
	if err := config.Bootstrap(ctx, store); err != nil {
		t.Fatal(err)
	}

	cfg, err := ensureConfig(ctx, discardLogger(), store, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
}

func TestEnsureConfig_NoConfigWithBootstrap(t *testing.T) {
	t.Parallel()
	store := memory.NewStore()
	ctx := context.Background()

	cfg, err := ensureConfig(ctx, discardLogger(), store, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected bootstrapped config, got nil")
	}
	// Should have created server settings with a JWT secret.
	ss, _ := store.LoadServerSettings(ctx)
	if ss.Auth.JWTSecret == "" {
		t.Fatal("expected JWT secret after bootstrap")
	}
}

func TestEnsureConfig_ConfigWithoutSecret(t *testing.T) {
	t.Parallel()
	store := memory.NewStore()
	ctx := context.Background()
	// Add a dummy filter so Load() returns non-nil, but leave server settings empty.
	if err := store.PutFilter(ctx, config.FilterConfig{
		ID: uuid.Must(uuid.NewV7()), Name: "dummy", Expression: "*",
	}); err != nil {
		t.Fatal(err)
	}

	cfg, err := ensureConfig(ctx, discardLogger(), store, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config after minimal bootstrap")
	}
	ss, _ := store.LoadServerSettings(ctx)
	if ss.Auth.JWTSecret == "" {
		t.Fatal("expected JWT secret after minimal bootstrap")
	}
}

func TestEnsureConfig_LoadError(t *testing.T) {
	t.Parallel()
	store := &startupStub{loadErr: errors.New("corrupt")}

	_, err := ensureConfig(context.Background(), discardLogger(), store, false)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// awaitReplication
// ---------------------------------------------------------------------------

func TestAwaitReplication_SkipsWhenConfigPresent(t *testing.T) {
	t.Parallel()
	err := awaitReplication(context.Background(), &config.Config{}, "raft", nil, discardLogger())
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestAwaitReplication_SkipsWhenNotRaft(t *testing.T) {
	t.Parallel()
	err := awaitReplication(context.Background(), nil, "memory", nil, discardLogger())
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// waitForServerSettings
// ---------------------------------------------------------------------------

func TestWaitForServerSettings_ImmediateSuccess(t *testing.T) {
	t.Parallel()
	store := &startupStub{settings: config.ServerSettings{
		Auth: config.AuthConfig{JWTSecret: "test-secret"},
	}}

	err := waitForServerSettings(context.Background(), store, 5*time.Second, discardLogger())
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestWaitForServerSettings_Timeout(t *testing.T) {
	t.Parallel()
	store := &startupStub{} // empty settings — never resolves

	err := waitForServerSettings(context.Background(), store, 100*time.Millisecond, discardLogger())
	if err == nil || err.Error() != "timed out waiting for server settings replication" {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestWaitForServerSettings_ContextCancel(t *testing.T) {
	t.Parallel()
	store := &startupStub{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := waitForServerSettings(ctx, store, 5*time.Second, discardLogger())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// loadLocalConfig
// ---------------------------------------------------------------------------

func TestLoadLocalConfig_JoinAddrReturnsNil(t *testing.T) {
	t.Parallel()
	cfg := RunConfig{JoinAddr: "leader:9876", ConfigType: "raft", ClusterAddr: ""}

	appCfg, fromFSM, err := loadLocalConfig(context.Background(), discardLogger(), cfg, &startupStub{}, nil, "node1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if appCfg != nil || fromFSM {
		t.Fatalf("expected nil config and fromFSM=false when joining")
	}
}

func TestLoadLocalConfig_RaftWithLocalFSM(t *testing.T) {
	t.Parallel()
	existingCfg := &config.Config{}
	store := &startupStub{
		cfg:      existingCfg,
		settings: config.ServerSettings{Auth: config.AuthConfig{JWTSecret: "s"}},
	}
	cfg := RunConfig{ConfigType: "raft"}

	appCfg, fromFSM, err := loadLocalConfig(context.Background(), discardLogger(), cfg, store, nil, "node1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if appCfg != existingCfg {
		t.Fatal("expected config from local FSM")
	}
	if !fromFSM {
		t.Fatal("expected fromFSM=true")
	}
}

func TestLoadLocalConfig_MemoryBootstraps(t *testing.T) {
	t.Parallel()
	store := memory.NewStore()
	cfg := RunConfig{ConfigType: "memory", Bootstrap: true}

	appCfg, fromFSM, err := loadLocalConfig(context.Background(), discardLogger(), cfg, store, nil, "node1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if appCfg == nil {
		t.Fatal("expected bootstrapped config")
	}
	if fromFSM {
		t.Fatal("expected fromFSM=false for memory store")
	}
}

// ---------------------------------------------------------------------------
// buildAuthTokens
// ---------------------------------------------------------------------------

func TestBuildAuthTokens_NoAuth(t *testing.T) {
	t.Parallel()
	tokens, err := buildAuthTokens(context.Background(), discardLogger(), nil, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokens != nil {
		t.Fatal("expected nil token service when noAuth=true")
	}
}

func TestBuildAuthTokens_ValidSecret(t *testing.T) {
	t.Parallel()
	secret := base64.StdEncoding.EncodeToString([]byte("test-secret-key-32-bytes-long!!!"))
	store := &startupStub{settings: config.ServerSettings{
		Auth: config.AuthConfig{JWTSecret: secret},
	}}

	tokens, err := buildAuthTokens(context.Background(), discardLogger(), store, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokens == nil {
		t.Fatal("expected token service")
	}
}

func TestBuildAuthTokens_MissingSecret(t *testing.T) {
	t.Parallel()
	store := &startupStub{settings: config.ServerSettings{}}

	_, err := buildAuthTokens(context.Background(), discardLogger(), store, false)
	if err == nil {
		t.Fatal("expected error for missing JWT secret")
	}
}

func TestBuildAuthTokens_LoadError(t *testing.T) {
	t.Parallel()
	store := &startupStub{settingsErr: errors.New("store down")}

	_, err := buildAuthTokens(context.Background(), discardLogger(), store, false)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// loadMaxConcurrentJobs
// ---------------------------------------------------------------------------

func TestLoadMaxConcurrentJobs_Success(t *testing.T) {
	t.Parallel()
	store := &startupStub{settings: config.ServerSettings{
		Scheduler: config.SchedulerConfig{MaxConcurrentJobs: 5},
	}}
	if n := loadMaxConcurrentJobs(context.Background(), store); n != 5 {
		t.Fatalf("expected 5, got %d", n)
	}
}

func TestLoadMaxConcurrentJobs_Error(t *testing.T) {
	t.Parallel()
	store := &startupStub{settingsErr: errors.New("fail")}
	if n := loadMaxConcurrentJobs(context.Background(), store); n != 0 {
		t.Fatalf("expected 0 on error, got %d", n)
	}
}
