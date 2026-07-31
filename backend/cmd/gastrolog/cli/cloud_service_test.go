package cli

// Config-accept validation must surface through the existing CLI error path.
// `config cloud-service create` with a bare endpoint gets the server's
// InvalidArgument error back verbatim — the operator sees the offending value
// and both accepted forms instead of a silently persisted config that kills
// vault init on every node.

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"
)

// newCloudServiceTestServer starts an in-process HTTP server backed by a
// real server.Server and memory config store, so the cobra command runs
// the genuine client→RPC→handler path.
func newCloudServiceTestServer(t *testing.T) (*httptest.Server, *sysmem.Store) {
	t.Helper()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{
		SystemLoader: cfgStore,
		SegmentsDir:  filepath.Join(t.TempDir(), "segments"),
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	t.Cleanup(func() { _ = orch.Stop() })

	srv := server.New(orch, cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil, server.Config{NoAuth: true})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, cfgStore
}

func runCloudServiceCreate(t *testing.T, addr string, extra ...string) error {
	t.Helper()
	cmd := NewConfigCommand()
	AddClientFlags(cmd)
	args := append([]string{"cloud-service", "create", "--addr", addr}, extra...)
	cmd.SetArgs(args)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	return cmd.Execute()
}

func TestCloudServiceCreateSurfacesValidationError(t *testing.T) {
	ts, cfgStore := newCloudServiceTestServer(t)
	ctx := context.Background()

	// Bare endpoint — the motivating incident — must fail loudly at the
	// CLI with the full actionable message from the server.
	err := runCloudServiceCreate(t, ts.URL,
		"--name", "minio", "--provider", "s3",
		"--bucket", "chunks", "--region", "us-east-1",
		"--endpoint", "localhost:19000",
	)
	if err == nil {
		t.Fatal("create with bare endpoint succeeded, want validation error")
	}
	for _, want := range []string{
		`cloud service "minio"`,
		`endpoint "localhost:19000" has no scheme`,
		`"https://localhost:19000"`,
		`"http://localhost:19000"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CLI error = %q, want it to contain %q", err, want)
		}
	}

	// Nothing persisted.
	services, listErr := cfgStore.ListCloudServices(ctx)
	if listErr != nil {
		t.Fatalf("ListCloudServices: %v", listErr)
	}
	if len(services) != 0 {
		t.Fatalf("rejected config persisted: %+v", services)
	}

	// Same command with a scheme succeeds and persists.
	if err := runCloudServiceCreate(t, ts.URL,
		"--name", "minio", "--provider", "s3",
		"--bucket", "chunks", "--region", "us-east-1",
		"--endpoint", "http://localhost:19000",
	); err != nil {
		t.Fatalf("create with scheme endpoint: %v", err)
	}
	services, _ = cfgStore.ListCloudServices(ctx)
	if len(services) != 1 || services[0].Endpoint != "http://localhost:19000" {
		t.Fatalf("valid config not persisted, got %+v", services)
	}
}
