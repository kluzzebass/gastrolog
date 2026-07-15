package server_test

// Config-accept validation for cloud services (gastrolog-7au6u9).
//
// PutCloudService must reject provider configs that would fail blobstore
// store creation at vault init — bare endpoints, missing bucket/region/
// container/connection_string — with a field-specific InvalidArgument
// error, before anything is persisted or replicated. Previously a bad
// config (e.g. --endpoint localhost:19000) was accepted, replicated, and
// then killed vault init on every node with the cause visible only in
// server logs.

import (
	"context"
	"net/http"
	"strings"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
)

// putCloudService issues a PutCloudService for the given config.
func putCloudService(ctx context.Context, client gastrologv1connect.SystemServiceClient, cfg *gastrologv1.CloudService) error {
	_, err := client.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{Config: cfg}))
	return err
}

func TestPutCloudServiceRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name     string
		cfg      *gastrologv1.CloudService
		wantText string
	}{
		{
			"s3 bare endpoint",
			&gastrologv1.CloudService{Name: "minio", Provider: "s3", Bucket: "chunks", Region: "us-east-1", Endpoint: "localhost:19000"},
			`endpoint "localhost:19000" has no scheme`,
		},
		{
			"s3 missing bucket",
			&gastrologv1.CloudService{Name: "s3-no-bucket", Provider: "s3", Region: "us-east-1"},
			"missing required parameter: bucket",
		},
		{
			"s3 missing region",
			&gastrologv1.CloudService{Name: "s3-no-region", Provider: "s3", Bucket: "chunks"},
			"missing required parameter: region",
		},
		{
			"gcs bare endpoint",
			&gastrologv1.CloudService{Name: "gcs-bare", Provider: "gcs", Bucket: "chunks", Endpoint: "gcs.internal:4443"},
			`endpoint "gcs.internal:4443" has no scheme`,
		},
		{
			"gcs missing bucket",
			&gastrologv1.CloudService{Name: "gcs-no-bucket", Provider: "gcs"},
			"missing required parameter: bucket",
		},
		{
			"azure missing container",
			&gastrologv1.CloudService{Name: "az-no-container", Provider: "azure", ConnectionString: "conn"},
			"missing required parameter: container",
		},
		{
			"azure missing connection_string",
			&gastrologv1.CloudService{Name: "az-no-conn", Provider: "azure", Container: "chunks"},
			"missing required parameter: connection_string",
		},
		{
			"missing provider",
			&gastrologv1.CloudService{Name: "no-provider", Bucket: "chunks"},
			"missing required parameter: provider",
		},
		{
			"unknown provider",
			&gastrologv1.CloudService{Name: "bogus-provider", Provider: "bogus"},
			"unknown provider",
		},
	}

	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := putCloudService(ctx, client, tc.cfg)
			if err == nil {
				t.Fatalf("PutCloudService accepted invalid config %v", tc.cfg)
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("code = %v, want InvalidArgument: %v", connect.CodeOf(err), err)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("error = %q, want mention of %q", err, tc.wantText)
			}
			// The rejected config must not be persisted.
			services, listErr := cfgStore.ListCloudServices(ctx)
			if listErr != nil {
				t.Fatalf("ListCloudServices: %v", listErr)
			}
			for _, cs := range services {
				if cs.Name == tc.cfg.Name {
					t.Errorf("rejected config %q was persisted", tc.cfg.Name)
				}
			}
		})
	}
}

// TestPutCloudServiceBareEndpointErrorIsActionable pins the exact error
// shape for the incident that motivated gastrolog-7au6u9: the message must
// name the offending value and both accepted forms so the operator can fix
// the flag without reading server logs.
func TestPutCloudServiceBareEndpointErrorIsActionable(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)

	err := putCloudService(context.Background(), client, &gastrologv1.CloudService{
		Name: "minio", Provider: "s3", Bucket: "chunks", Region: "us-east-1", Endpoint: "localhost:19000",
	})
	if err == nil {
		t.Fatal("bare endpoint accepted")
	}
	for _, want := range []string{
		`cloud service "minio"`,
		`endpoint "localhost:19000" has no scheme`,
		`"https://localhost:19000"`,
		`"http://localhost:19000"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	}
}

func TestPutCloudServiceAcceptsValidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  *gastrologv1.CloudService
	}{
		{"s3 with scheme endpoint", &gastrologv1.CloudService{Name: "minio", Provider: "s3", Bucket: "chunks", Region: "us-east-1", Endpoint: "http://localhost:19000"}},
		{"s3 no endpoint", &gastrologv1.CloudService{Name: "aws", Provider: "s3", Bucket: "chunks", Region: "eu-north-1"}},
		{"gcs", &gastrologv1.CloudService{Name: "gcs", Provider: "gcs", Bucket: "chunks"}},
		{"azure", &gastrologv1.CloudService{Name: "az", Provider: "azure", Container: "chunks", ConnectionString: "conn"}},
	}

	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := putCloudService(ctx, client, tc.cfg); err != nil {
				t.Fatalf("PutCloudService rejected valid config: %v", err)
			}
			services, err := cfgStore.ListCloudServices(ctx)
			if err != nil {
				t.Fatalf("ListCloudServices: %v", err)
			}
			found := false
			for _, cs := range services {
				if cs.Name == tc.cfg.Name {
					found = true
				}
			}
			if !found {
				t.Errorf("accepted config %q not persisted", tc.cfg.Name)
			}
		})
	}
}

// TestPutCloudServiceUpdateValidation covers the update path: PutCloudService
// is an upsert, so breaking an existing valid config must be rejected and the
// stored config must stay untouched.
func TestPutCloudServiceUpdateValidation(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	valid := &gastrologv1.CloudService{
		Id: id.Bytes(), Name: "minio", Provider: "s3",
		Bucket: "chunks", Region: "us-east-1", Endpoint: "http://localhost:19000",
	}
	if err := putCloudService(ctx, client, valid); err != nil {
		t.Fatalf("initial PutCloudService: %v", err)
	}

	// Update to a bare endpoint — rejected.
	broken := &gastrologv1.CloudService{
		Id: id.Bytes(), Name: "minio", Provider: "s3",
		Bucket: "chunks", Region: "us-east-1", Endpoint: "localhost:19000",
	}
	err := putCloudService(ctx, client, broken)
	if err == nil {
		t.Fatal("update to bare endpoint accepted")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument: %v", connect.CodeOf(err), err)
	}

	// Stored config unchanged.
	stored, err := cfgStore.GetCloudService(ctx, id)
	if err != nil || stored == nil {
		t.Fatalf("GetCloudService: %v (stored=%v)", err, stored)
	}
	if stored.Endpoint != "http://localhost:19000" {
		t.Errorf("stored endpoint = %q, want the original valid value", stored.Endpoint)
	}

	// Update to another valid endpoint — accepted.
	valid.Endpoint = "https://minio.internal:19000"
	if err := putCloudService(ctx, client, valid); err != nil {
		t.Fatalf("valid update rejected: %v", err)
	}
	stored, _ = cfgStore.GetCloudService(ctx, id)
	if stored == nil || stored.Endpoint != "https://minio.internal:19000" {
		t.Errorf("valid update not persisted, stored = %+v", stored)
	}
}

// mnSystemClient builds a SystemService client bound to the given node's own
// server, so config RPCs can be sent to any node of the multi-node harness —
// not just the coordinator.
func mnSystemClient(t *testing.T, h *multiNodeHarness, nodeID string) gastrologv1connect.SystemServiceClient {
	t.Helper()
	node := h.Node(t, nodeID)
	srv := server.New(node.orch, h.cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil, server.Config{
		NodeID: nodeID,
		NoAuth: true,
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded")
}

// TestMultiNodePutCloudServiceValidation proves the config-accept boundary
// is cluster-symmetric: a bad cloud-service config is rejected with the
// identical field error no matter which of the 4 nodes receives the RPC,
// nothing is persisted anywhere, and a valid config accepted on a non-
// coordinator node is visible cluster-wide.
func TestMultiNodePutCloudServiceValidation(t *testing.T) {
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2", "data-3"})
	ctx := context.Background()

	clients := map[string]gastrologv1connect.SystemServiceClient{
		"coord": h.configClient,
	}
	for _, nodeID := range []string{"data-1", "data-2", "data-3"} {
		clients[nodeID] = mnSystemClient(t, h, nodeID)
	}

	// The bad config from the motivating incident: bare endpoint.
	bad := &gastrologv1.CloudService{
		Name: "minio", Provider: "s3", Bucket: "chunks", Region: "us-east-1", Endpoint: "localhost:19000",
	}
	var msgs []string
	for _, nodeID := range []string{"coord", "data-1", "data-2", "data-3"} {
		err := putCloudService(ctx, clients[nodeID], bad)
		if err == nil {
			t.Fatalf("node %s accepted bad config", nodeID)
		}
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("node %s: code = %v, want InvalidArgument: %v", nodeID, connect.CodeOf(err), err)
		}
		if !strings.Contains(err.Error(), `endpoint "localhost:19000" has no scheme`) {
			t.Errorf("node %s: error = %q, want the endpoint-scheme message", nodeID, err)
		}
		msgs = append(msgs, err.Error())
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i] != msgs[0] {
			t.Errorf("verdict differs by receiving node:\n  node[0]: %s\n  node[%d]: %s", msgs[0], i, msgs[i])
		}
	}

	// Nothing persisted anywhere (shared replicated config store).
	services, err := h.cfgStore.ListCloudServices(ctx)
	if err != nil {
		t.Fatalf("ListCloudServices: %v", err)
	}
	if len(services) != 0 {
		t.Fatalf("rejected config persisted: %+v", services)
	}

	// A valid config accepted on a non-coordinator node replicates: the
	// coordinator's GetSystem sees it.
	good := &gastrologv1.CloudService{
		Name: "minio", Provider: "s3", Bucket: "chunks", Region: "us-east-1", Endpoint: "http://localhost:19000",
	}
	if err := putCloudService(ctx, clients["data-2"], good); err != nil {
		t.Fatalf("data-2 rejected valid config: %v", err)
	}
	resp, err := h.configClient.GetSystem(ctx, connect.NewRequest(&gastrologv1.GetSystemRequest{}))
	if err != nil {
		t.Fatalf("GetSystem via coordinator: %v", err)
	}
	found := false
	for _, cs := range resp.Msg.CloudServices {
		if cs.Name == "minio" && cs.Endpoint == "http://localhost:19000" {
			found = true
		}
	}
	if !found {
		t.Errorf("valid config accepted on data-2 not visible via coordinator GetSystem")
	}
}
