package server_test

// Cloud service credentials are write-only over the API: reads report
// whether credentials exist, never what they are. Two consequences are
// load-bearing and tested here.
//
// First, a client editing a service never received its credentials, so it
// cannot send them back — an empty credential field on a write must keep
// the stored value. If it cleared instead, the first save after a config
// read would strip every cloud service of its credentials and the cluster
// would lose read access to sealed chunks already in the object store.
//
// Second, only an admin asking for secrets gets them; every other read,
// and every mutation echo regardless of caller, is redacted.

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"
)

// newConfigTestSetupWithCloudTester is newConfigTestSetup with a stub
// connection tester, so a test can see the params a connection test runs with.
func newConfigTestSetupWithCloudTester(t *testing.T, tester server.CloudServiceTester) (gastrologv1connect.SystemServiceClient, system.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, SegmentsDir: filepath.Join(t.TempDir(), "segments")})
	if err != nil {
		t.Fatal(err)
	}
	factories := orchestrator.Factories{VaultsDir: t.TempDir()}
	srv := server.New(orch, cfgStore, factories, nil, server.Config{
		TrustContextClaims: true,
		CloudTesters:       map[string]server.CloudServiceTester{"file": tester},
	})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded"), cfgStore, orch
}

// newConfigTestSetupWithLogger is newConfigTestSetup with the server's
// structured logger captured, so a test can assert on what it recorded.
func newConfigTestSetupWithLogger(t *testing.T, logger *slog.Logger) (gastrologv1connect.SystemServiceClient, system.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, SegmentsDir: filepath.Join(t.TempDir(), "segments")})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(orch, cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil, server.Config{
		TrustContextClaims: true, Logger: logger})
	httpClient := &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
	return gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded"), cfgStore, orch
}

func adminContext(ctx context.Context) context.Context {
	return auth.WithClaims(ctx, &auth.Claims{
		Role:             "admin",
		UserID:           glid.New().String(),
		RegisteredClaims: jwt.RegisteredClaims{Subject: "root"},
	})
}

func userContext(ctx context.Context) context.Context {
	return auth.WithClaims(ctx, &auth.Claims{
		Role:             "user",
		UserID:           glid.New().String(),
		RegisteredClaims: jwt.RegisteredClaims{Subject: "reader"},
	})
}

// s3ServiceWithCredentials returns a valid S3 cloud service carrying both keys.
func s3ServiceWithCredentials(id []byte, name string) *gastrologv1.CloudService {
	return &gastrologv1.CloudService{
		Id:        id,
		Name:      name,
		Provider:  "s3",
		Bucket:    "chunks",
		Region:    "us-east-1",
		AccessKey: "AKIAEXAMPLE",
		SecretKey: "s3cr3t-key-value",
	}
}

func getCloudService(t *testing.T, ctx context.Context, client gastrologv1connect.SystemServiceClient, id glid.GLID, includeSecrets bool) *gastrologv1.CloudService {
	t.Helper()
	resp, err := client.GetSystem(ctx, connect.NewRequest(&gastrologv1.GetSystemRequest{IncludeSecrets: includeSecrets}))
	if err != nil {
		t.Fatalf("GetSystem: %v", err)
	}
	for _, cs := range resp.Msg.CloudServices {
		if glid.FromBytes(cs.Id) == id {
			return cs
		}
	}
	t.Fatalf("cloud service %s not in GetSystem response", id)
	return nil
}

// TestPutCloudServicePreservesCredentials is the round-trip that the
// redaction depends on: read a service (credentials absent), save it back
// unchanged, and the stored credentials must survive.
func TestPutCloudServicePreservesCredentials(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := adminContext(context.Background())

	id := glid.New()
	if err := putCloudService(ctx, client, s3ServiceWithCredentials(id.ToProto(), "archive")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Premise: the credentials really are stored, so a later assertion that
	// they survived is not passing against an empty store.
	stored, err := cfgStore.GetCloudService(ctx, id)
	if err != nil || stored == nil {
		t.Fatalf("load stored service: %v", err)
	}
	if stored.AccessKey != "AKIAEXAMPLE" || stored.SecretKey != "s3cr3t-key-value" {
		t.Fatalf("credentials were not stored on create: access=%q secret=%q", stored.AccessKey, stored.SecretKey)
	}

	// What a UI actually sends back: everything it was given, which never
	// included the credentials, plus an edit to an ordinary field.
	redacted := getCloudService(t, ctx, client, id, false)
	if redacted.AccessKey != "" || redacted.SecretKey != "" {
		t.Fatalf("read returned credentials: access=%q secret=%q", redacted.AccessKey, redacted.SecretKey)
	}
	redacted.Region = "eu-west-1"
	if err := putCloudService(ctx, client, redacted); err != nil {
		t.Fatalf("save unchanged credentials: %v", err)
	}

	after, err := cfgStore.GetCloudService(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("reload service: %v", err)
	}
	if after.AccessKey != "AKIAEXAMPLE" || after.SecretKey != "s3cr3t-key-value" {
		t.Fatalf("save wiped credentials: access=%q secret=%q", after.AccessKey, after.SecretKey)
	}
	if after.Region != "eu-west-1" {
		t.Fatalf("save did not apply the edited field: region=%q", after.Region)
	}
}

// TestPutCloudServiceReplacesSuppliedCredential covers the other half:
// a credential the operator did type replaces the stored one, and only
// that one.
func TestPutCloudServiceReplacesSuppliedCredential(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := adminContext(context.Background())

	id := glid.New()
	if err := putCloudService(ctx, client, s3ServiceWithCredentials(id.ToProto(), "archive")); err != nil {
		t.Fatalf("create: %v", err)
	}

	rotated := getCloudService(t, ctx, client, id, false)
	rotated.SecretKey = "rotated-secret"
	if err := putCloudService(ctx, client, rotated); err != nil {
		t.Fatalf("rotate secret: %v", err)
	}

	after, err := cfgStore.GetCloudService(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("reload service: %v", err)
	}
	if after.SecretKey != "rotated-secret" {
		t.Fatalf("supplied credential was not applied: secret=%q", after.SecretKey)
	}
	if after.AccessKey != "AKIAEXAMPLE" {
		t.Fatalf("untouched credential was not preserved: access=%q", after.AccessKey)
	}
}

// TestPutCloudServiceClearsCredentialsOnRequest — an empty field can no
// longer mean "remove this", so moving a service to its provider's ambient
// credential chain needs an explicit instruction.
func TestPutCloudServiceClearsCredentialsOnRequest(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := adminContext(context.Background())

	id := glid.New()
	if err := putCloudService(ctx, client, s3ServiceWithCredentials(id.ToProto(), "archive")); err != nil {
		t.Fatalf("create: %v", err)
	}

	cleared := getCloudService(t, ctx, client, id, false)
	if _, err := client.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{
		Config:           cleared,
		ClearCredentials: true,
	})); err != nil {
		t.Fatalf("clear credentials: %v", err)
	}

	after, err := cfgStore.GetCloudService(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("reload service: %v", err)
	}
	if after.AccessKey != "" || after.SecretKey != "" {
		t.Fatalf("credentials survived an explicit clear: access=%q secret=%q", after.AccessKey, after.SecretKey)
	}
	if after.HasCredentials() {
		t.Fatal("service still reports configured credentials after a clear")
	}
}

// TestCloudCredentialRemovalIsLoggedOnlyWhenItHappens — credentials are
// write-only, so this log line is the only record that a service stopped
// carrying them. A line for a write that was rejected would make the one
// auditable surface of this change untrustworthy.
func TestCloudCredentialRemovalIsLoggedOnlyWhenItHappens(t *testing.T) {
	var logged bytes.Buffer
	client, cfgStore, _ := newConfigTestSetupWithLogger(t, slog.New(slog.NewTextHandler(&logged, nil)))
	ctx := adminContext(context.Background())

	id := glid.New()
	// Only an access key: partial material still counts as something to lose.
	if err := putCloudService(ctx, client, &gastrologv1.CloudService{
		Id: id.ToProto(), Name: "half", Provider: "s3",
		Bucket: "chunks", Region: "us-east-1", AccessKey: "AKIAEXAMPLE",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A write that would have removed the credentials but is rejected: the
	// merge has already computed a credential-less config by the time
	// ValidateConfig refuses it for the missing region.
	rejected := getCloudService(t, ctx, client, id, false)
	rejected.Region = ""
	if _, err := client.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{
		Config: rejected, ClearCredentials: true,
	})); err == nil {
		t.Fatal("a config missing a required parameter was accepted")
	}
	if stored, err := cfgStore.GetCloudService(ctx, id); err != nil || stored == nil || stored.AccessKey != "AKIAEXAMPLE" {
		t.Fatalf("the rejected write reached the store: %+v (%v)", stored, err)
	}
	if strings.Contains(logged.String(), "credentials removed") {
		t.Fatalf("a rejected write logged a credential removal:\n%s", logged.String())
	}

	// The real thing does log.
	if _, err := client.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{
		Config: getCloudService(t, ctx, client, id, false), ClearCredentials: true,
	})); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if !strings.Contains(logged.String(), "credentials removed") {
		t.Fatalf("removing partial credentials was not logged:\n%s", logged.String())
	}
}

// TestGetSystemRedactsCloudCredentials pins who sees credential material.
func TestGetSystemRedactsCloudCredentials(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	id := glid.New()
	if err := putCloudService(adminContext(ctx), client, s3ServiceWithCredentials(id.ToProto(), "archive")); err != nil {
		t.Fatalf("create: %v", err)
	}

	cases := []struct {
		name           string
		ctx            context.Context
		includeSecrets bool
		wantSecrets    bool
	}{
		{"admin without the flag", adminContext(ctx), false, false},
		{"admin with the flag", adminContext(ctx), true, true},
		{"non-admin with the flag", userContext(ctx), true, false},
		{"unauthenticated with the flag", ctx, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := getCloudService(t, tc.ctx, client, id, tc.includeSecrets)
			gotSecrets := cs.AccessKey != "" || cs.SecretKey != ""
			if gotSecrets != tc.wantSecrets {
				t.Fatalf("credentials returned = %v, want %v (access=%q secret=%q)", gotSecrets, tc.wantSecrets, cs.AccessKey, cs.SecretKey)
			}
			if !cs.CredentialsConfigured {
				t.Fatalf("credentials_configured = false for a service that has both keys")
			}
		})
	}
}

// TestCredentialsConfiguredPerProvider checks the flag reports the
// credential shape each provider actually uses, not just "some field set".
func TestCredentialsConfiguredPerProvider(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := adminContext(context.Background())

	cases := []struct {
		name string
		cfg  *gastrologv1.CloudService
		want bool
	}{
		{"s3 both keys", &gastrologv1.CloudService{Provider: "s3", Bucket: "c", Region: "r", AccessKey: "ak", SecretKey: "sk"}, true},
		{"s3 access key only", &gastrologv1.CloudService{Provider: "s3", Bucket: "c", Region: "r", AccessKey: "ak"}, false},
		{"s3 ambient chain", &gastrologv1.CloudService{Provider: "s3", Bucket: "c", Region: "r"}, false},
		{"azure connection string", &gastrologv1.CloudService{Provider: "azure", Container: "c", ConnectionString: "conn"}, true},
		{"gcs credentials json", &gastrologv1.CloudService{Provider: "gcs", Bucket: "c", CredentialsJson: "{}"}, true},
		{"gcs application default", &gastrologv1.CloudService{Provider: "gcs", Bucket: "c"}, false},
		{"memory needs none", &gastrologv1.CloudService{Provider: "memory"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := glid.New()
			tc.cfg.Id = id.ToProto()
			tc.cfg.Name = tc.name
			if err := putCloudService(ctx, client, tc.cfg); err != nil {
				t.Fatalf("create: %v", err)
			}
			cs := getCloudService(t, ctx, client, id, false)
			if cs.CredentialsConfigured != tc.want {
				t.Fatalf("credentials_configured = %v, want %v", cs.CredentialsConfigured, tc.want)
			}
		})
	}
}

// TestPutCloudServiceEchoNeverCarriesCredentials — mutation responses echo
// the whole system, and that echo is not a secret-retrieval path for
// anyone, admin included.
func TestPutCloudServiceEchoNeverCarriesCredentials(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := adminContext(context.Background())

	id := glid.New()
	resp, err := client.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{
		Config: s3ServiceWithCredentials(id.ToProto(), "archive"),
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, cs := range resp.Msg.System.CloudServices {
		if cs.AccessKey != "" || cs.SecretKey != "" || cs.ConnectionString != "" || cs.CredentialsJson != "" {
			t.Fatalf("PutCloudService echoed credentials for %q", cs.Name)
		}
	}

	// The same echo rides on unrelated mutations.
	del, err := client.DeleteCloudService(ctx, connect.NewRequest(&gastrologv1.DeleteCloudServiceRequest{Id: glid.New().ToProto()}))
	if err == nil {
		for _, cs := range del.Msg.System.CloudServices {
			if cs.AccessKey != "" || cs.SecretKey != "" {
				t.Fatalf("DeleteCloudService echoed credentials for %q", cs.Name)
			}
		}
	}
}

// TestCloudServiceConnectionTestUsesStoredCredentials — a connection test on
// a saved service runs against credentials the caller was never given, and
// only against the destination those credentials were stored for: every
// bucket, region, container and endpoint comes from the stored service, not
// from the request.
func TestCloudServiceConnectionTestUsesStoredCredentials(t *testing.T) {
	var seen map[string]string
	client, _, _ := newConfigTestSetupWithCloudTester(t, func(_ context.Context, params map[string]string) (string, error) {
		seen = params
		return "ok", nil
	})
	ctx := adminContext(context.Background())

	id := glid.New()
	cfg := s3ServiceWithCredentials(id.ToProto(), "archive")
	cfg.Endpoint = "https://minio.example.com:9000"
	if err := putCloudService(ctx, client, cfg); err != nil {
		t.Fatalf("create: %v", err)
	}

	t.Run("no credentials supplied tests the stored service as stored", func(t *testing.T) {
		seen = nil
		resp, err := client.TestCloudService(ctx, connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type:           "file",
			CloudServiceId: id.ToProto(),
			Params:         map[string]string{"sealed_backing": "s3", "bucket": "chunks"},
		}))
		if err != nil {
			t.Fatalf("TestCloudService: %v", err)
		}
		if !resp.Msg.Success {
			t.Fatalf("test reported failure: %s", resp.Msg.Message)
		}
		if seen["access_key"] != "AKIAEXAMPLE" || seen["secret_key"] != "s3cr3t-key-value" {
			t.Fatalf("stored credentials were not used: %v", seen)
		}
		if seen["endpoint"] != "https://minio.example.com:9000" || seen["region"] != "us-east-1" {
			t.Fatalf("the test did not run against the stored destination: %v", seen)
		}
	})

	// The destination is the whole exposure: stored credentials spent on a
	// caller-named bucket or endpoint confirm the credentials work, under the
	// operator's cloud identity, in storage the caller controls.
	t.Run("caller-named destinations do not redirect stored credentials", func(t *testing.T) {
		for _, override := range []map[string]string{
			{"endpoint": "https://attacker.example.net"},
			{"bucket": "attacker-bucket"},
			{"region": "eu-central-1"},
			{"container": "attacker-container"},
		} {
			seen = nil
			params := map[string]string{"sealed_backing": "s3", "bucket": "chunks"}
			for k, v := range override {
				params[k] = v
			}
			if _, err := client.TestCloudService(ctx, connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
				Type: "file", CloudServiceId: id.ToProto(), Params: params,
			})); err != nil {
				t.Fatalf("TestCloudService %v: %v", override, err)
			}
			for k, v := range override {
				if seen[k] == v {
					t.Fatalf("stored credentials were spent on a caller-supplied %s = %q", k, v)
				}
			}
		}
	})

	// Half a supplied key pair completed from the store would answer
	// "was the missing half right?" — an online guessing oracle.
	t.Run("a partial credential is never completed from the store", func(t *testing.T) {
		seen = nil
		if _, err := client.TestCloudService(ctx, connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type:           "file",
			CloudServiceId: id.ToProto(),
			Params: map[string]string{
				"sealed_backing": "s3",
				"bucket":         "chunks",
				"endpoint":       "https://minio.example.com:9000",
				"secret_key":     "guessed-secret",
			},
		})); err != nil {
			t.Fatalf("TestCloudService: %v", err)
		}
		if seen["secret_key"] != "guessed-secret" {
			t.Fatalf("supplied credential was replaced: %q", seen["secret_key"])
		}
		if seen["access_key"] != "" {
			t.Fatalf("the missing half was completed from the store: %q", seen["access_key"])
		}
	})

	t.Run("spending stored credentials requires the admin role", func(t *testing.T) {
		seen = nil
		_, err := client.TestCloudService(userContext(context.Background()), connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type:           "file",
			CloudServiceId: id.ToProto(),
			Params:         map[string]string{"sealed_backing": "s3", "bucket": "chunks"},
		}))
		if err == nil {
			t.Fatal("a non-admin spent the stored credentials")
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("code = %v, want PermissionDenied", connect.CodeOf(err))
		}
		if seen != nil {
			t.Fatalf("the tester ran anyway with %v", seen)
		}
	})

	// Partial credentials are still credentials to spend: the gate asks
	// whether any material is stored, not whether a complete set is.
	t.Run("a partially credentialed service is still admin-only", func(t *testing.T) {
		partialID := glid.New()
		if err := putCloudService(ctx, client, &gastrologv1.CloudService{
			Id: partialID.ToProto(), Name: "half", Provider: "s3",
			Bucket: "chunks", Region: "us-east-1", AccessKey: "AKIAEXAMPLE",
		}); err != nil {
			t.Fatalf("create: %v", err)
		}
		seen = nil
		_, err := client.TestCloudService(userContext(context.Background()), connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type:           "file",
			CloudServiceId: partialID.ToProto(),
			Params:         map[string]string{"sealed_backing": "s3", "bucket": "chunks"},
		}))
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("code = %v, want PermissionDenied", connect.CodeOf(err))
		}
		if seen != nil {
			t.Fatalf("the tester ran anyway with %v", seen)
		}
	})

	t.Run("a caller supplying its own credentials needs no service and no role", func(t *testing.T) {
		seen = nil
		if _, err := client.TestCloudService(userContext(context.Background()), connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type: "file",
			Params: map[string]string{
				"sealed_backing": "s3",
				"bucket":         "my-own-bucket",
				"access_key":     "my-own-key",
				"secret_key":     "my-own-secret",
			},
		})); err != nil {
			t.Fatalf("TestCloudService: %v", err)
		}
		if seen["bucket"] != "my-own-bucket" || seen["access_key"] != "my-own-key" {
			t.Fatalf("the caller's own configuration was not used: %v", seen)
		}
	})
}

// TestPutCloudServiceDropsCredentialsTheProviderCannotUse — a provider
// switch must not leave the old provider's secrets behind, unreachable by
// any read and unclearable by any edit.
func TestPutCloudServiceDropsCredentialsTheProviderCannotUse(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := adminContext(context.Background())

	id := glid.New()
	if err := putCloudService(ctx, client, s3ServiceWithCredentials(id.ToProto(), "archive")); err != nil {
		t.Fatalf("create: %v", err)
	}

	switched := getCloudService(t, ctx, client, id, false)
	switched.Provider = "gcs"
	switched.Region = ""
	switched.CredentialsJson = "{}"
	if err := putCloudService(ctx, client, switched); err != nil {
		t.Fatalf("switch provider: %v", err)
	}

	after, err := cfgStore.GetCloudService(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("reload service: %v", err)
	}
	if after.AccessKey != "" || after.SecretKey != "" {
		t.Fatalf("the old provider's credentials were left behind: access=%q secret=%q", after.AccessKey, after.SecretKey)
	}
	if after.CredentialsJSON != "{}" {
		t.Fatalf("the new provider's credential was not stored: %q", after.CredentialsJSON)
	}
}

// TestClusterPutCloudServicePreservesCredentials runs the round trip on a
// multi-node harness: several orchestrators, the routing interceptor
// attached, and clear_credentials carried as a request field rather than as
// part of the config.
//
// What it does NOT cover, so nobody reads more into it than is there: the
// harness gives every node the same in-memory config store object, so there
// is no Raft, no replication lag and no per-node view, and PutCloudService is
// RouteLeader, which the routing interceptor passes through without
// forwarding. A save arriving at a node whose replicated state is behind is
// therefore untested here.
func TestClusterPutCloudServicePreservesCredentials(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"coord", "data-1", "data-2"}, WithoutVault("coord"))
	ctx := adminContext(context.Background())

	id := glid.New()
	if _, err := h.configClient.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{
		Config: s3ServiceWithCredentials(id.ToProto(), "archive"),
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	stored, err := h.cfgStore.GetCloudService(ctx, id)
	if err != nil || stored == nil || !stored.HasCredentials() {
		t.Fatalf("credentials were not stored on create: %+v (%v)", stored, err)
	}

	sys, err := h.configClient.GetSystem(ctx, connect.NewRequest(&gastrologv1.GetSystemRequest{}))
	if err != nil {
		t.Fatalf("GetSystem: %v", err)
	}
	var redacted *gastrologv1.CloudService
	for _, cs := range sys.Msg.CloudServices {
		if glid.FromBytes(cs.Id) == id {
			redacted = cs
		}
	}
	if redacted == nil {
		t.Fatal("cloud service missing from the cluster config read")
	}
	if redacted.AccessKey != "" || redacted.SecretKey != "" {
		t.Fatalf("cluster read returned credentials: access=%q", redacted.AccessKey)
	}

	redacted.Region = "eu-west-1"
	if _, err := h.configClient.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{
		Config: redacted,
	})); err != nil {
		t.Fatalf("save: %v", err)
	}
	after, err := h.cfgStore.GetCloudService(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("reload: %v", err)
	}
	if after.AccessKey != "AKIAEXAMPLE" || after.SecretKey != "s3cr3t-key-value" {
		t.Fatalf("cluster save wiped credentials: access=%q secret=%q", after.AccessKey, after.SecretKey)
	}
	if after.Region != "eu-west-1" {
		t.Fatalf("cluster save did not apply the edit: region=%q", after.Region)
	}

	// clear_credentials must survive the same path — it is a request field,
	// not part of the replicated config, so nothing carries it implicitly.
	if _, err := h.configClient.PutCloudService(ctx, connect.NewRequest(&gastrologv1.PutCloudServiceRequest{
		Config: redacted, ClearCredentials: true,
	})); err != nil {
		t.Fatalf("clear: %v", err)
	}
	cleared, err := h.cfgStore.GetCloudService(ctx, id)
	if err != nil || cleared == nil {
		t.Fatalf("reload after clear: %v", err)
	}
	if cleared.HasCredentials() {
		t.Fatalf("clear_credentials did not survive the cluster path: access=%q", cleared.AccessKey)
	}
}

// TestGetSettingsSecretsRequireAdmin — the MaxMind license key is the same
// defect one struct over: include_secrets must not hand it to a reader.
func TestGetSettingsSecretsRequireAdmin(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	ss.MaxMind.AccountID = "123456"
	ss.MaxMind.LicenseKey = "maxmind-license"
	if err := cfgStore.SaveServerSettings(ctx, ss); err != nil {
		t.Fatalf("save settings: %v", err)
	}

	cases := []struct {
		name        string
		ctx         context.Context
		wantSecrets bool
	}{
		{"admin", adminContext(ctx), true},
		{"non-admin", userContext(ctx), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := client.GetSettings(tc.ctx, connect.NewRequest(&gastrologv1.GetSettingsRequest{IncludeSecrets: true}))
			if err != nil {
				t.Fatalf("GetSettings: %v", err)
			}
			mm := resp.Msg.Maxmind
			if mm == nil {
				t.Fatal("no maxmind settings in response")
			}
			gotSecrets := mm.LicenseKey != "" || len(mm.AccountId) > 0
			if gotSecrets != tc.wantSecrets {
				t.Fatalf("secrets returned = %v, want %v (key=%q account=%q)", gotSecrets, tc.wantSecrets, mm.LicenseKey, mm.AccountId)
			}
			if !mm.LicenseConfigured {
				t.Fatal("license_configured = false for settings that carry both fields")
			}
		})
	}
}
