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
	"context"
	"net/http"
	"path/filepath"
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
		CloudTesters: map[string]server.CloudServiceTester{"file": tester},
	})
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
// a saved service runs against credentials the caller was never given, but
// only against the endpoint those credentials were stored for.
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

	t.Run("matching endpoint fills the stored credentials", func(t *testing.T) {
		seen = nil
		resp, err := client.TestCloudService(ctx, connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type:           "file",
			CloudServiceId: id.ToProto(),
			Params: map[string]string{
				"sealed_backing": "s3",
				"bucket":         "chunks",
				"region":         "us-east-1",
				"endpoint":       "https://minio.example.com:9000",
			},
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
	})

	t.Run("a different endpoint is refused rather than credited", func(t *testing.T) {
		seen = nil
		_, err := client.TestCloudService(ctx, connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type:           "file",
			CloudServiceId: id.ToProto(),
			Params: map[string]string{
				"sealed_backing": "s3",
				"bucket":         "chunks",
				"endpoint":       "https://attacker.example.net",
			},
		}))
		if err == nil {
			t.Fatal("test against a different endpoint was allowed")
		}
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument", connect.CodeOf(err))
		}
		if seen != nil {
			t.Fatalf("the tester ran anyway with %v", seen)
		}
	})

	t.Run("a supplied credential is not overwritten by the stored one", func(t *testing.T) {
		seen = nil
		if _, err := client.TestCloudService(ctx, connect.NewRequest(&gastrologv1.TestCloudServiceRequest{
			Type:           "file",
			CloudServiceId: id.ToProto(),
			Params: map[string]string{
				"sealed_backing": "s3",
				"bucket":         "chunks",
				"endpoint":       "https://minio.example.com:9000",
				"secret_key":     "typed-secret",
			},
		})); err != nil {
			t.Fatalf("TestCloudService: %v", err)
		}
		if seen["secret_key"] != "typed-secret" {
			t.Fatalf("supplied credential was replaced: %v", seen["secret_key"])
		}
	})
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
