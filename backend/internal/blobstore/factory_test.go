package blobstore

// Factory config-time validation: endpoint scheme enforcement, provider
// dispatch, and the sealed_backing provider alias. Everything here runs
// against config parsing or the in-memory provider — no network, no
// credentials.

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"gastrolog/internal/system"
)

func TestValidateEndpoint(t *testing.T) {
	t.Parallel()

	t.Run("empty passes through", func(t *testing.T) {
		t.Parallel()
		got, err := validateEndpoint("")
		if err != nil || got != "" {
			t.Fatalf("validateEndpoint(\"\") = %q, %v; want \"\", nil", got, err)
		}
	})

	t.Run("explicit scheme is kept verbatim", func(t *testing.T) {
		t.Parallel()
		for _, ep := range []string{"https://s3.example.com", "http://localhost:9000"} {
			got, err := validateEndpoint(ep)
			if err != nil || got != ep {
				t.Fatalf("validateEndpoint(%q) = %q, %v; want %q, nil", ep, got, err, ep)
			}
		}
	})

	t.Run("bare host:port is rejected, naming both accepted forms", func(t *testing.T) {
		t.Parallel()
		_, err := validateEndpoint("localhost:9000")
		if err == nil {
			t.Fatal("bare endpoint must not be silently upgraded to plaintext http://")
		}
		msg := err.Error()
		if !strings.Contains(msg, "https://localhost:9000") || !strings.Contains(msg, "http://localhost:9000") {
			t.Fatalf("error must name both accepted forms, got: %v", err)
		}
	})
}

func TestCreateStoreBareEndpointFailsAtConfigTime(t *testing.T) {
	t.Parallel()

	for _, provider := range []string{"s3", "gcs"} {
		_, err := CreateStore(provider, map[string]string{
			ParamBucket:   "b",
			ParamRegion:   "r",
			ParamEndpoint: "minio.internal:9000",
		}, nil)
		if err == nil || !strings.Contains(err.Error(), "no scheme") {
			t.Fatalf("%s with bare endpoint: err = %v, want scheme error", provider, err)
		}
	}
}

// TestCreateStoreMissingParams pins the per-provider required-parameter
// guards in createStore: each missing param fails at config time, naming
// the parameter, before any provider SDK or network is touched.
func TestCreateStoreMissingParams(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		provider  string
		params    map[string]string
		wantParam string
	}{
		{"s3 missing bucket", "s3", map[string]string{ParamRegion: "eu-north-1"}, "bucket"},
		{"s3 missing region", "s3", map[string]string{ParamBucket: "b"}, "region"},
		{"azure missing container", "azure", map[string]string{ParamConnectionString: "cs"}, "container"},
		{"azure missing connection_string", "azure", map[string]string{ParamContainer: "c"}, "connection_string"},
		{"gcs missing bucket", "gcs", map[string]string{}, "bucket"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := CreateStore(tc.provider, tc.params, nil)
			if err == nil {
				t.Fatalf("CreateStore(%s, %v): expected error, got nil", tc.provider, tc.params)
			}
			if !strings.Contains(err.Error(), "missing required parameter") {
				t.Fatalf("error = %q, want mention of missing required parameter", err)
			}
			if !strings.Contains(err.Error(), tc.wantParam) {
				t.Fatalf("error = %q, want mention of %q", err, tc.wantParam)
			}
		})
	}
}

func TestCreateStoreUnknownProviderNamesAllProviders(t *testing.T) {
	t.Parallel()

	_, err := CreateStore("bogus", nil, nil)
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("err = %v, want ErrUnknownProvider", err)
	}
	for _, name := range []string{"s3", "azure", "gcs", "memory"} {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("ErrUnknownProvider must name %q, got: %v", name, err)
		}
	}
}

func TestConnectionTesterMemoryProvider(t *testing.T) {
	t.Parallel()

	test := NewConnectionTester(nil)

	t.Run("provider param", func(t *testing.T) {
		t.Parallel()
		msg, err := test(context.Background(), map[string]string{ParamProvider: "memory"})
		if err != nil {
			t.Fatalf("connection test: %v", err)
		}
		if !strings.Contains(msg, "memory") {
			t.Fatalf("result should name the provider, got %q", msg)
		}
	})

	t.Run("sealed_backing alias", func(t *testing.T) {
		t.Parallel()
		if _, err := test(context.Background(), map[string]string{ParamSealedBacking: "memory"}); err != nil {
			t.Fatalf("connection test via sealed_backing: %v", err)
		}
	})

	t.Run("missing provider", func(t *testing.T) {
		t.Parallel()
		if _, err := test(context.Background(), map[string]string{}); !errors.Is(err, ErrMissingProvider) {
			t.Fatalf("err = %v, want ErrMissingProvider", err)
		}
	})
}

// --- Config-accept validation ----------------------------------------------
//
// ValidateConfig is the shared shape validator: createStore runs it before
// building a store, and PutCloudService runs it before accepting a config.
// These tests pin every provider branch and prove the two paths agree.

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	valid := []struct {
		name     string
		provider string
		params   map[string]string
	}{
		{"s3 full", "s3", map[string]string{ParamBucket: "b", ParamRegion: "r", ParamEndpoint: "http://localhost:19000"}},
		{"s3 empty endpoint ok", "s3", map[string]string{ParamBucket: "b", ParamRegion: "r"}},
		{"s3 uppercase scheme ok", "s3", map[string]string{ParamBucket: "b", ParamRegion: "r", ParamEndpoint: "HTTPS://s3.example.com"}},
		{"gcs bucket only", "gcs", map[string]string{ParamBucket: "b"}},
		{"gcs with endpoint", "gcs", map[string]string{ParamBucket: "b", ParamEndpoint: "https://gcs.example.com"}},
		{"azure full", "azure", map[string]string{ParamContainer: "c", ParamConnectionString: "cs"}},
		{"memory no params", "memory", nil},
	}
	for _, tc := range valid {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateConfig(tc.provider, tc.params); err != nil {
				t.Fatalf("ValidateConfig(%s, %v) = %v, want nil", tc.provider, tc.params, err)
			}
		})
	}

	invalid := []struct {
		name     string
		provider string
		params   map[string]string
		wantText string
	}{
		{"s3 bare endpoint", "s3", map[string]string{ParamBucket: "b", ParamRegion: "r", ParamEndpoint: "localhost:19000"}, "no scheme"},
		{"s3 missing bucket", "s3", map[string]string{ParamRegion: "r"}, "missing required parameter: bucket"},
		{"s3 missing region", "s3", map[string]string{ParamBucket: "b"}, "missing required parameter: region"},
		{"gcs bare endpoint", "gcs", map[string]string{ParamBucket: "b", ParamEndpoint: "minio.internal:9000"}, "no scheme"},
		{"gcs missing bucket", "gcs", map[string]string{}, "missing required parameter: bucket"},
		{"azure missing container", "azure", map[string]string{ParamConnectionString: "cs"}, "missing required parameter: container"},
		{"azure missing connection_string", "azure", map[string]string{ParamContainer: "c"}, "missing required parameter: connection_string"},
		{"empty provider", "", nil, ErrMissingProvider.Error()},
		{"unknown provider", "bogus", nil, ErrUnknownProvider.Error()},
	}
	for _, tc := range invalid {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateConfig(tc.provider, tc.params)
			if err == nil {
				t.Fatalf("ValidateConfig(%s, %v) = nil, want error containing %q", tc.provider, tc.params, tc.wantText)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error = %q, want mention of %q", err, tc.wantText)
			}

			// Single source of truth: CreateStore must reject the same
			// shape with the same error text — the validator cannot
			// drift from store creation.
			_, createErr := CreateStore(tc.provider, tc.params, nil)
			if createErr == nil {
				t.Fatalf("CreateStore accepted a shape ValidateConfig rejects: %s %v", tc.provider, tc.params)
			}
			if createErr.Error() != err.Error() {
				t.Fatalf("CreateStore error %q != ValidateConfig error %q", createErr, err)
			}
		})
	}
}

// TestStoreParamsKeysMatchFactoryConstants pins the string-literal keys in
// system.CloudService.StoreParams to the blobstore.Param* constants. system
// cannot import blobstore (provider SDKs would leak into the config
// package), so this test is the drift guard for the shared mapping.
func TestStoreParamsKeysMatchFactoryConstants(t *testing.T) {
	t.Parallel()

	cs := system.CloudService{
		Bucket:           "b",
		Region:           "r",
		Endpoint:         "https://e.example.com",
		AccessKey:        "ak",
		SecretKey:        "sk",
		Container:        "c",
		ConnectionString: "conn",
		CredentialsJSON:  "{}",
	}
	got := cs.StoreParams()
	want := map[string]string{
		ParamBucket:           "b",
		ParamRegion:           "r",
		ParamEndpoint:         "https://e.example.com",
		ParamAccessKey:        "ak",
		ParamSecretKey:        "sk",
		ParamContainer:        "c",
		ParamConnectionString: "conn",
		ParamCredentialsJSON:  "{}",
	}
	if !maps.Equal(got, want) {
		t.Fatalf("StoreParams() = %v, want %v", got, want)
	}

	if empty := (system.CloudService{}).StoreParams(); len(empty) != 0 {
		t.Fatalf("StoreParams() on zero config = %v, want empty (empty fields omitted)", empty)
	}
}
