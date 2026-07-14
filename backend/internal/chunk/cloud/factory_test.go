package cloud

// Factory config-time validation (gastrolog-3nfvo1): endpoint scheme
// enforcement, provider dispatch, and the sealed_backing provider alias.
// Everything here runs against config parsing or the in-memory provider —
// no network, no credentials.

import (
	"context"
	"errors"
	"strings"
	"testing"
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
