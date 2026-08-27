package server_test

// The lookup converters skip entries they cannot represent — an http lookup
// with no url_template, a file-backed lookup with no file_id. That filtering is
// the right validation outcome, but it must not happen silently:
// PutLookupSettings rejects such entries up front, naming the entry and the
// field, rather than returning success for a write that stored nothing.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"

	"connectrpc.com/connect"
)

func TestPutLookupSettingsRejectsUnstorableEntries(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	cases := []struct {
		name   string
		lookup *gastrologv1.PutLookupSettings
	}{
		{"http without url_template", &gastrologv1.PutLookupSettings{
			HttpLookups: []*gastrologv1.HTTPLookupEntry{{Name: "api"}},
		}},
		{"json file without file_id", &gastrologv1.PutLookupSettings{
			JsonFileLookups: []*gastrologv1.JSONFileLookupEntry{{Name: "hosts"}},
		}},
		{"yaml file without file_id", &gastrologv1.PutLookupSettings{
			YamlFileLookups: []*gastrologv1.YAMLFileLookupEntry{{Name: "hosts"}},
		}},
		{"csv without file_id", &gastrologv1.PutLookupSettings{
			CsvLookups: []*gastrologv1.CSVLookupEntry{{Name: "assets"}},
		}},
		{"http without name", &gastrologv1.PutLookupSettings{
			HttpLookups: []*gastrologv1.HTTPLookupEntry{{UrlTemplate: "http://x/{v}"}},
		}},
		{"static without name", &gastrologv1.PutLookupSettings{
			StaticLookups: []*gastrologv1.StaticLookupEntry{{KeyColumn: "host"}},
		}},
		{"mmdb without name", &gastrologv1.PutLookupSettings{
			MmdbLookups: []*gastrologv1.MMDBLookupEntry{{DbType: "city"}},
		}},
		// A placeholder in the host lets an ingested field pick the destination,
		// so the template is refused where it is configured.
		{"http with a placeholder in the host", &gastrologv1.PutLookupSettings{
			HttpLookups: []*gastrologv1.HTTPLookupEntry{{Name: "api", UrlTemplate: "http://{tenant}.internal/x"}},
		}},
		{"http with a non-fetchable scheme", &gastrologv1.PutLookupSettings{
			HttpLookups: []*gastrologv1.HTTPLookupEntry{{Name: "api", UrlTemplate: "file:///etc/{v}"}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.PutLookupSettings(ctx,
				connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{Lookup: tc.lookup}))
			if err == nil {
				t.Fatal("expected an error: the entry cannot be stored, so reporting success " +
					"leaves the caller believing in a lookup that does not exist")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("got %v, want InvalidArgument: %v", connect.CodeOf(err), err)
			}
		})
	}
}

// An MMDB lookup with no file ID is NOT an error — the proto documents an empty
// file_id as "use the auto-downloaded database". The validator must not become
// stricter than the converter it mirrors.
func TestPutLookupSettingsAcceptsMMDBWithoutFileID(t *testing.T) {
	client, store, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	if _, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
		Lookup: &gastrologv1.PutLookupSettings{
			MmdbLookups: []*gastrologv1.MMDBLookupEntry{{Name: "geoip", DbType: "city"}},
		},
	})); err != nil {
		t.Fatalf("mmdb lookup with no file_id must be accepted (auto-download): %v", err)
	}

	// Read through the store: the harness's client is unauthenticated, and
	// GetSettings deliberately returns only the password policy in that case.
	ss, err := system.LoadServerSettings(ctx, store)
	if err != nil {
		t.Fatalf("LoadServerSettings: %v", err)
	}
	if n := len(ss.Lookup.MMDBLookups); n != 1 {
		t.Errorf("stored mmdb lookups = %d, want 1", n)
	}
}

// What is accepted must actually be stored. This is the other half of the
// contract: no success response for an entry that then does not exist.
func TestPutLookupSettingsStoresWhatItAccepts(t *testing.T) {
	client, store, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	fileID := glid.New().Bytes()
	if _, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
		Lookup: &gastrologv1.PutLookupSettings{
			CsvLookups: []*gastrologv1.CSVLookupEntry{{Name: "assets", FileId: fileID, KeyColumn: "host"}},
		},
	})); err != nil {
		t.Fatalf("PutLookupSettings: %v", err)
	}

	ss, err := system.LoadServerSettings(ctx, store)
	if err != nil {
		t.Fatalf("LoadServerSettings: %v", err)
	}
	if len(ss.Lookup.CSVLookups) != 1 || ss.Lookup.CSVLookups[0].Name != "assets" {
		t.Fatalf("stored csv lookups = %+v, want the one submitted", ss.Lookup.CSVLookups)
	}
}

// The lookup test procedure fetches a caller-supplied URL, so it is bound by
// the same destination policy as a configured lookup: it must not become a way
// to read the node's own network or the cloud instance metadata endpoint.
func TestTestHTTPLookupRefusesInternalDestinations(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	for _, target := range []string{
		"http://169.254.169.254/latest/meta-data/iam/security-credentials/",
		"http://127.0.0.1:9/{value}",
		"http://10.0.0.1/{value}",
	} {
		resp, err := client.TestHTTPLookup(ctx, connect.NewRequest(&gastrologv1.TestHTTPLookupRequest{
			Config: &gastrologv1.HTTPLookupEntry{Name: "probe", UrlTemplate: target, Timeout: "1s"},
			Values: map[string]string{"value": "x"},
		}))
		if err != nil {
			continue // refused outright, which is also a safe outcome
		}
		for _, r := range resp.Msg.GetResults() {
			if len(r.GetFields()) > 0 {
				t.Errorf("probe of %q returned fields %v", target, r.GetFields())
			}
		}
	}
}

// The escape hatch has to work, or an operator with a lookup service on their
// own network has no way to configure it.
func TestTestHTTPLookupAllowsPrivateWhenPermitted(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"team":"platform"}`))
	}))
	defer srv.Close()

	cfg := &gastrologv1.HTTPLookupEntry{Name: "probe", UrlTemplate: srv.URL + "/{value}", Timeout: "5s"}

	resp, err := client.TestHTTPLookup(ctx, connect.NewRequest(&gastrologv1.TestHTTPLookupRequest{
		Config: cfg,
		Values: map[string]string{"value": "x"},
	}))
	if err != nil {
		t.Fatalf("TestHTTPLookup: %v", err)
	}
	if fields := resp.Msg.GetResults()[0].GetFields(); len(fields) != 0 {
		t.Fatalf("loopback fetch succeeded without the operator opting in: %v", fields)
	}

	cfg.AllowPrivateDestinations = true
	resp, err = client.TestHTTPLookup(ctx, connect.NewRequest(&gastrologv1.TestHTTPLookupRequest{
		Config: cfg,
		Values: map[string]string{"value": "x"},
	}))
	if err != nil {
		t.Fatalf("TestHTTPLookup with the escape hatch: %v", err)
	}
	if got := resp.Msg.GetResults()[0].GetFields()["team"]; got != "platform" {
		t.Fatalf("fields = %v, want the served object", resp.Msg.GetResults()[0].GetFields())
	}
}
