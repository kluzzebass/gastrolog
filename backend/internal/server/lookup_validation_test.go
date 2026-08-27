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
	"strings"
	"sync/atomic"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/safefetch"
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

// The lookup test procedure fetches a caller-supplied URL. The request's own
// allow-private flag must not be believed: only a stored lookup can carry the
// operator's word that a destination on their own network is intended.
func TestTestHTTPLookupIgnoresWirePrivateFlag(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"team":"platform"}`))
	}))
	defer srv.Close()

	resp, err := client.TestHTTPLookup(ctx, connect.NewRequest(&gastrologv1.TestHTTPLookupRequest{
		Config: &gastrologv1.HTTPLookupEntry{
			Name:                     "probe",
			UrlTemplate:              srv.URL + "/{value}",
			Timeout:                  "5s",
			AllowPrivateDestinations: true, // the caller asserting it for themselves
		},
		Values: map[string]string{"value": "x"},
	}))
	if err != nil {
		t.Fatalf("TestHTTPLookup: %v", err)
	}
	if !strings.Contains(resp.Msg.GetError(), safefetch.ErrBlockedDestination.Error()) {
		t.Fatalf("error = %q, want the destination policy's refusal", resp.Msg.GetError())
	}
	for _, r := range resp.Msg.GetResults() {
		if len(r.GetFields()) > 0 {
			t.Errorf("probe returned fields %v", r.GetFields())
		}
	}
	if n := requests.Load(); n != 0 {
		t.Fatalf("the probe reached the endpoint %d times", n)
	}
}

// Once the lookup is saved with the operator's opt-in, testing that same lookup
// works — otherwise a private lookup service could never be configured. The
// opt-in is tied to the saved URL, so an unsaved edit does not inherit it.
func TestTestHTTPLookupUsesStoredPrivateFlag(t *testing.T) {
	client, _, _ := newConfigTestSetupWithIngesters(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"team":"platform"}`))
	}))
	defer srv.Close()

	stored := &gastrologv1.HTTPLookupEntry{
		Name:                     "probe",
		UrlTemplate:              srv.URL + "/{value}",
		AllowPrivateDestinations: true,
	}
	if _, err := client.PutLookupSettings(ctx, connect.NewRequest(&gastrologv1.PutLookupSettingsRequest{
		Lookup: &gastrologv1.PutLookupSettings{HttpLookups: []*gastrologv1.HTTPLookupEntry{stored}},
	})); err != nil {
		t.Fatalf("PutLookupSettings: %v", err)
	}

	resp, err := client.TestHTTPLookup(ctx, connect.NewRequest(&gastrologv1.TestHTTPLookupRequest{
		Config: stored,
		Values: map[string]string{"value": "x"},
	}))
	if err != nil {
		t.Fatalf("TestHTTPLookup: %v", err)
	}
	if resp.Msg.GetError() != "" {
		t.Fatalf("error = %q, want the stored opt-in to permit the fetch", resp.Msg.GetError())
	}
	if got := resp.Msg.GetResults()[0].GetFields()["team"]; got != "platform" {
		t.Fatalf("fields = %v, want the served object", resp.Msg.GetResults()[0].GetFields())
	}

	// An unsaved edit of the URL is a different destination and loses the opt-in.
	edited := &gastrologv1.HTTPLookupEntry{
		Name:                     "probe",
		UrlTemplate:              "http://127.0.0.1:9/{value}",
		Timeout:                  "1s",
		AllowPrivateDestinations: true,
	}
	resp, err = client.TestHTTPLookup(ctx, connect.NewRequest(&gastrologv1.TestHTTPLookupRequest{
		Config: edited,
		Values: map[string]string{"value": "x"},
	}))
	if err != nil {
		t.Fatalf("TestHTTPLookup (edited): %v", err)
	}
	if !strings.Contains(resp.Msg.GetError(), safefetch.ErrBlockedDestination.Error()) {
		t.Fatalf("edited error = %q, want the destination policy's refusal", resp.Msg.GetError())
	}
}
