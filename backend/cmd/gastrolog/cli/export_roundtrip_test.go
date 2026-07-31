package cli

// `config export` claims to export the full configuration and `config import`
// is its counterpart. This test is what keeps that claim honest: it creates
// one of every config entity, exports, imports into a fresh store and diffs
// the two exports section by section. A config type added to the store but
// not to the export shows up here as an empty section instead of being
// discovered on a live cluster at 3am.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// restorableSections must come back byte-for-byte after export → import →
// export. Anything an operator configures belongs in this list.
var restorableSections = []string{
	"rotation_policies",
	"retention_policies",
	"cloud_services",
	"vaults",
	"ingesters",
	"routes",
	"nodes",
	"node_storage_configs",
	"certificates",
	"log_levels",
	"auth",
	"query",
	"scheduler",
	"tls",
	"maxmind",
	"cluster",
	"lookup",
}

// referenceOnlySections are exported so the document is readable, but cannot
// be restored — each one has an entry in the export's "excluded" list saying
// so. They are asserted to be present in the export and absent after import,
// which is what the excluded note promises.
var referenceOnlySections = []string{
	"users",
	"managed_files",
}

func TestConfigExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()

	srcURL, srcStore := newRoundTripServer(t)
	certFile, keyFile := writeTestCertFiles(t)
	seedEveryConfigType(t, ctx, srcURL, srcStore, certFile, keyFile)

	exported := runExport(t, srcURL)

	// Every section the fixture created must actually be in the document.
	// This is the "silently omits a type" guard.
	for _, section := range append(slices.Clone(restorableSections), referenceOnlySections...) {
		if len(exported[section]) == 0 || string(exported[section]) == "null" {
			t.Errorf("export is missing section %q — the fixture created one of every type", section)
		}
	}
	if len(exported["excluded"]) == 0 {
		t.Error("export must state what it excludes")
	}
	// The fixture stores cloud credentials and a MaxMind license key, so the
	// document must say it carries them.
	for _, want := range []string{"cloud service credentials", "maxmind"} {
		if !bytes.Contains(exported["contains_secrets"], []byte(want)) {
			t.Errorf("contains_secrets = %s, want it to name %q", exported["contains_secrets"], want)
		}
	}

	// Import into a fresh store, then export again and diff.
	dstURL, _ := newRoundTripServer(t)
	runImport(t, dstURL, exported)
	reimported := runExport(t, dstURL)

	for _, section := range restorableSections {
		want, got := canonicalSection(t, exported[section]), canonicalSection(t, reimported[section])
		if want != got {
			t.Errorf("section %q did not survive the round trip:\n before: %s\n  after: %s", section, want, got)
		}
	}
	for _, section := range referenceOnlySections {
		if raw := reimported[section]; len(raw) > 0 && string(raw) != "null" {
			t.Errorf("section %q is documented as not restorable but came back after import: %s", section, raw)
		}
	}
	if !bytes.Equal(exported["setup_wizard_dismissed"], reimported["setup_wizard_dismissed"]) {
		t.Errorf("setup_wizard_dismissed: before %s, after %s",
			exported["setup_wizard_dismissed"], reimported["setup_wizard_dismissed"])
	}
}

// TestConfigExportRoutesSurviveImport is the issue's concrete failure: a vault
// with retention_disposition=route whose routes vanished on restore, leaving a
// config that looks complete and routes nothing.
func TestConfigExportRoutesSurviveImport(t *testing.T) {
	ctx := context.Background()
	srcURL, _ := newRoundTripServer(t)
	client := server.NewClient(srcURL)

	vaultID := glid.New()
	mustPutVault(t, ctx, client, vaultID, "second-vault", nil)

	firstID := glid.New()
	mustPutVault(t, ctx, client, firstID, "first-vault", nil)
	if _, err := client.System.PutRoute(ctx, connect.NewRequest(&v1.PutRouteRequest{
		Config: &v1.RouteConfig{
			Id:           glid.New().ToProto(),
			Name:         "first-retention-to-second",
			Priority:     10,
			Stages:       []*v1.RouteStage{{Stage: &v1.RouteStage_Match{Match: &v1.MatchStage{Expression: `_source == "retention"`}}}},
			Destinations: []*v1.RouteDestination{{VaultId: vaultID.ToProto()}},
			Distribution: "fanout",
			Enabled:      true,
		},
	})); err != nil {
		t.Fatalf("PutRoute: %v", err)
	}

	exported := runExport(t, srcURL)
	dstURL, dstStore := newRoundTripServer(t)
	runImport(t, dstURL, exported)

	routes, err := dstStore.ListRoutes(ctx)
	if err != nil {
		t.Fatalf("ListRoutes: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("after import: got %d routes, want 1", len(routes))
	}
	got := routes[0]
	if got.Name != "first-retention-to-second" || got.Priority != 10 || !got.Enabled {
		t.Errorf("route did not round-trip: %+v", got)
	}
	if len(got.Stages) != 1 || got.Stages[0].Match == nil || got.Stages[0].Match.Expression != `_source == "retention"` {
		t.Errorf("route match expression did not round-trip: %+v", got.Stages)
	}
	if len(got.Destinations) != 1 || got.Destinations[0] != vaultID {
		t.Errorf("route destinations = %v, want [%s]", got.Destinations, vaultID)
	}
}

// TestConfigExportIsSameFromEveryNode: config is replicated, so the document
// must not depend on which node the operator's CLI reached. A section that is
// only populated from local state would show up here as a diff.
func TestConfigExportIsSameFromEveryNode(t *testing.T) {
	ctx := context.Background()

	store := sysmem.NewStore()
	nodeA := newServerOnStore(t, store)
	certFile, keyFile := writeTestCertFiles(t)
	seedEveryConfigType(t, ctx, nodeA, store, certFile, keyFile)

	// Three more nodes over the same replicated config store.
	for _, nodeN := range []string{
		newServerOnStore(t, store),
		newServerOnStore(t, store),
		newServerOnStore(t, store),
	} {
		fromA, fromN := runExport(t, nodeA), runExport(t, nodeN)
		for section := range fromA {
			if canonicalSection(t, fromA[section]) != canonicalSection(t, fromN[section]) {
				t.Errorf("section %q differs by node:\n node A: %s\n node N: %s",
					section, fromA[section], fromN[section])
			}
		}
		if len(fromN) != len(fromA) {
			t.Errorf("node exports have different sections: %d vs %d", len(fromA), len(fromN))
		}
	}
}

// TestConfigImportReplaceRemovesStaleEntities covers the --replace path for
// the entity kinds this fix added: a stale route or cloud service left behind
// by --replace would route records to a vault the restored config never
// mentions.
func TestConfigImportReplaceRemovesStaleEntities(t *testing.T) {
	ctx := context.Background()

	srcURL, srcStore := newRoundTripServer(t)
	certFile, keyFile := writeTestCertFiles(t)
	seedEveryConfigType(t, ctx, srcURL, srcStore, certFile, keyFile)
	exported := runExport(t, srcURL)

	// A target cluster with entities the document does not contain.
	dstURL, dstStore := newRoundTripServer(t)
	client := server.NewClient(dstURL)
	staleVault := glid.New()
	mustPutVault(t, ctx, client, staleVault, "stale-vault", nil)
	// A lookup the document does not contain. proto3 cannot express "clear this
	// list", so a stale enrichment table left behind by --replace would keep
	// enriching records the restored config never mentions.
	// Deliberately a YAML-file lookup: the document contains http, static, mmdb
	// and csv lookups, and PutLookupSettings replaces each list it CONTAINS —
	// so a stale entry of one of those kinds is cleared as a side effect and
	// proves nothing. Only a kind the document omits entirely exercises the
	// gap, because proto3 cannot express "clear this list".
	if _, err := client.System.PutLookupSettings(ctx, connect.NewRequest(&v1.PutLookupSettingsRequest{
		Lookup: &v1.PutLookupSettings{
			YamlFileLookups: []*v1.YAMLFileLookupEntry{{
				Name: "stale-yaml", FileId: glid.New().Bytes(), KeyColumn: "host",
			}},
		},
	})); err != nil {
		t.Fatalf("PutLookupSettings (stale): %v", err)
	}
	if _, err := client.System.PutRoute(ctx, connect.NewRequest(&v1.PutRouteRequest{
		Config: &v1.RouteConfig{
			Id: glid.New().ToProto(), Name: "stale-route",
			Destinations: []*v1.RouteDestination{{VaultId: staleVault.ToProto()}},
			Enabled:      true,
		},
	})); err != nil {
		t.Fatalf("PutRoute: %v", err)
	}
	if _, err := client.System.PutCloudService(ctx, connect.NewRequest(&v1.PutCloudServiceRequest{
		Config: &v1.CloudService{
			Id: glid.New().ToProto(), Name: "stale-cloud", Provider: "s3",
			Bucket: "old", Region: "us-east-1", Endpoint: "http://localhost:19001",
		},
	})); err != nil {
		t.Fatalf("PutCloudService: %v", err)
	}

	runImportReplace(t, dstURL, exported)

	routes, err := dstStore.ListRoutes(ctx)
	if err != nil {
		t.Fatalf("ListRoutes: %v", err)
	}
	if len(routes) != 1 || routes[0].Name != "ingest-to-logs" {
		t.Errorf("after --replace: routes = %+v, want only the imported one", routes)
	}
	services, err := dstStore.ListCloudServices(ctx)
	if err != nil {
		t.Fatalf("ListCloudServices: %v", err)
	}
	if len(services) != 1 || services[0].Name != "minio" {
		t.Errorf("after --replace: cloud services = %+v, want only the imported one", services)
	}
	// The stale lookup must be gone, and only the document's remain.
	settingsAfter, err := server.NewClient(dstURL).System.GetSettings(ctx, connect.NewRequest(&v1.GetSettingsRequest{}))
	if err != nil {
		t.Fatalf("GetSettings: %v", err)
	}
	for _, e := range settingsAfter.Msg.GetLookup().GetYamlFileLookups() {
		if e.GetName() == "stale-yaml" {
			t.Errorf("after --replace: stale lookup %q survived — the document has no "+
				"yaml lookups, so nothing replaced that list", e.GetName())
		}
	}
	if got := len(settingsAfter.Msg.GetLookup().GetStaticLookups()); got != 1 {
		t.Errorf("after --replace: static lookups = %d, want 1 (the imported one)", got)
	}

	// The archival chain must survive the round trip intact. An empty class
	// here would read as "delete at this age" rather than "move to a colder
	// storage class" — losing this field on the round trip is a data-destroying
	// outcome,
	// not a cosmetic one.
	if got := services[0].Transitions; len(got) != 2 {
		t.Fatalf("archival transitions after round trip: %+v, want 2", got)
	} else {
		if got[0].After != "30d" || got[0].CloudStorageClass != "GLACIER" {
			t.Errorf("transition[0] = %+v, want {30d GLACIER}", got[0])
		}
		if got[1].After != "365d" || got[1].CloudStorageClass != "DEEP_ARCHIVE" {
			t.Errorf("transition[1] = %+v, want {365d DEEP_ARCHIVE}", got[1])
		}
	}
	vaults, err := dstStore.ListVaults(ctx)
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	// Exactly the fixture's vaults, and nothing the target had before. The
	// fixture seeds two: a memory vault and a file-backed, cloud-backed one whose
	// storage_class, replication_factor and cache_* fields a memory vault never
	// exercises.
	gotVaults := map[string]bool{}
	for _, v := range vaults {
		gotVaults[v.Name] = true
	}
	if len(vaults) != 2 || !gotVaults["logs"] || !gotVaults["cloud-backed"] {
		t.Errorf("after --replace: vaults = %+v, want exactly the two imported ones", vaults)
	}
	if gotVaults["stale-vault"] {
		t.Error("after --replace: the target's pre-existing vault survived")
	}
}

// TestImportDecodesExportedIDs pins the encoding contract between the two
// commands: export renders IDs as base32hex GLID strings, so import has to
// accept them. It also has to keep accepting the raw base64 form a
// hand-written document may use.
func TestImportDecodesExportedIDs(t *testing.T) {
	id := glid.New()
	doc := exportDoc{RotationPolicies: []*v1.RotationPolicyConfig{{Id: id.ToProto(), Name: "rp"}}}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}

	for name, data := range map[string][]byte{
		"as exported (base32hex)": convertGLIDFields(raw),
		"hand-written (base64)":   raw,
	} {
		t.Run(name, func(t *testing.T) {
			var back exportDoc
			if err := json.Unmarshal(decodeGLIDFields(data), &back); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(back.RotationPolicies) != 1 {
				t.Fatalf("got %d policies, want 1", len(back.RotationPolicies))
			}
			if got := glid.FromBytes(back.RotationPolicies[0].Id); got != id {
				t.Errorf("id = %s, want %s", got, id)
			}
		})
	}
}

// TestConfigImportOrdersTransferTargets: a vault with
// retention_disposition=transfer names another vault, and PutVault rejects a
// target that does not exist yet. Entity order in the document is whatever the
// store's map iteration produced, so an import that puts vaults in document
// order restores this config only when it gets lucky. The document below is
// deliberately ordered source-first.
func TestConfigImportOrdersTransferTargets(t *testing.T) {
	ctx := context.Background()
	srcURL, _ := newRoundTripServer(t)
	client := server.NewClient(srcURL)

	targetID, sourceID := glid.New(), glid.New()
	putFileVault(t, ctx, client, targetID, "archive", nil)
	putFileVault(t, ctx, client, sourceID, "hot", &targetID)

	exported := runExport(t, srcURL)
	exported["vaults"] = reorderVaults(t, exported["vaults"], "hot", "archive")

	dstURL, dstStore := newRoundTripServer(t)
	runImport(t, dstURL, exported)

	vaults, err := dstStore.ListVaults(ctx)
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	if len(vaults) != 2 {
		t.Fatalf("after import: got %d vaults, want 2", len(vaults))
	}
	for _, v := range vaults {
		if v.Name != "hot" {
			continue
		}
		if v.RetentionTransferTargetVaultID == nil || *v.RetentionTransferTargetVaultID != targetID {
			t.Errorf("hot vault transfer target = %v, want %s", v.RetentionTransferTargetVaultID, targetID)
		}
	}
}

// TestOrderVaultsForImportHandlesCycle: a hand-edited document can name a
// transfer cycle. Ordering must hand every vault over anyway so the server's
// cycle detection reports it, rather than spinning or dropping vaults.
func TestOrderVaultsForImportHandlesCycle(t *testing.T) {
	a, b := glid.New(), glid.New()
	vaults := protoList[*v1.VaultConfig]{
		{Id: a.ToProto(), Name: "a", RetentionTransferTargetVaultId: b.ToProto()},
		{Id: b.ToProto(), Name: "b", RetentionTransferTargetVaultId: a.ToProto()},
	}
	got := orderVaultsForImport(vaults)
	if len(got) != 2 {
		t.Fatalf("got %d vaults, want both", len(got))
	}
	seen := map[string]bool{}
	for _, v := range got {
		seen[v.Name] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Errorf("ordering dropped a vault: %v", seen)
	}
}

// reorderVaults rewrites the vaults section into the given name order.
func reorderVaults(t *testing.T, raw json.RawMessage, names ...string) json.RawMessage {
	t.Helper()
	var items []map[string]any
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatal(err)
	}
	ordered := make([]map[string]any, 0, len(items))
	for _, name := range names {
		for _, item := range items {
			if item["name"] == name {
				ordered = append(ordered, item)
			}
		}
	}
	if len(ordered) != len(items) {
		t.Fatalf("reorderVaults: named %d of %d vaults", len(ordered), len(items))
	}
	out, err := json.Marshal(ordered)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestConfigImportRejectsUnknownEntityFields: a field the server does not know
// must stop the import, not be dropped on the way in. Silently ignoring it is
// how a config ends up looking restored while a setting is missing.
func TestConfigImportRejectsUnknownEntityFields(t *testing.T) {
	dstURL, dstStore := newRoundTripServer(t)
	doc := map[string]json.RawMessage{
		"vaults": json.RawMessage(`[{"id":"` + glid.New().String() + `","name":"logs","type":"VAULT_TYPE_MEMORY","retention_dispositoin":"route"}]`),
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := NewConfigCommand()
	AddClientFlags(cmd)
	cmd.SetArgs([]string{"import", path, "--addr", dstURL})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	err = cmd.Execute()
	if err == nil {
		t.Fatal("import accepted a misspelled field, want an error")
	}
	vaults, listErr := dstStore.ListVaults(context.Background())
	if listErr != nil {
		t.Fatalf("ListVaults: %v", listErr)
	}
	if len(vaults) != 0 {
		t.Errorf("rejected document still persisted %d vaults", len(vaults))
	}
}

// TestConfigExportImportEmptyCluster: nothing configured is a legitimate
// state, and it must not turn into an error or a phantom entity. Server
// settings still appear — those are the effective values, which is what an
// operator reviewing an export wants to see.
func TestConfigExportImportEmptyCluster(t *testing.T) {
	ctx := context.Background()
	srcURL, _ := newRoundTripServer(t)
	exported := runExport(t, srcURL)

	entitySections := append(slices.Clone(referenceOnlySections),
		"rotation_policies", "retention_policies", "cloud_services", "vaults",
		"ingesters", "routes", "nodes", "node_storage_configs", "certificates")
	for _, section := range entitySections {
		if raw, ok := exported[section]; ok {
			t.Errorf("empty cluster exported entity section %q: %s", section, raw)
		}
	}
	// No cloud service, no MaxMind key: the document must not claim to carry
	// credentials it does not have.
	if got := string(exported["contains_secrets"]); got != "[]" {
		t.Errorf("contains_secrets = %s, want [] for a cluster with no credentials", got)
	}

	dstURL, dstStore := newRoundTripServer(t)
	runImport(t, dstURL, exported)

	vaults, err := dstStore.ListVaults(ctx)
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	routes, err := dstStore.ListRoutes(ctx)
	if err != nil {
		t.Fatalf("ListRoutes: %v", err)
	}
	if len(vaults) != 0 || len(routes) != 0 {
		t.Errorf("importing an empty document created entities: %d vaults, %d routes", len(vaults), len(routes))
	}
}

// --- fixture ---------------------------------------------------------------

func newRoundTripServer(t *testing.T) (string, *sysmem.Store) {
	t.Helper()
	store := sysmem.NewStore()
	return newServerOnStore(t, store), store
}

// newServerOnStore starts an in-process server over an existing config store,
// so several "nodes" can be pointed at one replicated config the way a real
// cluster shares its Raft-backed store.
func newServerOnStore(t *testing.T, cfgStore *sysmem.Store) string {
	t.Helper()
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
	return ts.URL
}

// seedEveryConfigType creates one of every entity the export document carries.
func seedEveryConfigType(t *testing.T, ctx context.Context, addr string, store *sysmem.Store, certFile, keyFile string) {
	t.Helper()
	client := server.NewClient(addr)

	rotationID := glid.New()
	if _, err := client.System.PutRotationPolicy(ctx, connect.NewRequest(&v1.PutRotationPolicyRequest{
		Config: &v1.RotationPolicyConfig{Id: rotationID.ToProto(), Name: "hourly", MaxSize: "512MiB", MaxAge: "1h"},
	})); err != nil {
		t.Fatalf("PutRotationPolicy: %v", err)
	}

	retentionID := glid.New()
	if _, err := client.System.PutRetentionPolicy(ctx, connect.NewRequest(&v1.PutRetentionPolicyRequest{
		Config: &v1.RetentionPolicyConfig{Id: retentionID.ToProto(), Name: "thirty-days", MaxAge: "30d", MaxSize: "50GB"},
	})); err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}

	// A file-backed, cloud-backed vault with the fields a memory vault never
	// exercises. The round trip is a byte-diff of export -> import -> export, so
	// it can only detect the loss of fields the FIXTURE populated: a vault that
	// only ever sets id/name/enabled/type/memory_budget leaves storage_class,
	// replication_factor, cache_* and the retention disposition untested.
	cloudBackedID := glid.New()
	if _, err := client.System.PutVault(ctx, connect.NewRequest(&v1.PutVaultRequest{
		Config: &v1.VaultConfig{
			Id: cloudBackedID.ToProto(), Name: "cloud-backed", Enabled: true,
			Type:                 v1.VaultType_VAULT_TYPE_FILE,
			StorageClass:         2,
			ReplicationFactor:    3,
			CacheEviction:        "ttl",
			CacheBudget:          "256MiB",
			CacheTtl:             "12h",
			RetentionDisposition: "delete",
		},
	})); err != nil {
		t.Fatalf("PutVault cloud-backed: %v", err)
	}

	// Enrichment lookup tables. Every kind, with non-default fields set: these
	// were absent from the document entirely, and a fixture that leaves a field
	// zero cannot detect that field being dropped.
	if _, err := client.System.PutLookupSettings(ctx, connect.NewRequest(&v1.PutLookupSettingsRequest{
		Lookup: &v1.PutLookupSettings{
			HttpLookups: []*v1.HTTPLookupEntry{{
				Name: "users", UrlTemplate: "http://api/users/{value}",
				Headers: map[string]string{"Authorization": "Bearer x"},
				Timeout: "3s", CacheTtl: "5m", CacheSize: 500,
				ResponsePaths: []string{"$.data.user"},
			}},
			StaticLookups: []*v1.StaticLookupEntry{{
				Name: "teams", KeyColumn: "host", ValueColumns: []string{"team", "owner"},
				Rows: []*v1.StaticLookupRow{
					{Values: map[string]string{"host": "web-1", "team": "frontend", "owner": "ana"}},
				},
			}},
			MmdbLookups: []*v1.MMDBLookupEntry{{Name: "geoip", DbType: "city"}},
			CsvLookups: []*v1.CSVLookupEntry{{
				Name: "assets", FileId: glid.New().Bytes(), KeyColumn: "hostname",
				ValueColumns: []string{"rack", "site"},
			}},
		},
	})); err != nil {
		t.Fatalf("PutLookupSettings: %v", err)
	}

	cloudID := glid.New()
	if _, err := client.System.PutCloudService(ctx, connect.NewRequest(&v1.PutCloudServiceRequest{
		Config: &v1.CloudService{
			Id: cloudID.ToProto(), Name: "minio", Provider: "s3", Bucket: "chunks",
			Region: "us-east-1", Endpoint: "http://localhost:19000",
			AccessKey: "access", SecretKey: "secret",
			ArchivalMode: "none", ReconcileSchedule: "0 3 * * *",
			// A two-step archival chain, because the transition's cloud class
			// is exported by its PROTO field name (UseProtoNames) and an
			// absent class does not mean "unset" — it means DELETE the chunk
			// at that age. A round trip that drops it would turn a
			// move-to-colder-storage policy into an expiry policy, silently.
			Transitions: []*v1.CloudStorageTransition{
				{After: "30d", CloudStorageClass: "GLACIER"},
				{After: "365d", CloudStorageClass: "DEEP_ARCHIVE"},
			},
		},
	})); err != nil {
		t.Fatalf("PutCloudService: %v", err)
	}

	vaultID := glid.New()
	mustPutVault(t, ctx, client, vaultID, "logs", &v1.RetentionRule{RetentionPolicyId: retentionID.ToProto()})

	nodeID := glid.New()
	if _, err := client.System.PutNodeConfig(ctx, connect.NewRequest(&v1.PutNodeConfigRequest{
		Config: &v1.NodeConfig{Id: nodeID.ToProto(), Name: "node-1"},
	})); err != nil {
		t.Fatalf("PutNodeConfig: %v", err)
	}

	if _, err := client.System.SetNodeStorageConfig(ctx, connect.NewRequest(&v1.SetNodeStorageConfigRequest{
		Config: &v1.NodeStorageConfig{
			NodeId: []byte(nodeID.String()),
			FileStorages: []*v1.FileStorage{{
				Id: glid.New().ToProto(), Name: "fast", Path: "/var/lib/gastrolog/fast",
				StorageClass: 1, DiskFreeWarn: "10%", DiskFreeFloor: "3%",
			}},
		},
	})); err != nil {
		t.Fatalf("SetNodeStorageConfig: %v", err)
	}

	// Disabled: an enabled ingester is dry-run constructed on the local node,
	// which a config round trip has no business depending on.
	if _, err := client.System.PutIngester(ctx, connect.NewRequest(&v1.PutIngesterRequest{
		Config: &v1.IngesterConfig{
			Id: glid.New().ToProto(), Name: "syslog", Type: "syslog",
			Params: map[string]string{"port": "5514"}, Enabled: false,
			NodeIds: [][]byte{[]byte(nodeID.String())},
		},
	})); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	if _, err := client.System.PutRoute(ctx, connect.NewRequest(&v1.PutRouteRequest{
		Config: &v1.RouteConfig{
			Id: glid.New().ToProto(), Name: "ingest-to-logs", Priority: 100,
			Stages:       []*v1.RouteStage{{Stage: &v1.RouteStage_Match{Match: &v1.MatchStage{Expression: `_source == "ingest"`}}}},
			Destinations: []*v1.RouteDestination{{VaultId: vaultID.ToProto()}},
			Distribution: "fanout", Enabled: true,
		},
	})); err != nil {
		t.Fatalf("PutRoute: %v", err)
	}

	if _, err := client.System.PutCertificate(ctx, connect.NewRequest(&v1.PutCertificateRequest{
		Id: glid.New().ToProto(), Name: "server", CertFile: certFile, KeyFile: keyFile,
	})); err != nil {
		t.Fatalf("PutCertificate: %v", err)
	}

	if _, err := client.Auth.CreateUser(ctx, connect.NewRequest(&v1.CreateUserRequest{
		Username: "operator", Password: "Correct-Horse-9", Role: "admin",
	})); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Managed files are registered by upload, not by an RPC — seed the store
	// directly so the export has one to list.
	if err := store.PutManagedFile(ctx, system.ManagedFileConfig{
		ID: glid.New(), Name: "GeoLite2-City.mmdb", SHA256: "abc123", Size: 4096,
		UploadedAt: time.Now().UTC().Truncate(time.Second),
	}); err != nil {
		t.Fatalf("PutManagedFile: %v", err)
	}

	if _, err := client.System.PutLogLevels(ctx, connect.NewRequest(&v1.PutLogLevelsRequest{
		Config: &v1.LogLevelConfig{
			DefaultLevel: v1.LogLevel_LOG_LEVEL_INFO,
			Rules:        []*v1.LogLevelRule{{Pattern: "orchestrator.**", Level: v1.LogLevel_LOG_LEVEL_DEBUG}},
		},
	})); err != nil {
		t.Fatalf("PutLogLevels: %v", err)
	}

	tokenDuration, refresh := "168h", "720h"
	timeout, follow := "30s", "4h"
	maxResults, maxJobs, minLen := int32(5000), int32(4), int32(12)
	tlsEnabled, redirect := true, true
	httpsPort := "8443"
	broadcast, backlog := "5s", "1GiB"
	if _, err := client.System.PutServiceSettings(ctx, connect.NewRequest(&v1.PutServiceSettingsRequest{
		Auth: &v1.PutAuthSettings{
			TokenDuration: &tokenDuration, RefreshTokenDuration: &refresh,
			PasswordPolicy: &v1.PutPasswordPolicySettings{MinLength: &minLen},
		},
		Query:     &v1.PutQuerySettings{Timeout: &timeout, MaxFollowDuration: &follow, MaxResultCount: &maxResults},
		Scheduler: &v1.PutSchedulerSettings{MaxConcurrentJobs: &maxJobs},
		Tls: &v1.PutTLSSettings{
			Enabled: &tlsEnabled, DefaultCert: strPtr("server"),
			HttpToHttpsRedirect: &redirect, HttpsPort: &httpsPort,
		},
		Cluster: &v1.PutClusterSettings{
			BroadcastInterval: &broadcast, PipelineBacklogMax: &backlog,
		},
	})); err != nil {
		t.Fatalf("PutServiceSettings: %v", err)
	}

	autoDownload := true
	licenseKey := "license-key"
	if _, err := client.System.PutMaxMindSettings(ctx, connect.NewRequest(&v1.PutMaxMindSettingsRequest{
		Maxmind: &v1.PutMaxMindSettings{AutoDownload: &autoDownload, AccountId: []byte("123456"), LicenseKey: &licenseKey},
	})); err != nil {
		t.Fatalf("PutMaxMindSettings: %v", err)
	}

	dismissed := true
	if _, err := client.System.PutSetupSettings(ctx, connect.NewRequest(&v1.PutSetupSettingsRequest{
		SetupWizardDismissed: &dismissed,
	})); err != nil {
		t.Fatalf("PutSetupSettings: %v", err)
	}
}

func strPtr(s string) *string { return &s }

func mustPutVault(t *testing.T, ctx context.Context, client *server.Client, id glid.GLID, name string, rule *v1.RetentionRule) {
	t.Helper()
	cfg := &v1.VaultConfig{
		Id: id.ToProto(), Name: name, Enabled: true,
		Type: v1.VaultType_VAULT_TYPE_MEMORY, MemoryBudget: "64MiB",
	}
	if rule != nil {
		cfg.RetentionRules = []*v1.RetentionRule{rule}
	}
	if _, err := client.System.PutVault(ctx, connect.NewRequest(&v1.PutVaultRequest{Config: cfg})); err != nil {
		t.Fatalf("PutVault %q: %v", name, err)
	}
}

// putFileVault creates a file vault, optionally with retention transfer to
// another vault.
func putFileVault(t *testing.T, ctx context.Context, client *server.Client, id glid.GLID, name string, transferTo *glid.GLID) {
	t.Helper()
	cfg := &v1.VaultConfig{
		Id: id.ToProto(), Name: name, Enabled: true, Type: v1.VaultType_VAULT_TYPE_FILE,
	}
	if transferTo != nil {
		cfg.RetentionDisposition = "transfer"
		cfg.RetentionTransferTargetVaultId = transferTo.ToProto()
	}
	if _, err := client.System.PutVault(ctx, connect.NewRequest(&v1.PutVaultRequest{Config: cfg})); err != nil {
		t.Fatalf("PutVault %q: %v", name, err)
	}
}

func writeTestCertFiles(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gastrolog-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// --- command drivers -------------------------------------------------------

// runExport runs `config export` against addr and returns the document as
// top-level sections.
func runExport(t *testing.T, addr string) map[string]json.RawMessage {
	t.Helper()
	out := captureStdout(t, func() {
		cmd := NewConfigCommand()
		AddClientFlags(cmd)
		cmd.SetArgs([]string{"export", "--addr", addr})
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		if err := cmd.Execute(); err != nil {
			t.Errorf("config export: %v", err)
		}
	})
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("export output is not JSON: %v\n%s", err, out)
	}
	return doc
}

// runImport writes the document to a file and runs `config import` on it, the
// way an operator restores a backup.
func runImport(t *testing.T, addr string, doc map[string]json.RawMessage) {
	t.Helper()
	runImportWith(t, addr, doc)
}

// runImportReplace runs the destructive form, which deletes existing entities
// before importing.
func runImportReplace(t *testing.T, addr string, doc map[string]json.RawMessage) {
	t.Helper()
	runImportWith(t, addr, doc, "--replace", "--yes")
}

func runImportWith(t *testing.T, addr string, doc map[string]json.RawMessage, extra ...string) {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = captureStdout(t, func() {
		cmd := NewConfigCommand()
		AddClientFlags(cmd)
		cmd.SetArgs(append([]string{"import", path, "--addr", addr}, extra...))
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		if err := cmd.Execute(); err != nil {
			t.Fatalf("config import: %v", err)
		}
	})
}

func captureStdout(t *testing.T, fn func()) []byte {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	fn()

	_ = w.Close()
	os.Stdout = old
	<-done
	_ = r.Close()
	return buf.Bytes()
}

// canonicalSection renders one export section order-independently: entity
// lists come out of the store in map order, which says nothing about whether
// the config survived.
func canonicalSection(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	if len(raw) == 0 {
		return "<missing>"
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return string(raw) // an object (settings section) — compare as-is
	}
	lines := make([]string, len(items))
	for i, item := range items {
		lines[i] = string(item)
	}
	slices.Sort(lines)
	out, err := json.Marshal(lines)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
