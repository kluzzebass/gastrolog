package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"gastrolog/internal/glid"
	"io"
	"os"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

func newImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import [file]",
		Short: "Import configuration from JSON",
		Long:  "Imports configuration from a JSON file (or stdin if no file given). Use --merge (default) to upsert or --replace to delete all existing entities first.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			replace, _ := cmd.Flags().GetBool("replace")
			yes, _ := cmd.Flags().GetBool("yes")

			doc, err := readImportDoc(args)
			if err != nil {
				return err
			}

			if replace && !yes {
				fmt.Print("This will DELETE all existing entities before importing. Continue? [y/N] ")
				reader := bufio.NewReader(os.Stdin)
				line, _ := reader.ReadString('\n')
				if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "y") {
					fmt.Println("Aborted.")
					return nil
				}
			}

			client := clientFromCmd(cmd)
			ctx := context.Background()

			if replace {
				if err := deleteAll(ctx, client); err != nil {
					return fmt.Errorf("delete existing entities: %w", err)
				}
			}

			imported, err := importEntities(ctx, client, &doc)
			if err != nil {
				return err
			}

			fmt.Printf("Imported %d entities\n", imported)
			return nil
		},
	}
	cmd.Flags().Bool("merge", true, "merge with existing config (default)")
	cmd.Flags().Bool("replace", false, "delete all existing entities before importing")
	cmd.Flags().Bool("yes", false, "skip confirmation for --replace")
	cmd.MarkFlagsMutuallyExclusive("merge", "replace")
	return cmd
}

// readImportDoc reads and decodes an exportDoc from a file argument or stdin.
func readImportDoc(args []string) (exportDoc, error) {
	var doc exportDoc
	var raw []byte
	var err error
	if len(args) == 1 {
		// Path is the CLI argument the operator typed.
		raw, err = os.ReadFile(args[0]) //ok:os-readfile a config document, already decoded whole into memory
		if err != nil {
			return doc, err
		}
	} else {
		raw, err = io.ReadAll(os.Stdin) //ok:io-readall a config document, already decoded whole into memory
		if err != nil {
			return doc, fmt.Errorf("read JSON from stdin: %w", err)
		}
	}
	if err := json.Unmarshal(decodeGLIDFields(raw), &doc); err != nil {
		return doc, fmt.Errorf("decode JSON: %w", err)
	}
	return doc, nil
}

// protoIDSections are the export-document sections that decode into generated
// proto messages, where an ID field is a bytes field that encoding/json reads
// as base64. `config export` renders those IDs as base32hex GLID strings for
// readability (printer.json → convertGLIDFields), so the document has to be
// turned back before it can be decoded — without this, importing an exported
// document failed on the first entity with an ID ("illegal base64 data"), so
// no export/import round trip worked at all.
//
// Sections that decode into local structs with string IDs (certificates,
// users, nodes, managed_files) are left alone: their IDs are already text.
var protoIDSections = map[string]bool{
	"rotation_policies":    true,
	"retention_policies":   true,
	"cloud_services":       true,
	"vaults":               true,
	"ingesters":            true,
	"routes":               true,
	"node_storage_configs": true,
	// Lookups carry file_id fields with the same treatment. Omitting the
	// section meant an exported file-backed lookup came back as base64-decoded
	// garbage — 19 bytes that glid.MustParse then PANICKED on, taking down the
	// settings endpoint.
	"lookup": true,
}

// decodeGLIDFields is the inverse of convertGLIDFields: within the proto
// sections it rewrites base32hex GLID strings in ID fields back to the base64
// encoding of the raw 16 bytes that encoding/json expects. Values that are not
// GLIDs — including IDs already written as base64 by hand — are left as they
// are, so a hand-authored document still imports.
func decodeGLIDFields(data []byte) []byte {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return data // not an object; let the real decode report the error
	}
	changed := false
	for key, raw := range top {
		if !protoIDSections[key] {
			continue
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		unwalkJSON(v, "")
		encoded, err := json.Marshal(v)
		if err != nil {
			continue
		}
		top[key] = encoded
		changed = true
	}
	if !changed {
		return data
	}
	out, err := json.Marshal(top)
	if err != nil {
		return data
	}
	return out
}

// unwalkJSON mirrors walkJSON, converting GLID strings back to base64 bytes.
func unwalkJSON(v any, key string) {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			if s, ok := child.(string); ok && isIDField(k) {
				if converted, ok := glidToBase64(s); ok {
					val[k] = converted
				}
			} else {
				unwalkJSON(child, k)
			}
		}
	case []any:
		for i, item := range val {
			if s, ok := item.(string); ok && isIDField(key) {
				if converted, ok := glidToBase64(s); ok {
					val[i] = converted
					continue
				}
			}
			unwalkJSON(item, key)
		}
	}
}

// glidToBase64 converts a base32hex GLID string to the base64 form
// encoding/json uses for proto bytes fields.
func glidToBase64(s string) (string, bool) {
	g, err := glid.Parse(s)
	if err != nil || g.IsZero() {
		return "", false
	}
	return base64.StdEncoding.EncodeToString(g.Bytes()), true
}

// importStep imports one section of the document, returning how many entities
// it put.
type importStep func(context.Context, *server.Client, *resolver, *exportDoc) (int, error)

// importEntities imports all entities from the document.
//
// The steps run in dependency order: a referenced entity is put before the
// entities that name it. Cloud services before vaults (cloud_service_id),
// nodes before node storage (node_id) and ingesters (node_ids), vaults before
// routes (destination vault_id).
//
// There is no FilterConfig entity: a route's match expression is inlined on
// the route's stages and is imported with the route by importRoutes.
func importEntities(ctx context.Context, client *server.Client, doc *exportDoc) (int, error) {
	r, err := newResolver(ctx, client)
	if err != nil {
		return 0, err
	}

	var imported int
	for _, step := range []importStep{
		importRotationPolicies,
		importRetentionPolicies,
		importCloudServices,
		importNodes,
		importNodeStorageConfigs,
		importVaults,
		importIngesters,
		importRoutes,
		importLogLevels,
		importLookups,
		importCertificates,
		reportUnrestorable,
	} {
		n, stepErr := step(ctx, client, r, doc)
		imported += n
		if stepErr != nil {
			return imported, stepErr
		}
	}

	n, err := importServerSettings(ctx, client, doc)
	if err != nil {
		return imported, err
	}
	imported += n

	return imported, nil
}

func importRotationPolicies(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, p := range doc.RotationPolicies {
		ensureProtoID(p.Name, r.rotationPolicies, &p.Id)
		_, err := client.System.PutRotationPolicy(ctx, connect.NewRequest(&v1.PutRotationPolicyRequest{
			Config: p,
		}))
		if err != nil {
			return imported, fmt.Errorf("import rotation policy %q: %w", p.Name, err)
		}
		imported++
	}
	return imported, nil
}

func importRetentionPolicies(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, p := range doc.RetentionPolicies {
		ensureProtoID(p.Name, r.retentionPolicies, &p.Id)
		_, err := client.System.PutRetentionPolicy(ctx, connect.NewRequest(&v1.PutRetentionPolicyRequest{
			Config: p,
		}))
		if err != nil {
			return imported, fmt.Errorf("import retention policy %q: %w", p.Name, err)
		}
		imported++
	}
	return imported, nil
}

func importCloudServices(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, cs := range doc.CloudServices {
		ensureProtoID(cs.Name, r.cloudServices, &cs.Id)
		_, err := client.System.PutCloudService(ctx, connect.NewRequest(&v1.PutCloudServiceRequest{
			Config: cs,
		}))
		if err != nil {
			return imported, fmt.Errorf("import cloud service %q: %w", cs.Name, err)
		}
		imported++
	}
	return imported, nil
}

func importNodes(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, n := range doc.Nodes {
		ensureID(n.Name, r.nodes, &n.ID)
		nodeID, parseErr := glid.ParseUUID(n.ID)
		if parseErr != nil {
			return imported, fmt.Errorf("import node %q: invalid ID %q: %w", n.Name, n.ID, parseErr)
		}
		_, err := client.System.PutNodeConfig(ctx, connect.NewRequest(&v1.PutNodeConfigRequest{
			Config: &v1.NodeConfig{Id: nodeID.ToProto(), Name: n.Name},
		}))
		if err != nil {
			return imported, fmt.Errorf("import node %q: %w", n.Name, err)
		}
		imported++
	}
	return imported, nil
}

func importNodeStorageConfigs(ctx context.Context, client *server.Client, _ *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, nsc := range doc.NodeStorageConfigs {
		nsc.NodeId = textIDBytes(nsc.NodeId)
		_, err := client.System.SetNodeStorageConfig(ctx, connect.NewRequest(&v1.SetNodeStorageConfigRequest{
			Config: nsc,
		}))
		if err != nil {
			return imported, fmt.Errorf("import storage config for node %s: %w", formatIDBytes(nsc.NodeId), err)
		}
		imported++
	}
	return imported, nil
}

func importVaults(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, v := range orderVaultsForImport(doc.Vaults) {
		ensureProtoID(v.Name, r.vaults, &v.Id)
		_, err := client.System.PutVault(ctx, connect.NewRequest(&v1.PutVaultRequest{
			Config: v,
		}))
		if err != nil {
			return imported, fmt.Errorf("import vault %q: %w", v.Name, err)
		}
		imported++
	}
	return imported, nil
}

// orderVaultsForImport puts a vault's retention transfer target ahead of the
// vault that names it: PutVault rejects a target that does not exist yet, and
// the document's order is whatever the store's map iteration produced, so
// importing in document order would restore a transfer pair only by luck.
// Vaults whose target is not in the document (already on the target cluster,
// or a genuine dangling reference the server should reject) keep their place.
func orderVaultsForImport(vaults protoList[*v1.VaultConfig]) []*v1.VaultConfig {
	inDoc := make(map[string]bool, len(vaults))
	for _, v := range vaults {
		inDoc[string(v.GetId())] = true
	}

	ordered := make([]*v1.VaultConfig, 0, len(vaults))
	placed := make(map[string]bool, len(vaults))
	remaining := slices.Clone([]*v1.VaultConfig(vaults))
	for len(remaining) > 0 {
		progressed := false
		still := remaining[:0:0]
		for _, v := range remaining {
			target := string(v.GetRetentionTransferTargetVaultId())
			if target == "" || placed[target] || !inDoc[target] {
				ordered = append(ordered, v)
				placed[string(v.GetId())] = true
				progressed = true
				continue
			}
			still = append(still, v)
		}
		if !progressed {
			// A cycle in the document. Hand the rest over unchanged and let
			// the server's cycle detection produce the real error.
			return append(ordered, still...)
		}
		remaining = still
	}
	return ordered
}

func importIngesters(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, ig := range doc.Ingesters {
		ensureProtoID(ig.Name, r.ingesters, &ig.Id)
		for i := range ig.NodeIds {
			ig.NodeIds[i] = textIDBytes(ig.NodeIds[i])
		}
		ig.NodeId = textIDBytes(ig.NodeId)
		_, err := client.System.PutIngester(ctx, connect.NewRequest(&v1.PutIngesterRequest{
			Config: ig,
		}))
		if err != nil {
			return imported, fmt.Errorf("import ingester %q: %w", ig.Name, err)
		}
		imported++
	}
	return imported, nil
}

func importRoutes(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, rt := range doc.Routes {
		ensureProtoID(rt.Name, r.routes, &rt.Id)
		_, err := client.System.PutRoute(ctx, connect.NewRequest(&v1.PutRouteRequest{
			Config: rt,
		}))
		if err != nil {
			return imported, fmt.Errorf("import route %q: %w", rt.Name, err)
		}
		imported++
	}
	return imported, nil
}

// importLookups restores the enrichment lookup tables. PutLookupSettings
// replaces each list wholesale when present, so this is also the --replace
// behaviour: a lookup on the target that the document does not contain is
// removed, which is what "restore this configuration" has to mean.
func importLookups(ctx context.Context, client *server.Client, _ *resolver, doc *exportDoc) (int, error) {
	if doc.Lookup == nil || doc.Lookup.Msg == nil {
		return 0, nil
	}
	l := doc.Lookup.Msg
	if _, err := client.System.PutLookupSettings(ctx, connect.NewRequest(&v1.PutLookupSettingsRequest{
		Lookup: &v1.PutLookupSettings{
			HttpLookups:     l.GetHttpLookups(),
			JsonFileLookups: l.GetJsonFileLookups(),
			YamlFileLookups: l.GetYamlFileLookups(),
			MmdbLookups:     l.GetMmdbLookups(),
			CsvLookups:      l.GetCsvLookups(),
			StaticLookups:   l.GetStaticLookups(),
		},
	})); err != nil {
		return 0, fmt.Errorf("import lookups: %w", err)
	}
	return 1, nil
}

func importLogLevels(ctx context.Context, client *server.Client, _ *resolver, doc *exportDoc) (int, error) {
	if doc.LogLevels == nil || doc.LogLevels.Msg == nil {
		return 0, nil
	}
	if _, err := client.System.PutLogLevels(ctx, connect.NewRequest(&v1.PutLogLevelsRequest{
		Config: doc.LogLevels.Msg,
	})); err != nil {
		return 0, fmt.Errorf("import log levels: %w", err)
	}
	return 1, nil
}

func importCertificates(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	var imported int
	for _, c := range doc.Certificates {
		ensureID(c.Name, r.certs, &c.ID)
		certIDBytes, parseErr := glid.ParseUUID(c.ID)
		if parseErr != nil {
			return imported, fmt.Errorf("import certificate %q: invalid ID %q: %w", c.Name, c.ID, parseErr)
		}
		_, err := client.System.PutCertificate(ctx, connect.NewRequest(&v1.PutCertificateRequest{
			Id:       certIDBytes.ToProto(),
			Name:     c.Name,
			CertFile: c.CertFile,
			KeyFile:  c.KeyFile,
		}))
		if err != nil {
			return imported, fmt.Errorf("import certificate %q: %w", c.Name, err)
		}
		imported++
	}
	return imported, nil
}

// reportUnrestorable announces what the document lists but cannot restore —
// see exportExclusions(). It imports nothing; the point is that the operator
// sees per entity what did not come back instead of inferring it from a
// count.
func reportUnrestorable(ctx context.Context, client *server.Client, r *resolver, doc *exportDoc) (int, error) {
	for _, u := range doc.Users {
		// Users can only be created with passwords, which we don't export.
		// On import, we skip users that already exist and warn about new ones.
		if _, ok := r.users[strings.ToLower(u.Username)]; ok {
			continue // already exists, skip (no way to update role without password)
		}
		_, _ = fmt.Fprintf(os.Stderr, "Warning: skipping user %q — cannot create without password (use 'config user create')\n", u.Username)
	}

	for _, f := range doc.ManagedFiles {
		// Managed file content is an uploaded blob on disk, not config, so it
		// is not in the document. Existing files with the same name stay.
		if existing, err := client.System.ListManagedFiles(ctx, connect.NewRequest(&v1.ListManagedFilesRequest{})); err == nil {
			if slices.ContainsFunc(existing.Msg.Files, func(mf *v1.ManagedFileInfo) bool { return mf.Name == f.Name }) {
				continue
			}
		}
		_, _ = fmt.Fprintf(os.Stderr, "Warning: skipping managed file %q — contents are not part of a config export (use 'config file upload')\n", f.Name)
	}
	return 0, nil
}

// textIDBytes normalises a node ID that a proto bytes field carries as text
// rather than as raw bytes. IngesterConfig.node_id(s) and
// NodeStorageConfig.node_id are the exceptions: the store keys those by the ID
// string, so the server does string(b) on them, while every other proto ID
// field is the raw 16 bytes. decodeGLIDFields cannot tell the two apart from
// the document alone, so it produces raw bytes and the text-form fields are
// converted back here.
func textIDBytes(b []byte) []byte {
	if len(b) != glid.Size {
		return b
	}
	return []byte(glid.FromBytes(b).String())
}

// ensureID reuses an existing ID if the name matches, or generates a new UUIDv7.
func ensureID(name string, existing map[string]string, id *string) {
	if existingID, ok := existing[strings.ToLower(name)]; ok {
		*id = existingID
	} else if *id == "" {
		*id = glid.New().String()
	}
}

// ensureProtoID works like ensureID but for proto []byte ID fields.
func ensureProtoID(name string, existing map[string]string, id *[]byte) {
	if existingID, ok := existing[strings.ToLower(name)]; ok {
		if parsed, err := glid.ParseUUID(existingID); err == nil {
			*id = parsed.ToProto()
		}
	} else if len(*id) == 0 {
		*id = glid.New().ToProto()
	}
}

// deleteAll removes all config entities (not server config).
// deleteAllLookups removes every enrichment lookup table from the target.
//
// Needed because PutLookupSettings replaces a list only when the request
// CONTAINS it, and proto3 cannot distinguish an empty repeated field from an
// absent one — so importing a document with no yaml lookups (say) could not
// clear the target's, and stale enrichment tables would survive a --replace.
// Split out of deleteAll to keep that function within the cognitive-complexity
// budget.
func deleteAllLookups(ctx context.Context, client *server.Client) error {
	settings, err := client.System.GetSettings(ctx, connect.NewRequest(&v1.GetSettingsRequest{}))
	if err != nil {
		// Settings unreadable: nothing to clear that we can name.
		return nil //nolint:nilerr // best-effort clear; the import itself still reports failures
	}
	l := settings.Msg.GetLookup()
	var names []string
	for _, e := range l.GetHttpLookups() {
		names = append(names, e.GetName())
	}
	for _, e := range l.GetJsonFileLookups() {
		names = append(names, e.GetName())
	}
	for _, e := range l.GetYamlFileLookups() {
		names = append(names, e.GetName())
	}
	for _, e := range l.GetMmdbLookups() {
		names = append(names, e.GetName())
	}
	for _, e := range l.GetCsvLookups() {
		names = append(names, e.GetName())
	}
	for _, e := range l.GetStaticLookups() {
		names = append(names, e.GetName())
	}
	for _, name := range names {
		if _, err := client.System.DeleteLookup(ctx, connect.NewRequest(&v1.DeleteLookupRequest{Name: name})); err != nil {
			return fmt.Errorf("delete lookup %s: %w", name, err)
		}
	}
	return nil
}

func deleteAll(ctx context.Context, client *server.Client) error {
	resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
	if err != nil {
		return err
	}

	// Delete in reverse dependency order: routes and vaults first (routes name
	// vaults, vaults name cloud services), then ingesters, then policies.
	//
	// Managed files are deliberately not deleted: their content is an
	// uploaded blob that no import can restore, so --replace must not destroy
	// it. Node records and node storage configs are cluster-managed and stay
	// for the same reason — a node that is live keeps its identity.
	for _, rt := range resp.Msg.Routes {
		if _, err := client.System.DeleteRoute(ctx, connect.NewRequest(&v1.DeleteRouteRequest{Id: rt.Id})); err != nil {
			return fmt.Errorf("delete route %s: %w", rt.Name, err)
		}
	}
	for _, v := range resp.Msg.Vaults {
		if _, err := client.System.DeleteVault(ctx, connect.NewRequest(&v1.DeleteVaultRequest{Id: v.Id, Force: true})); err != nil {
			return fmt.Errorf("delete vault %s: %w", v.Name, err)
		}
	}
	for _, ig := range resp.Msg.Ingesters {
		if _, err := client.System.DeleteIngester(ctx, connect.NewRequest(&v1.DeleteIngesterRequest{Id: ig.Id})); err != nil {
			return fmt.Errorf("delete ingester %s: %w", ig.Name, err)
		}
	}
	// There are no filters to delete; expressions live on routes, which are
	// deleted below.
	for _, p := range resp.Msg.RotationPolicies {
		if _, err := client.System.DeleteRotationPolicy(ctx, connect.NewRequest(&v1.DeleteRotationPolicyRequest{Id: p.Id})); err != nil {
			return fmt.Errorf("delete rotation policy %s: %w", p.Name, err)
		}
	}
	for _, p := range resp.Msg.RetentionPolicies {
		if _, err := client.System.DeleteRetentionPolicy(ctx, connect.NewRequest(&v1.DeleteRetentionPolicyRequest{Id: p.Id})); err != nil {
			return fmt.Errorf("delete retention policy %s: %w", p.Name, err)
		}
	}
	// After the vaults that referenced them are gone.
	for _, cs := range resp.Msg.CloudServices {
		if _, err := client.System.DeleteCloudService(ctx, connect.NewRequest(&v1.DeleteCloudServiceRequest{Id: cs.Id})); err != nil {
			return fmt.Errorf("delete cloud service %s: %w", cs.Name, err)
		}
	}

	if err := deleteAllLookups(ctx, client); err != nil {
		return err
	}

	// Delete certs.
	certResp, err := client.System.ListCertificates(ctx, connect.NewRequest(&v1.ListCertificatesRequest{}))
	if err == nil {
		for _, c := range certResp.Msg.Certificates {
			if _, err := client.System.DeleteCertificate(ctx, connect.NewRequest(&v1.DeleteCertificateRequest{Id: c.Id})); err != nil {
				return fmt.Errorf("delete certificate %s: %w", c.Name, err)
			}
		}
	}

	return nil
}

// importServerSettings applies hierarchical export fields via the split
// settings RPCs. Returns how many RPCs ran.
func importServerSettings(ctx context.Context, client *server.Client, doc *exportDoc) (int, error) {
	n := 0
	if svc := buildPutServiceSettingsRequest(doc); svc != nil {
		if _, err := client.System.PutServiceSettings(ctx, connect.NewRequest(svc)); err != nil {
			return n, fmt.Errorf("import server config (service): %w", err)
		}
		n++
	}
	if mm := buildPutMaxMindSettingsRequest(doc); mm != nil {
		if _, err := client.System.PutMaxMindSettings(ctx, connect.NewRequest(mm)); err != nil {
			return n, fmt.Errorf("import server config (maxmind): %w", err)
		}
		n++
	}
	if doc.SetupWizardDismissed {
		dismiss := true
		if _, err := client.System.PutSetupSettings(ctx, connect.NewRequest(&v1.PutSetupSettingsRequest{
			SetupWizardDismissed: &dismiss,
		})); err != nil {
			return n, fmt.Errorf("import server config (setup): %w", err)
		}
		n++
	}
	return n, nil
}

func buildPutServiceSettingsRequest(doc *exportDoc) *v1.PutServiceSettingsRequest {
	if doc.Auth == nil && doc.Query == nil && doc.Scheduler == nil && doc.TLS == nil && doc.Cluster == nil {
		return nil
	}
	req := &v1.PutServiceSettingsRequest{}
	if doc.Auth != nil {
		req.Auth = buildAuthSettings(doc.Auth)
	}
	if doc.Query != nil {
		req.Query = buildQuerySettings(doc.Query)
	}
	if doc.Scheduler != nil {
		req.Scheduler = buildSchedulerSettings(doc.Scheduler)
	}
	if doc.TLS != nil {
		req.Tls = buildTLSSettings(doc.TLS)
	}
	if doc.Cluster != nil {
		req.Cluster = buildClusterSettings(doc.Cluster)
	}
	return req
}

func buildClusterSettings(c *clusterExport) *v1.PutClusterSettings {
	pc := &v1.PutClusterSettings{}
	if c.BroadcastInterval != "" {
		pc.BroadcastInterval = &c.BroadcastInterval
	}
	if c.PipelineBacklogMax != "" {
		pc.PipelineBacklogMax = &c.PipelineBacklogMax
	}
	return pc
}

func buildPutMaxMindSettingsRequest(doc *exportDoc) *v1.PutMaxMindSettingsRequest {
	if doc.MaxMind == nil {
		return nil
	}
	return &v1.PutMaxMindSettingsRequest{Maxmind: buildMaxMindSettings(doc.MaxMind)}
}

func buildAuthSettings(a *authExport) *v1.PutAuthSettings {
	pa := &v1.PutAuthSettings{}
	if a.TokenDuration != "" {
		pa.TokenDuration = &a.TokenDuration
	}
	if a.RefreshTokenDuration != "" {
		pa.RefreshTokenDuration = &a.RefreshTokenDuration
	}
	if pp := a.PasswordPolicy; pp != nil {
		pa.PasswordPolicy = buildPasswordPolicySettings(pp)
	}
	return pa
}

func buildPasswordPolicySettings(pp *passwordPolicyExport) *v1.PutPasswordPolicySettings {
	ppp := &v1.PutPasswordPolicySettings{}
	if pp.MinLength != 0 {
		ppp.MinLength = &pp.MinLength
	}
	if pp.RequireMixedCase {
		ppp.RequireMixedCase = &pp.RequireMixedCase
	}
	if pp.RequireDigit {
		ppp.RequireDigit = &pp.RequireDigit
	}
	if pp.RequireSpecial {
		ppp.RequireSpecial = &pp.RequireSpecial
	}
	if pp.MaxConsecutiveRepeats != 0 {
		ppp.MaxConsecutiveRepeats = &pp.MaxConsecutiveRepeats
	}
	if pp.ForbidAnimalNoise {
		ppp.ForbidAnimalNoise = &pp.ForbidAnimalNoise
	}
	return ppp
}

func buildQuerySettings(q *queryExport) *v1.PutQuerySettings {
	pq := &v1.PutQuerySettings{}
	if q.Timeout != "" {
		pq.Timeout = &q.Timeout
	}
	if q.MaxFollowDuration != "" {
		pq.MaxFollowDuration = &q.MaxFollowDuration
	}
	if q.MaxResultCount != 0 {
		pq.MaxResultCount = &q.MaxResultCount
	}
	return pq
}

func buildSchedulerSettings(s *schedulerExport) *v1.PutSchedulerSettings {
	ps := &v1.PutSchedulerSettings{}
	if s.MaxConcurrentJobs != 0 {
		ps.MaxConcurrentJobs = &s.MaxConcurrentJobs
	}
	return ps
}

func buildTLSSettings(t *tlsExport) *v1.PutTLSSettings {
	pt := &v1.PutTLSSettings{}
	if t.DefaultCert != "" {
		pt.DefaultCert = &t.DefaultCert
	}
	if t.TLSEnabled {
		pt.Enabled = &t.TLSEnabled
	}
	if t.HTTPToHTTPSRedirect {
		pt.HttpToHttpsRedirect = &t.HTTPToHTTPSRedirect
	}
	if t.HTTPSPort != "" {
		pt.HttpsPort = &t.HTTPSPort
	}
	return pt
}

func buildMaxMindSettings(mm *maxmindExport) *v1.PutMaxMindSettings {
	pmm := &v1.PutMaxMindSettings{}
	if mm.AutoDownload {
		pmm.AutoDownload = &mm.AutoDownload
	}
	if mm.AccountID != "" {
		pmm.AccountId = []byte(mm.AccountID)
	}
	if mm.LicenseKey != "" {
		pmm.LicenseKey = &mm.LicenseKey
	}
	return pmm
}
