package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// settingsField maps a CLI flag to a proto field within a sub-message.
type settingsField struct {
	flag   string // CLI flag name (kebab-case). Empty = display-only, no flag.
	label  string // display label. Empty = derive from getKey.
	getKey string // proto field name in the Get sub-message. Empty = write-only.
	setKey string // proto field name in the Put sub-message. Empty = read-only.
	desc   string // description for --help and display
	secret bool   // display as "(configured)" / "(not set)"
}

func (f settingsField) displayLabel() string {
	if f.label != "" {
		return f.label
	}
	if f.getKey != "" {
		return f.getKey
	}
	return f.setKey
}

// settingsGroup describes a CLI subcommand that reads/writes a nested sub-message.
// getPath / setPath are the proto field names to navigate from the top-level message
// down to the sub-message (e.g. ["auth", "password_policy"] for the password-policy group).
type settingsGroup struct {
	name    string
	short   string
	fields  []settingsField
	getPath []string // path from GetSettingsResponse to the Get sub-message
	setPath []string // path from PutSettingsRequest to the Put sub-message
}

// Groups mirror the internal config hierarchy.
var settingsGroups = []settingsGroup{
	{name: "auth", short: "Configure authentication", getPath: []string{"auth"}, setPath: []string{"auth"}, fields: []settingsField{
		{flag: "token-duration", label: "token_duration", getKey: "token_duration", setKey: "token_duration", desc: "Access token lifetime (e.g. \"15m\", \"1h\")"},
		{flag: "refresh-duration", label: "refresh_token_duration", getKey: "refresh_token_duration", setKey: "refresh_token_duration", desc: "Refresh token lifetime (e.g. \"168h\")"},
		{flag: "jwt-secret", label: "jwt_secret", getKey: "jwt_secret_configured", setKey: "jwt_secret", desc: "JWT signing secret", secret: true},
	}},
	{name: "password-policy", short: "Configure password policy", getPath: []string{"auth", "password_policy"}, setPath: []string{"auth", "password_policy"}, fields: []settingsField{
		{flag: "min-length", label: "min_length", getKey: "min_length", setKey: "min_length", desc: "Minimum password length"},
		{flag: "require-mixed-case", label: "require_mixed_case", getKey: "require_mixed_case", setKey: "require_mixed_case", desc: "Require upper and lowercase letters"},
		{flag: "require-digit", label: "require_digit", getKey: "require_digit", setKey: "require_digit", desc: "Require at least one digit"},
		{flag: "require-special", label: "require_special", getKey: "require_special", setKey: "require_special", desc: "Require at least one special character"},
		{flag: "max-consecutive-repeats", label: "max_consecutive_repeats", getKey: "max_consecutive_repeats", setKey: "max_consecutive_repeats", desc: "Max consecutive repeated characters (0 = no limit)"},
		{flag: "forbid-animal-noise", label: "forbid_animal_noise", getKey: "forbid_animal_noise", setKey: "forbid_animal_noise", desc: "Forbid animal noises as passwords"},
	}},
	{name: "query", short: "Configure query engine", getPath: []string{"query"}, setPath: []string{"query"}, fields: []settingsField{
		{flag: "timeout", label: "timeout", getKey: "timeout", setKey: "timeout", desc: "Query timeout (e.g. \"30s\", \"1m\")"},
		{flag: "max-follow-duration", label: "max_follow_duration", getKey: "max_follow_duration", setKey: "max_follow_duration", desc: "Max Follow stream lifetime (e.g. \"4h\")"},
		{flag: "max-result-count", label: "max_result_count", getKey: "max_result_count", setKey: "max_result_count", desc: "Max records per Search request (0 = unlimited)"},
	}},
	{name: "scheduler", short: "Configure job scheduler", getPath: []string{"scheduler"}, setPath: []string{"scheduler"}, fields: []settingsField{
		{flag: "max-concurrent-jobs", label: "max_concurrent_jobs", getKey: "max_concurrent_jobs", setKey: "max_concurrent_jobs", desc: "Maximum concurrent background jobs"},
	}},
	{name: "tls", short: "Configure TLS", getPath: []string{"tls"}, setPath: []string{"tls"}, fields: []settingsField{
		{flag: "enabled", label: "enabled", getKey: "enabled", setKey: "enabled", desc: "Enable HTTPS"},
		{flag: "default-cert", label: "default_cert", getKey: "default_cert", setKey: "default_cert", desc: "Certificate ID for HTTPS"},
		{flag: "http-redirect", label: "http_to_https_redirect", getKey: "http_to_https_redirect", setKey: "http_to_https_redirect", desc: "Redirect HTTP to HTTPS"},
		{flag: "https-port", label: "https_port", getKey: "https_port", setKey: "https_port", desc: "HTTPS port (empty = HTTP port + 1)"},
	}},
	{name: "lookup", short: "Configure GeoIP/ASN lookups", getPath: []string{"lookup"}, setPath: []string{"lookup"}, fields: []settingsField{
		{flag: "geoip-db", label: "geoip_db_path", getKey: "geoip_db_path", setKey: "geoip_db_path", desc: "Path to GeoIP MMDB file"},
		{flag: "asn-db", label: "asn_db_path", getKey: "asn_db_path", setKey: "asn_db_path", desc: "Path to ASN MMDB file"},
	}},
	{name: "maxmind", short: "Configure MaxMind database downloads", getPath: []string{"lookup", "maxmind"}, setPath: []string{"lookup", "maxmind"}, fields: []settingsField{
		{flag: "auto-download", label: "auto_download", getKey: "auto_download", setKey: "auto_download", desc: "Auto-download MaxMind databases"},
		{flag: "account-id", label: "account_id", setKey: "account_id", desc: "MaxMind account ID (write-only)"},
		{flag: "license-key", label: "license_key", getKey: "license_configured", setKey: "license_key", desc: "MaxMind license key", secret: true},
		{label: "last_update", getKey: "last_update"},
	}},
}

// Per-group command constructors registered in cli.go.

func newAuthCmd() *cobra.Command {
	cmd := newGroupCmd("auth")
	cmd.AddCommand(newGroupCmd("password-policy"))
	return cmd
}

func newQueryCmd() *cobra.Command     { return newGroupCmd("query") }
func newSchedulerCmd() *cobra.Command { return newGroupCmd("scheduler") }
func newTLSCmd() *cobra.Command       { return newGroupCmd("tls") }

func newLookupCmd() *cobra.Command {
	cmd := newGroupCmd("lookup")
	cmd.AddCommand(newGroupCmd("maxmind"))
	return cmd
}

func newGroupCmd(name string) *cobra.Command {
	g := findGroup(name)

	// Resolve the Put sub-message descriptor for flag registration.
	putSubDesc := navigateDescriptor(
		(&v1.PutSettingsRequest{}).ProtoReflect().Descriptor(),
		g.setPath,
	)

	cmd := &cobra.Command{
		Use:   g.name,
		Short: g.short,
		Long: g.short + ".\n\nWithout flags, displays current values.\nWith flags, updates the specified settings.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var changed []settingsField
			for _, f := range g.fields {
				if f.flag != "" && cmd.Flags().Changed(f.flag) {
					changed = append(changed, f)
				}
			}
			if len(changed) > 0 {
				return groupSet(cmd, g, changed)
			}
			return groupGet(cmd, g)
		},
	}

	registerGroupFlags(cmd, g, putSubDesc)
	return cmd
}

func groupSet(cmd *cobra.Command, g settingsGroup, changed []settingsField) error {
	client := clientFromCmd(cmd)
	req := &v1.PutSettingsRequest{}
	subMsg := ensureSubMessage(req.ProtoReflect(), g.setPath)
	for _, f := range changed {
		if err := applyFlag(cmd, subMsg, f); err != nil {
			return err
		}
	}
	if _, err := client.Config.PutSettings(context.Background(), connect.NewRequest(req)); err != nil {
		return err
	}
	for _, f := range changed {
		val := cmd.Flags().Lookup(f.flag).Value.String()
		fmt.Printf("Set %s = %s\n", f.displayLabel(), val)
	}
	return nil
}

func groupGet(cmd *cobra.Command, g settingsGroup) error {
	client := clientFromCmd(cmd)
	resp, err := client.Config.GetSettings(context.Background(), connect.NewRequest(&v1.GetSettingsRequest{}))
	if err != nil {
		return err
	}
	subMsg := navigateMessage(resp.Msg.ProtoReflect(), g.getPath)
	p := newPrinter(outputFormat(cmd))

	if outputFormat(cmd) == "json" {
		m := make(map[string]any)
		for _, f := range g.fields {
			if f.getKey == "" {
				continue
			}
			m[f.displayLabel()] = protoGetTyped(subMsg, f)
		}
		return p.json(m)
	}

	var pairs [][2]string
	for _, f := range g.fields {
		if f.getKey == "" {
			continue
		}
		pairs = append(pairs, [2]string{f.displayLabel(), fieldDisplayValue(subMsg, f)})
	}
	p.kv(pairs)
	return nil
}

func registerGroupFlags(cmd *cobra.Command, g settingsGroup, putSubDesc protoreflect.MessageDescriptor) {
	for _, f := range g.fields {
		if f.flag == "" || putSubDesc == nil {
			continue
		}
		fd := putSubDesc.Fields().ByName(protoreflect.Name(f.setKey))
		if fd == nil {
			continue
		}
		switch fd.Kind() { //nolint:exhaustive // only string/bool/int32 used
		case protoreflect.StringKind:
			cmd.Flags().String(f.flag, "", f.desc)
		case protoreflect.BoolKind:
			cmd.Flags().Bool(f.flag, false, f.desc)
		case protoreflect.Int32Kind:
			cmd.Flags().Int32(f.flag, 0, f.desc)
		}
	}
}

// navigateDescriptor walks a message descriptor through a path of field names.
func navigateDescriptor(desc protoreflect.MessageDescriptor, path []string) protoreflect.MessageDescriptor {
	cur := desc
	for _, name := range path {
		fd := cur.Fields().ByName(protoreflect.Name(name))
		if fd == nil || fd.Kind() != protoreflect.MessageKind {
			return nil
		}
		cur = fd.Message()
	}
	return cur
}

// navigateMessage walks a proto message through a path of field names, returning the sub-message.
// Returns nil if any field along the path is nil/unset.
func navigateMessage(msg protoreflect.Message, path []string) protoreflect.Message {
	cur := msg
	for _, name := range path {
		fd := cur.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil || fd.Kind() != protoreflect.MessageKind {
			return nil
		}
		if !cur.Has(fd) {
			return nil
		}
		cur = cur.Get(fd).Message()
	}
	return cur
}

// ensureSubMessage walks a mutable proto message through a path, creating sub-messages as needed.
func ensureSubMessage(msg protoreflect.Message, path []string) protoreflect.Message {
	cur := msg
	for _, name := range path {
		fd := cur.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil || fd.Kind() != protoreflect.MessageKind {
			return nil
		}
		if !cur.Has(fd) {
			cur.Set(fd, protoreflect.ValueOfMessage(cur.NewField(fd).Message()))
		}
		cur = cur.Mutable(fd).Message()
	}
	return cur
}

func findGroup(name string) settingsGroup {
	for _, g := range settingsGroups {
		if g.name == name {
			return g
		}
	}
	panic("unknown settings group: " + name)
}

func applyFlag(cmd *cobra.Command, msg protoreflect.Message, f settingsField) error {
	if msg == nil {
		return fmt.Errorf("sub-message is nil for field %q", f.setKey)
	}
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(f.setKey))
	if fd == nil {
		return fmt.Errorf("unknown proto field %q", f.setKey)
	}
	switch fd.Kind() { //nolint:exhaustive // only string/bool/int32 used
	case protoreflect.StringKind:
		v, _ := cmd.Flags().GetString(f.flag)
		msg.Set(fd, protoreflect.ValueOfString(v))
	case protoreflect.BoolKind:
		v, _ := cmd.Flags().GetBool(f.flag)
		msg.Set(fd, protoreflect.ValueOfBool(v))
	case protoreflect.Int32Kind:
		v, _ := cmd.Flags().GetInt32(f.flag)
		msg.Set(fd, protoreflect.ValueOfInt32(v))
	}
	return nil
}

func protoGetTyped(msg protoreflect.Message, f settingsField) any {
	if msg == nil {
		if f.secret {
			return false
		}
		return nil
	}
	if f.secret {
		raw := protoGetString(msg, f.getKey)
		if raw == "" || raw == "false" || raw == "0" {
			return false
		}
		return true
	}
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(f.getKey))
	if fd == nil {
		return nil
	}
	v := msg.Get(fd)
	switch fd.Kind() { //nolint:exhaustive // only string/bool/int32 used
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		return v.Int()
	default:
		return v.String()
	}
}

func protoGetString(msg protoreflect.Message, name string) string {
	if msg == nil {
		return ""
	}
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return ""
	}
	v := msg.Get(fd)
	switch fd.Kind() { //nolint:exhaustive // only string/bool/int used
	case protoreflect.BoolKind:
		return strconv.FormatBool(v.Bool())
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		return strconv.FormatInt(v.Int(), 10)
	default:
		return v.String()
	}
}

func fieldDisplayValue(msg protoreflect.Message, f settingsField) string {
	if msg == nil {
		if f.secret {
			return "(not set)"
		}
		return ""
	}
	if f.secret {
		raw := protoGetString(msg, f.getKey)
		if raw == "" || raw == "false" || raw == "0" {
			return "(not set)"
		}
		return "(configured)"
	}
	return protoGetString(msg, f.getKey)
}

func init() {
	getDesc := (&v1.GetSettingsResponse{}).ProtoReflect().Descriptor()
	putDesc := (&v1.PutSettingsRequest{}).ProtoReflect().Descriptor()

	var missing []string
	for _, g := range settingsGroups {
		getSubDesc := navigateDescriptor(getDesc, g.getPath)
		putSubDesc := navigateDescriptor(putDesc, g.setPath)
		for _, f := range g.fields {
			if f.getKey != "" {
				if getSubDesc == nil || getSubDesc.Fields().ByName(protoreflect.Name(f.getKey)) == nil {
					missing = append(missing, g.name+"."+f.getKey+" (get)")
				}
			}
			if f.setKey != "" {
				if putSubDesc == nil || putSubDesc.Fields().ByName(protoreflect.Name(f.setKey)) == nil {
					missing = append(missing, g.name+"."+f.setKey+" (set)")
				}
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		panic("settings: broken field mappings: " + strings.Join(missing, ", "))
	}
}
