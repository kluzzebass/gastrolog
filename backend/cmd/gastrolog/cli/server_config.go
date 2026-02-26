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

func newServerConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Manage server settings",
	}
	cmd.AddCommand(
		newServerGetCmd(),
		newServerSetCmd(),
	)
	return cmd
}

// settingsField describes a single server config field.
type settingsField struct {
	key      string // display key and proto field name in GetServerConfigResponse (e.g. "token_duration")
	setField string // proto field name in PutServerConfigRequest; empty = read-only
	secret   bool   // display "(configured)" / "(not set)" instead of raw value
	setOnly  bool   // field exists only in PutServerConfigRequest (not in Get response)
}

// settingsGroup is an ordered group of related fields.
type settingsGroup struct {
	name   string
	fields []settingsField
}

// settingsGroups defines all server config fields grouped by concern.
// Order matters for display.
var settingsGroups = []settingsGroup{
	{name: "node", fields: []settingsField{
		{key: "node_id"},
		{key: "node_name"},
	}},
	{name: "auth", fields: []settingsField{
		{key: "token_duration", setField: "token_duration"},
		{key: "refresh_token_duration", setField: "refresh_token_duration"},
		{key: "jwt_secret_configured"},
		{setField: "jwt_secret", setOnly: true},
		{key: "min_password_length", setField: "min_password_length"},
		{key: "require_mixed_case", setField: "require_mixed_case"},
		{key: "require_digit", setField: "require_digit"},
		{key: "require_special", setField: "require_special"},
		{key: "max_consecutive_repeats", setField: "max_consecutive_repeats"},
		{key: "forbid_animal_noise", setField: "forbid_animal_noise"},
	}},
	{name: "query", fields: []settingsField{
		{key: "query_timeout", setField: "query_timeout"},
		{key: "max_follow_duration", setField: "max_follow_duration"},
		{key: "max_result_count", setField: "max_result_count"},
	}},
	{name: "scheduler", fields: []settingsField{
		{key: "max_concurrent_jobs", setField: "max_concurrent_jobs"},
	}},
	{name: "tls", fields: []settingsField{
		{key: "tls_enabled", setField: "tls_enabled"},
		{key: "tls_default_cert", setField: "tls_default_cert"},
		{key: "http_to_https_redirect", setField: "http_to_https_redirect"},
		{key: "https_port", setField: "https_port"},
	}},
	{name: "lookup", fields: []settingsField{
		{key: "geoip_db_path", setField: "geoip_db_path"},
		{key: "asn_db_path", setField: "asn_db_path"},
		{key: "maxmind_auto_download", setField: "maxmind_auto_download"},
		{setField: "maxmind_account_id", setOnly: true},
		{setField: "maxmind_license_key", setOnly: true},
		{key: "maxmind_license_configured"},
		{key: "maxmind_last_update"},
	}},
	{name: "other", fields: []settingsField{
		{key: "setup_wizard_dismissed", setField: "setup_wizard_dismissed"},
	}},
}

// protoGetString reads a field from the GetServerConfigResponse by proto field name.
func protoGetString(msg protoreflect.Message, name string) string {
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return ""
	}
	v := msg.Get(fd)
	switch fd.Kind() { //nolint:exhaustive // only string/bool/int used in server config
	case protoreflect.BoolKind:
		return strconv.FormatBool(v.Bool())
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		return strconv.FormatInt(v.Int(), 10)
	default:
		return v.String()
	}
}

// protoSet parses a string value and sets it on a PutServerConfigRequest field.
func protoSet(msg protoreflect.Message, name, value string) error {
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return fmt.Errorf("unknown proto field %q", name)
	}
	switch fd.Kind() { //nolint:exhaustive // only string/bool/int used in server config
	case protoreflect.BoolKind:
		b, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		msg.Set(fd, protoreflect.ValueOfBool(b))
	case protoreflect.Int32Kind:
		n, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		msg.Set(fd, protoreflect.ValueOfInt32(int32(n)))
	case protoreflect.Int64Kind:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		msg.Set(fd, protoreflect.ValueOfInt64(n))
	case protoreflect.StringKind:
		msg.Set(fd, protoreflect.ValueOfString(value))
	default:
		return fmt.Errorf("unsupported field type %s", fd.Kind())
	}
	return nil
}

// fieldDisplayValue returns the display string for a field, respecting secrets.
func fieldDisplayValue(msg protoreflect.Message, f settingsField) string {
	if f.secret {
		raw := protoGetString(msg, f.key)
		// Secret fields: check if the value is non-zero.
		if raw == "" || raw == "false" || raw == "0" {
			return "(not set)"
		}
		return "(configured)"
	}
	return protoGetString(msg, f.key)
}

// fieldName returns the lookup key for a field: key for readable fields, setField for set-only.
func fieldName(f settingsField) string {
	if f.setOnly {
		return f.setField
	}
	return f.key
}

// findField looks up a field by "group.key" or bare "key".
// Returns the group name, field spec, and whether it was found.
func findField(query string) (string, settingsField, bool) {
	group, key, hasDot := strings.Cut(query, ".")

	if hasDot {
		for _, g := range settingsGroups {
			if g.name == group {
				for _, f := range g.fields {
					if fieldName(f) == key {
						return g.name, f, true
					}
				}
			}
		}
		return "", settingsField{}, false
	}

	// Bare key — search all groups, also match group name.
	for _, g := range settingsGroups {
		if g.name == query {
			return g.name, settingsField{}, true // group match, no specific field
		}
		for _, f := range g.fields {
			if fieldName(f) == query {
				return g.name, f, true
			}
		}
	}
	return "", settingsField{}, false
}

func newServerGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get [group[.key]]",
		Short: "Get server configuration",
		Long: `Without arguments, shows all server settings grouped by concern.
With a group name (e.g. "auth"), shows that group's settings.
With a dotted key (e.g. "auth.token_duration"), shows a single value.
Bare keys also work (e.g. "query_timeout").`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetServerConfig(context.Background(), connect.NewRequest(&v1.GetServerConfigRequest{}))
			if err != nil {
				return err
			}
			msg := resp.Msg.ProtoReflect()
			p := newPrinter(outputFormat(cmd))

			if len(args) == 0 {
				return showAllSettings(p, msg, resp.Msg, outputFormat(cmd))
			}
			return showSetting(p, msg, args[0], outputFormat(cmd))
		},
	}
}

func showAllSettings(p *printer, msg protoreflect.Message, raw *v1.GetServerConfigResponse, format string) error {
	if format == "json" {
		return p.json(raw)
	}
	printAllGroups(p, msg)
	return nil
}

func showSetting(p *printer, msg protoreflect.Message, query, format string) error {
	groupName, field, ok := findField(query)
	if !ok {
		return fmt.Errorf("unknown setting %q (run without args to see all)", query)
	}
	if fieldName(field) != "" {
		return showField(p, msg, groupName, field, format)
	}
	return showGroup(p, msg, groupName, format)
}

func showField(p *printer, msg protoreflect.Message, groupName string, field settingsField, format string) error {
	if field.setOnly {
		return fmt.Errorf("%s.%s is write-only (use \"set\" to change it)", groupName, field.setField)
	}
	val := fieldDisplayValue(msg, field)
	if format == "json" {
		return p.json(map[string]string{groupName + "." + field.key: val})
	}
	fmt.Println(val)
	return nil
}

func showGroup(p *printer, msg protoreflect.Message, groupName, format string) error {
	if format == "json" {
		m := make(map[string]string)
		for _, g := range settingsGroups {
			if g.name == groupName {
				for _, f := range g.fields {
					if f.setOnly {
						continue
					}
					m[f.key] = fieldDisplayValue(msg, f)
				}
			}
		}
		return p.json(m)
	}
	for _, g := range settingsGroups {
		if g.name == groupName {
			printGroup(p, msg, g)
		}
	}
	return nil
}

func printAllGroups(p *printer, msg protoreflect.Message) {
	for i, g := range settingsGroups {
		if i > 0 {
			fmt.Println()
		}
		printGroup(p, msg, g)
	}
}

func printGroup(p *printer, msg protoreflect.Message, g settingsGroup) {
	fmt.Printf("[%s]\n", g.name)
	var pairs [][2]string
	for _, f := range g.fields {
		if f.setOnly {
			continue
		}
		pairs = append(pairs, [2]string{f.key, fieldDisplayValue(msg, f)})
	}
	p.kv(pairs)
}

func newServerSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set <group.key> <value>",
		Short: "Set a server configuration value",
		Long: `Sets a single server setting. Use dotted notation: "auth.token_duration 24h".
Bare keys also work: "query_timeout 60s".

Run "server get" to see available keys.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			query, value := args[0], args[1]
			_, field, ok := findField(query)
			if !ok {
				return fmt.Errorf("unknown setting %q", query)
			}
			if fieldName(field) == "" {
				// User passed a group name, not a specific field.
				return fmt.Errorf("%q is a group, not a setting — use %s.<key>", query, query)
			}
			if field.setField == "" {
				return fmt.Errorf("setting %q is read-only", query)
			}

			req := &v1.PutServerConfigRequest{}
			if err := protoSet(req.ProtoReflect(), field.setField, value); err != nil {
				return fmt.Errorf("invalid value for %s: %w", query, err)
			}

			client := clientFromCmd(cmd)
			_, err := client.Config.PutServerConfig(context.Background(), connect.NewRequest(req))
			if err != nil {
				return err
			}
			fmt.Printf("Set %s = %s\n", query, value)
			return nil
		},
	}
}

func init() {
	// Validate that all field keys resolve to real proto fields at startup.
	getDesc := (&v1.GetServerConfigResponse{}).ProtoReflect().Descriptor()
	setDesc := (&v1.PutServerConfigRequest{}).ProtoReflect().Descriptor()

	var missing []string
	for _, g := range settingsGroups {
		for _, f := range g.fields {
			if !f.setOnly && f.key != "" && getDesc.Fields().ByName(protoreflect.Name(f.key)) == nil {
				missing = append(missing, g.name+"."+f.key+" (get)")
			}
			if f.setField != "" && setDesc.Fields().ByName(protoreflect.Name(f.setField)) == nil {
				missing = append(missing, g.name+"."+f.setField+" (set)")
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		panic("server_config: broken field mappings: " + strings.Join(missing, ", "))
	}
}
