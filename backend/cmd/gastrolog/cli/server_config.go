package cli

import (
	"context"
	"fmt"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

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

// serverKey describes a single server config field with typed get/set.
type serverKey struct {
	get func(*v1.GetServerConfigResponse) string
	set func(*v1.PutServerConfigRequest, string) error
}

var serverKeys = map[string]serverKey{
	"token_duration": {
		get: func(r *v1.GetServerConfigResponse) string { return r.TokenDuration },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.TokenDuration = &v; return nil },
	},
	"min_password_length": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.Itoa(int(r.MinPasswordLength)) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return err
			}
			i32 := int32(n)
			req.MinPasswordLength = &i32
			return nil
		},
	},
	"max_concurrent_jobs": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.Itoa(int(r.MaxConcurrentJobs)) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return err
			}
			i32 := int32(n)
			req.MaxConcurrentJobs = &i32
			return nil
		},
	},
	"tls_default_cert": {
		get: func(r *v1.GetServerConfigResponse) string { return r.TlsDefaultCert },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.TlsDefaultCert = &v; return nil },
	},
	"tls_enabled": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.TlsEnabled) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.TlsEnabled = &b
			return nil
		},
	},
	"http_to_https_redirect": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.HttpToHttpsRedirect) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.HttpToHttpsRedirect = &b
			return nil
		},
	},
	"require_mixed_case": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.RequireMixedCase) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.RequireMixedCase = &b
			return nil
		},
	},
	"require_digit": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.RequireDigit) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.RequireDigit = &b
			return nil
		},
	},
	"require_special": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.RequireSpecial) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.RequireSpecial = &b
			return nil
		},
	},
	"max_consecutive_repeats": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.Itoa(int(r.MaxConsecutiveRepeats)) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return err
			}
			i32 := int32(n)
			req.MaxConsecutiveRepeats = &i32
			return nil
		},
	},
	"forbid_animal_noise": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.ForbidAnimalNoise) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.ForbidAnimalNoise = &b
			return nil
		},
	},
	"https_port": {
		get: func(r *v1.GetServerConfigResponse) string { return r.HttpsPort },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.HttpsPort = &v; return nil },
	},
	"max_follow_duration": {
		get: func(r *v1.GetServerConfigResponse) string { return r.MaxFollowDuration },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.MaxFollowDuration = &v; return nil },
	},
	"query_timeout": {
		get: func(r *v1.GetServerConfigResponse) string { return r.QueryTimeout },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.QueryTimeout = &v; return nil },
	},
	"refresh_token_duration": {
		get: func(r *v1.GetServerConfigResponse) string { return r.RefreshTokenDuration },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.RefreshTokenDuration = &v; return nil },
	},
	"max_result_count": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.Itoa(int(r.MaxResultCount)) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return err
			}
			i32 := int32(n)
			req.MaxResultCount = &i32
			return nil
		},
	},
	"setup_wizard_dismissed": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.SetupWizardDismissed) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.SetupWizardDismissed = &b
			return nil
		},
	},
	"geoip_db_path": {
		get: func(r *v1.GetServerConfigResponse) string { return r.GeoipDbPath },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.GeoipDbPath = &v; return nil },
	},
	"asn_db_path": {
		get: func(r *v1.GetServerConfigResponse) string { return r.AsnDbPath },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.AsnDbPath = &v; return nil },
	},
	"maxmind_auto_download": {
		get: func(r *v1.GetServerConfigResponse) string { return strconv.FormatBool(r.MaxmindAutoDownload) },
		set: func(req *v1.PutServerConfigRequest, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			req.MaxmindAutoDownload = &b
			return nil
		},
	},
	"maxmind_account_id": {
		get: func(r *v1.GetServerConfigResponse) string { return "(configured)" },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.MaxmindAccountId = &v; return nil },
	},
	"maxmind_license_key": {
		get: func(r *v1.GetServerConfigResponse) string { return "(configured)" },
		set: func(req *v1.PutServerConfigRequest, v string) error { req.MaxmindLicenseKey = &v; return nil },
	},
	"jwt_secret": {
		get: func(r *v1.GetServerConfigResponse) string {
			if r.JwtSecretConfigured {
				return "(configured)"
			}
			return "(not set)"
		},
		set: func(req *v1.PutServerConfigRequest, v string) error { req.JwtSecret = &v; return nil },
	},
	"node_id": {
		get: func(r *v1.GetServerConfigResponse) string { return r.NodeId },
		set: nil,
	},
	"node_name": {
		get: func(r *v1.GetServerConfigResponse) string { return r.NodeName },
		set: nil,
	},
}

func newServerGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get [key]",
		Short: "Get server configuration",
		Long:  "Without arguments, shows all server settings. With a key, shows that single setting.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetServerConfig(context.Background(), connect.NewRequest(&v1.GetServerConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))

			if len(args) == 1 {
				key := args[0]
				sk, ok := serverKeys[key]
				if !ok {
					return fmt.Errorf("unknown key %q (run without args to see all keys)", key)
				}
				val := sk.get(resp.Msg)
				if outputFormat(cmd) == "json" {
					return p.json(map[string]string{key: val})
				}
				fmt.Println(val)
				return nil
			}

			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg)
			}
			var pairs [][2]string
			for key, sk := range serverKeys {
				pairs = append(pairs, [2]string{key, sk.get(resp.Msg)})
			}
			p.kv(pairs)
			return nil
		},
	}
}

func newServerSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a server configuration value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, value := args[0], args[1]
			sk, ok := serverKeys[key]
			if !ok {
				return fmt.Errorf("unknown key %q", key)
			}
			if sk.set == nil {
				return fmt.Errorf("key %q is read-only", key)
			}

			req := &v1.PutServerConfigRequest{}
			if err := sk.set(req, value); err != nil {
				return fmt.Errorf("invalid value for %s: %w", key, err)
			}

			client := clientFromCmd(cmd)
			_, err := client.Config.PutServerConfig(context.Background(), connect.NewRequest(req))
			if err != nil {
				return err
			}
			fmt.Printf("Set %s = %s\n", key, value)
			return nil
		},
	}
}
