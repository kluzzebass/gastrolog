package cli

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

func newVaultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vault",
		Short: "Manage vaults",
	}
	cmd.AddCommand(
		newVaultListCmd(),
		newVaultGetCmd(),
		newVaultCreateCmd(),
		newVaultDeleteCmd(),
	)
	return cmd
}

func newVaultListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all vaults",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Vaults)
			}
			var rows [][]string
			for _, v := range resp.Msg.Vaults {
				rows = append(rows, []string{
					glid.FromBytes(v.Id).String(), v.Name,
					vaultTypeName(v.Type),
					strconv.FormatBool(v.Enabled),
				})
			}
			p.table([]string{"ID", "NAME", "TYPE", "ENABLED"}, rows)
			return nil
		},
	}
}

func newVaultGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <name-or-id>",
		Short: "Get vault details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.vaults, "vault")
			if err != nil {
				return err
			}
			for _, v := range resp.Msg.Vaults {
				if glid.FromBytes(v.Id).String() == id {
					p := newPrinter(outputFormat(cmd))
					if outputFormat(cmd) == "json" {
						return p.json(v)
					}
					p.kv(vaultDetailPairs(v))
					return nil
				}
			}
			return fmt.Errorf("vault %q not found", args[0])
		},
	}
}

// vaultDetailPairs builds the key-value pairs for vault detail rendering.
func vaultDetailPairs(v *v1.VaultConfig) [][2]string {
	pairs := [][2]string{
		{"ID", glid.FromBytes(v.Id).String()},
		{"Name", v.Name},
		{"Type", vaultTypeName(v.Type)},
		{"Enabled", strconv.FormatBool(v.Enabled)},
		{"Storage Class", strconv.FormatUint(uint64(v.StorageClass), 10)},
		{"Replication Factor", strconv.FormatUint(uint64(v.ReplicationFactor), 10)},
	}
	if len(v.CloudServiceId) > 0 {
		pairs = append(pairs, [2]string{"Cloud Service ID", glid.FromBytes(v.CloudServiceId).String()})
	}
	if v.Path != "" {
		pairs = append(pairs, [2]string{"Path", v.Path})
	}
	if v.MemoryBudgetBytes > 0 {
		pairs = append(pairs, [2]string{"Memory Budget", strconv.FormatUint(v.MemoryBudgetBytes, 10)})
	}
	if v.CacheEviction != "" {
		pairs = append(pairs, [2]string{"Cache Eviction", v.CacheEviction})
	}
	if v.CacheBudget != "" {
		pairs = append(pairs, [2]string{"Cache Budget", v.CacheBudget})
	}
	if v.CacheTtl != "" {
		pairs = append(pairs, [2]string{"Cache TTL", v.CacheTtl})
	}
	if len(v.RotationPolicyId) > 0 {
		pairs = append(pairs, [2]string{"Rotation Policy ID", glid.FromBytes(v.RotationPolicyId).String()})
	}
	for i, r := range v.RetentionRules {
		pairs = append(pairs, [2]string{
			fmt.Sprintf("Retention Rule[%d]", i),
			fmt.Sprintf("policy=%s action=%s", glid.FromBytes(r.RetentionPolicyId), r.Action),
		})
	}
	return pairs
}

func newVaultCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create or update a vault",
		Long: `Create or update a vault with its storage shape and lifecycle policies.

A vault is the unit of independent storage. Each vault has a single storage
shape (memory, file, file+cloud, JSONL) defined by --type, --storage-class
(file backings) and optionally --cloud-service (cloud-backed).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.VaultConfig{
				Id:                glid.New().ToProto(),
				Name:              name,
				Enabled:           true,
				Type:              v1.VaultType_VAULT_TYPE_FILE,
				StorageClass:      1,
				ReplicationFactor: 1,
			}
			verb := "Created"
			resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
			if err != nil {
				return err
			}
			for _, v := range resp.Msg.Vaults {
				if v.Name == name {
					cfg = v
					verb = "Updated"
					break
				}
			}

			if err := applyVaultFlags(ctx, cmd, client, cfg); err != nil {
				return err
			}
			if cfg.Type == v1.VaultType_VAULT_TYPE_UNSPECIFIED {
				return errors.New("--type is required for new vaults")
			}

			if _, err := client.System.PutVault(ctx, connect.NewRequest(&v1.PutVaultRequest{
				Config: cfg,
			})); err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s vault %q (%s)\n", verb, name, glid.FromBytes(cfg.Id))
			return nil
		},
	}
	cmd.Flags().String("name", "", "vault name (required)")
	cmd.Flags().Bool("enabled", true, "enable the vault")
	cmd.Flags().String("type", "file", "vault storage type: memory, file, jsonl")
	cmd.Flags().Uint32("replication-factor", 1, "replication factor")
	cmd.Flags().Uint32("storage-class", 1, "storage class for file vaults")
	cmd.Flags().String("cloud-service", "", "cloud service name or ID — sets cloud_service_id, making the vault cloud-backed")
	cmd.Flags().String("rotation-policy", "", "rotation policy name or ID")
	cmd.Flags().String("retention-policy", "", "retention policy name or ID")
	cmd.Flags().String("cache-eviction", "lru", "cache eviction strategy: lru or ttl")
	cmd.Flags().String("cache-budget", "", "max cache size (e.g. 1GB, 500MB, 1GiB)")
	cmd.Flags().String("cache-ttl", "", "cache TTL duration for ttl eviction mode (e.g. 1h, 7d)")
	cmd.Flags().String("path", "", "direct path for JSONL sinks")
	cmd.Flags().Uint64("memory-budget", 0, "memory budget in bytes (memory vaults)")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

// applyVaultFlags overlays explicitly-set CLI flags onto the vault config.
func applyVaultFlags(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.VaultConfig) error {
	if cmd.Flags().Changed("enabled") {
		cfg.Enabled, _ = cmd.Flags().GetBool("enabled")
	}
	if cmd.Flags().Changed("type") {
		t, _ := cmd.Flags().GetString("type")
		vt, ok := parseVaultType(t)
		if !ok {
			return fmt.Errorf("invalid vault type %q (valid: memory, file, jsonl)", t)
		}
		cfg.Type = vt
	}
	if cmd.Flags().Changed("replication-factor") {
		cfg.ReplicationFactor, _ = cmd.Flags().GetUint32("replication-factor")
	}
	if cmd.Flags().Changed("storage-class") {
		cfg.StorageClass, _ = cmd.Flags().GetUint32("storage-class")
	}
	if cmd.Flags().Changed("cache-eviction") {
		cfg.CacheEviction, _ = cmd.Flags().GetString("cache-eviction")
	}
	if cmd.Flags().Changed("cache-budget") {
		cfg.CacheBudget, _ = cmd.Flags().GetString("cache-budget")
	}
	if cmd.Flags().Changed("cache-ttl") {
		cfg.CacheTtl, _ = cmd.Flags().GetString("cache-ttl")
	}
	if cmd.Flags().Changed("path") {
		cfg.Path, _ = cmd.Flags().GetString("path")
	}
	if cmd.Flags().Changed("memory-budget") {
		cfg.MemoryBudgetBytes, _ = cmd.Flags().GetUint64("memory-budget")
	}
	if cmd.Flags().Changed("cloud-service") {
		if err := resolveVaultCloudService(ctx, cmd, client, cfg); err != nil {
			return err
		}
	}
	if cmd.Flags().Changed("rotation-policy") {
		if err := resolveVaultRotationPolicy(ctx, cmd, client, cfg); err != nil {
			return err
		}
	}
	if cmd.Flags().Changed("retention-policy") {
		if err := resolveVaultRetentionPolicy(ctx, cmd, client, cfg); err != nil {
			return err
		}
	}
	return nil
}

func resolveVaultCloudService(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.VaultConfig) error {
	csName, _ := cmd.Flags().GetString("cloud-service")
	if csName == "" {
		cfg.CloudServiceId = nil
		return nil
	}
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	cfg.CloudServiceId, err = resolveToProto(csName, r.cloudServices, "cloud service")
	return err
}

func resolveVaultRotationPolicy(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.VaultConfig) error {
	rotPolicy, _ := cmd.Flags().GetString("rotation-policy")
	if rotPolicy == "" {
		cfg.RotationPolicyId = nil
		return nil
	}
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	cfg.RotationPolicyId, err = resolveToProto(rotPolicy, r.rotationPolicies, "rotation policy")
	return err
}

func resolveVaultRetentionPolicy(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.VaultConfig) error {
	retPolicy, _ := cmd.Flags().GetString("retention-policy")
	if retPolicy == "" {
		cfg.RetentionRules = nil
		return nil
	}
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	retIDBytes, err := resolveToProto(retPolicy, r.retentionPolicies, "retention policy")
	if err != nil {
		return err
	}
	cfg.RetentionRules = []*v1.RetentionRule{{
		RetentionPolicyId: retIDBytes,
	}}
	return nil
}

// parseVaultType maps a CLI string to a VaultType enum.
func parseVaultType(s string) (v1.VaultType, bool) {
	switch s {
	case "memory":
		return v1.VaultType_VAULT_TYPE_MEMORY, true
	case "file":
		return v1.VaultType_VAULT_TYPE_FILE, true
	case "jsonl":
		return v1.VaultType_VAULT_TYPE_JSONL, true
	}
	return v1.VaultType_VAULT_TYPE_UNSPECIFIED, false
}

// vaultTypeName renders a VaultType enum as a CLI string.
func vaultTypeName(t v1.VaultType) string {
	switch t {
	case v1.VaultType_VAULT_TYPE_MEMORY:
		return "memory"
	case v1.VaultType_VAULT_TYPE_FILE:
		return "file"
	case v1.VaultType_VAULT_TYPE_JSONL:
		return "jsonl"
	case v1.VaultType_VAULT_TYPE_UNSPECIFIED:
		return "unspecified"
	}
	return "unspecified"
}

func newVaultDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a vault",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			force, _ := cmd.Flags().GetBool("force")
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			idBytes, err := resolveToProto(args[0], r.vaults, "vault")
			if err != nil {
				return err
			}
			_, err = client.System.DeleteVault(context.Background(), connect.NewRequest(&v1.DeleteVaultRequest{
				Id:    idBytes,
				Force: force,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted vault %q\n", args[0])
			return nil
		},
	}
	cmd.Flags().Bool("force", false, "force delete even if vault has data")
	return cmd
}
