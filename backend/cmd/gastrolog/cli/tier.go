package cli

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
)

func newTierCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tier",
		Short: "Manage tiers",
	}
	cmd.AddCommand(
		newTierListCmd(),
		newTierCreateCmd(),
		newTierDeleteCmd(),
	)
	return cmd
}

func newTierListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all tiers",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Config.GetConfig(context.Background(), connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg.Tiers)
			}
			var rows [][]string
			for _, t := range resp.Msg.Tiers {
				rows = append(rows, []string{
					t.Id, t.Name, tierTypeName(t.Type),
					strconv.FormatUint(uint64(t.ReplicationFactor), 10),
				})
			}
			p.table([]string{"ID", "NAME", "TYPE", "RF"}, rows)
			return nil
		},
	}
}

func newTierCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create or update a tier",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")

			client := clientFromCmd(cmd)
			ctx := context.Background()

			cfg := &v1.TierConfig{
				Id:                uuid.Must(uuid.NewV7()).String(),
				Name:              name,
				Type:              v1.TierType_TIER_TYPE_FILE,
				ReplicationFactor: 1,
				StorageClass:      1,
			}
			verb := "Created"
			resp, err := client.Config.GetConfig(ctx, connect.NewRequest(&v1.GetConfigRequest{}))
			if err != nil {
				return err
			}
			for _, t := range resp.Msg.Tiers {
				if t.Name == name {
					cfg = t
					verb = "Updated"
					break
				}
			}

			if err := applyTierFlags(ctx, cmd, client, cfg); err != nil {
				return err
			}

			if cfg.Type == v1.TierType_TIER_TYPE_UNSPECIFIED {
				return errors.New("--type is required for new tiers")
			}
			if verb == "Created" && !cmd.Flags().Changed("vault") {
				return errors.New("--vault is required when creating a new tier")
			}

			_, err = client.Config.PutTier(ctx, connect.NewRequest(&v1.PutTierRequest{
				Config: cfg,
			}))
			if err != nil {
				return err
			}

			// Add tier to vault's tier list (tiers must always belong to a vault).
			if cmd.Flags().Changed("vault") {
				if err := addTierToVault(ctx, cmd, client, cfg.Id); err != nil {
					return err
				}
			}

			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(cfg)
			}
			fmt.Printf("%s tier %q (%s)\n", verb, name, cfg.Id)
			return nil
		},
	}
	cmd.Flags().String("name", "", "tier name (required)")
	cmd.Flags().String("vault", "", "vault to assign this tier to (required for new tiers)")
	cmd.Flags().String("type", "file", "tier type: memory, file, cloud, jsonl")
	cmd.Flags().Uint32("replication-factor", 1, "replication factor")
	cmd.Flags().String("rotation-policy", "", "rotation policy name or ID")
	cmd.Flags().String("retention-policy", "", "retention policy name or ID")
	cmd.Flags().Uint32("storage-class", 1, "storage class for file/cloud tiers")
	cmd.Flags().String("cloud-service", "", "cloud service name or ID (required for cloud tiers)")
	cmd.Flags().Uint32("active-chunk-class", 0, "storage class for cloud tier active chunks")
	cmd.Flags().Uint32("cache-class", 0, "storage class for cloud tier read cache")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

// applyTierFlags overlays explicitly-set CLI flags onto the tier config.
func applyTierFlags(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.TierConfig) error {
	if cmd.Flags().Changed("type") {
		tierType, _ := cmd.Flags().GetString("type")
		tt, ok := parseTierType(tierType)
		if !ok {
			return fmt.Errorf("invalid tier type %q (valid: memory, file, cloud, jsonl)", tierType)
		}
		cfg.Type = tt
	}
	if cmd.Flags().Changed("replication-factor") {
		cfg.ReplicationFactor, _ = cmd.Flags().GetUint32("replication-factor")
	}
	if cmd.Flags().Changed("storage-class") {
		cfg.StorageClass, _ = cmd.Flags().GetUint32("storage-class")
	}
	if cmd.Flags().Changed("active-chunk-class") {
		cfg.ActiveChunkClass, _ = cmd.Flags().GetUint32("active-chunk-class")
	}
	if cmd.Flags().Changed("cache-class") {
		cfg.CacheClass, _ = cmd.Flags().GetUint32("cache-class")
	}
	if cmd.Flags().Changed("cloud-service") {
		if err := resolveCloudService(ctx, cmd, client, cfg); err != nil {
			return err
		}
	}
	if cmd.Flags().Changed("rotation-policy") {
		if err := resolveRotationPolicy(ctx, cmd, client, cfg); err != nil {
			return err
		}
	}
	if cmd.Flags().Changed("retention-policy") {
		if err := resolveRetentionPolicy(ctx, cmd, client, cfg); err != nil {
			return err
		}
	}
	return nil
}

// resolveCloudService resolves the --cloud-service flag value to an ID and sets
// it on cfg. An empty value clears the cloud service.
func resolveCloudService(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.TierConfig) error {
	csName, _ := cmd.Flags().GetString("cloud-service")
	if csName == "" {
		cfg.CloudServiceId = ""
		return nil
	}
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	csID, err := resolve(csName, r.cloudServices, "cloud service")
	if err != nil {
		return err
	}
	cfg.CloudServiceId = csID
	return nil
}

// resolveRotationPolicy resolves the --rotation-policy flag value to an ID and
// sets it on cfg.
func resolveRotationPolicy(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.TierConfig) error {
	rotPolicy, _ := cmd.Flags().GetString("rotation-policy")
	if rotPolicy == "" {
		cfg.RotationPolicyId = ""
		return nil
	}
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	rpID, err := resolve(rotPolicy, r.rotationPolicies, "rotation policy")
	if err != nil {
		return err
	}
	cfg.RotationPolicyId = rpID
	return nil
}

// resolveRetentionPolicy resolves the --retention-policy flag value to an ID
// and sets it on cfg.
func resolveRetentionPolicy(ctx context.Context, cmd *cobra.Command, client *server.Client, cfg *v1.TierConfig) error {
	retPolicy, _ := cmd.Flags().GetString("retention-policy")
	if retPolicy == "" {
		cfg.RetentionRules = nil
		return nil
	}
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	retID, err := resolve(retPolicy, r.retentionPolicies, "retention policy")
	if err != nil {
		return err
	}
	cfg.RetentionRules = []*v1.RetentionRule{{
		RetentionPolicyId: retID,
	}}
	return nil
}

// addTierToVault resolves the --vault flag and sets VaultId/Position on the tier config.
func addTierToVault(ctx context.Context, cmd *cobra.Command, client *server.Client, tierID string) error {
	vaultName, _ := cmd.Flags().GetString("vault")
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	vaultID, err := resolve(vaultName, r.vaults, "vault")
	if err != nil {
		return err
	}
	resp, err := client.Config.GetConfig(ctx, connect.NewRequest(&v1.GetConfigRequest{}))
	if err != nil {
		return err
	}
	// Find the tier config to update.
	for _, t := range resp.Msg.Tiers {
		if t.Id != tierID {
			continue
		}
		if t.VaultId == vaultID {
			return nil // already assigned to this vault
		}
		// Count existing tiers for this vault to determine position.
		var maxPos uint32
		for _, other := range resp.Msg.Tiers {
			if other.VaultId == vaultID && other.Position >= maxPos {
				maxPos = other.Position + 1
			}
		}
		t.VaultId = vaultID
		t.Position = maxPos
		_, err = client.Config.PutTier(ctx, connect.NewRequest(&v1.PutTierRequest{Config: t}))
		if err != nil {
			return fmt.Errorf("tier created but failed to set vault: %w", err)
		}
		return nil
	}
	return nil
}

func newTierDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a tier",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			drain, _ := cmd.Flags().GetBool("drain")
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.tiers, "tier")
			if err != nil {
				return err
			}
			_, err = client.Config.DeleteTier(context.Background(), connect.NewRequest(&v1.DeleteTierRequest{
				Id:    id,
				Drain: drain,
			}))
			if err != nil {
				return err
			}
			if drain {
				fmt.Printf("Tier %s draining to next tier (will be removed after completion)\n", args[0])
			} else {
				fmt.Printf("Deleted tier %s\n", args[0])
			}
			return nil
		},
	}
	cmd.Flags().Bool("drain", false, "drain chunks to next tier before deleting")
	return cmd
}

func parseTierType(s string) (v1.TierType, bool) {
	switch s {
	case "memory":
		return v1.TierType_TIER_TYPE_MEMORY, true
	case "file":
		return v1.TierType_TIER_TYPE_FILE, true
	case "cloud":
		return v1.TierType_TIER_TYPE_CLOUD, true
	case "jsonl":
		return v1.TierType_TIER_TYPE_JSONL, true
	default:
		return v1.TierType_TIER_TYPE_UNSPECIFIED, false
	}
}

func tierTypeName(t v1.TierType) string {
	switch t {
	case v1.TierType_TIER_TYPE_MEMORY:
		return "memory"
	case v1.TierType_TIER_TYPE_FILE:
		return "file"
	case v1.TierType_TIER_TYPE_CLOUD:
		return "cloud"
	case v1.TierType_TIER_TYPE_JSONL:
		return "jsonl"
	case v1.TierType_TIER_TYPE_UNSPECIFIED:
		return "unspecified"
	default:
		return "unknown"
	}
}
