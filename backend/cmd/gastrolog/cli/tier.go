package cli

import (
	"context"
	"fmt"
	"strconv"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
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
		Short: "Create a tier",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			tierType, _ := cmd.Flags().GetString("type")
			rf, _ := cmd.Flags().GetUint32("replication-factor")
			rotPolicy, _ := cmd.Flags().GetString("rotation-policy")
			retPolicy, _ := cmd.Flags().GetString("retention-policy")
			storageClass, _ := cmd.Flags().GetUint32("storage-class")

			tt, ok := parseTierType(tierType)
			if !ok {
				return fmt.Errorf("invalid tier type %q (valid: memory, file, cloud, jsonl)", tierType)
			}

			client := clientFromCmd(cmd)
			id := uuid.Must(uuid.NewV7()).String()

			tc := &v1.TierConfig{
				Id:                id,
				Name:              name,
				Type:              tt,
				ReplicationFactor: rf,
				StorageClass:      storageClass,
			}

			// Resolve rotation policy by name.
			if rotPolicy != "" {
				r, err := newResolver(context.Background(), client)
				if err != nil {
					return err
				}
				rpID, err := resolve(rotPolicy, r.rotationPolicies, "rotation policy")
				if err != nil {
					return err
				}
				tc.RotationPolicyId = rpID
			}

			// Resolve retention policy by name and add as a rule.
			if retPolicy != "" {
				r, err := newResolver(context.Background(), client)
				if err != nil {
					return err
				}
				retID, err := resolve(retPolicy, r.retentionPolicies, "retention policy")
				if err != nil {
					return err
				}
				tc.RetentionRules = []*v1.RetentionRule{{
					RetentionPolicyId: retID,
				}}
			}

			_, err := client.Config.PutTier(context.Background(), connect.NewRequest(&v1.PutTierRequest{
				Config: tc,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Created tier %q (%s) type=%s rf=%d\n", name, id, tierType, rf)
			return nil
		},
	}
	cmd.Flags().String("name", "", "tier name (required)")
	cmd.Flags().String("type", "file", "tier type: memory, file, cloud, jsonl")
	cmd.Flags().Uint32("replication-factor", 1, "replication factor")
	cmd.Flags().String("rotation-policy", "", "rotation policy name or ID")
	cmd.Flags().String("retention-policy", "", "retention policy name or ID")
	cmd.Flags().Uint32("storage-class", 1, "storage class for file/cloud tiers")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func newTierDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name-or-id>",
		Short: "Delete a tier",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			deleteData, _ := cmd.Flags().GetBool("delete-data")
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
				Id:         id,
				DeleteData: deleteData,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Deleted tier %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().Bool("delete-data", false, "also delete tier data from disk")
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
