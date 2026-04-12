package cli

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"
	"gastrolog/internal/units"
)

const tsFormat = "2006-01-02 15:04:05 UTC"

// NewInspectCommand returns the "inspect" command tree.
func NewInspectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect vault, tier, and chunk details",
	}
	cmd.AddCommand(
		newInspectVaultCmd(),
		newInspectChunkCmd(),
	)
	return cmd
}

func newInspectVaultCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "vault <name-or-id>",
		Short: "Show vault tiers and chunks with status badges",
		Args:  cobra.ExactArgs(1),
		RunE:  runInspectVault,
	}
}

func runInspectVault(cmd *cobra.Command, args []string) error {
	client := clientFromCmd(cmd)
	r, err := newResolver(context.Background(), client)
	if err != nil {
		return err
	}
	vaultID, err := resolve(args[0], r.vaults, "vault")
	if err != nil {
		return err
	}

	cfgResp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
	if err != nil {
		return err
	}

	vaultTiers := collectVaultTiers(cfgResp.Msg.Tiers, vaultID)

	chunksResp, err := client.Vault.ListChunks(context.Background(),
		connect.NewRequest(&v1.ListChunksRequest{Vault: vaultID}))
	if err != nil {
		return err
	}

	if outputFormat(cmd) == "json" {
		return newPrinter("json").json(map[string]any{
			"tiers":  vaultTiers,
			"chunks": chunksResp.Msg.Chunks,
		})
	}

	chunksByTier := make(map[string][]*v1.ChunkMeta)
	for _, c := range chunksResp.Msg.Chunks {
		chunksByTier[c.TierId] = append(chunksByTier[c.TierId], c)
	}

	vaultName := resolveVaultName(cfgResp.Msg.Vaults, vaultID, args[0])
	fmt.Printf("Vault: %s (%s)\n\n", vaultName, vaultID)

	for _, tier := range vaultTiers {
		printTierSection(tier, chunksByTier[tier.Id])
	}

	return nil
}

func collectVaultTiers(tiers []*v1.TierConfig, vaultID string) []*v1.TierConfig {
	var out []*v1.TierConfig
	for _, t := range tiers {
		if t.VaultId == vaultID {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Position < out[j].Position
	})
	return out
}

func resolveVaultName(vaults []*v1.VaultConfig, vaultID, fallback string) string {
	for _, v := range vaults {
		if v.Id == vaultID {
			return v.Name
		}
	}
	return fallback
}

func printTierSection(tier *v1.TierConfig, chunks []*v1.ChunkMeta) {
	tierType := strings.TrimPrefix(tier.Type.String(), "TIER_TYPE_")
	var totalRecords, totalBytes int64
	for _, c := range chunks {
		totalRecords += c.RecordCount
		totalBytes += c.DiskBytes
	}

	fmt.Printf("  TIER %d: %s  %q  %d chunks  %d records  %s\n",
		tier.Position+1, tierType, tier.Name,
		len(chunks), totalRecords, units.FormatBytesDisplay(totalBytes))

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].WriteStart.AsTime().After(chunks[j].WriteStart.AsTime())
	})

	for _, c := range chunks {
		short := c.Id
		if len(short) > 12 {
			short = short[:12]
		}
		fmt.Printf("    %s...  %-40s  %5d records  %s\n",
			short, chunkBadges(c), c.RecordCount, units.FormatBytesDisplay(c.DiskBytes))
	}
	fmt.Println()
}

func newInspectChunkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chunk <chunk-id>",
		Short: "Show detailed chunk information",
		Args:  cobra.ExactArgs(1),
		RunE:  runInspectChunk,
	}
	cmd.Flags().String("vault", "", "vault name or ID (required)")
	return cmd
}

func runInspectChunk(cmd *cobra.Command, args []string) error {
	client := clientFromCmd(cmd)
	vaultFlag, _ := cmd.Flags().GetString("vault")
	if vaultFlag == "" {
		return errors.New("--vault is required")
	}

	r, err := newResolver(context.Background(), client)
	if err != nil {
		return err
	}
	vaultID, err := resolve(vaultFlag, r.vaults, "vault")
	if err != nil {
		return err
	}

	resp, err := client.Vault.GetChunk(context.Background(),
		connect.NewRequest(&v1.GetChunkRequest{Vault: vaultID, ChunkId: args[0]}))
	if err != nil {
		return err
	}
	c := resp.Msg.Chunk

	if outputFormat(cmd) == "json" {
		return newPrinter("json").json(c)
	}

	tierName := resolveTierName(client, c.TierId)
	pairs := buildChunkKV(c, tierName)
	newPrinter(outputFormat(cmd)).kv(pairs)
	return nil
}

func resolveTierName(client *server.Client, tierID string) string {
	if tierID == "" {
		return ""
	}
	cfgResp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
	if err != nil {
		return tierID
	}
	for _, t := range cfgResp.Msg.Tiers {
		if t.Id == tierID {
			return fmt.Sprintf("%s (%s)", t.Name, strings.TrimPrefix(t.Type.String(), "TIER_TYPE_"))
		}
	}
	return tierID
}

func buildChunkKV(c *v1.ChunkMeta, tierName string) [][2]string {
	pairs := [][2]string{
		{"Chunk ID", c.Id},
		{"Tier", tierName},
		{"Status", chunkBadges(c)},
		{"Records", strconv.FormatInt(c.RecordCount, 10)},
		{"Logical Size", units.FormatBytesDisplay(c.Bytes)},
		{"Disk Size", formatDiskSize(c)},
		{"Replicas", strconv.Itoa(int(c.ReplicaCount))},
	}

	if c.CloudBacked && c.NumFrames > 0 {
		pairs = append(pairs, [2]string{"Cloud", fmt.Sprintf("GLCB, %d seekable zstd frame(s)", c.NumFrames)})
	}
	if c.StorageClass != "" {
		pairs = append(pairs, [2]string{"Storage Class", c.StorageClass})
	}
	pairs = appendTS(pairs, "Write Start", c.WriteStart)
	pairs = appendTS(pairs, "Write End", c.WriteEnd)
	pairs = appendTS(pairs, "Ingest Start", c.IngestStart)
	pairs = appendTS(pairs, "Ingest End", c.IngestEnd)
	return pairs
}

func formatDiskSize(c *v1.ChunkMeta) string {
	s := units.FormatBytesDisplay(c.DiskBytes)
	if c.Compressed && c.Bytes > 0 && c.DiskBytes > 0 && c.DiskBytes < c.Bytes {
		pct := 100 - (float64(c.DiskBytes)/float64(c.Bytes))*100
		s += fmt.Sprintf(" (%.0f%% compression)", pct)
	}
	return s
}

func appendTS(pairs [][2]string, label string, ts *timestamppb.Timestamp) [][2]string {
	if ts != nil {
		pairs = append(pairs, [2]string{label, ts.AsTime().UTC().Format(tsFormat)})
	}
	return pairs
}

// chunkBadges returns a space-separated string of status badges for a chunk.
func chunkBadges(c *v1.ChunkMeta) string {
	var parts []string
	if c.Sealed {
		parts = append(parts, "sealed")
	} else {
		parts = append(parts, "active")
	}
	if c.Compressed {
		parts = append(parts, "compressed")
	}
	if c.CloudBacked {
		parts = append(parts, "cloud")
	}
	if c.Archived {
		parts = append(parts, "archived")
	}
	if c.RetentionPending {
		parts = append(parts, "retention-pending")
	}
	return strings.Join(parts, " ")
}
