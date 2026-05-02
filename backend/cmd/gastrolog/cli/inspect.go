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
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
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
		key := glid.FromBytes(c.TierId).String()
		chunksByTier[key] = append(chunksByTier[key], c)
	}

	vaultName := resolveVaultName(cfgResp.Msg.Vaults, vaultID, args[0])
	fmt.Printf("Vault: %s (%s)\n\n", vaultName, vaultID)

	nodeNames := nodeIDToNameMap(cfgResp.Msg.NodeConfigs)
	for _, tier := range vaultTiers {
		printTierSection(tier, chunksByTier[glid.FromBytes(tier.Id).String()], nodeNames)
	}

	return nil
}

// nodeIDToNameMap builds a lookup of node ID → human name from the system
// config, so the inspector can render replica residency as friendly node
// names ("node-1, node-3") rather than raw GLIDs.
func nodeIDToNameMap(nodes []*v1.NodeConfig) map[string]string {
	m := make(map[string]string, len(nodes))
	for _, n := range nodes {
		id := glid.FromBytes(n.Id).String()
		if n.Name != "" {
			m[id] = n.Name
		} else {
			m[id] = id
		}
	}
	return m
}

func collectVaultTiers(tiers []*v1.TierConfig, vaultID string) []*v1.TierConfig {
	var out []*v1.TierConfig
	for _, t := range tiers {
		if glid.FromBytes(t.VaultId).String() == vaultID {
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
		if glid.FromBytes(v.Id).String() == vaultID {
			return v.Name
		}
	}
	return fallback
}

func printTierSection(tier *v1.TierConfig, chunks []*v1.ChunkMeta, nodeNames map[string]string) {
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
		idStr := glid.FromBytes(c.Id).String()
		short := idStr
		if len(short) > 12 {
			short = short[:12]
		}
		fmt.Printf("    %s...  %-40s  %5d records  %s  on %s%s\n",
			short, chunkBadges(c), c.RecordCount, units.FormatBytesDisplay(c.DiskBytes),
			renderReplicaResidency(c.ReplicaNodeIds, nodeNames),
			renderPendingAcks(c.PendingAckNodeIds, nodeNames))
	}
	fmt.Println()
}

// renderPendingAcks formats the receipt-protocol's still-owed-ack node
// list as a trailing "  pending-ack: node-2, node-3" suffix. Empty
// list renders as empty string so chunks without a stuck delete don't
// get a noisy column. See gastrolog-51gme.
func renderPendingAcks(nodeIDs []string, nodeNames map[string]string) string {
	if len(nodeIDs) == 0 {
		return ""
	}
	names := make([]string, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if n, ok := nodeNames[id]; ok {
			names = append(names, n)
		} else {
			names = append(names, id)
		}
	}
	sort.Strings(names)
	return "  pending-ack: " + strings.Join(names, ", ")
}

// renderReplicaResidency turns a chunk's replica node-ID list into a
// readable "node-1, node-3" string. Replica IDs come from the merged
// ListChunks fan-out — the set of nodes that actually reported the
// chunk locally, which is distinct from placement (where it should
// live). Empty list renders as "—" so a chunk that nobody holds is
// visually distinct from one with replicas.
func renderReplicaResidency(nodeIDs []string, nodeNames map[string]string) string {
	if len(nodeIDs) == 0 {
		return "—"
	}
	names := make([]string, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if n, ok := nodeNames[id]; ok {
			names = append(names, n)
		} else {
			names = append(names, id)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
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

	chunkID, parseErr := chunk.ParseChunkID(args[0])
	if parseErr != nil {
		return fmt.Errorf("invalid chunk ID: %w", parseErr)
	}
	resp, err := client.Vault.GetChunk(context.Background(),
		connect.NewRequest(&v1.GetChunkRequest{Vault: vaultID, ChunkId: glid.GLID(chunkID).ToProto()}))
	if err != nil {
		return err
	}
	c := resp.Msg.Chunk

	if outputFormat(cmd) == "json" {
		return newPrinter("json").json(c)
	}

	tierName := resolveTierName(client, glid.FromBytes(c.TierId).String())
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
		if glid.FromBytes(t.Id).String() == tierID {
			return fmt.Sprintf("%s (%s)", t.Name, strings.TrimPrefix(t.Type.String(), "TIER_TYPE_"))
		}
	}
	return tierID
}

func buildChunkKV(c *v1.ChunkMeta, tierName string) [][2]string {
	pairs := [][2]string{
		{"Chunk ID", glid.FromBytes(c.Id).String()},
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
	// "compressed" badge dropped — sealed chunks are GLCB which is
	// zstd-compressed by construction (gastrolog-24m1t step 7f).
	if c.CloudBacked {
		parts = append(parts, "cloud")
	}
	if c.Archived {
		parts = append(parts, "archived")
	}
	if c.RetentionPending {
		parts = append(parts, "retention-pending")
	}
	if c.TransitionStreamed {
		// Matches inspector "del" badge: streamed to next tier, awaiting dest receipt / expire.
		parts = append(parts, "streamed-await-delete")
	}
	return strings.Join(parts, " ")
}
