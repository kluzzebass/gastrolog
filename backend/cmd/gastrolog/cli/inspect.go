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
		Short: "Inspect vault and chunk details",
	}
	cmd.AddCommand(
		newInspectVaultCmd(),
		newInspectChunkCmd(),
		newInspectStorageCmd(),
	)
	return cmd
}

func newInspectVaultCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vault <name-or-id>",
		Short: "Show vault details and chunks with status badges",
		Args:  cobra.ExactArgs(1),
		RunE:  runInspectVault,
	}
	cmd.Flags().Bool("segments", false, inspectSegmentsFlagHelp)
	return cmd
}

func runInspectVault(cmd *cobra.Command, args []string) error {
	segments, _ := cmd.Flags().GetBool("segments")
	if segments {
		return runInspectVaultSegments(cmd, args[0])
	}

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

	vault := findVaultConfigInSystem(cfgResp.Msg.Vaults, vaultID)
	if vault == nil {
		return fmt.Errorf("vault %s not found", vaultID)
	}

	chunksResp, err := client.Vault.ListChunks(context.Background(),
		connect.NewRequest(&v1.ListChunksRequest{Vault: vaultID}))
	if err != nil {
		return err
	}

	if outputFormat(cmd) == "json" {
		return newPrinter("json").json(map[string]any{
			"vault":  vault,
			"chunks": chunksResp.Msg.Chunks,
		})
	}

	chunksByVault := make(map[string][]*v1.ChunkMeta)
	for _, c := range chunksResp.Msg.Chunks {
		key := glid.FromBytes(c.VaultId).String()
		chunksByVault[key] = append(chunksByVault[key], c)
	}

	vaultName := resolveVaultName(cfgResp.Msg.Vaults, vaultID, args[0])
	fmt.Printf("Vault: %s (%s)\n\n", vaultName, vaultID)

	nodeNames := nodeIDToNameMap(cfgResp.Msg.NodeConfigs)
	printVaultSection(vault, chunksByVault[glid.FromBytes(vault.Id).String()], nodeNames)

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

func findVaultConfigInSystem(vaults []*v1.VaultConfig, vaultID string) *v1.VaultConfig {
	for _, v := range vaults {
		if glid.FromBytes(v.Id).String() == vaultID {
			return v
		}
	}
	return nil
}

func resolveVaultName(vaults []*v1.VaultConfig, vaultID, fallback string) string {
	for _, v := range vaults {
		if glid.FromBytes(v.Id).String() == vaultID {
			return v.Name
		}
	}
	return fallback
}

func printVaultSection(vault *v1.VaultConfig, chunks []*v1.ChunkMeta, nodeNames map[string]string) {
	vaultType := strings.TrimPrefix(vault.Type.String(), "VAULT_TYPE_")
	var totalRecords, totalBytes int64
	for _, c := range chunks {
		totalRecords += c.RecordCount
		totalBytes += chunkSizeBytes(c)
	}

	fmt.Printf("  STORAGE: %s  %q  %d chunks  %d records  %s\n",
		vaultType, vault.Name,
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
			short, chunkBadges(c), c.RecordCount, units.FormatBytesDisplay(chunkSizeBytes(c)),
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

	vaultName := lookupVaultLabel(client, glid.FromBytes(c.VaultId).String())
	nodeNames := map[string]string{}
	if cfgResp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{})); err == nil {
		nodeNames = nodeIDToNameMap(cfgResp.Msg.NodeConfigs)
	}
	pairs := buildChunkKV(c, vaultName, nodeNames)
	newPrinter(outputFormat(cmd)).kv(pairs)
	return nil
}

// lookupVaultLabel fetches the system config via client and returns
// "Name (TYPE)" for the vault with the given ID, falling back to the ID.
// Distinct from resolveVaultName above, which takes an already-fetched
// vaults slice and returns just the name.
func lookupVaultLabel(client *server.Client, vaultID string) string {
	if vaultID == "" {
		return ""
	}
	cfgResp, err := client.System.GetSystem(context.Background(), connect.NewRequest(&v1.GetSystemRequest{}))
	if err != nil {
		return vaultID
	}
	for _, v := range cfgResp.Msg.Vaults {
		if glid.FromBytes(v.Id).String() == vaultID {
			return fmt.Sprintf("%s (%s)", v.Name, strings.TrimPrefix(v.Type.String(), "VAULT_TYPE_"))
		}
	}
	return vaultID
}

func buildChunkKV(c *v1.ChunkMeta, vaultName string, nodeNames map[string]string) [][2]string {
	pairs := [][2]string{
		{"Chunk ID", glid.FromBytes(c.Id).String()},
		{"Vault", vaultName},
		{"Status", chunkBadges(c)},
		{"Records", strconv.FormatInt(c.RecordCount, 10)},
		{"Logical Size", units.FormatBytesDisplay(c.Bytes)},
		{"Disk Size", formatDiskSize(c)},
		{"Replicas", strconv.Itoa(int(c.ReplicaCount))},
		// Holder-receipt residency, same truth the UI's seal pips render
		// (gastrolog-45ywhx parity). "—" = zero verified copies.
		{"Resident On", renderReplicaResidency(c.ReplicaNodeIds, nodeNames)},
	}
	if len(c.PendingAckNodeIds) > 0 {
		pairs = append(pairs, [2]string{"Pending Acks",
			strings.TrimPrefix(renderPendingAcks(c.PendingAckNodeIds, nodeNames), "  pending-ack: ")})
	}

	if c.CloudBacked {
		pairs = append(pairs, [2]string{"Cloud", "GLCB (zstd-wrapped on transport)"})
		// Distinguish the two currencies: Disk Size above is THIS node's
		// live local cache state (0 once evicted); Cloud Size is the fixed
		// compressed object size, unaffected by local eviction/re-warm.
		// See gastrolog-33ul6h.
		pairs = append(pairs, [2]string{"Cloud Size", units.FormatBytesDisplay(c.CloudBytes)})
	}
	if c.CloudStorageClass != "" {
		pairs = append(pairs, [2]string{"Storage Class", c.CloudStorageClass})
	}
	pairs = appendTS(pairs, "Write Start", c.WriteStart)
	pairs = appendTS(pairs, "Write End", c.WriteEnd)
	pairs = appendTS(pairs, "Ingest Start", c.IngestStart)
	pairs = appendTS(pairs, "Ingest End", c.IngestEnd)
	return pairs
}

func formatDiskSize(c *v1.ChunkMeta) string {
	return units.FormatBytesDisplay(chunkSizeBytes(c))
}

// chunkSizeBytes prefers the on-disk size, falling back to the logical
// record bytes when DiskBytes is unset — pipeline GLCB chunks never
// populate DiskBytes (the manager store holds no local copy; bytes live
// in the staging GLCB), and rendering their size as "0 B" while the UI
// shows the logical size was a parity bug (gastrolog-45ywhx).
//
// A cloud-backed chunk with DiskBytes==0 is different: it was evicted from
// this node's warm cache, not "never measured." It must report 0 here, not
// fall back to logical Bytes — the object still exists in the cloud store,
// at Cloud Size, a currency this local-disk stat never touches. See
// gastrolog-33ul6h.
func chunkSizeBytes(c *v1.ChunkMeta) int64 {
	if c.CloudBacked && c.DiskBytes == 0 {
		return 0
	}
	if c.DiskBytes > 0 {
		return c.DiskBytes
	}
	return c.Bytes
}

func appendTS(pairs [][2]string, label string, ts *timestamppb.Timestamp) [][2]string {
	if ts != nil {
		pairs = append(pairs, [2]string{label, ts.AsTime().UTC().Format(tsFormat)})
	}
	return pairs
}

// chunkBadges returns a space-separated string of status badges for a chunk.
//
// The lifecycle badge reads the three-state enum, never the legacy Sealed
// bool: Sealed is only true at CHUNK_STATE_SEALED, so a binary else-branch
// painted SEALING chunks as "active" — the opposite diagnosis when seals
// are wedged and retention cannot fire (gastrolog-5wh571). An unspecified
// state renders as "unknown", never a guess.
func chunkBadges(c *v1.ChunkMeta) string {
	var parts []string
	switch c.State {
	case v1.ChunkState_CHUNK_STATE_ACTIVE:
		parts = append(parts, "active")
	case v1.ChunkState_CHUNK_STATE_SEALING:
		parts = append(parts, "sealing")
	case v1.ChunkState_CHUNK_STATE_SEALED:
		parts = append(parts, "sealed")
	case v1.ChunkState_CHUNK_STATE_UNSPECIFIED:
		parts = append(parts, "unknown")
	default:
		parts = append(parts, "unknown")
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
	return strings.Join(parts, " ")
}
