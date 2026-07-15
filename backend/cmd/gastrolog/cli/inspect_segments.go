package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/units"
)

const inspectSegmentsFlagHelp = `Report per-vault segment staging areas on every cluster node.

Areas (see docs/ubiquitous_language.md):
  working/    Active segment being written by segmentation on ingest origins.
  completed/  Closed segments awaiting distribution publish to vault-ctl.
  pre-head/   Segments pulled from peers, awaiting verify/promote to head/.
  head/       Verified segments available for chunking on vault homes.

Registry counts come from the replicated vault-ctl FSM (identical on every voter).`

func runInspectVaultSegments(cmd *cobra.Command, vaultArg string) error {
	client := clientFromCmd(cmd)
	r, err := newResolver(context.Background(), client)
	if err != nil {
		return err
	}
	vaultID, err := resolve(vaultArg, r.vaults, "vault")
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

	backlogResp, err := client.Vault.GetPipelineBacklog(context.Background(),
		connect.NewRequest(&v1.GetPipelineBacklogRequest{Vault: vaultID}))
	if err != nil {
		return err
	}
	backlog := backlogResp.Msg.Backlog
	if backlog == nil {
		return fmt.Errorf("pipeline backlog unavailable for vault %s", vaultID)
	}

	if outputFormat(cmd) == "json" {
		return newPrinter("json").json(backlog)
	}

	vaultName := resolveVaultName(cfgResp.Msg.Vaults, vaultID, vaultArg)
	nodeNames := nodeIDToNameMap(cfgResp.Msg.NodeConfigs)
	printVaultSegmentStaging(vaultName, vaultID, backlog, nodeNames)
	return nil
}

func printVaultSegmentStaging(vaultName, vaultID string, backlog *v1.VaultPipelineBacklog, nodeNames map[string]string) {
	fmt.Printf("Vault: %s (%s)\n\n", vaultName, vaultID)

	fmt.Printf("Vault-ctl registry: %d eligible / %d published (%s records)\n",
		backlog.EligibleSegments, backlog.RegistrySegments,
		formatUint(backlog.RegistryRecords))
	if backlog.OpenManifestRefs > 0 || backlog.OpenManifestRecords > 0 {
		fmt.Printf("Open manifest: %d refs, %s records",
			backlog.OpenManifestRefs, formatUint(backlog.OpenManifestRecords))
		if ts := formatProtoTime(backlog.OpenManifestIngestEnd); ts != "" {
			fmt.Printf(", ingest end %s", ts)
		}
		fmt.Println()
	}
	if backlog.SealedManifestPending {
		fmt.Println("Sealed manifest: pending local GLCB build")
	}
	if ts := formatProtoTime(backlog.OldestEligibleLastIngest); ts != "" {
		fmt.Printf("Oldest unchunked ingest: %s\n", ts)
	}
	if backlog.ConnectedNodeIsVaultCtlLeader {
		fmt.Println("Connected node is vault-ctl leader")
	} else if leader := formatIDBytes(backlog.VaultCtlLeaderNodeId); leader != "" {
		fmt.Printf("Vault-ctl leader: %s\n", leaderName(leader, nodeNames))
	}
	fmt.Println()

	fmt.Println("Cluster segment staging (summed across nodes):")
	printSegmentAreaTable([]segmentAreaRow{
		{"working", int(backlog.WorkingSegments), protoSegmentBytes(backlog.WorkingBytes)},
		{"completed", int(backlog.CompletedStagingSegments), protoSegmentBytes(backlog.CompletedStagingBytes)},
		{"pre-head", int(backlog.PreHeadSegments), protoSegmentBytes(backlog.PreHeadBytes)},
		{"head", int(backlog.HeadSegments), protoSegmentBytes(backlog.HeadBytes)},
	})

	if len(backlog.NodeSegments) == 0 {
		return
	}

	fmt.Println("Per-node segment staging:")
	rows := make([]nodeSegmentRow, 0, len(backlog.NodeSegments))
	for _, ns := range backlog.NodeSegments {
		nodeID := formatIDBytes(ns.NodeId)
		if nodeID == "" {
			continue
		}
		rows = append(rows, nodeSegmentRow{
			nodeLabel:    leaderName(nodeID, nodeNames),
			working:      int(ns.WorkingSegments),
			staged:       int(ns.CompletedStagingSegments),
			preHead:      int(ns.PreHeadSegments),
			head:         int(ns.HeadSegments),
			workingBytes: protoSegmentBytes(ns.WorkingBytes),
			stagedBytes:  protoSegmentBytes(ns.CompletedStagingBytes),
			preHeadBytes: protoSegmentBytes(ns.PreHeadBytes),
			headBytes:    protoSegmentBytes(ns.HeadBytes),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].nodeLabel < rows[j].nodeLabel })
	printNodeSegmentTable(rows)
}

type segmentAreaRow struct {
	area  string
	count int
	bytes int64
}

type nodeSegmentRow struct {
	nodeLabel    string
	working      int
	staged       int
	preHead      int
	head         int
	workingBytes int64
	stagedBytes  int64
	preHeadBytes int64
	headBytes    int64
}

func printSegmentAreaTable(rows []segmentAreaRow) {
	p := newPrinter("table")
	var tableRows [][]string
	for _, row := range rows {
		tableRows = append(tableRows, []string{
			row.area,
			strconv.Itoa(row.count),
			units.FormatBytesDisplay(row.bytes),
		})
	}
	p.table([]string{"AREA", "SEGMENTS", "BYTES"}, tableRows)
	fmt.Println()
}

func printNodeSegmentTable(rows []nodeSegmentRow) {
	p := newPrinter("table")
	var tableRows [][]string
	for _, row := range rows {
		tableRows = append(tableRows, []string{
			row.nodeLabel,
			fmt.Sprintf("%d (%s)", row.working, units.FormatBytesDisplay(row.workingBytes)),
			fmt.Sprintf("%d (%s)", row.staged, units.FormatBytesDisplay(row.stagedBytes)),
			fmt.Sprintf("%d (%s)", row.preHead, units.FormatBytesDisplay(row.preHeadBytes)),
			fmt.Sprintf("%d (%s)", row.head, units.FormatBytesDisplay(row.headBytes)),
		})
	}
	p.table([]string{"NODE", "WORKING", "COMPLETED", "PRE-HEAD", "HEAD"}, tableRows)
	fmt.Println()
}

func leaderName(nodeID string, nodeNames map[string]string) string {
	if name, ok := nodeNames[nodeID]; ok && name != "" {
		return name
	}
	return nodeID
}

func formatUint(n uint64) string {
	return strconv.FormatUint(n, 10)
}

func protoSegmentBytes(n uint64) int64 {
	return int64(n) //nolint:gosec // segment staging byte totals are bounded on-disk sizes
}

func formatProtoTime(ts *timestamppb.Timestamp) string {
	if ts == nil || !ts.IsValid() {
		return ""
	}
	return ts.AsTime().UTC().Format(tsFormat)
}
