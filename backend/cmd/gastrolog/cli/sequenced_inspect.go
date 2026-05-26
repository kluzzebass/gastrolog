package cli

import (
	"fmt"
	"strconv"
	"strings"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

const sequencedTSFormat = "2006-01-02 15:04:05 UTC"

// formatSequencedWatermarkLines renders local sequenced diagnostics for inspect output.
func formatSequencedWatermarkLines(d *v1.GetSequencedVaultDiagnosticsResponse) []string {
	if d == nil {
		return nil
	}
	lines := []string{
		"  Node: " + d.NodeId,
		"  S_r (spool):           " + formatSeq(d.SpoolWatermark),
		"  H (ingest):            " + formatSeq(d.IngestHighWatermark),
		"  F_n (fence):           " + formatSeq(d.FenceHighWatermark),
		"  M_r (materialized):    " + formatSeq(d.MaterializationWatermark),
		"  C_r (converged):       " + formatSeq(d.ConvergenceWatermark),
	}
	return lines
}

func formatSeq(v uint64) string {
	if v == 0 {
		return "0"
	}
	return strconv.FormatUint(v, 10)
}

// formatSeqAllocatorLines renders allocator lease state for inspect output.
func formatSeqAllocatorLines(alloc *v1.SeqAllocatorDiagnostics) []string {
	if alloc == nil {
		return []string{"  (no allocator state)"}
	}
	lines := []string{
		"  Next seq: " + formatSeq(alloc.NextSeq),
		"  Epoch:    " + formatSeq(alloc.Epoch),
	}
	if len(alloc.ActiveSwaths) == 0 {
		lines = append(lines, "  Active swaths: none")
	} else {
		lines = append(lines, "  Active swaths:")
		for _, sw := range alloc.ActiveSwaths {
			lines = append(lines, fmt.Sprintf("    holder=%s epoch=%s range=%s-%s",
				sw.HolderId, formatSeq(sw.Epoch), formatSeq(sw.RangeStart), formatSeq(sw.RangeEnd)))
		}
	}
	if len(alloc.BurnedTails) == 0 {
		lines = append(lines, "  Burned tails: none")
	} else {
		lines = append(lines, "  Burned tails:")
		for _, tail := range alloc.BurnedTails {
			lines = append(lines, fmt.Sprintf("    %s-%s (epoch %s)",
				formatSeq(tail.Start), formatSeq(tail.End), formatSeq(tail.Epoch)))
		}
	}
	return lines
}

// formatFenceLines renders durable fence history for inspect output.
func formatFenceLines(fences []*v1.FenceRecordDiagnostics) []string {
	if len(fences) == 0 {
		return []string{"  (no fences published)"}
	}
	lines := make([]string, 0, len(fences))
	for _, f := range fences {
		created := "—"
		if f.CreatedAt != nil {
			created = f.CreatedAt.AsTime().UTC().Format(sequencedTSFormat)
		}
		lines = append(lines, fmt.Sprintf("  F_%s upper=%s prev=%s created=%s",
			formatSeq(f.Id), formatSeq(f.UpperBoundSeq), formatSeq(f.PrevBoundSeq), created))
	}
	return lines
}

// formatPeerSequencedWatermarkLines renders per-node replica watermarks from cluster stats.
func formatPeerSequencedWatermarkLines(vaultID string, nodes []*v1.ClusterNode, nodeNames map[string]string) []string {
	if len(nodes) == 0 {
		return nil
	}
	var lines []string
	for _, n := range nodes {
		if n.Stats == nil {
			continue
		}
		nodeID := glid.FromBytes(n.Id).String()
		name := nodeNames[nodeID]
		if name == "" {
			name = n.Name
		}
		if name == "" {
			name = nodeID
		}
		for _, vs := range n.Stats.Vaults {
			if glid.FromBytes(vs.Id).String() != vaultID {
				continue
			}
			if vs.IngestHighWatermark == 0 && vs.SpoolWatermark == 0 &&
				vs.FenceHighWatermark == 0 && vs.MaterializationWatermark == 0 &&
				vs.ConvergenceWatermark == 0 {
				continue
			}
			lines = append(lines, fmt.Sprintf("    %s: H=%s S_r=%s F_n=%s M_r=%s C_r=%s",
				name,
				formatSeq(vs.IngestHighWatermark),
				formatSeq(vs.SpoolWatermark),
				formatSeq(vs.FenceHighWatermark),
				formatSeq(vs.MaterializationWatermark),
				formatSeq(vs.ConvergenceWatermark),
			))
		}
	}
	return lines
}

// formatClusterSequencedVaultSummary renders one vault's cluster watermark summary.
func formatClusterSequencedVaultSummary(vaultName, vaultID string, nodes []*v1.ClusterNode, nodeNames map[string]string) string {
	lines := formatPeerSequencedWatermarkLines(vaultID, nodes, nodeNames)
	if len(lines) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  %s (%s):\n", vaultName, vaultID)
	for _, line := range lines {
		fmt.Fprintln(&b, line)
	}
	return strings.TrimRight(b.String(), "\n")
}
