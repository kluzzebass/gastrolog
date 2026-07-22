package cli

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/server"
	"gastrolog/internal/units"
)

// newInspectStorageCmd is the CLI parity surface for the storage inspector
// (gastrolog-3cobq4): the same published guard state the UI's storage cards
// render — free/total, effective thresholds with default-vs-explicit
// indication, warn/protect verdicts, and placed vaults — never re-derived
// from raw numbers here (operator directive, gastrolog-9akebz: render the
// wire).
func newInspectStorageCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "storage [name-or-id]",
		Short: "Show storage disk-guard state: free/total, thresholds, verdicts, placed vaults",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runInspectStorage,
	}
}

func runInspectStorage(cmd *cobra.Command, args []string) error {
	client := clientFromCmd(cmd)
	ctx := context.Background()
	resp, err := client.System.ListStorages(ctx, connect.NewRequest(&v1.ListStoragesRequest{}))
	if err != nil {
		return err
	}
	storages := resp.Msg.Storages

	if len(args) == 1 {
		return printOneStorage(cmd, ctx, client, storages, args[0])
	}

	if outputFormat(cmd) == "json" {
		return newPrinter("json").json(storages)
	}
	var rows [][]string
	for _, st := range storages {
		rows = append(rows, storageRow(st))
	}
	newPrinter(outputFormat(cmd)).table(
		[]string{"NAME", "NODE", "VERDICT", "FREE", "TOTAL", "WARN", "FLOOR", "PLACED VAULTS"}, rows)
	return nil
}

func printOneStorage(cmd *cobra.Command, ctx context.Context, client *server.Client, storages []*v1.StorageState, nameOrID string) error {
	r, err := newResolver(ctx, client)
	if err != nil {
		return err
	}
	id, err := resolve(nameOrID, r.storages, "storage")
	if err != nil {
		return err
	}
	for _, st := range storages {
		if glid.FromBytes(st.Id).String() != id {
			continue
		}
		if outputFormat(cmd) == "json" {
			return newPrinter("json").json(st)
		}
		newPrinter(outputFormat(cmd)).kv(storageDetailPairs(st, vaultNameLookup(ctx, client)))
		return nil
	}
	return fmt.Errorf("storage %q not found", nameOrID)
}

// storageVerdict renders the storage's badge grammar as text (consistent
// with the vault card's badge grammar, gastrolog-3cobq4): "no sample" when
// the owning node hasn't statfs'd it yet (SampledAt unset — the honest
// "facts before speculation" fallback, gastrolog-9akebz), else "protected"
// when the admission gate is engaged, "warn" when below the warn threshold
// but not yet floor, "ok" otherwise. Both verdict booleans are
// server-computed hysteresis-aware verdicts, never re-derived from
// free/warn/floor here.
func storageVerdict(st *v1.StorageState) string {
	switch {
	case st.SampledAt == nil:
		return "no sample"
	case st.ProtectVerdict:
		return "protected"
	case st.WarnVerdict:
		return "warn"
	default:
		return "ok"
	}
}

// freeTotalLabel renders the free/total pair, or "—" for both when no
// statfs sample has landed yet — "0 B / 0 B" would read as a full disk,
// not as "unmeasured" (gastrolog-3cobq4 review).
func freeTotalLabel(st *v1.StorageState) (free, total string) {
	if st.SampledAt == nil {
		return "—", "—"
	}
	return formatStorageBytes(st.FreeBytes), formatStorageBytes(st.TotalBytes)
}

func storageRow(st *v1.StorageState) []string {
	free, total := freeTotalLabel(st)
	return []string{
		st.Name,
		st.NodeName,
		storageVerdict(st),
		free,
		total,
		thresholdLabel(st.WarnExpr, st.WarnIsDefault, st.WarnBytes),
		thresholdLabel(st.FloorExpr, st.FloorIsDefault, st.FloorBytes),
		strconv.Itoa(len(st.PlacedVaultIds)),
	}
}

// thresholdLabel renders an effective threshold with its provenance — the
// resolved bytes value always leads (placeholder-style: the effective
// value is what matters). expr is the EFFECTIVE expression from the wire,
// verbatim, never re-derived here (gastrolog-9akebz: render the wire).
//
// isDefault storages get "(expr, default)" — there is no configurable
// node-level override to inherit from (gastrolog-2mrfdw removed the env
// channel), so an unset expression is DEFAULTED, never "inherited"
// (gastrolog-3cobq4 review). An explicit percentage expression ("10%")
// still gets "(expr)": a percentage carries information the resolved byte
// count alone can't (it rescales with the volume). An explicit
// absolute-size expression ("20GiB") resolves to exactly the shown byte
// count, so appending it would just repeat the same number in a second
// spelling — the bytes alone are the complete, non-redundant answer.
func thresholdLabel(expr string, isDefault bool, effectiveBytes uint64) string {
	eff := formatStorageBytes(effectiveBytes)
	switch {
	case isDefault && expr == "":
		// A peer running a build predating the effective-expression field
		// sends an empty expr; "(default)" alone beats "(, default)".
		return eff + " (default)"
	case isDefault:
		return eff + " (" + expr + ", default)"
	case strings.Contains(expr, "%"):
		return eff + " (" + expr + ")"
	default:
		return eff
	}
}

// formatStorageBytes renders a disk-guard byte count. Disk free/total
// values are always within int64 (physical volumes), same contract as
// orchestrator's fmtBytes.
func formatStorageBytes(b uint64) string {
	return units.FormatBytesDisplay(int64(b)) //nolint:gosec // disk bytes, display only
}

func storageDetailPairs(st *v1.StorageState, vaultNames map[string]string) [][2]string {
	free, total := freeTotalLabel(st)
	pairs := [][2]string{
		{"ID", glid.FromBytes(st.Id).String()},
		{"Name", st.Name},
		{"Node", st.NodeName},
		{"Path", st.Path},
		{"Storage Class", strconv.FormatUint(uint64(st.StorageClass), 10)},
		{"Verdict", storageVerdict(st)},
		{"Free / Total", free + " / " + total},
		{"Warn Threshold", thresholdLabel(st.WarnExpr, st.WarnIsDefault, st.WarnBytes)},
		{"Floor Threshold", thresholdLabel(st.FloorExpr, st.FloorIsDefault, st.FloorBytes)},
	}
	if st.SampledAt != nil {
		pairs = append(pairs, [2]string{"Sampled At", st.SampledAt.AsTime().UTC().Format(tsFormat)})
	}
	pairs = append(pairs, [2]string{"Placed Vaults", renderPlacedVaults(st.PlacedVaultIds, vaultNames)})
	return pairs
}

// renderPlacedVaults formats a storage's config-derived placed-vault list
// as readable names, falling back to the raw ID for a vault this node's
// config lookup doesn't (yet) know — same "—" empty convention as
// renderReplicaResidency.
func renderPlacedVaults(vaultIDs [][]byte, vaultNames map[string]string) string {
	if len(vaultIDs) == 0 {
		return "—"
	}
	names := make([]string, 0, len(vaultIDs))
	for _, vid := range vaultIDs {
		id := glid.FromBytes(vid).String()
		if n, ok := vaultNames[id]; ok && n != "" {
			names = append(names, n)
		} else {
			names = append(names, id)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// vaultNameLookup builds an ID -> name map from the system config, for
// resolving a storage's placed-vault IDs to readable names.
func vaultNameLookup(ctx context.Context, client *server.Client) map[string]string {
	m := make(map[string]string)
	resp, err := client.System.GetSystem(ctx, connect.NewRequest(&v1.GetSystemRequest{}))
	if err != nil {
		return m
	}
	for _, v := range resp.Msg.Vaults {
		m[glid.FromBytes(v.Id).String()] = v.Name
	}
	return m
}
