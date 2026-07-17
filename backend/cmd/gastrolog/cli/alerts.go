package cli

import (
	"context"
	"errors"
	"fmt"
	"os/user"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// NewAlertsCommand returns the top-level "alerts" command.
func NewAlertsCommand() *cobra.Command {
	return newAlertsCmd()
}

// newAlertsCmd lists the standing system alerts every node includes in its
// NodeStats broadcast (gastrolog-33d9n2). Alarms are state, not events: they
// stand while a condition holds and clear when it resolves, so this surface
// stays readable even when a suspended system writes no logs. Any node can
// answer — alerts ride the same PeerState aggregation GetClusterStatus
// already serves, so the view is cluster-wide from whichever node the CLI
// is pointed at.
func newAlertsCmd() *cobra.Command {
	var nodeFilter string
	cmd := &cobra.Command{
		Use:   "alerts",
		Short: "List standing system alerts across the cluster",
		Long: "List the standing system alerts raised by every cluster node. " +
			"Alerts are per-node state aggregated via the NodeStats broadcast: " +
			"the NODE column names the node whose component raised each alert, " +
			"because \"disk protect engaged\" on one node means something very " +
			"different from all of them. Works from any node.",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.GetClusterStatus(context.Background(), connect.NewRequest(&v1.GetClusterStatusRequest{}))
			if err != nil {
				return err
			}
			// A typo'd --node must fail loudly, not print a reassuring
			// "no standing alerts" for a node that does not exist —
			// exactly the wrong signal mid-incident.
			if nodeFilter != "" && !anyNodeMatches(resp.Msg.Nodes, nodeFilter) {
				return fmt.Errorf("node %q not found in cluster", nodeFilter)
			}
			alerts := collectNodeAlerts(resp.Msg.Nodes, nodeFilter)
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(alertsToJSON(alerts))
			}
			if len(alerts) == 0 {
				if nodeFilter != "" {
					fmt.Printf("No standing alerts on node %s.\n", nodeFilter)
				} else {
					fmt.Println("No standing alerts.")
				}
			} else {
				p := newPrinter(outputFormat(cmd))
				var rows [][]string
				for _, a := range alerts {
					rows = append(rows, []string{
						a.node,
						string(a.alert.Id),
						alarmStateStr(a.alert),
						alarmPriorityStr(a.alert),
						a.alert.Source,
						a.alert.Detail,
						formatAlertTS(a.alert.FirstSeen.AsTime()),
					})
				}
				p.table([]string{"NODE", "ID", "STATE", "PRIORITY", "SOURCE", "DETAIL", "FIRST SEEN"}, rows)
				printAlarmResponses(alerts)
			}
			// Absence of stats is not absence of alarms: a node that has
			// not broadcast NodeStats has UNKNOWN alert state, and saying
			// nothing would present that as healthy (Data Integrity:
			// facts before speculation).
			if missing := nodesWithoutStats(resp.Msg.Nodes, nodeFilter); len(missing) > 0 {
				fmt.Printf("note: no stats from node(s) %s — alert state unknown\n", strings.Join(missing, ", "))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&nodeFilter, "node", "", "only show alerts raised on this node (name or ID)")
	cmd.AddCommand(newAlertsAckCmd(), newAlertsShelveCmd(), newAlertsUnshelveCmd())
	return cmd
}

// operatorName is the identity recorded on ack/shelve from the CLI. The
// local Unix-socket path has no authenticated user, so the OS username is
// the most honest identity available; the server prefers an authenticated
// user's name when one exists (TCP + JWT).
func operatorName() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "operator"
}

// newAlertsAckCmd acknowledges a standing alarm by its full ID. Works from
// any node: the server resolves every raiser of the ID cluster-wide and
// fans the ack out to each — the same semantics as the inspector's Ack
// control.
func newAlertsAckCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ack <alarm-id>",
		Short: "Acknowledge a standing alarm (applies on every raising node)",
		Long: "Acknowledge the standing alarm with the given ID (as shown by `gastrolog alerts`, " +
			"e.g. \"disk-space-exhausted:vault1\"). Recorded with your identity on every node " +
			"raising the ID. A latched alarm whose condition has resolved is released by the ack; " +
			"a cleared-unacknowledged alarm is released likewise.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.AckAlarm(context.Background(), connect.NewRequest(&v1.AckAlarmRequest{
				AlarmId: []byte(args[0]),
				AckedBy: operatorName(),
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Acknowledged %s on %d node(s).\n", args[0], resp.Msg.Applied)
			return nil
		},
	}
}

// newAlertsShelveCmd shelves a standing alarm for a mandatory duration.
func newAlertsShelveCmd() *cobra.Command {
	var forDur time.Duration
	cmd := &cobra.Command{
		Use:   "shelve <alarm-id> --for <duration>",
		Short: "Shelve a standing alarm for a duration (expiry is mandatory)",
		Long: "Suppress the standing alarm with the given ID until the duration elapses. " +
			"Shelves always expire — there are no permanent shelves — and expiry with the " +
			"condition still true returns the alarm to the active list. Types where deferral " +
			"is meaningless (software faults, alarm-flood) refuse shelving.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if forDur <= 0 {
				return errors.New("--for must be a positive duration (e.g. --for 2h) — shelves always expire")
			}
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.ShelveAlarm(context.Background(), connect.NewRequest(&v1.ShelveAlarmRequest{
				AlarmId:         []byte(args[0]),
				DurationSeconds: int64(forDur / time.Second),
				ShelvedBy:       operatorName(),
			}))
			if err != nil {
				return err
			}
			until := ""
			if resp.Msg.ShelvedUntil != nil {
				until = " until " + formatAlertTS(resp.Msg.ShelvedUntil.AsTime())
			}
			fmt.Printf("Shelved %s on %d node(s)%s.\n", args[0], resp.Msg.Applied, until)
			return nil
		},
	}
	cmd.Flags().DurationVar(&forDur, "for", 0, "shelve duration (required, e.g. 30m, 2h)")
	_ = cmd.MarkFlagRequired("for")
	return cmd
}

// newAlertsUnshelveCmd ends a shelve early.
func newAlertsUnshelveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unshelve <alarm-id>",
		Short: "End a shelve early, returning the alarm to the active list",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.UnshelveAlarm(context.Background(), connect.NewRequest(&v1.UnshelveAlarmRequest{
				AlarmId: []byte(args[0]),
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Unshelved %s on %d node(s).\n", args[0], resp.Msg.Applied)
			return nil
		},
	}
}

// nodeAlert pairs a standing alert with the node that raised it. Alerts are
// raised per-node and aggregated cluster-wide, and which node raised an
// alert changes what it means — so attribution travels with the alert
// through every renderer.
type nodeAlert struct {
	node   string // display name of the raising node
	nodeID string
	alert  *v1.SystemAlert
}

// collectNodeAlerts flattens the per-node alert lists from a cluster status
// response into attributed rows, optionally filtered to one node (matched
// case-insensitively against the node's configured name, broadcast name, or
// ID). Sorted by FirstSeen across nodes — same order as the inspector's
// System Alerts panel — with node name as tiebreaker so output is stable.
func collectNodeAlerts(nodes []*v1.ClusterNode, nodeFilter string) []nodeAlert {
	var out []nodeAlert
	for _, n := range nodes {
		if n == nil || n.Stats == nil || !nodeMatchesFilter(n, nodeFilter) {
			continue
		}
		name := alertNodeName(n)
		for _, a := range n.Stats.Alerts {
			if a == nil {
				continue
			}
			out = append(out, nodeAlert{node: name, nodeID: formatIDBytes(n.Id), alert: a})
		}
	}
	slices.SortFunc(out, func(a, b nodeAlert) int {
		if c := a.alert.FirstSeen.AsTime().Compare(b.alert.FirstSeen.AsTime()); c != 0 {
			return c
		}
		return strings.Compare(a.node, b.node)
	})
	return out
}

// alertNodeName picks the display name for a node in alert output:
// configured name, else the self-reported name from its broadcast, else its
// ID. Attribution must never be blank — an unattributed alarm is exactly
// the ambiguity this surface exists to remove.
func alertNodeName(n *v1.ClusterNode) string {
	if n.Name != "" {
		return n.Name
	}
	if s := n.Stats.GetNodeName(); s != "" {
		return s
	}
	return formatIDBytes(n.Id)
}

// anyNodeMatches reports whether any cluster node matches the --node
// filter, regardless of whether it has alerts or stats.
func anyNodeMatches(nodes []*v1.ClusterNode, filter string) bool {
	for _, n := range nodes {
		if n != nil && nodeMatchesFilter(n, filter) {
			return true
		}
	}
	return false
}

// nodesWithoutStats returns display names of matching cluster nodes that
// have no broadcast stats. Their alert state is unknown — an offline node
// must never silently read as "no alerts".
func nodesWithoutStats(nodes []*v1.ClusterNode, filter string) []string {
	var out []string
	for _, n := range nodes {
		if n != nil && n.Stats == nil && nodeMatchesFilter(n, filter) {
			out = append(out, alertNodeName(n))
		}
	}
	return out
}

// nodeMatchesFilter reports whether n matches the --node filter value.
// Empty filter matches everything.
func nodeMatchesFilter(n *v1.ClusterNode, filter string) bool {
	if filter == "" {
		return true
	}
	return strings.EqualFold(filter, n.Name) ||
		strings.EqualFold(filter, n.Stats.GetNodeName()) ||
		strings.EqualFold(filter, formatIDBytes(n.Id))
}

// alarmStateStr is the single lifecycle-state→display mapping for the CLI:
// active (unacked), acked, cleared (unacked), shelved. Unspecified renders
// as active for wire back-compat with pre-lifecycle broadcasts.
func alarmStateStr(a *v1.SystemAlert) string {
	switch a.State {
	case v1.AlarmState_ALARM_STATE_ACTIVE_ACKED:
		if a.AckedBy != "" {
			return "acked:" + a.AckedBy
		}
		return "acked"
	case v1.AlarmState_ALARM_STATE_CLEARED_UNACKED:
		return "cleared"
	case v1.AlarmState_ALARM_STATE_SHELVED:
		if a.ShelvedUntil != nil {
			return "shelved→" + formatAlertTS(a.ShelvedUntil.AsTime())
		}
		return "shelved"
	case v1.AlarmState_ALARM_STATE_ACTIVE_UNACKED, v1.AlarmState_ALARM_STATE_UNSPECIFIED:
		return "active"
	default:
		return "active"
	}
}

// alarmPriorityStr is the single priority→display mapping for the CLI.
// Software faults sit outside the consequence×urgency scale (their proto
// priority is UNSPECIFIED) and render as FAULT — same ranking the UI uses,
// where a fault outranks Critical because it means the system itself is
// defective, not merely a process condition.
func alarmPriorityStr(a *v1.SystemAlert) string {
	if a.SoftwareFault {
		return "FAULT"
	}
	return strings.TrimPrefix(a.Priority.String(), "ALARM_PRIORITY_")
}

// printAlarmResponses prints the catalog response text for the alarm types
// present, deduplicated by type: response text is a property of the type,
// not the instance, so 200 chunk-unreadable alarms get one guidance line.
// The CLI is where an operator lands when the UI is unreachable, which is
// why the response text renders here by default rather than behind a flag.
func printAlarmResponses(alerts []nodeAlert) {
	lines := alarmResponseLines(alerts)
	if len(lines) == 0 {
		return
	}
	fmt.Println()
	fmt.Println("RESPONSE")
	for _, l := range lines {
		fmt.Println(l)
	}
}

// alarmResponseLines builds the deduplicated guidance lines, one per alarm
// type with a non-empty response, in first-seen order.
func alarmResponseLines(alerts []nodeAlert) []string {
	seen := make(map[string]bool)
	var lines []string
	for _, a := range alerts {
		typeID := alarmTypeID(string(a.alert.Id))
		if a.alert.Response == "" || seen[typeID] {
			continue
		}
		seen[typeID] = true
		lines = append(lines, fmt.Sprintf("  %s: %s", typeID, a.alert.Response))
	}
	return lines
}

// alarmTypeID extracts the catalog type ID from a full alarm ID
// ("disk-space-exhausted:vault1" → "disk-space-exhausted").
func alarmTypeID(id string) string {
	typeID, _, _ := strings.Cut(id, ":")
	return typeID
}

// formatAlertTS renders an alert timestamp for table output.
func formatAlertTS(t time.Time) string {
	return t.UTC().Format(tsFormat)
}

// alertJSON is the -o json shape for one attributed alert. A plain struct
// rather than the raw proto because attribution (node name + ID) is not a
// field on SystemAlert — it comes from the ClusterNode carrying it.
type alertJSON struct {
	Node          string `json:"node"`
	NodeID        string `json:"node_id"`
	ID            string `json:"id"`
	State         string `json:"state"`
	Priority      string `json:"priority"`
	SoftwareFault bool   `json:"software_fault"`
	Shelveable    bool   `json:"shelveable"`
	Source        string `json:"source"`
	Detail        string `json:"detail"`
	Cause         string `json:"cause"`
	Response      string `json:"response"`
	FirstSeen     string `json:"first_seen"`
	LastSeen      string `json:"last_seen"`
	Occurrences   uint32 `json:"occurrences"`
	AckedBy       string `json:"acked_by,omitempty"`
	AckedAt       string `json:"acked_at,omitempty"`
	ShelvedUntil  string `json:"shelved_until,omitempty"`
}

func alertsToJSON(alerts []nodeAlert) []alertJSON {
	out := make([]alertJSON, 0, len(alerts))
	for _, a := range alerts {
		j := alertJSON{
			Node:          a.node,
			NodeID:        a.nodeID,
			ID:            string(a.alert.Id),
			State:         strings.ToLower(strings.TrimPrefix(a.alert.State.String(), "ALARM_STATE_")),
			Priority:      alarmPriorityStr(a.alert),
			SoftwareFault: a.alert.SoftwareFault,
			Shelveable:    a.alert.Shelveable,
			Source:        a.alert.Source,
			Detail:        a.alert.Detail,
			Cause:         a.alert.Cause,
			Response:      a.alert.Response,
			FirstSeen:     a.alert.FirstSeen.AsTime().UTC().Format(time.RFC3339),
			LastSeen:      a.alert.LastSeen.AsTime().UTC().Format(time.RFC3339),
			Occurrences:   a.alert.Occurrences,
			AckedBy:       a.alert.AckedBy,
		}
		if a.alert.AckedAt != nil {
			j.AckedAt = a.alert.AckedAt.AsTime().UTC().Format(time.RFC3339)
		}
		if a.alert.ShelvedUntil != nil {
			j.ShelvedUntil = a.alert.ShelvedUntil.AsTime().UTC().Format(time.RFC3339)
		}
		out = append(out, j)
	}
	return out
}

// systemAlertRows builds the standing-alert table for `cluster status`
// (gastrolog-33d9n2) — parity with the inspector's System Alerts panel.
// Alerts arrive in the same NodeStats the liveness tables already read, so
// this is rendering data in hand, not new plumbing. No standing alerts →
// no table: quiet until needed.
func systemAlertRows(nodes []*v1.ClusterNode) [][]string {
	var rows [][]string
	for _, a := range collectNodeAlerts(nodes, "") {
		rows = append(rows, []string{
			a.node,
			alarmStateStr(a.alert),
			alarmPriorityStr(a.alert),
			a.alert.Source,
			a.alert.Detail,
			formatAlertTS(a.alert.FirstSeen.AsTime()),
		})
	}
	return rows
}
