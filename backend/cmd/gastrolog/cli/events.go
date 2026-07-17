package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

// NewEventsCommand returns the top-level "events" command.
func NewEventsCommand() *cobra.Command {
	return newEventsCmd()
}

// newEventsCmd lists the cluster-wide event journal (gastrolog-1m3e0d).
// Events are records of occurrence — alarm lifecycle transitions, demoted
// diagnostics — requiring no operator action; the standing conditions that
// DO require action live in `gastrolog alerts`. Any node can answer: the
// serving node merges its own in-memory ring with a fan-out leg to every
// peer. Journals do not survive restart — each node's history begins with
// a node-started entry at its boot instant, and nodes the serving node
// could not reach are named explicitly.
func newEventsCmd() *cobra.Command {
	var (
		typeFilter   string
		sourceFilter string
		nodeFilter   string
		sinceFlag    string
		untilFlag    string
		limit        uint32
	)
	cmd := &cobra.Command{
		Use:   "events",
		Short: "List the cluster-wide event journal (records of occurrence, not calls to action)",
		Long: "List recent events from every cluster node's journal: alarm lifecycle transitions " +
			"(raised, acked, cleared, shelved, ...) and demoted diagnostics (election storms, WAL " +
			"latency, ingest pressure edges). Events are history, not standing conditions — " +
			"see `gastrolog alerts` for what currently needs action. Journals are per-node and " +
			"in-memory: each begins with a node-started entry at that node's boot, and older " +
			"history is gone by design, not missing.",
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &v1.ListEventsRequest{
				Type:   typeFilter,
				Source: sourceFilter,
				Limit:  limit,
			}
			var err error
			if req.Since, err = parseEventTimeFlag("--since", sinceFlag); err != nil {
				return err
			}
			if req.Until, err = parseEventTimeFlag("--until", untilFlag); err != nil {
				return err
			}
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.ListEvents(context.Background(), connect.NewRequest(req))
			if err != nil {
				return err
			}
			events := resp.Msg.Events
			if nodeFilter != "" {
				events = filterEventsByNode(events, nodeFilter)
			}
			if outputFormat(cmd) == "json" {
				return newPrinter("json").json(eventsToJSON(events, resp.Msg.UnreachableNodes))
			}
			if len(events) == 0 {
				fmt.Println("No events match.")
			} else {
				p := newPrinter(outputFormat(cmd))
				var rows [][]string
				for _, e := range events {
					rows = append(rows, []string{
						formatAlertTS(e.Time.AsTime()),
						eventNodeName(e),
						e.Type,
						e.Source,
						eventDetail(e),
					})
				}
				p.table([]string{"TIME", "NODE", "EVENT", "SOURCE", "DETAIL"}, rows)
			}
			// A node the serving node could not reach contributes NOTHING —
			// silence from it is unknown state, never quiet history.
			if len(resp.Msg.UnreachableNodes) > 0 {
				fmt.Printf("note: no journal from node(s) %s — their events are missing, not absent\n",
					strings.Join(resp.Msg.UnreachableNodes, ", "))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&typeFilter, "type", "", "only this event type (e.g. alarm-raised, alarm-acked, node-started)")
	cmd.Flags().StringVar(&sourceFilter, "source", "", "only events from this component (e.g. storage, raft, placement)")
	cmd.Flags().StringVar(&nodeFilter, "node", "", "only events journaled on this node (name or ID)")
	cmd.Flags().StringVar(&sinceFlag, "since", "", "lower time bound: a duration back from now (e.g. 2h) or RFC3339")
	cmd.Flags().StringVar(&untilFlag, "until", "", "upper time bound: a duration back from now (e.g. 30m) or RFC3339")
	cmd.Flags().Uint32Var(&limit, "limit", 0, "max events after the cluster merge, keeping the newest (0 = server default)")
	return cmd
}

// parseEventTimeFlag accepts either a duration back from now ("2h", "45m")
// or an RFC3339 instant. Empty means unset.
func parseEventTimeFlag(flag, value string) (*timestamppb.Timestamp, error) {
	if value == "" {
		return nil, nil
	}
	if d, err := time.ParseDuration(value); err == nil {
		if d < 0 {
			return nil, fmt.Errorf("%s: duration must be positive (it counts back from now)", flag)
		}
		return timestamppb.New(time.Now().Add(-d)), nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("%s: %q is neither a duration (2h) nor RFC3339 (2026-07-01T08:00:00Z)", flag, value)
	}
	return timestamppb.New(t), nil
}

// filterEventsByNode keeps events journaled on the named node, matched
// case-insensitively against the resolved display name or the node ID.
func filterEventsByNode(events []*v1.SystemEvent, filter string) []*v1.SystemEvent {
	var out []*v1.SystemEvent
	for _, e := range events {
		if strings.EqualFold(filter, e.NodeName) || strings.EqualFold(filter, string(e.NodeId)) {
			out = append(out, e)
		}
	}
	return out
}

// eventNodeName picks the display name for the journaling node: resolved
// name first, raw ID as fallback — attribution must never be blank.
func eventNodeName(e *v1.SystemEvent) string {
	if e.NodeName != "" {
		return e.NodeName
	}
	return formatIDBytes(e.NodeId)
}

// eventDetail renders the detail column, prefixing the alarm ID and the
// operator identity when the event carries them.
func eventDetail(e *v1.SystemEvent) string {
	var parts []string
	if len(e.AlarmId) > 0 {
		parts = append(parts, string(e.AlarmId)+":")
	}
	parts = append(parts, e.Detail)
	if e.By != "" {
		parts = append(parts, "(by "+e.By+")")
	}
	return strings.Join(parts, " ")
}

// eventJSON is the -o json shape for one event plus the response-level
// unreachable list.
type eventJSON struct {
	Node    string `json:"node"`
	NodeID  string `json:"node_id"`
	Time    string `json:"time"`
	Seq     uint64 `json:"seq"`
	Type    string `json:"type"`
	Source  string `json:"source"`
	AlarmID string `json:"alarm_id,omitempty"`
	Detail  string `json:"detail"`
	By      string `json:"by,omitempty"`
}

type eventsJSON struct {
	Events []eventJSON `json:"events"`
	// UnreachableNodes lists nodes whose journals are MISSING from this
	// listing (not reachable at collection time).
	UnreachableNodes []string `json:"unreachable_nodes,omitempty"`
}

func eventsToJSON(events []*v1.SystemEvent, unreachable []string) eventsJSON {
	out := eventsJSON{Events: make([]eventJSON, 0, len(events)), UnreachableNodes: unreachable}
	for _, e := range events {
		out.Events = append(out.Events, eventJSON{
			Node:    eventNodeName(e),
			NodeID:  formatIDBytes(e.NodeId),
			Time:    e.Time.AsTime().UTC().Format(time.RFC3339),
			Seq:     e.Seq,
			Type:    e.Type,
			Source:  e.Source,
			AlarmID: string(e.AlarmId),
			Detail:  e.Detail,
			By:      e.By,
		})
	}
	return out
}
