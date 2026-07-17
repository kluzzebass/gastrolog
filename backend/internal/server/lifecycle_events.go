package server

// Event journal RPC (gastrolog-1m3e0d): ListEvents serves the merged
// cluster-wide event journal from ANY node. The journal is per-node
// in-memory state (an alert.EventJournal ring), so the serving node reads
// its own journal and fans a local_only ListEvents leg out to every other
// cluster node via ForwardRPC, merging chronologically. An unreachable
// node is NAMED in the response (unreachable_nodes) rather than silently
// elided — an empty stretch of history from a node the serving node could
// not reach is unknown state, not quiet history (Data Integrity: facts
// before speculation).

import (
	"context"
	"errors"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/alert"
)

// SetEventJournal wires the local event journal into the lifecycle server.
// Nil disables the ListEvents RPC (some tests); production always wires it.
// Remote collection reuses the alarm forwarder from SetAlarmLifecycle.
func (s *LifecycleServer) SetEventJournal(events *alert.EventJournal) {
	s.events = events
}

// defaultListEventsLimit caps the merged response when the request leaves
// limit unset. Big enough to cover any UI page and CLI table; small enough
// that a many-node cluster with full 10k rings does not ship megabytes per
// refresh.
const defaultListEventsLimit = 1000

// eventFilter is the per-node filter evaluated against journal entries.
// Forwarded legs carry the same request, so every node filters identically
// and the merge only sorts and truncates.
type eventFilter struct {
	typ    string
	source string
	since  *timestamppb.Timestamp
	until  *timestamppb.Timestamp
}

func (f eventFilter) matches(e alert.Event) bool {
	if f.typ != "" && e.Type != f.typ {
		return false
	}
	if f.source != "" && e.Source != f.source {
		return false
	}
	if f.since != nil && e.Time.Before(f.since.AsTime()) {
		return false
	}
	if f.until != nil && e.Time.After(f.until.AsTime()) {
		return false
	}
	return true
}

// localEvents converts this node's filtered journal to wire events,
// stamped with the local node ID.
func (s *LifecycleServer) localEvents(f eventFilter) []*apiv1.SystemEvent {
	var out []*apiv1.SystemEvent
	for _, e := range s.events.Events() {
		if !f.matches(e) {
			continue
		}
		out = append(out, &apiv1.SystemEvent{
			NodeId:  []byte(s.nodeID),
			Time:    timestamppb.New(e.Time),
			Seq:     e.Seq,
			Type:    e.Type,
			Source:  e.Source,
			AlarmId: []byte(e.AlarmID),
			Detail:  e.Detail,
			By:      e.By,
		})
	}
	return out
}

// ListEvents returns the merged cluster-wide event journal. Cluster-first:
// any node serves the full view; see the file comment.
func (s *LifecycleServer) ListEvents(
	ctx context.Context,
	req *connect.Request[apiv1.ListEventsRequest],
) (*connect.Response[apiv1.ListEventsResponse], error) {
	if s.events == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("event journal not available on this node"))
	}
	f := eventFilter{
		typ:    req.Msg.Type,
		source: req.Msg.Source,
		since:  req.Msg.Since,
		until:  req.Msg.Until,
	}
	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = defaultListEventsLimit
	}

	events := s.localEvents(f)
	if req.Msg.LocalOnly || s.cluster == nil {
		// Fan-out leg from a serving node, or single-node mode: this
		// journal only. Already chronological (ring order); trim to the
		// newest `limit` so a leg never ships more than the merge keeps.
		return connect.NewResponse(&apiv1.ListEventsResponse{
			Events: trimOldest(events, limit),
		}), nil
	}

	servers, err := s.cluster.Servers()
	if err != nil {
		return nil, errInternal(err)
	}
	var remoteNodes []string
	for _, srv := range servers {
		if srv.ID != s.nodeID {
			remoteNodes = append(remoteNodes, srv.ID)
		}
	}

	var unreachable []string
	if len(remoteNodes) > 0 && s.alarmForwarder == nil {
		// Cluster mode without a forwarder cannot read peer journals;
		// their history is unknown, and the response says so.
		unreachable = remoteNodes
	} else if len(remoteNodes) > 0 {
		fwdReq := proto.CloneOf(req.Msg)
		fwdReq.LocalOnly = true
		fwdReq.Limit = uint32(limit)
		results, ok := peerFanOut(ctx, s.logger, "list events", remoteNodes,
			func(fctx context.Context, nodeID string) (*apiv1.ListEventsResponse, error) {
				resp := &apiv1.ListEventsResponse{}
				if ferr := s.forwardAlarmOp(fctx, nodeID, gastrologv1connect.LifecycleServiceListEventsProcedure, fwdReq, resp); ferr != nil {
					return nil, ferr
				}
				return resp, nil
			})
		for i, nodeID := range remoteNodes {
			if !ok[i] {
				unreachable = append(unreachable, nodeID)
				continue
			}
			events = append(events, results[i].Events...)
		}
	}

	// Merge: chronological across nodes, per-node seq as tiebreaker, node
	// ID as the final stabilizer.
	slices.SortFunc(events, func(a, b *apiv1.SystemEvent) int {
		if c := a.Time.AsTime().Compare(b.Time.AsTime()); c != 0 {
			return c
		}
		if c := strings.Compare(string(a.NodeId), string(b.NodeId)); c != 0 {
			return c
		}
		return int(a.Seq) - int(b.Seq) //nolint:gosec // G115: same-node seqs are close
	})
	events = trimOldest(events, limit)
	s.stampEventNodeNames(ctx, events)

	return connect.NewResponse(&apiv1.ListEventsResponse{
		Events:           events,
		UnreachableNodes: unreachable,
	}), nil
}

// trimOldest keeps the NEWEST n events of a chronologically sorted slice —
// when a limit truncates, recent history survives.
func trimOldest(events []*apiv1.SystemEvent, n int) []*apiv1.SystemEvent {
	if len(events) <= n {
		return events
	}
	return events[len(events)-n:]
}

// stampEventNodeNames resolves display names for the raising nodes, same
// config-first resolution GetClusterStatus uses for ClusterNode.Name. Best
// effort: an unresolvable node keeps an empty name and consumers fall back
// to the ID.
func (s *LifecycleServer) stampEventNodeNames(ctx context.Context, events []*apiv1.SystemEvent) {
	if s.cfgStore == nil || len(events) == 0 {
		return
	}
	nodes, err := s.cfgStore.ListNodes(ctx)
	if err != nil {
		return
	}
	names := make(map[string]string, len(nodes))
	for _, n := range nodes {
		names[n.ID.String()] = n.Name
	}
	for _, e := range events {
		e.NodeName = names[string(e.NodeId)]
	}
}
