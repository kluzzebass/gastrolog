package server

import (
	"context"
	"fmt"
	"maps"
	"net"
	"sort"
	"strings"
	"sync"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// Ingester validation is per-node, so it has to be asked of every node the
// ingester will run on.
//
// Two callers share this: the settings UI's live per-keystroke check
// (TestIngester) and save-time validation (PutIngester -> validateIngester).
// They share it deliberately. The regression this replaces came from those two
// drifting apart — multi-node assignment landed, the client-side conflict check
// was generalized to node sets, and both server-side trial binds were left
// asking whichever single node received the call.

// LocalIngesterCheck runs this node's own verdict on a candidate ingester
// config: the factory must construct, and for listener types the listen
// addresses must be bindable here.
//
// Returns (ok, message) rather than an error because a negative verdict is a
// fact about this node, not a failure of the call. Exported for wiring as the
// cluster layer's ValidateIngesterExecutor.
func (s *SystemServer) LocalIngesterCheck(_ context.Context, ingType string, params map[string]string, rawID []byte) (bool, string) {
	reg, ok := s.factories.IngesterTypes[ingType]
	if !ok {
		return false, fmt.Sprintf("unknown ingester type %q", ingType)
	}
	if _, err := reg.Factory(ingesterCheckID(rawID), s.withStateDir(params), s.factories.Logger); err != nil {
		return false, err.Error()
	}
	if reg.ListenAddrs == nil {
		return true, "ok"
	}
	// A running ingester legitimately holds its own ports, so re-binding them
	// would report a conflict with itself. This lookup is node-local, which is
	// why it can only be applied by the node actually running it.
	if id, err := parseProtoID(rawID); err == nil && s.orch.GetIngesterStats(id) != nil {
		return true, "ports held by running ingester"
	}
	if err := checkListenAddrs(reg.ListenAddrs(params)); err != nil {
		return false, err.Error()
	}
	return true, "listen addresses available"
}

// withStateDir adds the state-dir param the factories need, without mutating
// the caller's map.
func (s *SystemServer) withStateDir(params map[string]string) map[string]string {
	if s.factories.HomeDir == "" {
		return params
	}
	out := make(map[string]string, len(params)+1)
	maps.Copy(out, params)
	out["_state_dir"] = s.factories.HomeDir
	return out
}

func ingesterCheckID(rawID []byte) glid.GLID {
	if id, err := parseProtoID(rawID); err == nil {
		return id
	}
	return glid.New()
}

// ingesterCheckTargets resolves which nodes must answer for a candidate
// config. AllNodes wins over NodeIDs — every node is then eligible regardless
// of what NodeIDs says — matching how the orchestrator decides where to run it.
//
// An empty result means "just this node": the single-node case, and a form the
// operator has not assigned yet.
func (s *SystemServer) ingesterCheckTargets(ctx context.Context, nodeIDs []string, allNodes bool) []string {
	if allNodes {
		if s.sysStore == nil {
			return []string{s.localNodeID}
		}
		nodes, err := s.sysStore.ListNodes(ctx)
		if err != nil {
			// Better to check the one node we can than to skip the check.
			return []string{s.localNodeID}
		}
		var out []string
		for _, n := range nodes {
			// Nodes the operator has taken out of service cannot be expected to
			// answer, and their verdict would not gate anything: the ingester
			// will not start there until they are back, at which point the
			// save-time check runs again.
			switch n.EffectiveState() {
			case system.NodeStateUnreachable, system.NodeStateDecommissioning:
				continue
			case system.NodeStateUnknown, system.NodeStateLive,
				system.NodeStateMaintenance, system.NodeStateDraining:
				out = append(out, n.ID.String())
			}
		}
		sort.Strings(out)
		return out
	}
	if len(nodeIDs) == 0 {
		return []string{s.localNodeID}
	}
	out := append([]string(nil), nodeIDs...)
	sort.Strings(out)
	return out
}

// hostByNode maps node ID to the host part of its Raft address. Two nodes
// sharing a host cannot both hold a listen address, whatever either of them
// reports on its own.
//
// Empty when there is no cluster topology to read, which is single-node mode —
// where nothing can be co-located anyway.
func (s *SystemServer) hostByNode() map[string]string {
	if s.clusterTopology == nil {
		return nil
	}
	servers, err := s.clusterTopology.Servers()
	if err != nil {
		return nil
	}
	out := make(map[string]string, len(servers))
	for _, srv := range servers {
		host, _, splitErr := net.SplitHostPort(srv.Address)
		if splitErr != nil {
			host = srv.Address
		}
		out[srv.ID] = host
	}
	return out
}

// coLocatedListenerChecks decides, without probing, the nodes that cannot get
// the address because they share a host with another assigned node.
//
// This has to be settled up front rather than discovered by binding. The probes
// run concurrently, so co-located nodes collide with EACH OTHER: one wins the
// race and the losers report "address already in use" against a holder that is
// this very check. The verdict would be right — one host holds an address once
// — but it would blame an arbitrary node, and differently on each run.
//
// Probing them sequentially instead would be worse and deterministically wrong:
// each node would bind, release, and report the address available, and the
// conflict would surface at runtime after the operator saved a config that
// cannot work.
//
// Returns the settled checks plus the targets still worth asking.
func (s *SystemServer) coLocatedListenerChecks(
	ingType string, targets []string, nodeName func(string) string,
) (settled []*apiv1.IngesterNodeCheck, remaining []string) {
	reg, ok := s.factories.IngesterTypes[ingType]
	if !ok || reg.ListenAddrs == nil || len(targets) < 2 {
		return nil, targets
	}
	hosts := s.hostByNode()
	if len(hosts) == 0 {
		return nil, targets
	}

	byHost := map[string][]string{}
	for _, id := range targets {
		host, known := hosts[id]
		if !known {
			// No address for it: cannot reason about co-location, so let the
			// probe answer.
			byHost[""] = append(byHost[""], id)
			continue
		}
		byHost[host] = append(byHost[host], id)
	}

	for host, ids := range byHost {
		if host == "" || len(ids) < 2 {
			remaining = append(remaining, ids...)
			continue
		}
		names := make([]string, len(ids))
		for i, id := range ids {
			names[i] = nodeName(id)
		}
		sort.Strings(names)
		msg := fmt.Sprintf("shares host %s with %s; a listen address can only be held by one node",
			host, strings.Join(names, ", "))
		for _, id := range ids {
			settled = append(settled, &apiv1.IngesterNodeCheck{
				NodeId: []byte(id), Success: false, Message: msg,
			})
		}
	}
	sort.Strings(remaining)
	return settled, remaining
}

// checkIngesterOnNodes asks every target node for its verdict, concurrently.
// The local node answers in-process; the rest are asked over the cluster
// forward.
//
// A node that cannot be reached is reported as unreachable rather than as a
// failing check. Those are different operator actions — fix the config versus
// fix the node — and collapsing them would blame a config for a network fault.
func (s *SystemServer) checkIngesterOnNodes(
	ctx context.Context, ingType string, params map[string]string, rawID []byte, targets []string,
) []*apiv1.IngesterNodeCheck {
	settled, targets := s.coLocatedListenerChecks(ingType, targets, s.ingesterNodeLabel(ctx))

	results := make([]*apiv1.IngesterNodeCheck, len(targets))
	var wg sync.WaitGroup
	for i, nodeID := range targets {
		if nodeID == s.localNodeID {
			ok, msg := s.LocalIngesterCheck(ctx, ingType, params, rawID)
			results[i] = &apiv1.IngesterNodeCheck{
				NodeId: []byte(nodeID), Success: ok, Message: msg,
			}
			continue
		}
		if s.remoteIngesterCheck == nil {
			// Single-node build talking about a peer it has no way to ask. Say
			// so; do not report success.
			results[i] = &apiv1.IngesterNodeCheck{
				NodeId:      []byte(nodeID),
				Message:     "no cluster forwarder configured",
				Unreachable: true,
			}
			continue
		}
		wg.Add(1)
		go func(i int, nodeID string) {
			defer wg.Done()
			resp, err := s.remoteIngesterCheck.ValidateIngester(ctx, nodeID,
				&apiv1.ForwardValidateIngesterRequest{Type: ingType, Params: params, Id: rawID})
			if err != nil {
				results[i] = &apiv1.IngesterNodeCheck{
					NodeId: []byte(nodeID), Message: err.Error(), Unreachable: true,
				}
				return
			}
			results[i] = &apiv1.IngesterNodeCheck{
				NodeId: []byte(nodeID), Success: resp.GetSuccess(), Message: resp.GetMessage(),
			}
		}(i, nodeID)
	}
	wg.Wait()
	return append(settled, results...)
}

// summarizeIngesterChecks folds per-node verdicts into the overall answer.
//
// Only a definite negative blocks: a node that did not answer leaves the
// verdict undecided rather than failing it, so an unreachable node cannot stop
// an operator from saving a valid config. The node names stay in the summary
// because "port in use" without a node is unactionable on a cluster.
func summarizeIngesterChecks(checks []*apiv1.IngesterNodeCheck, nodeName func(string) string) (bool, string) {
	var failed, unreachable []string
	for _, c := range checks {
		id := string(c.GetNodeId())
		switch {
		case c.GetUnreachable():
			unreachable = append(unreachable, nodeName(id))
		case !c.GetSuccess():
			failed = append(failed, fmt.Sprintf("%s: %s", nodeName(id), c.GetMessage()))
		}
	}
	if len(failed) > 0 {
		return false, strings.Join(failed, "; ")
	}
	if len(unreachable) > 0 {
		return true, "ok on responding nodes; not checked on " + strings.Join(unreachable, ", ")
	}
	if len(checks) == 1 {
		return true, checks[0].GetMessage()
	}
	return true, fmt.Sprintf("ok on all %d assigned nodes", len(checks))
}

// ingesterNodeLabel resolves a node ID to its configured name for messages,
// falling back to the ID. An operator reading "port in use on node-3" can act;
// a GLID makes them go look it up.
func (s *SystemServer) ingesterNodeLabel(ctx context.Context) func(string) string {
	names := map[string]string{}
	if s.sysStore != nil {
		if nodes, err := s.sysStore.ListNodes(ctx); err == nil {
			for _, n := range nodes {
				if n.Name != "" {
					names[n.ID.String()] = n.Name
				}
			}
		}
	}
	return func(id string) string {
		if n, ok := names[id]; ok {
			return n
		}
		return id
	}
}
