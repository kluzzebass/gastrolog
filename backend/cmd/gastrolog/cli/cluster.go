package cli

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"os"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func newClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage cluster and server lifecycle",
	}
	cmd.AddCommand(
		newClusterStatusCmd(),
		newClusterThroughputCmd(),
		newClusterHealthCmd(),
		newClusterJoinTokenCmd(),
		newClusterShutdownCmd(),
		newClusterRemoveNodeCmd(),
		newClusterDemoteSelfCmd(),
		newClusterYieldLeadershipCmd(),
		newClusterPromoteCmd(),
		newClusterDemoteCmd(),
		newClusterJoinCmd(),
		newClusterMaintenanceCmd(),
		newClusterOnlineCmd(),
		newClusterDrainCmd(),
		newClusterCancelDrainCmd(),
	)
	return cmd
}

func newClusterStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show cluster topology and Raft state",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.GetClusterStatus(context.Background(), connect.NewRequest(&v1.GetClusterStatusRequest{}))
			if err != nil {
				return err
			}
			msg := resp.Msg
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(msg)
			}

			pairs := [][2]string{
				{"Cluster Enabled", strconv.FormatBool(msg.ClusterEnabled)},
				{"Local Node", formatIDBytes(msg.LocalNodeId)},
				{"Leader", formatIDBytes(msg.LeaderId)},
				{"Leader Address", msg.LeaderAddress},
			}
			if msg.ClusterAddress != "" {
				pairs = append(pairs, [2]string{"Cluster Address", msg.ClusterAddress})
			}
			if msg.JoinToken != "" {
				pairs = append(pairs, [2]string{"Join Token", msg.JoinToken})
			}
			p.kv(pairs)

			if len(msg.Nodes) > 0 {
				fmt.Println()
				var rows [][]string
				for _, n := range msg.Nodes {
					role := clusterRoleStr(n.Role)
					if n.IsLeader {
						role += " *"
					}
					rows = append(rows, []string{
						glid.FromBytes(n.Id).String(), n.Name, n.Address, role,
						clusterSuffrageStr(n.Suffrage),
					})
				}
				p.table([]string{"ID", "NAME", "ADDRESS", "ROLE", "SUFFRAGE"}, rows)
			}

			// Raft liveness per node (gastrolog-1io54g) — parity with the
			// inspector's Raft section.
			var liveRows [][]string
			for _, n := range msg.Nodes {
				if n.Stats == nil || n.Stats.RaftWalAppendsTotal == 0 {
					continue
				}
				liveRows = append(liveRows, []string{
					n.Name,
					fmt.Sprintf("%.1fms", n.Stats.RaftWalAppendAvgMs),
					fmt.Sprintf("%.0fms", n.Stats.RaftWalAppendMaxMs),
					fmt.Sprintf("%d (%.1f/min)", n.Stats.RaftElectionsTotal, n.Stats.RaftElectionsPerMin),
					strconv.FormatUint(n.Stats.RaftLeaderLossesTotal, 10),
					strconv.FormatUint(n.Stats.RaftFailedHeartbeatsTotal, 10),
				})
			}
			if len(liveRows) > 0 {
				fmt.Println()
				p.table([]string{"NODE", "WAL AVG", "WAL MAX", "ELECTIONS", "LEADER LOSSES", "FAILED HBS"}, liveRows)
			}

			if msg.LocalStats != nil {
				fmt.Println()
				s := msg.LocalStats
				statPairs := [][2]string{
					{"Raft State", s.State},
					{"Term", strconv.FormatUint(s.Term, 10)},
					{"Commit Index", strconv.FormatUint(s.CommitIndex, 10)},
					{"Applied Index", strconv.FormatUint(s.AppliedIndex, 10)},
					{"Last Log Index", strconv.FormatUint(s.LastLogIndex, 10)},
					{"Last Contact", s.LastContact},
					{"Peers", strconv.FormatUint(uint64(s.NumPeers), 10)},
				}
				p.kv(statPairs)
			}
			return nil
		},
	}
}

// newClusterThroughputCmd shows the pipeline throughput rates the inspector
// displays: cluster-total routing rates from GetRouteStats and per-node,
// per-vault segmentation append rates from the NodeStats broadcast
// (gastrolog-4eh5ns).
func newClusterThroughputCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "throughput",
		Short: "Show pipeline throughput (routing and per-vault append rates)",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			rs, err := client.System.GetRouteStats(context.Background(), connect.NewRequest(&v1.GetRouteStatsRequest{}))
			if err != nil {
				return err
			}
			cs, err := client.Lifecycle.GetClusterStatus(context.Background(), connect.NewRequest(&v1.GetClusterStatusRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(map[string]any{
					"route_stats": rs.Msg,
					"nodes":       cs.Msg.Nodes,
				})
			}

			p.kv([][2]string{
				{"Ingest Rate", formatRateTriple(rs.Msg.IngestedRate)},
				{"Route Rate", formatRateTriple(rs.Msg.RoutedRate)},
				{"Total Ingested", strconv.FormatInt(rs.Msg.TotalIngested, 10)},
				{"Total Routed", strconv.FormatInt(rs.Msg.TotalRouted, 10)},
				{"Total Dropped", strconv.FormatInt(rs.Msg.TotalDropped, 10)},
			})

			var rows [][]string
			for _, n := range cs.Msg.Nodes {
				if n.Stats == nil {
					continue
				}
				for _, vs := range n.Stats.Vaults {
					if vs.AppendQueueCapacity == 0 {
						continue // no segmentation writer on this node
					}
					rows = append(rows, []string{
						n.Name,
						vs.Name,
						fmt.Sprintf("%.1f/s", vs.AppendRecords.GetInstantPerSec()),
						fmt.Sprintf("%.1f/s", vs.AppendDurable.GetInstantPerSec()),
						formatBytesCLI(vs.AppendBytes.GetInstantPerSec()) + "/s",
						fmt.Sprintf("%d/%d", vs.AppendQueueDepth, vs.AppendQueueCapacity),
					})
				}
			}
			if len(rows) > 0 {
				fmt.Println()
				p.table([]string{"NODE", "VAULT", "APPEND", "DURABLE", "BYTES", "QUEUE"}, rows)
			}
			return nil
		},
	}
}

// formatRateTriple renders the instant rate with Unix-load-style 1m/5m/15m
// EWMAs, uptime-style.
func formatRateTriple(r *v1.ThroughputRate) string {
	if r == nil {
		return "0.0 rec/s"
	}
	return fmt.Sprintf("%.1f rec/s (1m: %.1f, 5m: %.1f, 15m: %.1f)",
		r.InstantPerSec, r.Avg_1MPerSec, r.Avg_5MPerSec, r.Avg_15MPerSec)
}

func formatBytesCLI(b float64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

func newClusterHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Check server health",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.Health(context.Background(), connect.NewRequest(&v1.HealthRequest{}))
			if err != nil {
				return err
			}
			p := newPrinter(outputFormat(cmd))
			if outputFormat(cmd) == "json" {
				return p.json(resp.Msg)
			}
			p.kv([][2]string{
				{"Status", healthStatusStr(resp.Msg.Status)},
				{"Version", resp.Msg.Version},
				{"Uptime", fmt.Sprintf("%ds", resp.Msg.UptimeSeconds)},
				{"Ingest Queue", fmt.Sprintf("%d/%d", resp.Msg.IngestQueueDepth, resp.Msg.IngestQueueCapacity)},
			})
			return nil
		},
	}
}

func newClusterJoinTokenCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "join-token",
		Short: "Print the cluster join token",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.GetClusterStatus(context.Background(), connect.NewRequest(&v1.GetClusterStatusRequest{}))
			if err != nil {
				return err
			}
			token := resp.Msg.JoinToken
			if token == "" {
				return errors.New("no join token available (cluster TLS may not be initialized)")
			}
			fmt.Println(token)
			return nil
		},
	}
}

func newClusterShutdownCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shutdown",
		Short: "Initiate graceful server shutdown",
		RunE: func(cmd *cobra.Command, args []string) error {
			drain, _ := cmd.Flags().GetBool("drain")
			client := clientFromCmd(cmd)
			_, err := client.Lifecycle.Shutdown(context.Background(), connect.NewRequest(&v1.ShutdownRequest{Drain: drain}))
			if err != nil {
				return err
			}
			fmt.Println("Shutdown initiated")
			return nil
		},
	}
	cmd.Flags().Bool("drain", false, "wait for in-flight requests to complete")
	return cmd
}

func newClusterRemoveNodeCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "remove-node <node-name-or-id>",
		Short: "Remove a node from the cluster",
		Long: "Remove a node from the cluster. Refuses by default if removal " +
			"would orphan any vault (sole-replica-on-this-node case); use " +
			"--force to override with explicit data-loss acknowledgement.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return err
			}
			id, err := resolve(args[0], r.nodes, "node")
			if err != nil {
				return err
			}
			_, err = client.Lifecycle.RemoveNode(context.Background(), connect.NewRequest(&v1.RemoveNodeRequest{
				NodeId: []byte(id),
				Force:  force,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Removed node %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false,
		"bypass the orphan-refusal gate (acknowledges potential data loss)")
	return cmd
}

// newClusterDemoteSelfCmd is the preStop-hook command for K8s
// rolling-restart / scale-down (gastrolog-24iv4 Step A): the pod tells
// the cluster to remove this node from Raft membership before SIGTERM
// arrives. Returns success once the RemoveServer commit has propagated,
// so the pod terminates without leaving a stranded voter behind.
//
// Distinct from `cluster remove-node`: that requires an operator to
// specify the target node by name. demote-self looks up its own name
// via os.Hostname() and resolves that name through the existing
// NodeConfig → ID mapping (newResolver / resolve) — the same code
// path the operator-facing `cluster remove-node <hostname>` already
// uses. Sharing the resolver matters: GetClusterStatus's LocalNodeId
// is in a different encoding than the raft.ServerID strings the
// leader compares against, so a naive "look up my own ID locally"
// approach yields a string raft can't match and the RemoveServer
// silently no-ops.
//
// Idempotent: if the node has already been removed (we're not in the
// cluster's config), the command logs that fact and returns success.
// preStop hooks must not block pod termination on a "node already
// gone" race (operator-driven removal happened first, or a previous
// preStop already fired).
func newClusterDemoteSelfCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "demote-self",
		Short: "Remove this node from the cluster (for preStop hooks)",
		Long: "Looks up this node's own name via os.Hostname() and calls " +
			"cluster.RemoveNode against it, returning when the membership " +
			"change has committed. Intended as a Kubernetes preStop lifecycle " +
			"hook so pods leaving via `kubectl scale` / rolling restart / " +
			"voluntary eviction take themselves out of the Raft voter set " +
			"before SIGTERM. See gastrolog-24iv4.",
		RunE: func(cmd *cobra.Command, args []string) error {
			hostname, err := os.Hostname()
			if err != nil {
				return fmt.Errorf("read hostname: %w", err)
			}
			if hostname == "" {
				return errors.New("hostname is empty; demote-self requires a hostname matching the node's NodeConfig.Name")
			}

			client := clientFromCmd(cmd)
			r, err := newResolver(context.Background(), client)
			if err != nil {
				return fmt.Errorf("build node resolver: %w", err)
			}
			// Direct map lookup rather than resolve() so the
			// hostname-not-found case is a clean "no-op success"
			// instead of an err-but-return-nil pattern. preStop must
			// not block pod termination just because the node has
			// already been removed.
			id, ok := r.nodes[strings.ToLower(hostname)]
			if !ok {
				fmt.Printf("demote-self: node %q not in cluster config (no-op)\n", hostname)
				return nil
			}

			_, err = client.Lifecycle.RemoveNode(context.Background(), connect.NewRequest(&v1.RemoveNodeRequest{
				NodeId:    []byte(id),
				AllowSelf: true, // gastrolog-24iv4: opt out of the operator-typo guard; this RPC IS the self-remove path.
				// Force bypasses the orphan-refusal gate (gastrolog-2ch9y).
				// demote-self runs from K8s preStop, where the pod is
				// terminating regardless — refusing here would leave the
				// Raft membership stale (the very hazard 24iv4 + 6bfwk
				// were designed to close) without preventing the orphan,
				// since the pod still goes away when SIGKILL hits the
				// preStop deadline. The operator-facing `cluster remove-node`
				// path keeps the gate; demote-self is the programmatic
				// shutdown sequence and runs unconditionally.
				Force: true,
			}))
			if err != nil {
				if isAlreadyRemoved(err) {
					fmt.Printf("demote-self: node %s (%s) already removed from cluster (no-op)\n", hostname, id)
					return nil
				}
				return fmt.Errorf("remove self %s (%s): %w", hostname, id, err)
			}
			fmt.Printf("demote-self: removed node %s (%s) from cluster\n", hostname, id)
			return nil
		},
	}
}

// newClusterYieldLeadershipCmd is the membership-preserving preStop
// counterpart to demote-self. It asks the local node to transfer Raft
// leadership if it holds it, and is a no-op otherwise. The node stays
// in the cluster's voter set; the brief absence during pod restart is
// handled by normal Raft heartbeat timeout, and the node rejoins as a
// known follower when it comes back. See gastrolog-2yeie.
//
// Use this for k8s preStop hooks where the pod is being restarted, not
// permanently removed. For true scale-down, the operator should call
// `cluster remove-node <hostname>` explicitly.
func newClusterYieldLeadershipCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "yield-leadership",
		Short: "Transfer Raft leadership to another voter if this node is leader",
		Long: "Asks the local node to hand off Raft leadership if it currently " +
			"holds it; no-op if it's already a follower. Designed for Kubernetes " +
			"preStop hooks so a pod restart triggers a clean leadership transfer " +
			"without removing the node from cluster membership. See gastrolog-2yeie.",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := clientFromCmd(cmd)
			resp, err := client.Lifecycle.YieldLeadership(context.Background(), connect.NewRequest(&v1.YieldLeadershipRequest{}))
			if err != nil {
				return fmt.Errorf("yield-leadership: %w", err)
			}
			if resp.Msg.Transferred {
				fmt.Println("yield-leadership: leadership transferred")
			} else {
				fmt.Println("yield-leadership: not leader, no-op")
			}
			return nil
		},
	}
}

// isAlreadyRemoved reports whether err from RemoveNode indicates the
// target was already absent from the cluster. preStop hooks must
// treat these as benign success.
func isAlreadyRemoved(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not in cluster") ||
		strings.Contains(msg, "not a voter") ||
		strings.Contains(msg, "already removed") ||
		strings.Contains(msg, "not in configuration")
}

func newClusterPromoteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "promote <node-name-or-id>",
		Short: "Promote a node to voter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setNodeSuffrage(cmd, args[0], true)
		},
	}
}

func newClusterDemoteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "demote <node-name-or-id>",
		Short: "Demote a node to non-voter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setNodeSuffrage(cmd, args[0], false)
		},
	}
}

// setNodeState resolves a node name/ID and proposes a SetNodeState
// transition through the Lifecycle RPC. Shared by the four operator
// verbs (maintenance / online / drain / cancel-drain) so each verb
// stays a one-liner around the state argument. Idempotent per the
// FSM contract: re-applying the same state is a no-op success.
//
// Honors --output json: emits a single-line JSON object instead of
// the human-friendly print so automation can consume the result.
func setNodeState(cmd *cobra.Command, nameOrID string, state v1.NodeState, action string) error {
	client := clientFromCmd(cmd)
	r, err := newResolver(context.Background(), client)
	if err != nil {
		return err
	}
	id, err := resolve(nameOrID, r.nodes, "node")
	if err != nil {
		return err
	}
	_, err = client.Lifecycle.SetNodeState(context.Background(), connect.NewRequest(&v1.SetNodeStateRequest{
		NodeId: []byte(id),
		State:  state,
	}))
	if err != nil {
		return err
	}
	if outputFormat(cmd) == "json" {
		p := newPrinter("json")
		return p.json(map[string]string{
			"node":  nameOrID,
			"id":    id,
			"state": strings.TrimPrefix(state.String(), "NODE_STATE_"),
		})
	}
	fmt.Printf("%s node %s\n", action, nameOrID)
	return nil
}

func newClusterMaintenanceCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "maintenance <node-name-or-id>",
		Short: "Put a node into Maintenance (operator-sticky offline)",
		Long: "Transition a node to Maintenance state. The placement guard " +
			"retains existing placements on the node and refuses to rotate " +
			"leadership off it; the unreachable sweep does NOT auto-clear " +
			"this state — only `cluster online` returns the node to Live.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setNodeState(cmd, args[0], v1.NodeState_NODE_STATE_MAINTENANCE, "Maintenance")
		},
	}
}

func newClusterOnlineCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "online <node-name-or-id>",
		Short: "Return a node to Live state (clear Maintenance / Draining)",
		Long: "Transition a node from Maintenance or Draining back to Live. " +
			"Cancels an in-flight drain when applied to a Draining node.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setNodeState(cmd, args[0], v1.NodeState_NODE_STATE_LIVE, "Onlined")
		},
	}
}

func newClusterDrainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drain <node-name-or-id>",
		Short: "Mark a node as Draining (preparing for decommission)",
		Long: "Transition a node to Draining state. The placement guard " +
			"retains existing placements and treats drain as authoritative " +
			"(no automatic rotation). To return the node to service before " +
			"decommission completes, run `cluster online`.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setNodeState(cmd, args[0], v1.NodeState_NODE_STATE_DRAINING, "Draining")
		},
	}
}

func newClusterCancelDrainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel-drain <node-name-or-id>",
		Short: "Cancel a drain in progress (Draining → Live)",
		Long: "Alias for `cluster online` when the target is currently " +
			"Draining; named for operator intent. Refuses if the node has " +
			"already progressed to Decommissioning.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setNodeState(cmd, args[0], v1.NodeState_NODE_STATE_LIVE, "Drain cancelled — onlined")
		},
	}
}

func newClusterJoinCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "join",
		Short: "Join this node to an existing cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			leader, _ := cmd.Flags().GetString("leader")
			token, _ := cmd.Flags().GetString("join-token")
			client := clientFromCmd(cmd)
			_, err := client.Lifecycle.JoinCluster(context.Background(), connect.NewRequest(&v1.JoinClusterRequest{
				LeaderAddress: leader,
				JoinToken:     token,
			}))
			if err != nil {
				return err
			}
			fmt.Println("Joined cluster")
			return nil
		},
	}
	cmd.Flags().String("leader", "", "cluster address of the leader node (required)")
	cmd.Flags().String("join-token", "", "join token from the leader (required)")
	_ = cmd.MarkFlagRequired("leader")
	_ = cmd.MarkFlagRequired("join-token")
	return cmd
}

func setNodeSuffrage(cmd *cobra.Command, nameOrID string, voter bool) error {
	client := clientFromCmd(cmd)
	r, err := newResolver(context.Background(), client)
	if err != nil {
		return err
	}
	id, err := resolve(nameOrID, r.nodes, "node")
	if err != nil {
		return err
	}
	_, err = client.Lifecycle.SetNodeSuffrage(context.Background(), connect.NewRequest(&v1.SetNodeSuffrageRequest{
		NodeId: []byte(id),
		Voter:  voter,
	}))
	if err != nil {
		return err
	}
	action := "Promoted"
	if !voter {
		action = "Demoted"
	}
	fmt.Printf("%s node %s\n", action, nameOrID)
	return nil
}

func clusterRoleStr(r v1.ClusterNodeRole) string {
	return strings.TrimPrefix(r.String(), "CLUSTER_NODE_ROLE_")
}

func clusterSuffrageStr(s v1.ClusterNodeSuffrage) string {
	return strings.TrimPrefix(s.String(), "CLUSTER_NODE_SUFFRAGE_")
}

func healthStatusStr(s v1.Status) string {
	return strings.TrimPrefix(s.String(), "STATUS_")
}
