package cli

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
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
		newClusterHealthCmd(),
		newClusterJoinTokenCmd(),
		newClusterShutdownCmd(),
		newClusterRemoveNodeCmd(),
		newClusterPromoteCmd(),
		newClusterDemoteCmd(),
		newClusterJoinCmd(),
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
				{"Local Node", string(msg.LocalNodeId)},
				{"Leader", string(msg.LeaderId)},
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
	return &cobra.Command{
		Use:   "remove-node <node-name-or-id>",
		Short: "Remove a node from the cluster",
		Args:  cobra.ExactArgs(1),
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
			_, err = client.Lifecycle.RemoveNode(context.Background(), connect.NewRequest(&v1.RemoveNodeRequest{NodeId: []byte(id)}))
			if err != nil {
				return err
			}
			fmt.Printf("Removed node %s\n", args[0])
			return nil
		},
	}
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
