package cluster

import (
	"context"
	"fmt"

	pb "github.com/Jille/raftadmin/proto"
	"google.golang.org/grpc"
)

// JoinCluster dials the leader's cluster port using mTLS and requests that
// this node be added as a Raft voter via raftadmin. This must be called after
// the local cluster server and Raft instance are running, so the leader can
// replicate to this node once AddVoter commits.
func JoinCluster(ctx context.Context, leaderAddr, nodeID, nodeAddr string, ctls *ClusterTLS) error {
	creds := ctls.TransportCredentials()

	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return fmt.Errorf("dial leader %s: %w", leaderAddr, err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewRaftAdminClient(conn)

	// AddVoter returns a Future with an operation token.
	fut, err := client.AddVoter(ctx, &pb.AddVoterRequest{
		Id:      nodeID,
		Address: nodeAddr,
	})
	if err != nil {
		return fmt.Errorf("add voter RPC: %w", err)
	}

	// Await blocks until the membership change is committed.
	resp, err := client.Await(ctx, fut)
	if err != nil {
		return fmt.Errorf("await add voter: %w", err)
	}
	if resp.GetError() != "" {
		return fmt.Errorf("add voter: %s", resp.GetError())
	}

	return nil
}
