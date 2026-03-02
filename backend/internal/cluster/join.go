package cluster

import (
	"context"
	"fmt"

	pb "github.com/Jille/raftadmin/proto"
	"google.golang.org/grpc"
)

// JoinCluster dials the leader's cluster port using mTLS and requests that
// this node be added to the Raft cluster via raftadmin. When voter is true the
// node joins as a voter; when false it joins as a nonvoter (receives log
// replication but does not participate in elections).
func JoinCluster(ctx context.Context, leaderAddr, nodeID, nodeAddr string, ctls *ClusterTLS, voter bool) error {
	creds := ctls.TransportCredentials()

	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return fmt.Errorf("dial leader %s: %w", leaderAddr, err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewRaftAdminClient(conn)

	var fut *pb.Future
	if voter {
		fut, err = client.AddVoter(ctx, &pb.AddVoterRequest{
			Id:      nodeID,
			Address: nodeAddr,
		})
	} else {
		fut, err = client.AddNonvoter(ctx, &pb.AddNonvoterRequest{
			Id:      nodeID,
			Address: nodeAddr,
		})
	}
	if err != nil {
		kind := "voter"
		if !voter {
			kind = "nonvoter"
		}
		return fmt.Errorf("add %s RPC: %w", kind, err)
	}

	// Await blocks until the membership change is committed.
	resp, err := client.Await(ctx, fut)
	if err != nil {
		return fmt.Errorf("await membership change: %w", err)
	}
	if resp.GetError() != "" {
		return fmt.Errorf("membership change: %s", resp.GetError())
	}

	return nil
}
