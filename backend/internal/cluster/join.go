package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/Jille/raftadmin/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// JoinCluster dials a cluster node using mTLS and requests that this node be
// added to the Raft cluster via raftadmin. If the target node is not the leader,
// it follows the leader address and retries (up to 3 hops).
func JoinCluster(ctx context.Context, addr, nodeID, nodeAddr string, ctls *ClusterTLS, voter bool) error {
	creds := ctls.TransportCredentials()

	for attempt := range 3 {
		err := tryJoinCluster(ctx, addr, nodeID, nodeAddr, creds, voter)
		if err == nil {
			return nil
		}

		// If the error indicates we hit a non-leader, ask it for the leader address.
		if !strings.Contains(err.Error(), "not the leader") {
			return err
		}

		leaderAddr, queryErr := queryLeader(ctx, addr, creds)
		if queryErr != nil || leaderAddr == "" {
			return err // return the original error
		}

		_ = attempt // used by range
		addr = leaderAddr
	}

	return errors.New("join cluster: exceeded leader-follow attempts")
}

func tryJoinCluster(ctx context.Context, addr, nodeID, nodeAddr string, creds credentials.TransportCredentials, voter bool) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
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

	resp, err := client.Await(ctx, fut)
	if err != nil {
		return fmt.Errorf("await membership change: %w", err)
	}
	if resp.GetError() != "" {
		return fmt.Errorf("membership change: %s", resp.GetError())
	}

	return nil
}

func queryLeader(ctx context.Context, addr string, creds credentials.TransportCredentials) (string, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewRaftAdminClient(conn).Leader(ctx, &pb.LeaderRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetAddress(), nil
}
