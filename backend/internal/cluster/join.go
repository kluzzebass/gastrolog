package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pb "github.com/Jille/raftadmin/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	// joinInitialBackoff is the first wait between retries when the
	// leader returns a transient error (membership change in flight,
	// just-elected, gRPC briefly unavailable). Short enough not to add
	// perceptible startup latency in the no-contention case.
	joinInitialBackoff = 100 * time.Millisecond
	// joinMaxBackoff caps the exponential backoff. Two seconds is well
	// above the typical Hashicorp Raft AddVoter commit latency on a
	// healthy cluster (~50ms), so by the time we hit the cap the
	// problem is no longer "contention" but "leader truly unavailable"
	// — which is what the caller's ctx deadline handles.
	joinMaxBackoff = 2 * time.Second
)

// JoinCluster dials a cluster node using mTLS and requests this node be
// added to the Raft cluster via raftadmin. Handles two kinds of
// transient failures by retrying until ctx deadline:
//
//   - "not the leader" — caller dialed the wrong node, or the leader
//     was mid-commit on another membership change. queryLeader is used
//     to follow to the new leader's address; if the same address comes
//     back (mid-commit case), the next attempt waits a backoff window
//     and retries against the same address.
//   - "Unavailable" / "connection refused" — leader's gRPC server is
//     transiently rejecting connections (still starting, just restarted,
//     etc). Pure backoff retry.
//
// Non-transient errors (TLS handshake failures, malformed advertise,
// permission denied, etc) return immediately. The caller's context
// deadline is the ultimate retry budget — JoinCluster never retries
// past it.
//
// See gastrolog-2cb2r: the previous 3-hop leader-follow loop did not
// recover from concurrent AddVoter races during fresh-cluster
// bootstrap, leaving joiners to rely on kubelet's CrashLoopBackOff for
// retry — visible as RESTARTS=1-2 on `kubectl get pods` and a slower
// time-to-quorum.
//
// logger may be nil, in which case retry attempts are silent. Callers
// from app.go pass their slog instance so retries land in the same
// log stream as the rest of cluster startup.
func JoinCluster(ctx context.Context, logger *slog.Logger, addr, nodeID, nodeAddr string, ctls *ClusterTLS, voter bool) error {
	creds := ctls.TransportCredentials()

	backoff := joinInitialBackoff
	attempt := 0
	for {
		attempt++
		err := tryJoinCluster(ctx, addr, nodeID, nodeAddr, creds, voter)
		if err == nil {
			return nil
		}
		if !isTransientJoinErr(err) {
			return err
		}

		// "not the leader" case: try to follow to the actual leader's
		// address. If queryLeader returns a different address than the
		// one we just tried, retry there immediately (no backoff —
		// genuine leader change, not contention). If it returns the
		// same address or fails, fall through to backoff retry.
		if strings.Contains(err.Error(), "not the leader") {
			if leaderAddr, qErr := queryLeader(ctx, addr, creds); qErr == nil && leaderAddr != "" && leaderAddr != addr {
				if logger != nil {
					logger.Info("join cluster: following to new leader",
						"attempt", attempt, "old_addr", addr, "new_addr", leaderAddr)
				}
				addr = leaderAddr
				continue
			}
		}

		if logger != nil {
			logger.Warn("join cluster: transient failure, retrying",
				"attempt", attempt, "backoff", backoff, "addr", addr, "error", err)
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > joinMaxBackoff {
			backoff = joinMaxBackoff
		}
	}
}

// isTransientJoinErr classifies errors from JoinCluster's underlying
// gRPC + raftadmin calls as transient (retry) vs fatal. Transient cases
// happen during cluster startup races; fatal cases are configuration
// problems that won't resolve by waiting.
//
// Error matching is on string contents because the underlying errors
// arrive wrapped through gRPC and raftadmin's pb.Result.Error string
// field, neither of which exposes typed sentinels we can errors.Is
// against. See gastrolog-2cb2r for the full set of observed errors.
func isTransientJoinErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not the leader"):
		// Leader transiently unknown — membership change mid-commit
		// on the leader, or just-elected leader still propagating.
		return true
	case strings.Contains(msg, "leadership lost"),
		strings.Contains(msg, "leadership transfer"):
		// Leader lost election mid-call. Cluster will re-elect; retry.
		return true
	case strings.Contains(msg, "code = Unavailable"):
		// gRPC server transiently not accepting connections (still
		// starting up, just restarted, transient connectivity blip).
		return true
	case strings.Contains(msg, "connection refused"):
		// TCP-level: pod just starting, listener not bound yet.
		return true
	}
	return false
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

// IsNodeInClusterConfig dials a known cluster peer over mTLS and asks
// for the current Raft configuration via raftadmin.GetConfiguration.
// Returns (true, nil) if nodeID appears as a voter or non-voter in the
// returned server list, (false, nil) if the cluster is reachable but
// the node is not in the configuration, or (false, err) if the cluster
// is unreachable.
//
// Used at boot time (gastrolog-24iv4 Step C) to detect the "evicted
// voter returning" case: if the local Raft snapshot lists this node
// as a voter but the cluster's current configuration does not, the
// node must wipe its stale Raft state and rejoin as a fresh node
// instead of crashlooping on FSM catchup.
func IsNodeInClusterConfig(ctx context.Context, addr, nodeID string, ctls *ClusterTLS) (bool, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(ctls.TransportCredentials()))
	if err != nil {
		return false, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewRaftAdminClient(conn).GetConfiguration(ctx, &pb.GetConfigurationRequest{})
	if err != nil {
		return false, fmt.Errorf("get cluster configuration: %w", err)
	}
	for _, srv := range resp.GetServers() {
		if srv.GetId() == nodeID {
			return true, nil
		}
	}
	return false, nil
}
