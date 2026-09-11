package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"syscall"
	"time"

	hraft "github.com/hashicorp/raft"

	pb "github.com/Jille/raftadmin/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
// The previous 3-hop leader-follow loop did not recover from concurrent
// AddVoter races during fresh-cluster bootstrap, leaving joiners to rely
// on kubelet's CrashLoopBackOff for retry — visible as RESTARTS=1-2 on
// `kubectl get pods` and a slower time-to-quorum.
//
// logger may be nil, in which case retry attempts are silent. Callers
// from app.go pass their slog instance so retries land in the same
// log stream as the rest of cluster startup.
// joinCredentials picks the transport for a join dial. Cluster TLS is
// bootstrapped or loaded before any node joins, so a holder without material
// means startup left it unloaded — the join is refused rather than retried
// in plaintext against a peer that will not answer it anyway. A caller with
// no holder at all runs without cluster TLS by construction.
func joinCredentials(ctls *ClusterTLS) (credentials.TransportCredentials, error) {
	if ctls == nil {
		return insecure.NewCredentials(), nil
	}
	if ctls.State() == nil {
		return nil, ErrClusterTLSUnloaded
	}
	return ctls.TransportCredentials(), nil
}

func JoinCluster(ctx context.Context, logger *slog.Logger, addr, nodeID, nodeAddr string, ctls *ClusterTLS, voter bool) error {
	creds, err := joinCredentials(ctls)
	if err != nil {
		return fmt.Errorf("join %s: %w", addr, err)
	}

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
		if isNotLeaderErr(err) {
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
// against. Only "not the leader" has been seen in the wild, on fresh
// K8s bootstrap; the other cases below are defensive.
func isTransientJoinErr(err error) bool {
	if err == nil {
		return false
	}
	if isNotLeaderErr(err) {
		// Leader transiently unknown — membership change mid-commit
		// on the leader, or just-elected leader still propagating.
		return true
	}
	if raftFutureFailedWith(err, hraft.ErrLeadershipLost) || raftFutureFailedWith(err, hraft.ErrLeadershipTransferInProgress) {
		// Leader lost election mid-call. Cluster will re-elect; retry.
		return true
	}
	// gRPC server transiently not accepting connections: still starting
	// up, just restarted, or the listener not bound yet. A refused TCP
	// connection surfaces as Unavailable through gRPC and as ECONNREFUSED
	// from a direct dial.
	return status.Code(err) == codes.Unavailable || errors.Is(err, syscall.ECONNREFUSED)
}

// isNotLeaderErr reports whether the membership change failed because the
// node addressed is not the Raft leader.
func isNotLeaderErr(err error) bool {
	return raftFutureFailedWith(err, hraft.ErrNotLeader)
}

// raftFutureFailedWith reports whether err carries the given Raft future
// error. The raftadmin service returns the leader's future error as text in
// its Await response, so the text is the only form that crosses the wire;
// comparing against the Raft library's own sentinel message keeps this tied
// to the library rather than to a phrase typed here.
func raftFutureFailedWith(err error, sentinel error) bool {
	return errors.Is(err, sentinel) || strings.Contains(err.Error(), sentinel.Error())
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
