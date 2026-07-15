package multiraft

import (
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// RaftConnLease is a scoped outbound raft-lane connection. Release when the RPC
// completes so the connection manager can track active purposes.
type RaftConnLease interface {
	GRPC() *grpc.ClientConn
	Release()
	Invalidate(err error)
}

// PeerConnPool supplies outbound raft-lane gRPC connections to cluster peers.
type PeerConnPool interface {
	AcquireRaft(addr raft.ServerAddress, groupID string, purpose string) (RaftConnLease, error)
}
