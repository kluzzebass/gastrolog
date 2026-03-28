// Package multiraft provides a group-multiplexed gRPC transport for
// hashicorp/raft. Multiple Raft groups share a single gRPC service,
// with each RPC tagged by group ID for routing.
package multiraft

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func encodeRPCHeader(s raft.RPCHeader) *gastrologv1.RPCHeader {
	return &gastrologv1.RPCHeader{
		ProtocolVersion: int64(s.ProtocolVersion),
		Id:              s.ID,
		Addr:            s.Addr,
	}
}

func encodeLogs(s []*raft.Log) []*gastrologv1.Log {
	ret := make([]*gastrologv1.Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func encodeLog(s *raft.Log) *gastrologv1.Log {
	return &gastrologv1.Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: timestamppb.New(s.AppendedAt),
	}
}

func encodeLogType(s raft.LogType) gastrologv1.Log_LogType {
	switch s {
	case raft.LogCommand:
		return gastrologv1.Log_LOG_COMMAND
	case raft.LogNoop:
		return gastrologv1.Log_LOG_NOOP
	case raft.LogAddPeerDeprecated:
		return gastrologv1.Log_LOG_ADD_PEER_DEPRECATED
	case raft.LogRemovePeerDeprecated:
		return gastrologv1.Log_LOG_REMOVE_PEER_DEPRECATED
	case raft.LogBarrier:
		return gastrologv1.Log_LOG_BARRIER
	case raft.LogConfiguration:
		return gastrologv1.Log_LOG_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}

func encodeAppendEntriesRequest(groupID []byte, s *raft.AppendEntriesRequest) *gastrologv1.MultiRaftAppendEntriesRequest {
	return &gastrologv1.MultiRaftAppendEntriesRequest{
		GroupId:           groupID,
		RpcHeader:         encodeRPCHeader(s.RPCHeader),
		Term:              s.Term,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
	}
}

func encodeAppendEntriesResponse(s *raft.AppendEntriesResponse) *gastrologv1.MultiRaftAppendEntriesResponse {
	return &gastrologv1.MultiRaftAppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func encodeRequestVoteRequest(groupID []byte, s *raft.RequestVoteRequest) *gastrologv1.MultiRaftRequestVoteRequest {
	return &gastrologv1.MultiRaftRequestVoteRequest{
		GroupId:            groupID,
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		Term:               s.Term,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
	}
}

func encodeRequestVoteResponse(s *raft.RequestVoteResponse) *gastrologv1.MultiRaftRequestVoteResponse {
	return &gastrologv1.MultiRaftRequestVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func encodeRequestPreVoteRequest(groupID []byte, s *raft.RequestPreVoteRequest) *gastrologv1.MultiRaftRequestPreVoteRequest {
	return &gastrologv1.MultiRaftRequestPreVoteRequest{
		GroupId:      groupID,
		RpcHeader:    encodeRPCHeader(s.RPCHeader),
		Term:         s.Term,
		LastLogIndex: s.LastLogIndex,
		LastLogTerm:  s.LastLogTerm,
	}
}

func encodeRequestPreVoteResponse(s *raft.RequestPreVoteResponse) *gastrologv1.MultiRaftRequestPreVoteResponse {
	return &gastrologv1.MultiRaftRequestPreVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Granted:   s.Granted,
	}
}

func encodeTimeoutNowRequest(groupID []byte, s *raft.TimeoutNowRequest) *gastrologv1.MultiRaftTimeoutNowRequest {
	return &gastrologv1.MultiRaftTimeoutNowRequest{
		GroupId:   groupID,
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func encodeTimeoutNowResponse(s *raft.TimeoutNowResponse) *gastrologv1.MultiRaftTimeoutNowResponse {
	return &gastrologv1.MultiRaftTimeoutNowResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func encodeInstallSnapshotRequest(groupID []byte, s *raft.InstallSnapshotRequest) *gastrologv1.MultiRaftInstallSnapshotRequest {
	return &gastrologv1.MultiRaftInstallSnapshotRequest{
		GroupId:            groupID,
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		SnapshotVersion:    int64(s.SnapshotVersion),
		Term:               s.Term,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		Peers:              s.Peers,
		Configuration:      s.Configuration,
		ConfigurationIndex: s.ConfigurationIndex,
		Size:               s.Size,
	}
}

func encodeInstallSnapshotResponse(s *raft.InstallSnapshotResponse) *gastrologv1.MultiRaftInstallSnapshotResponse {
	return &gastrologv1.MultiRaftInstallSnapshotResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}
