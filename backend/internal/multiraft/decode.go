package multiraft

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/hashicorp/raft"
)

func decodeRPCHeader(m *gastrologv1.RPCHeader) raft.RPCHeader {
	return raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersion(m.GetProtocolVersion()),
		ID:              m.GetId(),
		Addr:            m.GetAddr(),
	}
}

func decodeLogs(m []*gastrologv1.Log) []*raft.Log {
	ret := make([]*raft.Log, len(m))
	for i, l := range m {
		ret[i] = decodeLog(l)
	}
	return ret
}

func decodeLog(m *gastrologv1.Log) *raft.Log {
	return &raft.Log{
		Index:      m.GetIndex(),
		Term:       m.GetTerm(),
		Type:       decodeLogType(m.GetType()),
		Data:       m.GetData(),
		Extensions: m.GetExtensions(),
		AppendedAt: m.GetAppendedAt().AsTime(),
	}
}

func decodeLogType(m gastrologv1.Log_LogType) raft.LogType {
	switch m {
	case gastrologv1.Log_LOG_COMMAND:
		return raft.LogCommand
	case gastrologv1.Log_LOG_NOOP:
		return raft.LogNoop
	case gastrologv1.Log_LOG_ADD_PEER_DEPRECATED:
		return raft.LogAddPeerDeprecated
	case gastrologv1.Log_LOG_REMOVE_PEER_DEPRECATED:
		return raft.LogRemovePeerDeprecated
	case gastrologv1.Log_LOG_BARRIER:
		return raft.LogBarrier
	case gastrologv1.Log_LOG_CONFIGURATION:
		return raft.LogConfiguration
	default:
		panic("invalid LogType")
	}
}

func decodeAppendEntriesRequest(m *gastrologv1.MultiRaftAppendEntriesRequest) *raft.AppendEntriesRequest {
	return &raft.AppendEntriesRequest{
		RPCHeader:         decodeRPCHeader(m.GetRpcHeader()),
		Term:              m.GetTerm(),
		Leader:            m.GetRpcHeader().GetAddr(),
		PrevLogEntry:      m.GetPrevLogEntry(),
		PrevLogTerm:       m.GetPrevLogTerm(),
		Entries:           decodeLogs(m.GetEntries()),
		LeaderCommitIndex: m.GetLeaderCommitIndex(),
	}
}

func decodeAppendEntriesResponse(m *gastrologv1.MultiRaftAppendEntriesResponse) *raft.AppendEntriesResponse {
	return &raft.AppendEntriesResponse{
		RPCHeader:      decodeRPCHeader(m.GetRpcHeader()),
		Term:           m.GetTerm(),
		LastLog:        m.GetLastLog(),
		Success:        m.GetSuccess(),
		NoRetryBackoff: m.GetNoRetryBackoff(),
	}
}

func decodeRequestVoteRequest(m *gastrologv1.MultiRaftRequestVoteRequest) *raft.RequestVoteRequest {
	return &raft.RequestVoteRequest{
		RPCHeader:          decodeRPCHeader(m.GetRpcHeader()),
		Term:               m.GetTerm(),
		Candidate:          m.GetRpcHeader().GetAddr(),
		LastLogIndex:       m.GetLastLogIndex(),
		LastLogTerm:        m.GetLastLogTerm(),
		LeadershipTransfer: m.GetLeadershipTransfer(),
	}
}

func decodeRequestVoteResponse(m *gastrologv1.MultiRaftRequestVoteResponse) *raft.RequestVoteResponse {
	return &raft.RequestVoteResponse{
		RPCHeader: decodeRPCHeader(m.GetRpcHeader()),
		Term:      m.GetTerm(),
		Peers:     m.GetPeers(),
		Granted:   m.GetGranted(),
	}
}

func decodeRequestPreVoteRequest(m *gastrologv1.MultiRaftRequestPreVoteRequest) *raft.RequestPreVoteRequest {
	return &raft.RequestPreVoteRequest{
		RPCHeader:    decodeRPCHeader(m.GetRpcHeader()),
		Term:         m.GetTerm(),
		LastLogIndex: m.GetLastLogIndex(),
		LastLogTerm:  m.GetLastLogTerm(),
	}
}

func decodeRequestPreVoteResponse(m *gastrologv1.MultiRaftRequestPreVoteResponse) *raft.RequestPreVoteResponse {
	return &raft.RequestPreVoteResponse{
		RPCHeader: decodeRPCHeader(m.GetRpcHeader()),
		Term:      m.GetTerm(),
		Granted:   m.GetGranted(),
	}
}

func decodeTimeoutNowRequest(m *gastrologv1.MultiRaftTimeoutNowRequest) *raft.TimeoutNowRequest {
	return &raft.TimeoutNowRequest{
		RPCHeader: decodeRPCHeader(m.GetRpcHeader()),
	}
}

func decodeTimeoutNowResponse(m *gastrologv1.MultiRaftTimeoutNowResponse) *raft.TimeoutNowResponse {
	return &raft.TimeoutNowResponse{
		RPCHeader: decodeRPCHeader(m.GetRpcHeader()),
	}
}

func decodeInstallSnapshotRequest(m *gastrologv1.MultiRaftInstallSnapshotRequest) *raft.InstallSnapshotRequest {
	return &raft.InstallSnapshotRequest{
		RPCHeader:          decodeRPCHeader(m.GetRpcHeader()),
		SnapshotVersion:    raft.SnapshotVersion(m.GetSnapshotVersion()),
		Term:               m.GetTerm(),
		Leader:             m.GetRpcHeader().GetAddr(),
		LastLogIndex:       m.GetLastLogIndex(),
		LastLogTerm:        m.GetLastLogTerm(),
		Peers:              m.GetPeers(),
		Configuration:      m.GetConfiguration(),
		ConfigurationIndex: m.GetConfigurationIndex(),
		Size:               m.GetSize(),
	}
}

func decodeInstallSnapshotResponse(m *gastrologv1.MultiRaftInstallSnapshotResponse) *raft.InstallSnapshotResponse {
	return &raft.InstallSnapshotResponse{
		RPCHeader: decodeRPCHeader(m.GetRpcHeader()),
		Term:      m.GetTerm(),
		Success:   m.GetSuccess(),
	}
}
