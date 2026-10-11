package cluster

import "time"

// Operational timeouts for cross-node forwarding and consensus.
// Grouped by the class of operation they protect.

// RaftApplyTimeout bounds one Raft command reaching commit: a local
// raft.Apply on the group leader, or a follower forwarding the command to
// the leader (cluster-ctl Forwarder, VaultApplyForwarder,
// VaultCtlChunkApplyForwarder) and waiting for it to commit there.
const RaftApplyTimeout = 60 * time.Second

// ForwardingTimeout is the per-peer deadline for a lightweight single-shot
// cross-node RPC, such as one peer's leg of a broadcast.
const ForwardingTimeout = 5 * time.Second
