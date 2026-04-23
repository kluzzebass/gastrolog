package cluster

import "time"

// Operational timeouts for cross-node replication, forwarding, and consensus.
// Grouped by the class of operation they protect.

// ReplicationTimeout is the deadline for replicating data to follower nodes:
// sealed chunk transfer, ack-gated record forwarding, and Raft consensus applies.
// Long enough for any healthy transfer, short enough to release resources when
// a follower is down.
const ReplicationTimeout = 10 * time.Second

// ForwardingTimeout is the deadline for lightweight single-command
// operations: seal commands to followers, vault/tier apply forwarding,
// and tier follower append when using bounded waits. Cross-node vault-route
// ingestion uses RecordForwarder.ForwardSync without this cap so the
// pipeline can backpressure.
const ForwardingTimeout = 5 * time.Second

// CatchupTimeout is the deadline for bulk replication of all sealed chunks
// to a newly added follower node. Much longer than per-chunk replication
// because catchup may need to transfer many large chunks.
const CatchupTimeout = 5 * time.Minute
