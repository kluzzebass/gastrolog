package cluster

import "time"

// Operational timeouts for cross-node replication, forwarding, and consensus.
// Grouped by the class of operation they protect.

// ReplicationTimeout is the deadline for replicating one sealed chunk to a
// follower: ImportBegin → ImportRecords → ImportCommit. Must cover large
// sealed chunks (10k+ records) under ingest/replication load; 10s was
// too short and caused mid-import abandons that cascaded into catchup
// preempt storms.
const ReplicationTimeout = 60 * time.Second

// ForwardingTimeout is the deadline for lightweight single-command
// operations: vault apply forwarding and other bounded single-shot
// cross-node RPCs.
const ForwardingTimeout = 5 * time.Second

// CatchupTimeout is the deadline for bulk replication of all sealed chunks
// to a newly added follower node. Much longer than per-chunk replication
// because catchup may need to transfer many large chunks.
const CatchupTimeout = 5 * time.Minute
