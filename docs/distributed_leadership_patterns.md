# Distributed Leadership Patterns: Lessons from Production Systems

Research compiled 2026-04-12 for gastrolog-1s3mf (unify tier leadership).

## The Core Problem

GastroLog has per-tier Raft groups where the **tier leader** (the node designated `Leader: true` in the tier's placement config — receives ingested records, runs retention, replicates to followers) can diverge from the **tier Raft leader** (the node that won the tier's Raft group election). This "dual leadership" problem causes cascading failures: records land on a node without write authority, post-seal/compression/replication stops, transitions fail.

Note: GastroLog also has a separate **cluster Raft** group for config consensus. This document is exclusively about **tier Raft** leadership.

Every major distributed system using per-shard Raft groups has solved this. Here's how.

---

## CockroachDB: Leaseholder = Raft Leader

CockroachDB runs one Raft group per **range** (~512MB key range). They explicitly collapse two roles into one:

- The **leaseholder** holds a time-bounded lease and is the only node that proposes writes.
- The **Raft leader** coordinates consensus.
- These are **always the same node**. If they diverge (e.g., after an election), CockroachDB transfers Raft leadership to the leaseholder.

The lease is itself a Raft-replicated state machine entry, so all nodes agree on who holds it. Writes flow: client -> leaseholder/leader -> Raft log -> majority commit -> apply.

**MultiRaft optimization**: Batches Raft heartbeats per node-pair, not per range. The entire system runs on 3 goroutines per node regardless of range count. At scale (hundreds of thousands of ranges), this is essential.

**Self-healing**: After 5 minutes of unreachability, a node is declared dead and its ranges are automatically re-replicated to surviving nodes.

**Key takeaway**: The lease is the authority for who receives writes. Raft leadership follows the lease, not the other way around. Leadership transfer is the mechanism that keeps them aligned.

Sources: [Scaling Raft](https://www.cockroachlabs.com/blog/scaling-raft/), [Replication Layer](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer), [design.md](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)

---

## TiKV: PD Scheduler + Leader Transfer

TiKV (the storage engine behind TiDB) runs one Raft group per **Region** (~96MB). A separate **Placement Driver (PD)** acts as the cluster brain.

### Multi-Raft Performance

- A single thread polls ALL Raft groups every tick.
- All log appends across all groups are batched into **one RocksDB WriteBatch** (one fsync per tick, not per group).
- Inter-node messages for all groups sharing a node-pair go over a **single gRPC connection**.
- Regions split and merge dynamically.

### PD Scheduler

PD is not in the data path. It observes via heartbeats and issues scheduling operators:

1. `TransferLeader` — moves leadership to a different node (milliseconds, no data movement)
2. `AddPeer` / `RemovePeer` — adds/removes replicas (requires snapshot transfer)
3. `MergeRegion` — combines small regions

Scheduling priorities: (1) replica count safety, (2) balance region count, (3) balance leader count, (4) balance hot regions, (5) merge small regions.

### Leadership vs Data Location

All replicas hold full data. The Raft leader is just the node that accepts writes. **Leadership is ephemeral and transferable; data is persistent and replicated.** PD can move leadership independently of data placement.

**Learner replicas**: Hold data without voting. Used during migration to avoid double-vote risk.

**Key takeaway**: A centralized scheduler that continuously reconciles actual vs desired leader placement using `TransferLeader`. Leadership moves in milliseconds; data moves in seconds.

Sources: [TiKV Deep Dive: Multi-Raft](https://tikv.org/deep-dive/scalability/multi-raft/), [PD Scheduling](https://docs.pingcap.com/tidb/stable/tidb-scheduling)

---

## etcd / Consul: hashicorp/raft Patterns

etcd and Consul each use a **single Raft group** for the entire cluster.

### Follower Behavior

- Writes: followers forward to the leader (Consul) or reject with leader address (etcd).
- Reads: linearizable reads go through the leader. Stale/serializable reads can be served by followers.
- `Apply()` returns `ErrNotLeader` on non-leaders. The caller must forward or redirect.

### hashicorp/raft with Multiple Groups

The library was **designed for single-group-per-process**. Running multiple groups requires:

- Separate `raft.Raft`, `LogStore`, `StableStore`, `SnapshotStore`, and transport per group.
- Each group has its own goroutine set and election timers — resource cost scales linearly.
- **No built-in heartbeat coalescing** (unlike TiKV's MultiRaft).
- At tens of groups: manageable. At hundreds: needs custom batching.

### Non-Voting Members

`AddNonvoter()` creates members that receive log replication but don't participate in elections or quorum. Use cases:

- Catching up a new node before promoting to voter (avoids dragging commit latency).
- Standby replicas for fast failover.
- Read replicas.

**Key takeaway**: hashicorp/raft's `AddNonvoter` is the proper way to handle nodes that should receive data but not affect quorum — rather than a `--voteless` CLI flag that creates confusion about tier-level voting.

Sources: [hashicorp/raft](https://github.com/hashicorp/raft), [Consul Consensus](https://developer.hashicorp.com/consul/docs/concept/consensus), [etcd Learner](https://etcd.io/docs/v3.3/learning/learner/)

---

## Kafka / Redpanda: Partition Leadership

### Kafka (ISR-based)

- One leader per partition. All reads/writes through the leader.
- **ISR (In-Sync Replicas)**: leader tracks follower lag. Followers ejected from ISR if they fall behind.
- Committed = all ISR members acknowledged. A slow ISR member blocks the high watermark.
- New leader elected only from ISR (guarantees it has all committed data).
- Recovery: returning broker catches up from the current leader, rejoins ISR automatically.

### Redpanda (Raft-per-partition)

- Each partition is its own Raft group — directly analogous to per-tier Raft groups.
- Uses **majority quorum** instead of ISR: committed once a majority fsync. A slow follower doesn't block commits.
- Leadership and data flow are unified. No separate controller for data partitions.
- Failover is automatic within the Raft group.

**Key takeaway**: Redpanda validates the Raft-per-partition model for log systems. Majority quorum avoids the slow-follower stall that ISR can cause. Recovery is pull-based (follower catches up from leader).

Sources: [Redpanda Architecture](https://docs.redpanda.com/current/get-started/architecture/), [Simplifying Raft in Redpanda](https://www.redpanda.com/blog/simplifying-raft-replication-in-redpanda), [Kafka Replication](https://docs.confluent.io/kafka/design/replication.html)

---

## General Patterns: Solving Dual Leadership

### Pattern 1: Raft Leader Transfer

Raft has a built-in `TransferLeadership` mechanism: the current leader sends `MsgTimeoutNow` to the target follower, which immediately starts an election. TiKV and CockroachDB both use this extensively. The transfer requires the target to be up-to-date on the log.

**Application to GastroLog**: When the config says "node1 should lead tier X," proactively transfer tier Raft leadership to node1. If node1 is behind, wait for log catchup first.

### Pattern 2: Lease-Based Authority

CockroachDB uses a lease (Raft-replicated) to determine who serves reads/writes. The lease holder is the write authority. Raft leadership follows the lease. This decouples "who the cluster elected" from "who serves traffic" — the lease is the tiebreaker.

**Application to GastroLog**: A "tier lease" could replace the static IsFollower flag. The lease is proposed via the tier Raft group, so all nodes agree. Leadership transfer then aligns the tier Raft leader with the lease holder.

### Pattern 3: Follower Forwarding

When a client hits a non-leader, two options: **redirect** (return leader address, client retries) or **forward** (follower proxies to leader). ZippyDB and Consul use forwarding.

**Application to GastroLog**: The ingester node forwards records to the tier Raft leader. This is what we attempted in gastrolog-1s3mf but broke because the forwarding target resolution used the tier leader instead of the tier Raft leader.

### Pattern 4: Preferred Leader Placement

TiKV PD and CockroachDB both support placement rules declaring preferred leader location. A scheduler continuously evaluates rules and issues `TransferLeader` when violations are detected.

**Application to GastroLog**: The tier placement config declares the preferred leader. A background scheduler (like the rotation sweep) detects when Raft leadership doesn't match placement and issues a transfer.

---

## Recommendations for GastroLog

### Immediate (gastrolog-1s3mf retry)

1. **Keep `IsLeader() = !IsFollower`** for now. Don't derive from Raft.
2. **Add `TransferLeadership`** to the tier Raft group API. After a tier Raft election, the tier leader loop should check if the elected tier Raft leader matches the tier leader. If not, transfer tier Raft leadership to the tier leader.
3. **This makes the dual leadership problem disappear**: the tier Raft leader IS the tier leader because we proactively transfer. No need to rewire the data pipeline.

### Medium-term (gastrolog-10h56)

4. **Non-voting members**: Replace `--voteless` with hashicorp/raft `AddNonvoter`. Nodes that should receive data but not affect quorum are learners, not voters with a flag.
5. **Follower forwarding**: If a non-leader node receives a write (e.g., during the brief window after an election before transfer completes), forward to the tier Raft leader instead of rejecting.
6. **Replication backoff**: Circuit breaker for forwarding to dead peers (gastrolog-2b1xp).

### Long-term (scale)

7. **MultiRaft batching**: If tier count per node grows beyond ~50, batch Raft heartbeats per node-pair and coalesce log appends into a single fsync per tick.
8. **Placement scheduler**: A PD-like component that continuously reconciles actual vs desired leader placement across all tiers.
