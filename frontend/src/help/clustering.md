# Clustering

GastroLog uses [Raft consensus](https://raft.github.io/) to replicate configuration across multiple nodes, providing high availability and fault tolerance. Every GastroLog server automatically starts as a single-node cluster — no special setup is needed to run standalone, and adding nodes later requires no restart.

## How It Works

All configuration — [vaults](help:storage), [ingesters](help:ingestion), [filters](help:routing), [policies](help:policy-rotation), [users](help:user-management), [certificates](help:certificates), and [service settings](help:service-settings) — is stored in a replicated Raft log. One node is the **leader** and handles all writes; **followers** receive replicated state automatically. If the leader goes down, a new leader is elected from the remaining voters.

Writes to any follower are forwarded to the leader transparently. Reads are served locally from each node's replicated state, so all nodes see the same configuration.

## Cluster Formation

There are two ways to form a multi-node cluster:

### At startup (CLI)

Start the second node with the leader's cluster address and join token:

```
gastrolog server --join-addr 10.0.0.1:4566 --join-token <token>
```

The join token is displayed in the leader's [Nodes settings tab](settings:nodes) [![icon:help]()](help:clustering-nodes) and includes cryptographic material for mutual TLS enrollment.

> **Warning:** When a node joins a cluster, its local configuration is **replaced** by the cluster's replicated state. Any ingesters, vaults, filters, policies, or users configured on the joining node will be lost. Existing vault data on disk is not deleted, but the vault configurations pointing to it will be gone unless they also exist in the cluster.

### At runtime (UI)

A running single-node server can join an existing cluster without restarting. Go to **Settings > [Nodes](settings:nodes)** and use the **Join Cluster** form. The same config replacement warning above applies — the node's local configuration is replaced by the cluster's replicated state.

## Cluster Transport

Nodes communicate over a dedicated **cluster port** (default `:4566`), separate from the API port (`:4564`). This port carries Raft log replication, leader election messages, and peer-to-peer RPC for cross-node query forwarding.

TLS for the cluster transport is **auto-bootstrapped** — the first node generates a self-signed CA and certificates, and joining nodes receive their certificates during enrollment. No manual TLS configuration is needed for cluster communication.

## Node Roles

Every node has a **Raft role** and a **suffrage** level:

| Role | Meaning |
|------|---------|
| **Leader** | Handles all config writes, coordinates replication |
| **Follower** | Receives replicated state, can serve reads, forwards writes to leader |
| **Candidate** | Temporarily seeking election after leader loss |

| Suffrage | Meaning |
|----------|---------|
| **Voter** | Participates in leader elections, counts toward quorum |
| **Nonvoter** | Receives replicated state but cannot vote or become leader (read replica) |

See [Nodes](settings:nodes) [![icon:help]()](help:clustering-nodes) for managing node roles and suffrage.

## Quorum

Raft requires a **majority of voters** (quorum) to be online for writes and leader election. With 3 voters, 2 must be available; with 5, at least 3. Nonvoters don't affect quorum.

| Voters | Quorum | Tolerates |
|--------|--------|-----------|
| 1 | 1 | 0 failures |
| 3 | 2 | 1 failure |
| 5 | 3 | 2 failures |

For production clusters, **3 or 5 voters** is recommended. Even numbers (2, 4) don't improve fault tolerance and increase the risk of split-brain tie votes.

### Two-Node Warning

A 2-node cluster with 2 voters provides **no fault tolerance** — both nodes must be online for quorum (majority of 2 is 2). If either node goes down, the surviving node cannot elect a leader and the cluster loses write availability. This is actually **worse** than a single node, which is always its own quorum.

If you need two nodes for data distribution, consider making one a **nonvoter** (read replica). This keeps the quorum at 1, so the voter can still accept writes when the nonvoter goes down — at the cost of having no automatic failover.

### Quorum Loss (Read-Only Mode)

When a cluster loses quorum (not enough voters online to form a majority), no leader can be elected and all configuration changes are blocked. The cluster enters a **read-only** state:

- **Searches, follows, and explains continue working** — queries are served from each node's local vault data without requiring Raft consensus.
- **Configuration reads continue working** — settings, vault configs, and ingester configs are served from each node's local FSM replica.
- **Configuration writes fail** — creating, updating, or deleting vaults, ingesters, filters, routes, policies, and users all require a leader and will time out.

The header bar shows a **No Quorum** indicator when the cluster has no leader. To recover, bring enough voter nodes back online to restore quorum. See the [Nodes](settings:nodes) [![icon:help]()](help:clustering-nodes) settings for the current cluster topology.

## Data vs. Configuration

Clustering replicates **configuration only** — not log data. Each node has its own independent vaults and chunk storage. When you create a vault in a cluster, it is assigned to a specific node. Queries that span multiple nodes are automatically forwarded to the relevant peers and results are merged.

## Broadcasting

Nodes share runtime stats (CPU, memory, ingestion rates, queue depth) via periodic [broadcasting](help:clustering-broadcasting). This powers the [Inspector](help:inspector)'s cluster-wide view.
