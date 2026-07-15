# Jobs

The Jobs tab shows background work that GastroLog performs automatically. In a [cluster](help:clustering), each node runs jobs for its own vaults independently.

## Tasks

One-time operations like chunk [rotation](help:policy-rotation), [index builds](help:indexers), [retention](help:policy-retention) sweeps, and reindexing. Each task shows its description, status (pending, running, completed, or failed), and progress. Failed tasks include error details.

## Scheduled Jobs

Recurring operations registered on the orchestrator scheduler. The inspector table shows the **job name**, a **6-field cron schedule** (`second minute hour day month weekday`), time since last run, and countdown to the next run.

The **Max Concurrent Jobs** setting in [Cluster settings](settings:service) [![icon:help]()](help:service-settings) controls how many tasks can run in parallel.

### Built-in scheduled jobs

| Job name | What it does |
|----------|----------------|
| `archival-sweep` | Archives cloud-backed chunks per lifecycle policy |
| `cache-eviction` | Evicts warm-cache cloud-backed chunks (LRU + TTL) |
| `cloud-reconcile` | Reconciles cloud index metadata against blob store |
| `cluster-ctl-learner-promoter` | Promotes caught-up cluster-ctl learners to voters (leader-only) |
| `cluster-peer-heartbeat` | Lightweight peer liveness broadcast |
| `cluster-stats-broadcast` | Broadcasts local NodeStats to all cluster peers |
| `managed-files-reconcile` | Pulls missing managed files from peers when local disk drifts |
| `maxmind-update` | Downloads MaxMind GeoLite2 lookup databases |
| `node-unreachable-sweep` | Heartbeat-driven Live↔Unreachable node-state transitions and alerts |
| `peer-cache-reconcile` | Drops stale per-peer caches when Raft membership changes |
| `placement-reconcile` | Refreshes replication targets and routing table |
| `retention` | Retention sweep across all vaults |
| `vault-catchup-sweep` | Delete/orphan/replica convergence sweep |
| `vault-ctl-learner-promoter` | Promotes caught-up per-vault-ctl learners to voters |
| `vault-ctl-membership-reconcile` | Safety-net wake for vault-ctl membership reconciles |
| `vault-placement-reconcile` | Periodic vault/ingester placement safety net (leader acts) |

### Dynamic job names

Some scheduled work is created per vault or chunk. Names follow predictable patterns:

| Pattern | Example | Meaning |
|---------|---------|---------|
| `pipeline-rotation-*` | `pipeline-rotation-<vault-id>` | Pipeline chunk cron rotation for a vault |
| `post-seal-*` | `post-seal-<chunk-id>` | Post-seal pipeline for a chunk |
| `cloud-backfill-*` | `cloud-backfill-<chunk-id>` | Upload sealed chunk to cloud storage |
| `replicate-*` | `replicate-<chunk-id>` | Replicate a sealed chunk to followers |
| `replication-catchup-*` | `replication-catchup-<node>` | Catch up sealed chunks to a follower |
| `rebuild-index-*` | `rebuild-index-<chunk-id>` | Rebuild missing indexes for one chunk |
| `rebuild-all-*` | `rebuild-all-<vault-id>` | Rebuild all indexes for a vault |
| `drain-vault-*` | `drain-vault-<vault-id>` | Drain a vault off this node |

Task rows (one-time jobs) still show a human description in the card header when the scheduler has one.
