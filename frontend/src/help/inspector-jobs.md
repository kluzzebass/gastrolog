# Jobs

The Jobs tab shows background work that GastroLog performs automatically. In a [cluster](help:clustering), each node runs jobs for its own vaults independently, and the tab shows one card per node with its count of active and scheduled jobs. The rows arrive over a live stream from the server; a **reconnecting** badge means that stream dropped and the rows may be stale until it is back.

## Tasks

One-time operations like chunk [rotation](help:policy-rotation), [index builds](help:indexers), [retention](help:policy-retention) sweeps, and reindexing. Each task shows its description, status (pending, running, completed, or failed), and progress. Failed tasks include error details.

## Scheduled Jobs

Recurring operations registered on the orchestrator scheduler. The inspector table shows the **job name**, a **6-field cron schedule** (`second minute hour day month weekday`), time since last run, and countdown to the next run.

The **Max Concurrent Jobs** setting in [Cluster settings](settings:service) [![icon:help]()](help:service-settings) controls how many tasks can run in parallel.

### Built-in scheduled jobs

| Job name | What it does |
|----------|----------------|
| `archival-sweep` | Archives cloud-backed chunks per lifecycle policy |
| `cache-eviction` | Evicts cached copies of cloud-backed chunks (LRU + TTL) |
| `cloud-reconcile` | Reconciles cloud index metadata against blob store |
| `cluster-ctl-learner-promoter` | Promotes caught-up cluster-ctl learners to voters (leader-only) |
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
| `pipeline-chunk-cron:*` | `pipeline-chunk-cron:<vault-id>` | Pipeline chunk cron rotation for a vault |
| `post-seal:*` | `post-seal:<vault-id>:<chunk-id>` | Post-seal pipeline for a chunk |
| `cloud-upload:*` | `cloud-upload:<vault-id>:<chunk-id>` | Upload sealed chunk to cloud storage — one job per chunk, shared by the live seal effect and the catch-up sweep |
| `glcb-catchup:*` | `glcb-catchup:<vault-id>:<chunk-id>` | Pull this home's missing copy of a sealed chunk from a peer home |
| `index-rebuild:*` | `index-rebuild:<vault-id>:<chunk-id>` | Build missing indexes for one chunk |
| `home-index-sweep:*` | `home-index-sweep:<vault-id>` | Build missing indexes for the chunks this home holds of a vault, once its vault instance and home registration both exist |

Tearing a vault down on a node — deleting it, unregistering it, or removing its placement — cancels that vault's pending `post-seal`, `index-rebuild`, `home-index-sweep`, `glcb-catchup` and `cloud-upload` jobs on that node.

Task rows (one-time jobs) still show a human description in the card header when the scheduler has one.
