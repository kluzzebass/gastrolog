# Cluster Settings

Global server settings that affect authentication, query execution, background processing, and cluster behavior. These settings are **cluster-wide** — changes made on any node are replicated to all nodes via [Raft](help:clustering).

| Setting | Description | Default |
|---------|-------------|---------|
| **Token Duration** | How long [JWT access tokens](help:user-management) remain valid. Uses Go duration syntax (e.g., `15m`, `1h`, `24h`); at least 1 minute | `1h` |
| **Refresh Token Duration** | How long refresh tokens remain valid; users must re-authenticate after this period of inactivity. At least 1 hour and longer than the token duration | `168h` (7 days) |
| **JWT Secret** | The signing key for authentication tokens. Never displayed; the card says whether a key is configured, and **Regenerate Secret** replaces it after a confirmation. Regenerating invalidates all existing sessions cluster-wide | |
| **Minimum Password Length** | Minimum characters required for [user](help:user-management) passwords | `8` |
| **Query Timeout** | Maximum [query](help:query-engine) execution time. Uses Go duration syntax (e.g., `30s`, `1m`). Set to empty or `0s` to disable | Disabled |
| **Max Concurrent Jobs** | How many [background jobs](help:inspector-jobs) ([rotation](help:policy-rotation), [retention](help:policy-retention), [indexing](help:indexers)) can run in parallel | `4` |

## Broadcasting

Controls how [cluster nodes](settings:nodes) [![icon:help]()](help:clustering-nodes) share runtime metrics with each other. See [Broadcasting](help:clustering-broadcasting) for details.

| Setting | Description | Default |
|---------|-------------|---------|
| **Broadcast Interval** | How often each node sends its full stats payload (vault stats, ingester stats, alerts) to peers. Observability only — peer liveness comes from Raft contact, so raising this does not slow failure detection | `5s` |

## Pipeline

| Setting | Description | Default |
|---------|-------------|---------|
| **Backlog Budget** | Per-vault budget for the pipeline backlog (completed segments awaiting chunking). At the budget, new records for that vault are refused cluster-wide — retryable backpressure — until chunking drains it below the budget. Bounds the backlog by policy before disk pressure; the [disk-space guard](help:storage-config) remains the backstop. Applies to every vault | Unbounded |

## TLS Configuration

The TLS options are always on screen; each is disabled, with the reason beside it, until what it depends on is in place:

| Setting | Description | Requires |
|---------|-------------|----------|
| **Default certificate** | The [certificate](help:certificates) the HTTPS listener serves. With none stored the picker says so and links here. | — |
| **Enable TLS (HTTPS)** | Enable the HTTPS listener | A default certificate |
| **HTTPS port** | Port for the HTTPS listener; empty means API port + 1, e.g. `:4565` | TLS enabled |
| **Redirect HTTP to HTTPS** | Redirect plain HTTP requests to the HTTPS port | TLS enabled |
