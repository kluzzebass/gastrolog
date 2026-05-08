# Bundled dependencies

The compose, swarm, podman, and Kubernetes deployment recipes bring up
supporting services — MinIO, Azurite, fake-gcs, Kafka, rsyslog, OTel
collector, fluent-bit — alongside the gastrolog cluster, then auto-
wire a full operational config that mirrors what
[`scripts/cluster.sh`](../../scripts/cluster.sh) sets up for the dev
cluster: per-node file storage, three CloudServices, rotation /
retention policies, a hot→warm vault chain, routes, and the standard
ingester listeners.

The wiring lives in [`deploy/setup-deps.sh`](../../deploy/setup-deps.sh)
and is invoked from each `*-up` recipe via `docker exec` /
`kubectl exec` against the bootstrap container's unix socket
(`unix:///config/gastrolog.sock` — no auth needed). The script is
idempotent: every config-create CLI is upsert-by-name, so re-running
just refreshes the same entries.

## What gets created

### Storage

| Kind | Name | Pointing at |
|---|---|---|
| File storage (per node) | `disk-1` | `/vaults/storage/disk-1` on each node, storage class 1 |
| CloudService | `minio` | `s3` provider, `http://minio:9000`, bucket `gastrolog`, region `gastrolog` |
| CloudService | `azurite` | `azure` provider, default Azurite credentials, container `gastrolog` |
| CloudService | `fake-gcs` | `gcs` provider, `http://fake-gcs:4443/storage/v1/`, bucket `gastrolog` |

### Policies

| Kind | Name | Setting |
|---|---|---|
| Rotation | `1m-rotate` | `--max-age 1m` |
| Rotation | `100-rows` | `--max-records 100` |
| Retention | `3m-retain` | `--max-age 3m` |

### Vaults (hot → warm chain)

| Vault | Type | Storage | Replication | Rotation | Retention | Disposition | Cloud |
|---|---|---|---|---|---|---|---|
| `hot-vault` | file | class 1 (disk-1) | RF = node count | `100-rows` | `3m-retain` | `route` | — |
| `warm-vault` | file | class 1 (disk-1) | RF = node count | `100-rows` | (none) | (n/a) | `minio` |

When a chunk in `hot-vault` ages past 3 minutes, the retention sweep
streams its records back through the routing engine (rather than
deleting them — that's what `retention-disposition: route` controls).
The records are tagged with `_source = "retention"` and
`_vault = "<hot-id>"`; the `hot-retention-to-warm` route catches them
and lands them in `warm-vault` before the original chunk is destroyed.

### Routes

| Name | Match expression | Destination |
|---|---|---|
| `ingest-to-hot` | `_source = "ingest"` | `hot-vault` |
| `hot-retention-to-warm` | `_source = "retention" AND _vault = "<hot-vault-glid>"` | `warm-vault` |

### Ingesters

All assigned to **all nodes** (no node pinning), all **disabled by
default**. Enable from the UI when you want the cluster to actually
start consuming.

| Name | Type | Listener |
|---|---|---|
| `kafka` | kafka | `kafka:9092`, topic `gastrolog-logs`, group `gastrolog` |
| `syslog` | syslog | TCP `:1514` (skipped on Swarm) |
| `relp` | relp | `:2514` (skipped on Swarm) |
| `otlp` | otlp | gRPC `:4317`, HTTP `:4318` |
| `fluentfwd` | fluentfwd | `:24224` |
| `http` | http | `:3100` (Loki HTTP push) |
| `chatterbox` | chatterbox | synthetic generator |
| `scatterbox` | scatterbox | synthetic generator |

The bundled push services (Kafka producer, rsyslog, OTel collector,
fluent-bit) are running and generating traffic from the moment the
cluster comes up — but nothing reaches `hot-vault` until you flip
the matching ingester to enabled. That gives you a clean baseline
with zero records, and you can enable one ingester at a time to
isolate where data is coming from.

## What does NOT get created

Nothing. The auto-wire is the full hot/warm chain. To start fresh from
zero config, tear the deployment down with the matching `*-down`
recipe (which wipes volumes / PVCs / Swarm state) and re-`up` it.

## Uncontainerized

`just deploy uncontainerized-up` runs a single `go run` of the binary
with no companion deps and no auto-wiring (single-node, no DNS to in-
cluster service names). For the dev cluster, use
[`scripts/cluster.sh init`](../../scripts/cluster.sh) (which
`just backend cluster-init` wraps) — that sets up the same hot/warm
chain against `localhost:*` instead of in-cluster service names.
