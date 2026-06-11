# Bundled dependencies

The compose, swarm, podman, and Kubernetes deployment recipes bring up
supporting services — MinIO, Azurite, fake-gcs, Kafka, rsyslog, OTel
collector, fluent-bit — alongside the gastrolog cluster, then auto-
wire a full operational config that mirrors what
[`scripts/cluster.sh`](../../scripts/cluster.sh) sets up for the dev
cluster: per-node file storage, three CloudServices, rotation /
retention policies, a first→second vault chain, routes, and the standard
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
| Rotation | `100-records` | `--max-records 100` |
| Rotation | `10000-records` | `--max-records 10000` |
| Retention | `3m-retain` | `--max-age 3m` |
| Retention | `1h-retain` | `--max-age 1h` |

### Vaults (first → second chain)

| Vault | Type | Storage | Replication | Rotation | Retention | Disposition | Cloud |
|---|---|---|---|---|---|---|---|
| `first-vault` | file | class 1 (disk-1) | RF = node count | `10000-records` | `3m-retain` | `route` | — |
| `second-vault` | file | class 1 (disk-1) | RF = node count | `10000-records` | (none) | (n/a) | `minio` |

When a chunk in `first-vault` ages past 3 minutes, the retention sweep
streams its records back through the routing engine (rather than
deleting them — that's what `retention-disposition: route` controls).
The records are tagged with `_source = "retention"` and
`_vault = "<first-vault-id>"`; the `first-retention-to-second` route catches them
and lands them in `second-vault` before the original chunk is destroyed.

### Routes

| Name | Match expression | Destination |
|---|---|---|
| `ingest-to-first` | `_source = "ingest"` | `first-vault` |
| `first-retention-to-second` | `_source = "retention" AND _vault = "<first-vault-glid>"` | `second-vault` |

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
cluster comes up — but nothing reaches `first-vault` until you flip
the matching ingester to enabled. That gives you a clean baseline
with zero records, and you can enable one ingester at a time to
isolate where data is coming from.

## What does NOT get created

Nothing. The auto-wire is the full first→second chain. To start fresh from
zero config, tear the deployment down with the matching `*-down`
recipe (which wipes volumes / PVCs / Swarm state) and re-`up` it.

## Uncontainerized

If you run gastrolog nodes directly on hosts (no compose/k8s), use
[`scripts/cluster.sh`](../../scripts/cluster.sh) instead — same chain,
different CloudService name (`S3` vs `minio`) and data layout.
