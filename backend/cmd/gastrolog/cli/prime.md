GASTROLOG LOGGING GUIDE

## What is GastroLog?

GastroLog is a clustered log aggregation service. It runs as one or more
nodes that replicate configuration via Raft consensus. Applications send
logs to it via ingesters, and users query them with a pipeline query
language. You don't need to install anything in your application — just
configure your app to send logs to a running gastrolog instance.

Even a single node runs as a one-node Raft cluster. Additional nodes join
via `--join-addr` and a join token. Vaults and ingesters are assigned to specific nodes, and queries
automatically fan out across all nodes.

## How to Send Logs

Pick the ingester that matches your deployment:

| Scenario                  | Ingester     | How to send                                      |
|---------------------------|--------------|--------------------------------------------------|
| Local app, log to file    | tail         | Write to a file, gastrolog tails it               |
| Containerized app         | docker       | gastrolog reads container streams directly        |
| HTTP push (Loki-compat)   | http         | POST JSON to /loki/api/v1/push                   |
| Syslog (RFC 3164/5424)    | syslog       | Send UDP/TCP syslog to gastrolog's listen address |
| OpenTelemetry             | otlp         | Use OTLP HTTP or gRPC exporter                   |
| Fluentd/Fluent Bit        | fluentfwd    | Forward protocol over TCP                         |
| Kafka topic               | kafka        | Produce to a Kafka topic gastrolog consumes       |
| MQTT broker               | mqtt         | Publish to an MQTT topic gastrolog subscribes to  |
| RELP (rsyslog)            | relp         | Reliable Event Logging Protocol over TCP          |

For most applications: **just write structured logs to stdout/stderr** and
let the infrastructure (Docker ingester, file tail, syslog) handle delivery.

## Log Format Best Practices

GastroLog auto-extracts structure from your logs. No special SDK needed.

1. **Use JSON or key=value format** — fields become queryable automatically:
   `{"level":"error","msg":"connection failed","host":"db-1","latency_ms":450}`
   `level=error msg="connection failed" host=db-1 latency_ms=450`

2. **Include a level/severity field** — gastrolog normalizes it for filtering:
   Recognized values: error, warn, info, debug, trace (case-insensitive)

3. **Include timestamps** — gastrolog extracts source timestamps automatically,
   but explicit fields ensure precision across time zones

4. **Plain text works too** — gastrolog indexes all tokens for full-text search.
   Structure just makes filtering and aggregation more powerful.

## Query Language Quick Reference

Basic search:
  error                          — full-text token search
  level=error                    — key=value filter
  level=error AND host=db-*      — boolean + glob
  /timeout.*connection/          — regex search
  level=error | stats count()    — pipeline aggregation

Time bounds:
  start=-1h                      — last hour
  start=2024-01-15T00:00:00Z     — absolute RFC 3339
  start=yesterday end=today      — keywords

Pipeline operators:
  | stats count() by level       — aggregate
  | where latency_ms > 500       — filter
  | eval duration=latency_ms/1000 — compute fields
  | sort -latency_ms             — sort descending
  | timechart count() by level   — time-series chart
  | head 10                      — limit results

## Ingester-Provided Attributes

These fields are added automatically by each ingester:

- **syslog**: remote_ip, facility, severity, hostname, app_name, proc_id
- **http**: stream labels (job, env, host, etc.)
- **otlp**: resource/scope attributes, trace_id, span_id, severity
- **docker**: container_id, container_name, image, stream (stdout/stderr)
- **kafka**: kafka_topic, kafka_partition, kafka_offset
- **tail**: file (absolute path)
- **fluentfwd**: tag, plus all record keys

## CLI Quick Reference

  gastrolog server               — start the service
  gastrolog config vault list    — list vaults (log stores)
  gastrolog config ingester list — list ingesters
  gastrolog config cluster health — check server health
  gastrolog config query "error" — run a query from the CLI

## Key Concepts

- **Ingester**: receives logs from an external source (syslog, HTTP, Docker, etc.)
- **Vault**: a log store. Incoming records are written to chunks within a vault.
- **Route**: connects ingesters to vaults. A route has a filter and one or more
  destination vaults. Without a route, an ingester's logs go nowhere.
- **Filter**: a match expression that controls which records a route accepts.
  Routes without a filter accept everything.
- **Rotation policy**: controls when a vault seals its active chunk and starts
  a new one (e.g., by size or age).
- **Retention policy**: controls what happens to sealed chunks after they age
  out (delete, or migrate to another vault for cold storage).
- **Chunk**: a sealed, immutable block of log records within a vault.
- **Pipeline**: a query followed by transformation operators (stats, where, eval, etc.)

## Setting Up a Log Flow

The minimal path from log source to queryable data:

  1. Create an ingester (receives logs from your application)
  2. Create a vault (stores the logs)
  3. Create a route (connects the ingester to the vault)

For production, you'll also want:

  4. Create a rotation policy (auto-seal chunks by size/age)
  5. Assign the rotation policy to the vault
  6. Create retention policies (auto-delete or migrate old chunks)
  7. Create filters if you need to split logs across vaults by content

Example — minimal setup via CLI:

  gastrolog config ingester create --name my-syslog --type syslog
  gastrolog config vault create --name app-logs --type file
  gastrolog config route create --name default --destination app-logs

Logs flowing into the syslog ingester are now routed to the app-logs vault
and immediately queryable.
