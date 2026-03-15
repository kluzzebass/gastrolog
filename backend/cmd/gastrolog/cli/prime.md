GASTROLOG — AI AGENT PRIMER

Clustered log aggregation service. Raft consensus for config replication,
local vault storage, cross-node query fan-out. Single binary, embedded web UI.

Every command supports --help for full flag details.

═══════════════════════════════════════════════════
ENTITIES
═══════════════════════════════════════════════════

  Ingester   Receives logs (syslog, HTTP, OTLP, Docker, tail, Kafka, MQTT, RELP, Fluent Forward)
  Vault      Stores logs in time-ordered chunks (file or memory backend)
  Route      Connects ingesters → vaults. Without a route, logs are dropped.
  Filter     Match expression for routes. Routes without a filter accept everything.
  Rotation   Policy for when to seal chunks (size, age, cron)
  Retention  Policy for when to expire/migrate chunks (age, size, count)
  Node       Cluster member. Auto-created on join.

  Data flow: Ingester → Route (filter?) → Vault → Chunks → Indexes → Queryable

  All entities are managed via `gastrolog config <entity> <action>`.
  All entities accept --help: `gastrolog config vault create --help`

═══════════════════════════════════════════════════
CLI CONNECTION
═══════════════════════════════════════════════════

  The CLI connects to a running gastrolog server. All commands that talk
  to the server accept --addr, --token, and --home flags.

  1. Unix socket (default, no auth needed):
     Located at <home>/gastrolog.sock
     Default home: ~/.config/gastrolog (Linux), ~/Library/Application Support/gastrolog (macOS)
     Override with: --home /path/to/home

  2. TCP (when --addr is set or unix socket unavailable):
     gastrolog --addr http://host:4564 config vault list
     gastrolog --addr http://host:4564 cluster status

  Authentication (TCP only — unix socket bypasses auth):
     gastrolog --token <JWT> config vault list
     # or
     export GASTROLOG_TOKEN=<JWT>

  To authenticate remotely, log in to get a JWT:
     gastrolog login --addr http://host:4564 --username admin --password <pw>
     # → prints a JWT to stdout

     export GASTROLOG_TOKEN=$(gastrolog login --username admin --password <pw>)

  On the same machine, the unix socket bypasses auth entirely. This means
  you can create users, manage config, and run queries without a token:
     gastrolog user create --username admin --password <pw> --role admin

  First user (no auth required, only works when no users exist):
     gastrolog register --username admin --password <pw>
     # → prints a JWT token for immediate use

  After the first user, `user create` requires admin auth (or unix socket).

═══════════════════════════════════════════════════
DEPENDENCY ORDER
═══════════════════════════════════════════════════

  1. Vault must exist before a route can reference it
  2. Filter must exist before a route can reference it
  3. Route must exist for logs to flow from ingester to vault
  4. Ingester can exist without a route, but logs will be dropped

  Delete constraints:
  - Vault: blocked if referenced by a route destination
  - Filter: blocked if referenced by a route
  - Rotation/retention policy: auto-cleared from vaults on delete

═══════════════════════════════════════════════════
SERVER
═══════════════════════════════════════════════════

  # Start a single node (auto-bootstraps as 1-node Raft cluster)
  gastrolog server

  # Start with test data (memory vault + chatterbox ingester)
  gastrolog server --bootstrap

  # Start on custom ports
  gastrolog server --listen :8080 --cluster-addr :8081

  # Disable auth (dev/testing)
  gastrolog server --no-auth

═══════════════════════════════════════════════════
CLUSTERING
═══════════════════════════════════════════════════

  Every node is a single-node cluster on first start. Join others to form
  a multi-node cluster. 3 voters recommended (tolerates 1 failure).

  # Get the join token from node 1 (also printed in its startup log)
  gastrolog cluster join-token

  # Join node 2 to node 1's cluster
  gastrolog server --join-addr node1:4566 --join-token <TOKEN>

  # Join as read-only replica (no vote, no leader eligibility)
  gastrolog server --join-addr node1:4566 --join-token <TOKEN> --voteless

  # List nodes
  gastrolog config node list

  WARNING: Joining replaces the joining node's local config with the
  cluster's replicated state. Vault data files on disk survive.

═══════════════════════════════════════════════════
VAULTS
═══════════════════════════════════════════════════

  Types: file (persistent, on disk), memory (volatile, for testing)

  gastrolog config vault list
  gastrolog config vault create --name app-logs --type file
  gastrolog config vault create --name ephemeral --type memory
  gastrolog config vault create --help    # full flag list
  gastrolog config vault delete app-logs
  gastrolog config vault seal app-logs    # seal active chunk
  gastrolog config vault reindex app-logs # rebuild indexes

  Vaults are node-scoped. Created on the node handling the request.

═══════════════════════════════════════════════════
INGESTERS
═══════════════════════════════════════════════════

  Types: syslog, http, otlp, tail, docker, fluentfwd, kafka, mqtt, relp,
         chatterbox (test data), metrics (self-monitoring), self (internal logs)

  gastrolog config ingester list
  gastrolog config ingester create --name my-syslog --type syslog --param udp_addr=:514
  gastrolog config ingester create --name my-http --type http --param addr=:3100
  gastrolog config ingester create --name my-tail --type tail --param 'paths=["/var/log/app.log"]'
  gastrolog config ingester create --name my-docker --type docker
  gastrolog config ingester create --name my-otlp --type otlp
  gastrolog config ingester create --name my-kafka --type kafka --param brokers=localhost:9092 --param topic=logs
  gastrolog config ingester create --name my-mqtt --type mqtt --param broker=tcp://localhost:1883 --param topics=logs/#
  gastrolog config ingester create --help    # full flag list
  gastrolog config ingester test --type syslog --param udp_addr=:514  # test without creating
  gastrolog config ingester delete my-syslog

  Ingesters are node-scoped. Each type has its own params — use --help.

═══════════════════════════════════════════════════
ROUTES
═══════════════════════════════════════════════════

  gastrolog config route list
  gastrolog config route create --name default --destination app-logs
  gastrolog config route create --name errors --filter errors-only --destination error-vault
  gastrolog config route create --name balanced --destination vault-1 --destination vault-2 --distribution round-robin
  gastrolog config route create --help    # full flag list
  gastrolog config route delete default

  Distribution modes: fanout (all destinations), round-robin (rotate), failover (first available)
  Routes without a --filter accept all records.

═══════════════════════════════════════════════════
FILTERS
═══════════════════════════════════════════════════

  gastrolog config filter create --name errors-only --expression 'level=error'
  gastrolog config filter create --name web-traffic --expression 'service=web AND status>=400'
  gastrolog config filter delete errors-only

  Expressions use the query language: key=value, AND, OR, NOT, >=, <=, globs, regex.

═══════════════════════════════════════════════════
POLICIES
═══════════════════════════════════════════════════

  # Rotation: when to seal chunks
  gastrolog config rotation-policy create --name hourly-100mb --max-bytes 104857600 --max-age 1h
  gastrolog config rotation-policy create --name cron-midnight --cron '0 0 * * *'

  # Retention: when to expire/migrate old chunks
  gastrolog config retention-policy create --name keep-30d --max-age 720h
  gastrolog config retention-policy create --name max-10gb --max-bytes 10737418240

  Assign policies to vaults via the UI or vault update.

═══════════════════════════════════════════════════
QUERYING LOGS
═══════════════════════════════════════════════════

  gastrolog query 'level=error last=5m'
  gastrolog query 'status>=500 AND path=/api' --last 1h
  gastrolog query 'level=error' --count
  gastrolog query 'level=error' --explain
  gastrolog query 'level=error | stats count by host' --format table

  Output formats (--format):
    text     Human-readable, colored (default for TTY)
    json     JSONL/NDJSON, one object per line (default for pipes)
    csv      Header row + data rows
    raw      Raw log body only, one per line
    table    Columnar output for pipeline/stats results

  Flags:
    --last 5m          Time range shorthand
    --start/--end      Explicit time bounds (RFC3339)
    --limit 100        Cap output (agents need bounded output)
    --fields a,b,c     Select fields for JSON/CSV
    --count            Print count only, no records
    --explain          Print query plan, don't execute
    -r, --reverse      Newest first

  Exit codes: 0 = results found, 1 = no results, 2 = error
  Errors go to stderr, data to stdout — safe for piping.

  When stdout is not a TTY, format defaults to json (JSONL).
  Pipe to jq: gastrolog query 'level=error' | jq .attrs

═══════════════════════════════════════════════════
COMMON TASKS
═══════════════════════════════════════════════════

  --- Minimal log flow (3 commands) ---
  gastrolog config vault create --name logs --type file
  gastrolog config ingester create --name syslog-in --type syslog --param udp_addr=:514
  gastrolog config route create --name default --destination logs

  --- Split errors to separate vault ---
  gastrolog config vault create --name error-logs --type file
  gastrolog config filter create --name errors --expression 'level=error'
  gastrolog config route create --name error-route --filter errors --destination error-logs

  --- Docker container logging ---
  gastrolog config vault create --name containers --type file
  gastrolog config ingester create --name docker --type docker
  gastrolog config route create --name docker-route --destination containers

  --- Export/import config (backup or migration) ---
  gastrolog config export > config.json
  gastrolog config import < config.json

═══════════════════════════════════════════════════
LOG FORMAT ADVICE
═══════════════════════════════════════════════════

  Use JSON or key=value — fields become queryable automatically:
    {"level":"error","msg":"connection failed","host":"db-1","latency_ms":450}
    level=error msg="connection failed" host=db-1 latency_ms=450

  Include a level field (error/warn/info/debug/trace).
  Plain text works too — all tokens are indexed for full-text search.
  No SDK needed. Just write to stdout and let the ingester handle delivery.
