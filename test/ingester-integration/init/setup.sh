#!/bin/sh
# Configure gastrolog ingesters for the integration test environment.
# Run from the host after gastrolog and the compose services are up.
#
# Idempotent: skips ingesters whose name already exists in the config.
# Does NOT create filters, routes, or vaults — configure those via the UI.
#
# Usage:
#   ./setup.sh <path-to-gastrolog.sock>
#
# Uses the Unix socket (no auth required). Detects the local node ID
# and assigns all ingesters to it.

set -eu

SOCK="${1:-}"
if [ -z "$SOCK" ]; then
  echo "Usage: $0 <path-to-gastrolog.sock>" >&2
  echo "  e.g. $0 data/node1/gastrolog.sock" >&2
  exit 1
fi

if [ ! -S "$SOCK" ]; then
  echo "ERROR: $SOCK is not a socket. Is gastrolog running?" >&2
  exit 1
fi

CT="Content-Type: application/json"

api() {
  curl -s --fail-with-body --unix-socket "$SOCK" -X POST -H "$CT" "http://localhost$1" -d "${2:-{}}"
}

# Detect the local node ID.
cluster=$(api "/gastrolog.v1.LifecycleService/GetClusterStatus")
NODE_ID=$(echo "$cluster" | sed -n 's/.*"localNodeId":"\([^"]*\)".*/\1/p')
if [ -z "$NODE_ID" ]; then
  echo "ERROR: could not detect local node ID" >&2
  exit 1
fi

echo "Configuring ingesters via $SOCK (node $NODE_ID)..."

# Snapshot current config to detect existing ingesters by name.
CFG=$(api "/gastrolog.v1.ConfigService/GetConfig")

put_ingester() {
  name="$1"
  body="$2"
  if echo "$CFG" | grep -q "\"name\":\"$name\""; then
    echo "  skip  $name (already exists)"
  else
    if ! resp=$(api "/gastrolog.v1.ConfigService/PutIngester" "$body" 2>&1); then
      echo "  FAIL  $name" >&2
      echo "        $resp" >&2
      exit 1
    fi
    echo "  ok    $name"
  fi
}

put_ingester "kafka"     "{\"config\": {\"id\": \"00000000-0000-4000-8000-000000000011\", \"name\": \"kafka\",     \"type\": \"kafka\",     \"enabled\": true, \"nodeId\": \"$NODE_ID\", \"params\": {\"brokers\": \"localhost:9092\", \"topic\": \"gastrolog-logs\", \"group\": \"gastrolog\"}}}"
put_ingester "syslog"    "{\"config\": {\"id\": \"00000000-0000-4000-8000-000000000012\", \"name\": \"syslog\",    \"type\": \"syslog\",    \"enabled\": true, \"nodeId\": \"$NODE_ID\", \"params\": {\"tcp_addr\": \":1514\"}}}"
put_ingester "relp"      "{\"config\": {\"id\": \"00000000-0000-4000-8000-000000000013\", \"name\": \"relp\",      \"type\": \"relp\",      \"enabled\": true, \"nodeId\": \"$NODE_ID\", \"params\": {\"addr\": \":2514\"}}}"
put_ingester "otlp"      "{\"config\": {\"id\": \"00000000-0000-4000-8000-000000000014\", \"name\": \"otlp\",      \"type\": \"otlp\",      \"enabled\": true, \"nodeId\": \"$NODE_ID\", \"params\": {\"grpc_addr\": \":4317\", \"http_addr\": \":4318\"}}}"
put_ingester "fluentfwd" "{\"config\": {\"id\": \"00000000-0000-4000-8000-000000000015\", \"name\": \"fluentfwd\", \"type\": \"fluentfwd\", \"enabled\": true, \"nodeId\": \"$NODE_ID\", \"params\": {\"addr\": \":24224\"}}}"
put_ingester "http"      "{\"config\": {\"id\": \"00000000-0000-4000-8000-000000000016\", \"name\": \"http\",      \"type\": \"http\",      \"enabled\": true, \"nodeId\": \"$NODE_ID\", \"params\": {\"addr\": \":3100\"}}}"

# Detect Docker socket.
DOCKER_HOST="unix:///var/run/docker.sock"
if [ -S "$HOME/.docker/run/docker.sock" ]; then
  DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"
fi
put_ingester "docker"    "{\"config\": {\"id\": \"00000000-0000-4000-8000-000000000017\", \"name\": \"docker\",    \"type\": \"docker\",    \"enabled\": true, \"nodeId\": \"$NODE_ID\", \"params\": {\"host\": \"$DOCKER_HOST\", \"poll_interval\": \"10s\", \"tls\": \"false\", \"stdout\": \"true\", \"stderr\": \"true\"}}}"

echo "Done — 7 ingesters on node $NODE_ID."
