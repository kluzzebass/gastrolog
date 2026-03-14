#!/bin/sh
# Configure gastrolog ingesters for the integration test environment.
# Run from the host after gastrolog and the compose services are up.
#
# Idempotent: skips ingesters whose name already exists in the config.
# Does NOT create filters, routes, or vaults — configure those via the UI.
#
# Usage:
#   ./setup.sh <path-to-gastrolog.sock> [gastrolog-binary]
#
# Examples:
#   ./setup.sh backend/data/node1/gastrolog.sock
#   ./setup.sh backend/data/node1/gastrolog.sock ./backend/gastrolog

set -eu

SOCK="${1:-}"
GLOG="${2:-gastrolog}"

if [ -z "$SOCK" ]; then
  echo "Usage: $0 <path-to-gastrolog.sock> [gastrolog-binary]" >&2
  echo "  e.g. $0 backend/data/node1/gastrolog.sock" >&2
  exit 1
fi

if [ ! -S "$SOCK" ]; then
  echo "ERROR: $SOCK is not a socket. Is gastrolog running?" >&2
  exit 1
fi

glog() {
  "$GLOG" --addr "$SOCK" "$@"
}

# Detect the local node ID.
NODE_ID=$(glog cluster status -o json | sed -n 's/.*"local_node_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
if [ -z "$NODE_ID" ]; then
  echo "ERROR: could not detect local node ID" >&2
  exit 1
fi

echo "Configuring ingesters on node $NODE_ID..."

# Snapshot current ingester names for idempotency.
EXISTING=$(glog config ingester list -o json)

create_ingester() {
  name="$1"
  shift
  if echo "$EXISTING" | grep -q "\"name\":.*\"$name\""; then
    echo "  skip  $name (already exists)"
  else
    if ! glog config ingester create --name "$name" --node-id "$NODE_ID" "$@" > /dev/null 2>&1; then
      echo "  FAIL  $name" >&2
      exit 1
    fi
    echo "  ok    $name"
  fi
}

create_ingester kafka     --type kafka     --param brokers=localhost:9092 --param topic=gastrolog-logs --param group=gastrolog
create_ingester syslog    --type syslog    --param tcp_addr=:1514
create_ingester relp      --type relp      --param addr=:2514
create_ingester otlp      --type otlp      --param grpc_addr=:4317 --param http_addr=:4318
create_ingester fluentfwd --type fluentfwd --param addr=:24224
create_ingester http      --type http      --param addr=:3100

# Detect Docker socket.
DOCKER_HOST="unix:///var/run/docker.sock"
if [ -S "$HOME/.docker/run/docker.sock" ]; then
  DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"
fi
create_ingester docker --type docker --param "host=$DOCKER_HOST" --param poll_interval=10s --param tls=false --param stdout=true --param stderr=true

echo "Done — 7 ingesters on node $NODE_ID."
