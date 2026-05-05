#!/usr/bin/env bash
#
# GastroLog cluster management.
#
# Usage:
#   ./scripts/cluster.sh <command> [options]
#
# Commands:
#   init    Bootstrap a fresh cluster (clean, enroll, configure, run)
#   run     Start an existing cluster via imux TUI (https://github.com/kluzzebass/imux)
#
# Options (or environment variables):
#   --nodes N          Number of nodes (min 1, default: GLOG_NODES or 4)
#   --data-dir DIR     Data directory (default: GLOG_DATA_DIR or /tmp/gastrolog)
#   --admin-user USER  Admin username for init (default: GLOG_ADMIN_USER or "admin")
#   --admin-pass PASS  Admin password for init (default: GLOG_ADMIN_PASS or "admin123")
#   --base-port PORT   Base HTTP port for node 1 (default: GLOG_BASE_PORT or 4564)
#   --pprof            Enable pprof on each node (ports 6060, 6061, ...)

set -euo pipefail

# --- Parse command ---

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <init|run> [options]" >&2
  exit 1
fi

COMMAND="$1"; shift

# --- Parse options ---

NODES="${GLOG_NODES:-4}"
DATA_DIR="${GLOG_DATA_DIR:-/tmp/gastrolog}"
ADMIN_USER="${GLOG_ADMIN_USER:-admin}"
ADMIN_PASS="${GLOG_ADMIN_PASS:-admin123}"
BASE_PORT="${GLOG_BASE_PORT:-4564}"
PPROF="${GLOG_PPROF:-false}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)      NODES="$2"; shift 2 ;;
    --data-dir)   DATA_DIR="$2"; shift 2 ;;
    --admin-user) ADMIN_USER="$2"; shift 2 ;;
    --admin-pass) ADMIN_PASS="$2"; shift 2 ;;
    --base-port)  BASE_PORT="$2"; shift 2 ;;
    --pprof)      PPROF=true; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ "$NODES" -lt 1 ]]; then
  echo "Error: at least 1 node required" >&2
  exit 1
fi

# --- Helpers ---

GLOG="go run ./cmd/gastrolog"

http_port()    { echo $((BASE_PORT + ($1 - 1) * 10)); }
cluster_port() { echo $((BASE_PORT + ($1 - 1) * 10 + 2)); }
node_dir()     { echo "${DATA_DIR}/node${1}"; }
node_sock()    { echo "${DATA_DIR}/node${1}/gastrolog.sock"; }

# Build the imux command for running all nodes (no join flags).
build_imux_cmd() {
  local names=""
  local cmds=()
  for i in $(seq 1 "$NODES"); do
    if [[ -n "$names" ]]; then names="${names},"; fi
    names="${names}node${i}"
    local extra=""
    if [[ "$PPROF" == true ]]; then
      extra=" --pprof localhost:$((6059 + i))"
    fi
    cmds+=("$GLOG server --home $(node_dir "$i") --listen :$(http_port "$i") --cluster-addr :$(cluster_port "$i")${extra}")
  done
  # TUI: plain `imux` (flags + commands). `imux run` is non-interactive batch mode.
  echo "imux --name ${names} --tee ${DATA_DIR}/cluster.log $(printf ' "%s"' "${cmds[@]}")"
}

# --- Init: enroll nodes ---

enroll_nodes() {
  local PIDS=()

  # Cleanup nukes both the go-run wrapper PIDs we spawned AND any
  # gastrolog binary processes rooted at this cluster's data dir
  # (go run forks a compiled child that survives a SIGINT to the
  # wrapper alone). Wired to EXIT/INT/TERM so any failure path —
  # not just the timeout — leaves a clean process slate.
  cleanup() {
    for pid in "${PIDS[@]}"; do
      kill -INT "$pid" 2>/dev/null || true
    done
    pkill -INT -f "gastrolog.*--home ${DATA_DIR}/node" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
    sleep 1
    pkill -KILL -f "gastrolog.*--home ${DATA_DIR}/node" 2>/dev/null || true
  }
  trap 'cleanup; trap - EXIT INT TERM' EXIT INT TERM

  echo ">>> Cleaning ${DATA_DIR}..."
  for i in $(seq 1 "$NODES"); do
    rm -rf "$(node_dir "$i")"
  done
  rm -f "${DATA_DIR}/cluster-token"
  rm -f "${DATA_DIR}"/init-*.log
  mkdir -p "${DATA_DIR}"

  if [[ "$NODES" -eq 1 ]]; then
    # Single node: just start, wait for socket, done.
    echo ">>> Starting single node..."
    $GLOG server \
      --name "node-1" \
      --home "$(node_dir 1)" \
      --listen ":$(http_port 1)" \
      --cluster-addr ":$(cluster_port 1)" > "${DATA_DIR}/init-1.log" 2>&1 &
    PIDS+=($!)

    for _ in $(seq 1 60); do
      [[ -S "$(node_sock 1)" ]] && break
      sleep 0.5
    done
    return
  fi

  # Start node 1 and extract join token. tee duplicates the output so we
  # can both log it to init-1.log and scan for the token line.
  echo ">>> Starting node 1..."
  $GLOG server \
    --name "node-1" \
    --home "$(node_dir 1)" \
    --listen ":$(http_port 1)" \
    --cluster-addr ":$(cluster_port 1)" 2>&1 | tee "${DATA_DIR}/init-1.log" | while IFS= read -r line; do
      if [[ "$line" == *"cluster join token"*"token="* ]]; then
        token="${line##*token=}"
        token="${token%% *}"
        if [[ -n "$token" ]]; then
          echo "$token" > "${DATA_DIR}/cluster-token"
        fi
      fi
    done &
  PIDS+=($!)

  echo ">>> Waiting for join token..."
  for _ in $(seq 1 60); do
    [[ -f "${DATA_DIR}/cluster-token" ]] && break
    sleep 0.5
  done
  if [[ ! -f "${DATA_DIR}/cluster-token" ]]; then
    echo ">>> Error: timed out waiting for join token" >&2
    cleanup
    exit 1
  fi
  local TOKEN
  TOKEN=$(cat "${DATA_DIR}/cluster-token")
  echo ">>> Join token acquired."

  # Start and enroll nodes 2..N.
  for i in $(seq 2 "$NODES"); do
    echo ">>> Enrolling node ${i}..."
    $GLOG server \
      --name "node-${i}" \
      --home "$(node_dir "$i")" \
      --listen ":$(http_port "$i")" \
      --cluster-addr ":$(cluster_port "$i")" \
      --join-addr "localhost:$(cluster_port 1)" \
      --join-token "$TOKEN" > "${DATA_DIR}/init-${i}.log" 2>&1 &
    PIDS+=($!)
  done

  # Wait for all sockets.
  echo ">>> Waiting for nodes to be ready..."
  for i in $(seq 1 "$NODES"); do
    for _ in $(seq 1 60); do
      [[ -S "$(node_sock "$i")" ]] && break
      sleep 0.5
    done
  done
  sleep 2
}

# --- Init: configure ---

configure() {
  local S
  S="$(node_sock 1)"

  echo ">>> Registering admin user..."
  $GLOG register --addr "http://localhost:$(http_port 1)" \
    --username "$ADMIN_USER" --password "$ADMIN_PASS" 2>&1 | sed 's/^/  /'

  echo ">>> Creating file storage on each node..."
  for i in $(seq 1 "$NODES"); do
    $GLOG config node add-storage --addr "$S" \
      "node-${i}" --name "disk-1" --storage-class 1 --path "storage/disk-1" 2>&1 | sed 's/^/  /'
  done

  echo ">>> Creating cloud service..."
  $GLOG config cloud-service create --addr "$S" \
    --name "S3" --provider s3 --bucket gastrolog --region gastrolog \
    --endpoint "localhost:9000" --access-key gastrolog --secret-key gastrolog 2>&1 | sed 's/^/  /'

  echo ">>> Creating policies..."
  $GLOG config rotation-policy create --addr "$S" --name "1m-rotate" --max-age 1m 2>&1 | sed 's/^/  /'
  $GLOG config rotation-policy create --addr "$S" --name "100-rows" --max-records 100 2>&1 | sed 's/^/  /'
  $GLOG config retention-policy create --addr "$S" --name "3m-retain" --max-age 3m 2>&1 | sed 's/^/  /'

  echo ">>> Creating filter..."
  $GLOG config filter create --addr "$S" --name "catch-all" --expression "*" 2>&1 | sed 's/^/  /'

  echo ">>> Creating vault..."
  $GLOG config vault create --addr "$S" --name "default-vault" 2>&1 | sed 's/^/  /'

  echo ">>> Creating tiers..."
  $GLOG config tier create --addr "$S" --vault "default-vault" \
    --name "hot" --type file --rotation-policy "100-rows" --retention-policy "3m-retain" \
    --replication-factor "$NODES" --storage-class 1 2>&1 | sed 's/^/  /'
  $GLOG config tier create --addr "$S" --vault "default-vault" \
    --name "warm" --type file --rotation-policy "100-rows" --retention-policy "3m-retain" \
    --replication-factor "$NODES" --storage-class 1 2>&1 | sed 's/^/  /'
  $GLOG config tier create --addr "$S" --vault "default-vault" \
    --name "cold" --type file --rotation-policy "100-rows" \
    --cloud-service "S3" --storage-class 1 \
    --replication-factor "$NODES" 2>&1 | sed 's/^/  /'

  echo ">>> Creating route..."
  $GLOG config route create --addr "$S" \
    --name "default" --filter "catch-all" --destination "default-vault" 2>&1 | sed 's/^/  /'

  echo ">>> Creating ingesters (disabled)..."
  local NODE_IDS=()
  local node_json
  node_json=$($GLOG config node list --addr "$S" -o json 2>/dev/null)
  for i in $(seq 1 "$NODES"); do
    local nid
    nid=$(echo "$node_json" | jq -r ".[] | select(.name == \"node-${i}\") | .id")
    if [[ -n "$nid" ]]; then
      NODE_IDS+=("$nid")
    fi
  done
  local CHATTER_NODE SCATTER_NODE
  CHATTER_NODE="${NODE_IDS[$((RANDOM % ${#NODE_IDS[@]}))]}"
  SCATTER_NODE="${NODE_IDS[$((RANDOM % ${#NODE_IDS[@]}))]}"
  $GLOG config ingester create --addr "$S" \
    --name "chatterbox" --type chatterbox --node-id "$CHATTER_NODE" --enabled=false 2>&1 | sed 's/^/  /'
  $GLOG config ingester create --addr "$S" \
    --name "scatterbox" --type scatterbox --node-id "$SCATTER_NODE" --enabled=false 2>&1 | sed 's/^/  /'
}

# --- Main ---

case "$COMMAND" in
  init)
    enroll_nodes
    configure

    # Shut down enrollment processes.
    echo ">>> Stopping nodes..."
    # Kill both go-run wrappers and the actual gastrolog binaries.
    pkill -INT -f "gastrolog.*--home ${DATA_DIR}/node" 2>/dev/null || true
    sleep 3
    # Force-kill any stragglers.
    pkill -KILL -f "gastrolog.*--home ${DATA_DIR}/node" 2>/dev/null || true
    sleep 1

    echo ""
    echo ">>> Cluster bootstrapped!"
    echo "    Nodes:    ${NODES}"
    echo "    Data dir: ${DATA_DIR}"
    echo "    Admin:    ${ADMIN_USER}/${ADMIN_PASS}"
    echo "    Run with: $0 run --nodes ${NODES} --data-dir ${DATA_DIR}"
    ;;
  run)
    # imux --tee appends. Fresh log each run (truncate) keeps one run per file.
    rm -f "${DATA_DIR}/cluster.log"
    eval "$(build_imux_cmd)"
    ;;
  *)
    echo "Unknown command: $COMMAND" >&2
    echo "Usage: $0 <init|run> [options]" >&2
    exit 1
    ;;
esac
