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
#   --auth             Enable JWT auth (default: --no-auth for local dev clusters)
#   --base-port PORT   Base HTTP port for node 1 (default: GLOG_BASE_PORT or 4564)
#   --pprof            Enable pprof on each node (ports 6060, 6061, ...)
#   GLOG_NO_AUTH       Disable auth when truthy (default: true). Set false/0 to require login.

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
NO_AUTH="${GLOG_NO_AUTH:-true}"
BASE_PORT="${GLOG_BASE_PORT:-4564}"
PPROF="${GLOG_PPROF:-false}"
# Environment banner (gastrolog-4vr0l). Tags every node in this cluster as
# the local dev deployment in the UI header so operators don't confuse it
# with a K8s/staging instance. Single token only (no spaces).
ENV_LABEL="${GLOG_ENV_LABEL:-Development}"
ENV_COLOR="${GLOG_ENV_COLOR:-limegreen}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)      NODES="$2"; shift 2 ;;
    --data-dir)   DATA_DIR="$2"; shift 2 ;;
    --admin-user) ADMIN_USER="$2"; shift 2 ;;
    --admin-pass) ADMIN_PASS="$2"; shift 2 ;;
    --auth)       NO_AUTH=false; shift ;;
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

no_auth_enabled() {
  [[ "$NO_AUTH" == true || "$NO_AUTH" == "1" || "$NO_AUTH" == "yes" || "$NO_AUTH" == "y" || "$NO_AUTH" == "on" ]]
}

# env_flags emits optional server flags from cluster env (banner, auth, etc.).
# Empty/unset values produce no output for that flag.
env_flags() {
  local flags=""
  [[ -n "$ENV_LABEL" ]] && flags=" --environment-label $ENV_LABEL"
  [[ -n "$ENV_COLOR" ]] && flags="${flags} --environment-color $ENV_COLOR"
  if no_auth_enabled; then
    flags="${flags} --no-auth"
  fi
  echo "$flags"
}

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
    cmds+=("$GLOG server --home $(node_dir "$i") --listen :$(http_port "$i") --cluster-addr :$(cluster_port "$i")${extra}$(env_flags)")
  done
  # TUI: plain `imux` (flags + commands). `imux run` is non-interactive batch mode.
  echo "imux --name ${names} --tee ${DATA_DIR}/cluster.log $(printf ' "%s"' "${cmds[@]}")"
}

# --- Init: dependency preflight ---

# check_dependencies probes the external services the bootstrap depends on.
# Currently the only hard dependency is the S3-compatible object store at
# localhost:9000, which the cloud-vault setup needs for cloud-backed
# placement. Without it, the cloud-vault placement step blocks each node
# in a 3-attempt S3 retry loop (~5s/node) and leaves nodes hung in retry
# state at shutdown — the script appears to "take a loooong time" then
# emits Killed: 9 messages from the SIGKILL fallback. Failing here with
# a clear message is much better than that. See gastrolog-18du3.
check_dependencies() {
  local s3_host="localhost"
  local s3_port="9000"
  # Use bash's /dev/tcp pseudo-device so we don't depend on nc/curl/etc
  # being installed; works on macOS and Linux out of the box.
  if ! (exec 3<>/dev/tcp/${s3_host}/${s3_port}) 2>/dev/null; then
    echo "Error: S3-compatible service not reachable at ${s3_host}:${s3_port}." >&2
    echo "       The bootstrap creates a cloud-backed cloud-vault that needs" >&2
    echo "       MinIO (or another S3 emulator) running on this port." >&2
    echo "       Start the local cloud emulators and try again:" >&2
    echo "         just cloud-storage-up" >&2
    exit 1
  fi
  # Close the probe FD; success means the dial completed.
  exec 3>&- 2>/dev/null || true
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
      --cluster-addr ":$(cluster_port 1)" $(env_flags) > "${DATA_DIR}/init-1.log" 2>&1 &
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
    --cluster-addr ":$(cluster_port 1)" $(env_flags) 2>&1 | tee "${DATA_DIR}/init-1.log" | while IFS= read -r line; do
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
      --join-token "$TOKEN" $(env_flags) > "${DATA_DIR}/init-${i}.log" 2>&1 &
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
  if no_auth_enabled; then
    echo "  (skipped — cluster runs with --no-auth)"
  else
    $GLOG register --addr "http://localhost:$(http_port 1)" \
      --username "$ADMIN_USER" --password "$ADMIN_PASS" 2>&1 | sed 's/^/  /'
  fi

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

  # gastrolog-4kkoo (Phase 5): no filter entity — match expressions live
  # inline on routes via --expression. Synthetic attributes (_source,
  # _vault) tag the record's origin at routing-eval time and let one
  # route distinguish "from an ingester" from "from a retention sweep".

  echo ">>> Creating vaults..."
  # Two-vault local→cloud chain wired via inter-vault routing (gastrolog-4kkoo).
  #   - local-vault: file-backed on local disk, 100-row rotation, 3-minute
  #                  retention. Chunks past their TTL fire the retention
  #                  sweep, which streams their records back through the
  #                  routing engine.
  #   - cloud-vault: file-backed but cloud-served (S3). 100-row rotation
  #                  carries over so chunk granularity is consistent;
  #                  no retention policy means data lives forever.
  $GLOG config vault create --addr "$S" --name "local-vault" \
    --type file --storage-class 1 --replication-factor "$NODES" \
    --rotation-policy "100-rows" --retention-policy "3m-retain" 2>&1 | sed 's/^/  /'
  $GLOG config vault create --addr "$S" --name "cloud-vault" \
    --type file --storage-class 1 --replication-factor "$NODES" \
    --cloud-service "S3" \
    --rotation-policy "100-rows" 2>&1 | sed 's/^/  /'

  # The retention route needs the local vault's GLID inline in its
  # match expression — there's no name-resolution path for synthetic
  # attribute values. Pull the ID out of `vault list -o json`, which
  # emits canonical base32hex GLIDs ready to drop into a predicate.
  local LOCAL_VAULT_ID
  LOCAL_VAULT_ID=$(
    $GLOG config vault list --addr "$S" -o json 2>/dev/null \
    | jq -r '.[] | select(.name == "local-vault") | .id'
  )
  if [[ -z "$LOCAL_VAULT_ID" ]]; then
    echo "Error: failed to resolve local-vault ID for retention route" >&2
    exit 1
  fi

  echo ">>> Creating routes..."
  # Live ingest → local. `_source = "ingest"` tags every record arriving
  # from an ingester at routing-eval time.
  $GLOG config route create --addr "$S" \
    --name "ingest-to-local" \
    --expression '_source = "ingest"' \
    --destination "local-vault" 2>&1 | sed 's/^/  /'
  # Local retention firing → cloud. When local-vault's 3m retention expires
  # a chunk, its records stream back through the routing engine with
  # `_source = "retention"` and `_vault = "<local-id>"`. This route picks
  # them up and lands them in cloud-vault before the original chunk is
  # destroyed.
  $GLOG config route create --addr "$S" \
    --name "local-retention-to-cloud" \
    --expression "_source = \"retention\" AND _vault = \"${LOCAL_VAULT_ID}\"" \
    --destination "cloud-vault" 2>&1 | sed 's/^/  /'

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
    check_dependencies
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
    if no_auth_enabled; then
      echo "    Auth:     disabled (--no-auth; UI skips login)"
    else
      echo "    Admin:    ${ADMIN_USER}/${ADMIN_PASS}"
    fi
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
