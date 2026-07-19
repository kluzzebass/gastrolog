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
#   --pprof-debug      With --pprof: enable mutex/block sampling (dev/incident)
#   GLOG_NO_AUTH       Disable auth when truthy (default: true). Set false/0 to require login.
#   GLOG_SEGMENT_HOT_PATH_FSYNC  Segmentation group-commit fsync (default: true; set false/0 for load testing)
#                      Every node holds a full replica, so the volume needs at least NODES x this.

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
PPROF_DEBUG="${GLOG_PPROF_DEBUG:-false}"
# Environment banner (gastrolog-4vr0l). Tags every node in this cluster as
# the local dev deployment in the UI header so operators don't confuse it
# with a K8s/staging instance. Single token only (no spaces).
ENV_LABEL="${GLOG_ENV_LABEL:-Development}"
ENV_COLOR="${GLOG_ENV_COLOR:-limegreen}"
SEGMENT_HOT_PATH_FSYNC="${GLOG_SEGMENT_HOT_PATH_FSYNC:-true}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)      NODES="$2"; shift 2 ;;
    --data-dir)   DATA_DIR="$2"; shift 2 ;;
    --admin-user) ADMIN_USER="$2"; shift 2 ;;
    --admin-pass) ADMIN_PASS="$2"; shift 2 ;;
    --auth)       NO_AUTH=false; shift ;;
    --base-port)  BASE_PORT="$2"; shift 2 ;;
    --pprof)      PPROF=true; shift ;;
    --pprof-debug) PPROF_DEBUG=true; shift ;;
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

# Runtime tuning (GOMAXPROCS / GOGC / GOMEMLIMIT) is NOT set here: the
# justfile loads .env (dotenv-load) and node processes inherit whatever the
# operator configured there. The Go runtime honors those env vars natively.
# Recommended values for the co-hosted 4-node soak substrate and the
# reasoning (CPU fair-share, GC STW cadence under macOS madvise pressure)
# live in gastrolog-1io54g.
run_glog_server() {
  go run ./cmd/gastrolog server "$@"
}

# glog_server_cmd returns the imux command string for one node.
glog_server_cmd() {
  local i="$1"
  local extra=""
  if [[ "$PPROF" == true ]]; then
    extra=" --pprof localhost:$((6059 + i)) --pprof-debug"
  fi
  printf 'go run ./cmd/gastrolog server --home %s --listen :%s --cluster-addr :%s%s%s' \
    "$(node_dir "$i")" "$(http_port "$i")" "$(cluster_port "$i")" "$extra" "$(env_flags)"
}

# env_flags emits optional server flags from cluster env (banner, auth, segment fsync, etc.).
# Empty/unset values produce no output for that flag.
env_flags() {
  local flags=""
  [[ -n "$ENV_LABEL" ]] && flags=" --environment-label $ENV_LABEL"
  [[ -n "$ENV_COLOR" ]] && flags="${flags} --environment-color $ENV_COLOR"
  if no_auth_enabled; then
    flags="${flags} --no-auth"
  fi
  if [[ "$SEGMENT_HOT_PATH_FSYNC" == false || "$SEGMENT_HOT_PATH_FSYNC" == "0" || "$SEGMENT_HOT_PATH_FSYNC" == "no" ]]; then
    flags="${flags} --segment-hot-path-fsync=false"
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
    cmds+=("$(glog_server_cmd "$i")")
  done
  # TUI: plain `imux` (flags + commands). `imux run` is non-interactive batch mode.
  echo "imux --name ${names} --tee ${DATA_DIR}/cluster.log $(printf ' "%s"' "${cmds[@]}")"
}

# --- Init: dependency preflight ---

# check_dependencies probes the external services the bootstrap depends on.
# Currently the only hard dependency is the S3-compatible object store at
# localhost:19000, which the second-vault setup needs for cloud-backed
# placement. Without it, the second-vault placement step blocks each node
# in a 3-attempt S3 retry loop (~5s/node) and leaves nodes hung in retry
# state at shutdown — the script appears to "take a loooong time" then
# emits Killed: 9 messages from the SIGKILL fallback. Failing here with
# a clear message is much better than that. See gastrolog-18du3.
check_dependencies() {
  local s3_host="localhost"
  local s3_port="19000"
  # Use bash's /dev/tcp pseudo-device so we don't depend on nc/curl/etc
  # being installed; works on macOS and Linux out of the box.
  if ! (exec 3<>/dev/tcp/${s3_host}/${s3_port}) 2>/dev/null; then
    echo "Error: S3-compatible service not reachable at ${s3_host}:${s3_port}." >&2
    echo "       The bootstrap creates a cloud-backed second-vault that needs" >&2
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
    run_glog_server \
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
  run_glog_server \
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
    if [[ -f "${DATA_DIR}/init-1.log" ]]; then
      echo ">>> node 1 log (last 30 lines):" >&2
      tail -30 "${DATA_DIR}/init-1.log" >&2
    else
      echo ">>> (no ${DATA_DIR}/init-1.log — node 1 may have failed before logging)" >&2
    fi
    cleanup
    exit 1
  fi
  local TOKEN
  TOKEN=$(cat "${DATA_DIR}/cluster-token")
  echo ">>> Join token acquired."

  # Start and enroll nodes 2..N.
  for i in $(seq 2 "$NODES"); do
    echo ">>> Enrolling node ${i}..."
    run_glog_server \
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

  # A node's socket existing means ITS server is up — not that its name
  # registration has committed through cluster-ctl Raft and reached node 1,
  # which is where every config command below is addressed. Poll node 1's
  # registry until all names are visible (the old bare "sleep 2" lost this
  # race on slow disks).
  echo ">>> Waiting for all nodes in node 1's registry..."
  for _ in $(seq 1 120); do
    local listing missing=0
    listing="$($GLOG config node list --addr "$S" 2>/dev/null || true)"
    for i in $(seq 1 "$NODES"); do
      grep -q "node-${i}" <<<"$listing" || { missing=1; break; }
    done
    [[ "$missing" -eq 0 ]] && break
    sleep 0.5
  done
  if [[ "${missing:-1}" -ne 0 ]]; then
    echo "!!! Timed out waiting for all ${NODES} nodes in the registry:" >&2
    $GLOG config node list --addr "$S" >&2 || true
    exit 1
  fi

  echo ">>> Creating file storage on each node..."
  for i in $(seq 1 "$NODES"); do
    $GLOG config node add-storage --addr "$S" \
      "node-${i}" --name "disk-1" --storage-class 1 --path "storage/disk-1" 2>&1 | sed 's/^/  /'
  done

  echo ">>> Creating cloud service..."
  $GLOG config cloud-service create --addr "$S" \
    --name "S3" --provider s3 --bucket gastrolog --region gastrolog \
    --endpoint "http://localhost:19000" --access-key gastrolog --secret-key gastrolog 2>&1 | sed 's/^/  /'

  echo ">>> Creating policies..."
  $GLOG config rotation-policy create --addr "$S" --name "1m-rotate" --max-age 1m 2>&1 | sed 's/^/  /'
  $GLOG config rotation-policy create --addr "$S" --name "100-records" --max-records 100 2>&1 | sed 's/^/  /'
  $GLOG config rotation-policy create --addr "$S" --name "10000-records" --max-records 10000 2>&1 | sed 's/^/  /'
  $GLOG config rotation-policy create --addr "$S" --name "1M-1m" --max-records 1000000 --max-age 1m 2>&1 | sed 's/^/  /'
  $GLOG config retention-policy create --addr "$S" --name "3m-retain" --max-age 3m 2>&1 | sed 's/^/  /'
  # max-size is one field, drain+refuse in one bound now (gastrolog-33ul6h):
  # this policy is first-vault's whole size/age story. Draining fires at
  # whichever of max-age (1h) or max-size (50GB) comes first; the same 50GB
  # bound also refuses admission cluster-wide while the vault's local claim
  # stays at/over it. 50GB per node — every node holds a full replica (RF =
  # NODES), so the volume needs at least NODES x this; an unbounded vault is
  # how GastroLog1 filled a 466 GiB volume and deadlocked (gastrolog-2b2yyy,
  # gastrolog-5ct2av).
  $GLOG config retention-policy create --addr "$S" --name "1h-retain" --max-age 1h \
    --max-size "50GB" 2>&1 | sed 's/^/  /'

  # gastrolog-4kkoo (Phase 5): no filter entity — match expressions live
  # inline on routes via --expression. Synthetic attributes (_source,
  # _vault) tag the record's origin at routing-eval time and let one
  # route distinguish "from an ingester" from "from a retention sweep".

  echo ">>> Creating vaults..."
  # Two-vault local→cloud chain wired via inter-vault routing (gastrolog-4kkoo).
  #   - first-vault: file-backed on local disk, 1M-records / 1m rotation, with
  #                  a retention policy. Chunks past their TTL fire the
  #                  retention sweep, which streams their records back through
  #                  the routing engine.
  #   - second-vault: file-backed but cloud-served (S3). 10000-records rotation
  #                  carries over so chunk granularity is consistent; no
  #                  retention policy (data lives forever).
  #
  # Both are bounded via their retention policies' max-size bound
  # (gastrolog-33ul6h). A drain trigger alone does NOT bound a vault: it
  # acts on sealed chunks, and the bulk of a busy vault's disk can be
  # segments awaiting collection — which retention has no authority over
  # (gastrolog-2b2yyy: a 3-minute TTL sat next to 449 GiB of unpurged
  # segments). max-size's refuse half is what makes the vault-max-size
  # alarm reachable before the disk guard traps the cluster. second-vault
  # has no retention policy: cloud-served, its local claim is cache +
  # backlog, bounded by the creation-default floor.
  $GLOG config vault create --addr "$S" --name "first-vault" \
    --type file --storage-class 1 --replication-factor "$NODES" \
    --rotation-policy "1M-1m" --retention-policy "1h-retain" 2>&1 | sed 's/^/  /'
  $GLOG config vault create --addr "$S" --name "second-vault" \
    --type file --storage-class 1 --replication-factor "$NODES" \
    --cloud-service "S3" \
    --rotation-policy "10000-records" 2>&1 | sed 's/^/  /'

  # The retention route needs the first vault's GLID inline in its
  # match expression — there's no name-resolution path for synthetic
  # attribute values. Pull the ID out of `vault list -o json`, which
  # emits canonical base32hex GLIDs ready to drop into a predicate.
  local FIRST_VAULT_ID
  FIRST_VAULT_ID=$(
    $GLOG config vault list --addr "$S" -o json 2>/dev/null \
    | jq -r '.[] | select(.name == "first-vault") | .id'
  )
  if [[ -z "$FIRST_VAULT_ID" ]]; then
    echo "Error: failed to resolve first-vault ID for retention route" >&2
    exit 1
  fi

  echo ">>> Creating routes..."
  # Live ingest → first-vault. `_source = "ingest"` tags every record arriving
  # from an ingester at routing-eval time.
  $GLOG config route create --addr "$S" \
    --name "ingest-to-first" \
    --expression '_source = "ingest"' \
    --destination "first-vault" 2>&1 | sed 's/^/  /'
  # First-vault retention firing → second-vault. When first-vault's 1h retention
  # expires a chunk, its records stream back through the routing engine with
  # `_source = "retention"` and `_vault = "<first-vault-id>"`. This route picks
  # them up and lands them in second-vault before the original chunk is
  # destroyed.
  $GLOG config route create --addr "$S" \
    --name "first-retention-to-second" \
    --expression "_source = \"retention\" AND _vault = \"${FIRST_VAULT_ID}\"" \
    --destination "second-vault" 2>&1 | sed 's/^/  /'

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
  local CHATTER_NODE
  CHATTER_NODE="${NODE_IDS[$((RANDOM % ${#NODE_IDS[@]}))]}"
  $GLOG config ingester create --addr "$S" \
    --name "chatterbox" --type chatterbox --node-id "$CHATTER_NODE" --enabled=false 2>&1 | sed 's/^/  /'
  $GLOG config ingester create --addr "$S" \
    --name "scatterbox" --type scatterbox --all-nodes \
    --param interval=10ms --param burst=100 --enabled=false 2>&1 | sed 's/^/  /'
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
