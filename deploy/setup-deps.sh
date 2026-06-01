#!/bin/sh
# Auto-wire CloudServices, per-node file storage, rotation/retention
# policies, local+cloud vaults, routes, and ingesters into a freshly-
# deployed gastrolog cluster. Mirrors what scripts/cluster.sh sets up
# for the dev cluster, adapted to the in-cluster service-name DNS
# that the bundled deps expose.
#
# Runs INSIDE the bootstrap container, talking to its own gastrolog
# process over the unix socket — no auth needed. Pipe via stdin from
# each *-up recipe:
#
#   docker exec -i gastrolog-bootstrap sh < deploy/setup-deps.sh
#   kubectl -n gastrolog exec -i gastrolog-bootstrap-0 -- sh < deploy/setup-deps.sh
#
# Honors SKIP_RSYSLOG=1 for the Swarm path (rsyslog is omitted there
# because the rsyslog/syslog_appliance_alpine image is amd64-only).
#
# Idempotent: every config-create CLI is upsert-by-name, so re-running
# just refreshes the same entries.

set -eu

GLOG=/gastrolog
SOCK=unix:///config/gastrolog.sock

glog() {
  "$GLOG" --addr "$SOCK" "$@"
}

# Wait for the local socket to be live. The recipe already waits on
# /readyz from outside, but inside the bootstrap container we want a
# belt-and-braces check so the first RPC doesn't race the listener.
for i in $(seq 1 30); do
  if [ -S /config/gastrolog.sock ] && glog cluster status >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "Auto-wiring deps..."

# ── 1. CloudServices ─────────────────────────────────────────────────
# Three providers, all backed by emulators bundled with the deploy
# recipe (minio/azurite/fake-gcs). Match cluster.sh's S3 settings —
# bucket=gastrolog, region=gastrolog, plain user/pass — so chunks land
# in the same shape regardless of which deployment the user picks.

glog config cloud-service create \
  --name minio \
  --provider s3 \
  --endpoint http://minio:9000 \
  --access-key gastrolog \
  --secret-key gastrolog \
  --bucket gastrolog \
  --region gastrolog >/dev/null

# Default Azurite credentials per
# learn.microsoft.com/azure/storage/common/storage-use-azurite.
glog config cloud-service create \
  --name azurite \
  --provider azure \
  --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;' \
  --container gastrolog >/dev/null

glog config cloud-service create \
  --name fake-gcs \
  --provider gcs \
  --endpoint http://fake-gcs:4443/storage/v1/ \
  --bucket gastrolog >/dev/null

echo "  cloud-services: minio, azurite, fake-gcs"

# ── 2. File storage on every node ────────────────────────────────────
# Each node needs storage-class=1 backed by /vaults/storage/disk-1.
# `config node add-storage` is NOT idempotent — it appends regardless
# of name (backend/cmd/gastrolog/cli/node.go:163). So we check
# `node list-storage` first per node and skip nodes that already have
# a `disk-1` entry. Re-running this script after a `kubernetes-expand`
# / `swarm-expand` thus adds storage to the new pods without
# duplicating it on existing ones.
NODE_NAMES=$(glog config node list -o json | sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | sort -u)
NODE_COUNT=0
ADDED=0
SKIPPED=0
for n in $NODE_NAMES; do
  # `list-storage <node> -o json` ignores the node filter and returns
  # the full system list (CLI bug); use the text format which IS
  # filtered server-side, then grep for `disk-1` as a whole word.
  if glog config node list-storage "$n" 2>/dev/null | grep -qw disk-1; then
    SKIPPED=$((SKIPPED + 1))
  else
    glog config node add-storage "$n" \
      --name disk-1 --storage-class 1 --path storage/disk-1 >/dev/null
    ADDED=$((ADDED + 1))
  fi
  NODE_COUNT=$((NODE_COUNT + 1))
done
echo "  file storage 'disk-1' (class 1) on $NODE_COUNT node(s): $ADDED added, $SKIPPED already present"

# ── 3. Rotation / retention policies ─────────────────────────────────
# Two rotation policies (1m + 100-row) and one retention (3m), matching
# cluster.sh's local→cloud chain semantics.
glog config rotation-policy create --name 1m-rotate --max-age 1m >/dev/null
glog config rotation-policy create --name 100-rows --max-records 100 >/dev/null
glog config retention-policy create --name 3m-retain --max-age 3m >/dev/null
echo "  policies: rotation [1m-rotate, 100-rows], retention [3m-retain]"

# ── 4. Vaults (local→cloud chain) ────────────────────────────────────
# local-vault: file-backed on local disk-1, 100-row rotation, 3-minute
#               retention with disposition=route — chunks past their TTL
#               stream their records back through the routing engine
#               tagged `_source = "retention"` instead of being dropped.
#               The local-retention-to-cloud route below picks them up.
# cloud-vault: file-backed but cloud-served (minio). 100-row rotation
#              carries over so chunk granularity is consistent;
#              no retention policy means data lives forever.
# replication-factor matches the live node count so every chunk is
# fully replicated across the cluster.
glog config vault create --name local-vault --type file \
  --storage-class 1 --replication-factor "$NODE_COUNT" \
  --rotation-policy 100-rows --retention-policy 3m-retain \
  --retention-disposition route >/dev/null
glog config vault create --name cloud-vault --type file \
  --storage-class 1 --replication-factor "$NODE_COUNT" \
  --cloud-service minio \
  --rotation-policy 100-rows >/dev/null
echo "  vaults: local-vault (RF=$NODE_COUNT, retention 3m → route), cloud-vault (RF=$NODE_COUNT, cloud=minio)"

# ── 5. Routes ────────────────────────────────────────────────────────
# Live ingest → local. The retention route needs local-vault's GLID inline
# in its match expression — pull it from `vault get -o json` (single
# proto message, so protojson camelCase: "id").
LOCAL_VAULT_ID=$(glog config vault get local-vault -o json | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
if [ -z "$LOCAL_VAULT_ID" ]; then
  echo "ERROR: failed to resolve local-vault GLID" >&2
  exit 1
fi

glog config route create \
  --name ingest-to-local \
  --expression '_source = "ingest"' \
  --destination local-vault >/dev/null

# Local retention firing → cloud. When local-vault's 3m retention expires
# a chunk, its records stream back through the routing engine with
# `_source = "retention"` and `_vault = "<local-id>"`. This route picks
# them up and lands them in cloud-vault before the original chunk is
# destroyed.
glog config route create \
  --name local-retention-to-cloud \
  --expression "_source = \"retention\" AND _vault = \"$LOCAL_VAULT_ID\"" \
  --destination cloud-vault >/dev/null
echo "  routes: ingest-to-local, local-retention-to-cloud"

# ── 6. Ingesters ─────────────────────────────────────────────────────
# All defined and ready, assigned via --all-nodes=true so eligibility
# is the entire cluster (re-evaluated on every cluster-membership
# change — joiners pick up the ingester automatically). All DISABLED
# by default — operator opts in via the UI when they want the cluster
# to actually start consuming. Listener ports match the
# rsyslog/otel/fluent-bit configs' push targets; recipes substitute
# the bootstrap's hostname into those configs at deploy time.

glog config ingester create --name kafka --enabled=false --all-nodes=true \
  --type kafka \
  --param brokers=kafka:9092 \
  --param topic=gastrolog-logs \
  --param group=gastrolog >/dev/null

if [ "${SKIP_RSYSLOG:-0}" = "1" ]; then
  echo "  ingesters: kafka, otlp, fluentfwd, http, chatterbox, scatterbox (syslog/relp skipped — rsyslog unavailable)"
else
  glog config ingester create --name syslog --enabled=false --all-nodes=true \
    --type syslog --param tcp_addr=:1514 >/dev/null
  glog config ingester create --name relp --enabled=false --all-nodes=true \
    --type relp --param addr=:2514 >/dev/null
fi

glog config ingester create --name otlp --enabled=false --all-nodes=true \
  --type otlp --param grpc_addr=:4317 --param http_addr=:4318 >/dev/null
glog config ingester create --name fluentfwd --enabled=false --all-nodes=true \
  --type fluentfwd --param addr=:24224 >/dev/null
glog config ingester create --name http --enabled=false --all-nodes=true \
  --type http --param addr=:3100 >/dev/null

# Synthetic generators.
glog config ingester create --name chatterbox --enabled=false --all-nodes=true \
  --type chatterbox >/dev/null
glog config ingester create --name scatterbox --enabled=false --all-nodes=true \
  --type scatterbox >/dev/null

if [ "${SKIP_RSYSLOG:-0}" != "1" ]; then
  echo "  ingesters: kafka, syslog, relp, otlp, fluentfwd, http, chatterbox, scatterbox (all disabled, all-nodes)"
fi

echo "Done. Cluster wired with local→cloud chain; enable ingesters from the UI to start landing records."
