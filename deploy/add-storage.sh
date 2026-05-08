#!/bin/sh
# Add file storage `disk-1` (class 1) to every cluster node that
# doesn't already have it. Used by the `kubernetes-expand`/`-scale`
# and `swarm-expand`/`-scale` recipes to wire storage on newly-joined
# nodes without re-running the full setup-deps.sh (which would
# overwrite vault replication-factor to track the new node count).
#
# Idempotent: skips nodes that already have a `disk-1` entry.
#
# Runs INSIDE the bootstrap container, talking to its own gastrolog
# process over the unix socket — no auth needed. Pipe via stdin:
#
#   docker exec -i gastrolog-0 sh < deploy/add-storage.sh
#   kubectl -n gastrolog exec -i gastrolog-0 -- sh < deploy/add-storage.sh

set -eu

GLOG=/gastrolog
SOCK=unix:///config/gastrolog.sock

glog() {
  "$GLOG" --addr "$SOCK" "$@"
}

NODE_NAMES=$(glog config node list -o json | sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | sort -u)
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
    echo "  added disk-1 → $n"
    ADDED=$((ADDED + 1))
  fi
done
echo "Storage sync: $ADDED added, $SKIPPED already present"
