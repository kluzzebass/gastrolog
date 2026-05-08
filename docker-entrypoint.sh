#!/bin/sh
set -e

# Default data directories.
: "${GASTROLOG_HOME:=/config}"
: "${GASTROLOG_VAULTS:=/vaults}"

# is_truthy returns 0 (success) when a value is one of the conventional
# truthy strings, otherwise 1. Used so that env vars like
# GASTROLOG_NO_AUTH=false correctly DISABLE the flag instead of enabling
# it (the previous `[ -n "$VAR" ]` test enabled the flag for any
# non-empty value, including "false" and "0"). Trims leading/trailing
# whitespace so values from .env files or YAML survive intact.
is_truthy() {
  v="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  # Strip leading/trailing whitespace.
  v="${v#"${v%%[![:space:]]*}"}"
  v="${v%"${v##*[![:space:]]}"}"
  case "$v" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

# Build the base arguments.
args="--home ${GASTROLOG_HOME} --vaults ${GASTROLOG_VAULTS}"

# auto_cluster_addr resolves the container's own primary IP and
# emits "<ip>:4566". Used when GASTROLOG_CLUSTER_ADDR is "auto",
# which is the right answer for Docker Swarm and Kubernetes where:
#   - Service-name DNS resolves to a routing-mesh VIP that no
#     container can bind to.
#   - The container's overlay IP is bindable locally and reachable
#     from peer containers via overlay L3 routing.
# Uses `hostname -i` because busybox doesn't ship with getent; the
# tradeoff is that hostname -i returns the first IP from the
# container's primary interface, which is exactly what we want.
# Falling back to ":4566" if resolution fails (single-node `docker
# run` case where bind-on-all-interfaces is fine).
auto_cluster_addr() {
  ip=$(hostname -i 2>/dev/null | awk '{print $1; exit}')
  if [ -n "$ip" ]; then
    printf '%s:4566' "$ip"
  else
    printf ':4566'
  fi
}

# Pass through string env vars as flags.
[ -n "$GASTROLOG_LISTEN" ]       && args="$args --listen $GASTROLOG_LISTEN"
if [ "$GASTROLOG_CLUSTER_ADDR" = "auto" ]; then
  GASTROLOG_CLUSTER_ADDR="$(auto_cluster_addr)"
fi
[ -n "$GASTROLOG_CLUSTER_ADDR" ] && args="$args --cluster-addr $GASTROLOG_CLUSTER_ADDR"
[ -n "$GASTROLOG_NAME" ]         && args="$args --name $GASTROLOG_NAME"
[ -n "$GASTROLOG_JOIN_ADDR" ]    && args="$args --join-addr $GASTROLOG_JOIN_ADDR"
[ -n "$GASTROLOG_JOIN_TOKEN" ]   && args="$args --join-token $GASTROLOG_JOIN_TOKEN"
[ -n "$GASTROLOG_PPROF" ]        && args="$args --pprof $GASTROLOG_PPROF"
[ -n "$GASTROLOG_CONFIG_TYPE" ]  && args="$args --config-type $GASTROLOG_CONFIG_TYPE"

# Non-interactive cluster bootstrap (gastrolog-o9z6o).
[ -n "$GASTROLOG_WRITE_BOOTSTRAP_TOKEN" ]        && args="$args --write-bootstrap-token $GASTROLOG_WRITE_BOOTSTRAP_TOKEN"
[ -n "$GASTROLOG_BOOTSTRAP_TOKEN_FILE" ]         && args="$args --bootstrap-token-file $GASTROLOG_BOOTSTRAP_TOKEN_FILE"
[ -n "$GASTROLOG_BOOTSTRAP_TOKEN_SERVE_SECRET" ] && args="$args --bootstrap-token-serve-secret $GASTROLOG_BOOTSTRAP_TOKEN_SERVE_SECRET"
[ -n "$GASTROLOG_BOOTSTRAP_TOKEN_URL" ]          && args="$args --bootstrap-token-url $GASTROLOG_BOOTSTRAP_TOKEN_URL"
[ -n "$GASTROLOG_BOOTSTRAP_TOKEN_SECRET" ]       && args="$args --bootstrap-token-secret $GASTROLOG_BOOTSTRAP_TOKEN_SECRET"

# Initial admin provisioning (gastrolog-3ot7r). Bootstrap node only.
[ -n "$GASTROLOG_INITIAL_ADMIN_FILE" ]     && args="$args --initial-admin-file $GASTROLOG_INITIAL_ADMIN_FILE"
[ -n "$GASTROLOG_INITIAL_ADMIN_USER" ]     && args="$args --initial-admin-user $GASTROLOG_INITIAL_ADMIN_USER"
[ -n "$GASTROLOG_INITIAL_ADMIN_PASSWORD" ] && args="$args --initial-admin-password $GASTROLOG_INITIAL_ADMIN_PASSWORD"

# Bool env vars use truthy semantics: only 1/true/yes/y/on enable the flag.
is_truthy "$GASTROLOG_NO_AUTH" && args="$args --no-auth"

# Execute gastrolog with the constructed arguments.
# $@ contains the CMD from Dockerfile (default: "server") plus any
# user-supplied arguments from docker run.
# shellcheck disable=SC2086
# We deliberately want word-splitting on $args — it's a space-separated
# list of flag/value pairs. Quoting it would pass the whole string as
# a single argument.
exec /gastrolog "$@" $args
