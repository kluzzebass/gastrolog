#!/bin/sh
set -e

# Default data directories.
: "${GASTROLOG_HOME:=/config}"
: "${GASTROLOG_VAULTS:=/vaults}"

# Build the base arguments.
args="--home ${GASTROLOG_HOME} --vaults ${GASTROLOG_VAULTS}"

# Pass through common environment variables as flags.
[ -n "$GASTROLOG_LISTEN" ]       && args="$args --listen $GASTROLOG_LISTEN"
[ -n "$GASTROLOG_CLUSTER_ADDR" ] && args="$args --cluster-addr $GASTROLOG_CLUSTER_ADDR"
[ -n "$GASTROLOG_NAME" ]         && args="$args --name $GASTROLOG_NAME"
[ -n "$GASTROLOG_BOOTSTRAP" ]    && args="$args --bootstrap"
[ -n "$GASTROLOG_JOIN_ADDR" ]    && args="$args --join-addr $GASTROLOG_JOIN_ADDR"
[ -n "$GASTROLOG_JOIN_TOKEN" ]   && args="$args --join-token $GASTROLOG_JOIN_TOKEN"
[ -n "$GASTROLOG_NO_AUTH" ]      && args="$args --no-auth"
[ -n "$GASTROLOG_PPROF" ]        && args="$args --pprof $GASTROLOG_PPROF"

# Execute gastrolog with the constructed arguments.
# $@ contains the CMD from Dockerfile (default: "server") plus any
# user-supplied arguments from docker run.
exec /gastrolog "$@" $args
