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

# Pass through string env vars as flags.
[ -n "$GASTROLOG_LISTEN" ]       && args="$args --listen $GASTROLOG_LISTEN"
[ -n "$GASTROLOG_CLUSTER_ADDR" ] && args="$args --cluster-addr $GASTROLOG_CLUSTER_ADDR"
[ -n "$GASTROLOG_NAME" ]         && args="$args --name $GASTROLOG_NAME"
[ -n "$GASTROLOG_JOIN_ADDR" ]    && args="$args --join-addr $GASTROLOG_JOIN_ADDR"
[ -n "$GASTROLOG_JOIN_TOKEN" ]   && args="$args --join-token $GASTROLOG_JOIN_TOKEN"
[ -n "$GASTROLOG_PPROF" ]        && args="$args --pprof $GASTROLOG_PPROF"
[ -n "$GASTROLOG_CONFIG_TYPE" ]  && args="$args --config-type $GASTROLOG_CONFIG_TYPE"

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
