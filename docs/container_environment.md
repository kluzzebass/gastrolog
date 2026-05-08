# Container environment variables

The `docker-entrypoint.sh` script wraps the `gastrolog` binary and
translates a small set of environment variables into CLI flags. The
binary itself takes only flags — the env-var convention exists so that
container orchestrators (Docker, Podman, Kubernetes) can drive
configuration via their native mechanisms (env, ConfigMap, Secret)
without anyone having to replace the entrypoint or shadow the
Dockerfile.

If you're running `gastrolog` outside a container (e.g. via
`go install`, a release binary, or systemd), use the flags directly —
the env vars below are not consulted by the binary. See
[`docs/cluster_enrollment.md`](./cluster_enrollment.md) for the
operator-facing flag reference.

## Mapping

| Env var | Flag | Type | Notes |
|---|---|---|---|
| `GASTROLOG_HOME` | `--home` | string | Default: `/config`. Persistent home directory; mount as a volume. |
| `GASTROLOG_VAULTS` | `--vaults` | string | Default: `/vaults`. Vault storage directory; mount as a volume. |
| `GASTROLOG_LISTEN` | `--listen` | string | HTTP / Connect-RPC listen address (default `:4564`). |
| `GASTROLOG_CLUSTER_ADDR` | `--cluster-addr` | string | Cluster gRPC listen address (default `:4566`). Set to the literal string `auto` to have the entrypoint resolve the container's own hostname to its overlay IP and use that — required for Docker Swarm and Kubernetes overlay networks where service-name DNS resolves to a routing-mesh VIP that can't be bound. |
| `GASTROLOG_NAME` | `--name` | string | Node name. Defaults to a random petname; set explicitly when stable identity matters (e.g. `gastrolog-0` from a StatefulSet ordinal). |
| `GASTROLOG_JOIN_ADDR` | `--join-addr` | string | Bootstrap node's cluster address — set on joiners. Omit on the bootstrap node. |
| `GASTROLOG_JOIN_TOKEN` | `--join-token` | string | Cluster join token — set on joiners. Pair with `GASTROLOG_JOIN_ADDR`. |
| `GASTROLOG_NO_AUTH` | `--no-auth` | bool | Disable authentication. Truthy values: `1`, `true`, `yes`, `y`, `on` (case-insensitive). Anything else (including `false`, `0`, empty) is off. **Use only for testing.** |
| `GASTROLOG_PPROF` | `--pprof` | string | pprof HTTP server address (e.g. `localhost:6060`). Empty/unset = disabled. |
| `GASTROLOG_CONFIG_TYPE` | `--config-type` | string | Config store: `raft` (default) or `memory`. Use `memory` only for tests / ephemeral demos. |
| `GASTROLOG_WRITE_BOOTSTRAP_TOKEN` | `--write-bootstrap-token` | string | Bootstrap node only: atomically write the join token to this path (mode 0600) so joiners can read it via `GASTROLOG_BOOTSTRAP_TOKEN_FILE`. |
| `GASTROLOG_BOOTSTRAP_TOKEN_FILE` | `--bootstrap-token-file` | string | Joiner only: read the join token from this path, polling with backoff (1s → 30s, 10min total) until present. Alternative to `GASTROLOG_JOIN_TOKEN`. |
| `GASTROLOG_BOOTSTRAP_TOKEN_SERVE_SECRET` | `--bootstrap-token-serve-secret` | string | Bootstrap node only: serve the join token at `GET /cluster/bootstrap-token`, gated on this shared secret. Empty disables. |
| `GASTROLOG_BOOTSTRAP_TOKEN_URL` | `--bootstrap-token-url` | string | Joiner only: fetch the join token from this URL, polling with backoff. Pair with `GASTROLOG_BOOTSTRAP_TOKEN_SECRET`. |
| `GASTROLOG_BOOTSTRAP_TOKEN_SECRET` | `--bootstrap-token-secret` | string | Joiner only: secret sent in the `X-Bootstrap-Token-Secret` header when fetching from `GASTROLOG_BOOTSTRAP_TOKEN_URL`. |
| `GASTROLOG_INITIAL_ADMIN_FILE` | `--initial-admin-file` | string | Bootstrap node only: read initial admin credentials from this path. JSON `{"username": ..., "password": ...}` or single-line `username:password`. Wins over the env-var pair. No-op once any user exists. |
| `GASTROLOG_INITIAL_ADMIN_USER` | `--initial-admin-user` | string | Bootstrap node only: initial admin username (paired with `GASTROLOG_INITIAL_ADMIN_PASSWORD`). No-op once any user exists. |
| `GASTROLOG_INITIAL_ADMIN_PASSWORD` | `--initial-admin-password` | string | Bootstrap node only: initial admin password. **Prefer `GASTROLOG_INITIAL_ADMIN_FILE` for production** — env-var-based passwords show up in `docker inspect`, process listings, and Kubernetes Pod manifests. |

## Default-vs-explicit semantics

The entrypoint always sets `--home` and `--vaults` (using the env-var
defaults `/config` and `/vaults` if those env vars are unset). All
other flags are passed only when the corresponding env var is set.

This means:

- The container's defaults (`/config`, `/vaults`) take effect even
  when no env var is set, so a bare `docker run` produces a working
  single-node node.
- Setting `GASTROLOG_HOME=""` or `GASTROLOG_VAULTS=""` does NOT
  unset the flag — it sets it to the empty string. To use the
  binary's platform defaults instead of the container defaults,
  override the entrypoint or use the binary directly.

## Bool flag semantics

Bool env vars (`GASTROLOG_NO_AUTH` is the only one today) follow
standard truthy semantics: only `1`, `true`, `yes`, `y`, `on`
(case-insensitive) enable the corresponding flag. Anything else —
including `false`, `0`, `no`, `off`, or any unrecognized value —
disables it.

In particular: `GASTROLOG_NO_AUTH=false` correctly disables the
flag rather than enabling it.

## Persistence

The image declares `/config` and `/vaults` as VOLUMEs, signaling that
those paths must be persistent across container restarts. In
production you should mount these explicitly:

- **Docker Compose**: named volumes (`config_data:/config`,
  `vault_data:/vaults`).
- **Kubernetes StatefulSet**: PersistentVolumeClaim templates for
  each path.
- **`docker run` quickstart**: bind-mounts (`-v $(pwd)/config:/config
  -v $(pwd)/vaults:/vaults`) for inspection, or omit volumes entirely
  for an ephemeral demo.

Without persistent volumes, all cluster state — Raft logs, vault
data, the join token — is lost on container removal.

## Bootstrap vs joiner

A multi-node cluster has one bootstrap node and N joiners. The bootstrap
node generates a join token at startup; joiners need to receive it
before they can enroll. Three delivery paths exist:

### 1. Literal token (attended setup)

- **Bootstrap node**: do NOT set `GASTROLOG_JOIN_ADDR` or
  `GASTROLOG_JOIN_TOKEN`. The first node to start without those flags
  becomes the bootstrap and prints the token to its logs.
- **Joiners**: set both `GASTROLOG_JOIN_ADDR` and
  `GASTROLOG_JOIN_TOKEN` (the operator pastes the token from the
  bootstrap node's logs).

### 2. File-based delivery (Docker Compose, K8s with shared volume)

- **Bootstrap node**: set `GASTROLOG_WRITE_BOOTSTRAP_TOKEN=/path/to/token`
  on a path that joiners can read (a named volume in compose, a
  `PersistentVolumeClaim` or `emptyDir` in K8s). The bootstrap node
  writes the token atomically with mode 0600 once the cluster TLS
  is initialized.
- **Joiners**: set `GASTROLOG_JOIN_ADDR` and
  `GASTROLOG_BOOTSTRAP_TOKEN_FILE=/path/to/token` (same path). The
  joiner polls the file with exponential backoff (1s → 30s, 10
  minute total timeout) until it appears, then enrolls.

### 3. Endpoint-based delivery (cross-region, immutable infra)

- **Bootstrap node**: set
  `GASTROLOG_BOOTSTRAP_TOKEN_SERVE_SECRET=<secret>`. The bootstrap node
  serves the token at `GET /cluster/bootstrap-token` on its HTTP
  listener (port 4564 by default), gated on the secret in the
  `X-Bootstrap-Token-Secret` header.
- **Joiners**: set `GASTROLOG_JOIN_ADDR`,
  `GASTROLOG_BOOTSTRAP_TOKEN_URL=http://bootstrap-host:4564/cluster/bootstrap-token`,
  and `GASTROLOG_BOOTSTRAP_TOKEN_SECRET=<same-secret>`. The joiner
  polls the URL with the same backoff as the file-based path.

### Precedence

If multiple are set, precedence is:

1. `GASTROLOG_JOIN_TOKEN` (literal) wins outright.
2. `GASTROLOG_BOOTSTRAP_TOKEN_FILE` is consulted next.
3. `GASTROLOG_BOOTSTRAP_TOKEN_URL` is the fallback.

A joiner without `GASTROLOG_JOIN_ADDR` set is a bootstrap node, even
if a bootstrap-token source is configured (the bootstrap-token sources
are joiner-side and are no-ops without `--join-addr`).

## Initial admin user

A fresh GastroLog cluster has no users. Three ways to create the
initial admin:

1. **First-access UI (default).** When no one is registered, hitting
   the dashboard at `http://<host>:4564` shows an admin-creation
   screen. Username + password are entered interactively; the user
   is created and the screen is replaced by the normal login flow
   on next visit. This is the right answer for human-driven
   single-node setups.

2. **File-based provisioning** (recommended for production
   orchestration). Set `GASTROLOG_INITIAL_ADMIN_FILE=/path/to/creds`
   on the bootstrap node, with the file containing either:
   - JSON: `{"username": "admin", "password": "..."}`, or
   - One line: `admin:password-here`.
   The file is read once at startup, the user is created with role
   `admin`, and the first-access UI is suppressed. The file is best
   mounted as a Kubernetes `Secret`-as-volume or a Compose secret.

3. **Env-var provisioning.** Set `GASTROLOG_INITIAL_ADMIN_USER` +
   `GASTROLOG_INITIAL_ADMIN_PASSWORD`. Same result as file-based,
   but the password is visible to `docker inspect` and equivalent
   tooling — fine for development, **avoid in production**.

### Idempotency

All three paths are first-user-only. Once any user exists in the
cluster (created via any path), the file/env sources become no-ops
on subsequent restarts. The operator's password changes are not
overwritten by a Secret left in place.

### Validation

Username must be 3-64 characters, alphanumeric / underscores /
hyphens. Password must be at least 8 characters. Same rules the
interactive UI enforces.

### Joiners

Joiners (`GASTROLOG_JOIN_ADDR` set) skip provisioning entirely
regardless of file/env config. Only the bootstrap node creates
users; joiners inherit the user state from the cluster's Raft
replication.

## Ports

| Port | Purpose | When required |
|---|---|---|
| 4564 | HTTP / Connect-RPC (operator + ingestion API) | Always. |
| 4566 | Cluster gRPC (inter-node Raft + RPC) | Multi-node only; harmless to expose on single-node. |

Both ports are declared in the image's `EXPOSE`. The actual values are
configurable via `GASTROLOG_LISTEN` and `GASTROLOG_CLUSTER_ADDR`; the
defaults are baked into the binary, not the entrypoint.

## Health endpoints

Two HTTP endpoints expose health state. Both are unauthenticated so
container orchestrators can probe them without a token, and both are
served on the HTTP port (`4564` by default).

| Endpoint | Returns | Meaning |
|---|---|---|
| `GET /healthz` | `200` always (when the HTTP listener is serving) | **Liveness.** The process is up and the HTTP server is accepting requests. Failing this means the container is unrecoverable — kill and restart. |
| `GET /readyz` | `200` when ready, `503` otherwise | **Readiness.** The orchestrator has started, the node isn't draining, and every locally-hosted vault's tier FSM has applied at least one log entry. Failing this means "remove from load balancer, but don't restart" — typical during startup, leader change, or shutdown. |

The image's built-in `HEALTHCHECK` directive probes `/healthz` every
30 seconds (with a 30-second startup grace period and 3 retries before
marking the container unhealthy). This is appropriate for Docker; in
Kubernetes you typically configure both `livenessProbe` (against
`/healthz`) and `readinessProbe` (against `/readyz`) directly in the
Pod spec, which overrides the Dockerfile's `HEALTHCHECK`.

Example Kubernetes probe configuration:

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 4564
  initialDelaySeconds: 10
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: 4564
  initialDelaySeconds: 5
  periodSeconds: 5
```
