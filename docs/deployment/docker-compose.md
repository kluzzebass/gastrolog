# Docker Compose: 3-node cluster

Bring up a 3-node GastroLog cluster on a single Docker host.
Useful for local development, integration tests, and any
single-machine deployment that needs the redundancy a Raft cluster
provides.

## Quick start

```sh
cd deploy
docker compose up -d
open http://localhost:4564           # admin / change-me-please
```

To tear down (and wipe data):

```sh
docker compose down -v
```

## What's in the recipe

The compose file ([`deploy/compose.yml`](../../deploy/compose.yml))
defines three services — `bootstrap`, `joiner-1`, `joiner-2` —
that share a Compose `secret` for admin credentials and a named
volume for the cluster join token.

### Architecture

| Service | Role | Host port | Cluster addr |
|---|---|---|---|
| bootstrap | Cluster bootstrap (writes join token) | 4564 | bootstrap:4566 |
| joiner-1 | Joiner | 4574 | joiner-1:4566 |
| joiner-2 | Joiner | 4584 | joiner-2:4566 |

### Cluster bootstrap flow

1. Bootstrap starts; generates cluster TLS + a join token; writes
   the token atomically to `/shared/token` (mode 0600) on the
   `cluster-token` named volume.
2. Joiners start with `depends_on: condition: service_healthy`,
   so they wait for bootstrap's `/healthz` probe to pass before
   starting.
3. Each joiner reads `/shared/token` (same volume mounted into all
   services), enrolls with bootstrap via `bootstrap:4566`, and
   joins the Raft cluster.
4. Both joiners settle as `FOLLOWER` / `VOTER` within ~10 seconds.

### Admin credentials

Provisioned via a Compose `secret` mounted at
`/run/secrets/admin_creds`. The bootstrap node reads the file
once at startup, creates the admin user, and the file/secret
becomes a no-op on subsequent restarts (idempotency).

The secret file in the repo (`deploy/admin-creds`) is a **demo
placeholder** with `admin` / `change-me-please`. Replace it before
running this in any environment with real users:

```sh
echo '{"username": "admin", "password": "your-strong-password"}' > deploy/admin-creds
chmod 0400 deploy/admin-creds
```

Or use the colon format:

```sh
echo 'admin:your-strong-password' > deploy/admin-creds
chmod 0400 deploy/admin-creds
```

### Cluster addresses

Each service explicitly sets `GASTROLOG_CLUSTER_ADDR=<service-name>:4566`.
This matters: the binary's default is the port-only `:4566`, which
the Raft leader can't route back to a specific container. Setting
the address to the Compose service name lets the bridge network's
DNS resolve it correctly.

If you forget this setting, joiners enroll successfully (the join
token is delivered via the shared volume) but get stuck at
"waiting to be added to cluster by leader" because the leader
can't reach back. The compose file sets it explicitly so this
doesn't bite you.

## Verify

```sh
# Health on each node:
for p in 4564 4574 4584; do
  curl -s -o /dev/null -w "port $p ready=%{http_code}\n" http://localhost:$p/readyz
done

# Cluster topology:
docker exec gastrolog-bootstrap /gastrolog --home /config cluster status

# Admin login:
curl -s -X POST -H "Content-Type: application/json" \
  http://localhost:4564/gastrolog.v1.AuthService/Login \
  -d '{"username":"admin","password":"change-me-please"}'
```

## Scaling out

Adding a fourth or fifth joiner is a copy-paste job. Duplicate
the `joiner-2` service block, increment the name and host port,
add new volumes for `joiner-N-config` and `joiner-N-vaults`, and
`docker compose up -d` again. The new service will pick up the
existing cluster token and enroll automatically.

For more than ~5 nodes or any cross-host deployment, switch to
[Docker Swarm](./docker-swarm.md) or [Kubernetes](./kubernetes.md).

## Production caveats

- **Admin password**: change `deploy/admin-creds` before exposing
  this beyond `localhost`.
- **External access**: this recipe exposes the dashboard on
  `localhost:4564` only. For external access, put a TLS-terminating
  reverse proxy (Caddy, nginx, Traefik) in front of port 4564.
- **Persistence**: each service has its own named volumes for
  `/config` and `/vaults`. `docker compose down -v` deletes them.
  Use `docker compose down` (without `-v`) to stop without wiping.
- **Backups**: snapshot the named volumes (`docker run --rm -v
  bootstrap-config:/data alpine tar -cz -f - /data > backup.tgz`).
- **Image updates**: `docker compose pull && docker compose up -d`
  performs a rolling update; Compose recreates one service at a
  time and waits for health before moving on.

## Why this recipe instead of Docker Swarm or Kubernetes?

Compose is the simplest path to a multi-node cluster on one
machine. No orchestrator setup, no overlay networks, no operator.
Right answer when you want a cluster for development, demos, or
CI integration tests.

For multi-host deployments or production:
- See [docker-swarm.md](./docker-swarm.md) — Swarm services across
  multiple Docker hosts with first-class secrets and overlay
  networking.
- See [kubernetes.md](./kubernetes.md) — full StatefulSet recipe
  with PVCs, ConfigMaps, Secrets, and probe wiring.
