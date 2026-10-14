# Docker Compose: 3-node cluster

Bring up a 3-node GastroLog cluster on a single Docker host.
Useful for local development, integration tests, and any
single-machine deployment that needs the redundancy a Raft cluster
provides.

## Quick start

```sh
just deploy compose-up
open http://localhost:4564           # admin / change-me
```

`compose-up` builds the `gastrolog:test` image the compose file names,
writes the push-service configs (rsyslog, OTel collector, fluent-bit)
that the compose file bind-mounts from `/tmp/gastrolog-deploy/`, starts
the project as `gastrolog-deploy`, waits for every node's `/readyz`, and
runs [`deploy/setup-deps.sh`](../../deploy/setup-deps.sh) against
`gastrolog-0` (see [configuring-deps.md](./configuring-deps.md)). A bare
`docker compose up` in `deploy/` has neither the image nor those configs.
To use a release image, change the `image:` line in the compose file's
`x-gastrolog-base` block to `ghcr.io/kluzzebass/gastrolog:latest`.

To tear down (and wipe data):

```sh
just deploy compose-down
```

## What's in the recipe

The compose file ([`deploy/compose.yml`](../../deploy/compose.yml))
defines three gastrolog services — `gastrolog-0`, `gastrolog-1`,
`gastrolog-2` — plus the bundled cloud-storage emulators and
ingester-source containers. Service name = container name = hostname
= `GASTROLOG_NAME`, so what you see in `docker ps` matches what's in
the gastrolog UI.

`gastrolog-0` is the bootstrap (serves join tokens); the rest
enroll with it.

### Architecture

| Service | Role | Host port | Cluster addr |
|---|---|---|---|
| gastrolog-0 | Cluster bootstrap (serves join tokens) | 4564 | gastrolog-0:4566 |
| gastrolog-1 | Joiner | 4574 | gastrolog-1:4566 |
| gastrolog-2 | Joiner | 4584 | gastrolog-2:4566 |

### Cluster bootstrap flow

1. `gastrolog-0` starts; generates cluster TLS and the key it mints join
   tokens with. No token is written anywhere: tokens expire, so one is
   minted when a joiner asks for it.
2. `gastrolog-1` and `gastrolog-2` start with
   `depends_on: condition: service_healthy`, so they wait for
   `gastrolog-0`'s `/healthz` probe to pass before starting.
3. Each joiner fetches a freshly minted token from
   `GASTROLOG_BOOTSTRAP_TOKEN_URL`, authenticating with
   `GASTROLOG_BOOTSTRAP_TOKEN_SECRET`, then enrols with
   `gastrolog-0:4566` and joins the Raft cluster.
4. Both joiners settle as `FOLLOWER` / `VOTER` within ~10 seconds.

### Admin credentials

Provisioned via a Compose `secret` mounted at
`/run/secrets/admin_creds`. `gastrolog-0` reads the file once at
startup, creates the admin user, and the file/secret becomes a no-op
on subsequent restarts (idempotency).

The secret file in the repo (`deploy/admin-creds`) is a **demo
placeholder** with `admin` / `change-me`. Replace it before
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

If you forget this setting, joiners enroll successfully (enrolment
reaches `gastrolog-0` by its join address) but get stuck at
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
docker exec gastrolog-0 /gastrolog --home /config cluster status

# Admin login:
curl -s -X POST -H "Content-Type: application/json" \
  http://localhost:4564/gastrolog.v1.AuthService/Login \
  -d '{"username":"admin","password":"change-me"}'
```

## Scaling out

Adding a fourth or fifth joiner is a copy-paste job. Duplicate
the `gastrolog-2` service block, increment the ordinal in name /
hostname / `GASTROLOG_NAME` / `GASTROLOG_CLUSTER_ADDR`, add a new
host port, declare new volumes for `gastrolog-N-config` and
`gastrolog-N-vaults`, and run `docker compose -p gastrolog-deploy -f
compose.yml up -d` from `deploy/` again. The copied block keeps
`GASTROLOG_JOIN_ADDR`, `GASTROLOG_BOOTSTRAP_TOKEN_URL` and
`GASTROLOG_BOOTSTRAP_TOKEN_SECRET`, so the new service fetches a
freshly minted token from `gastrolog-0` and enrolls automatically.
Give it file storage afterwards (`docker exec -i gastrolog-0 sh <
deploy/add-storage.sh`) so vaults can place on it.

For more than ~5 nodes or any cross-host deployment, switch to
[Docker Swarm](./docker-swarm.md) or [Kubernetes](./kubernetes.md).

## Production caveats

- **Admin password**: change `deploy/admin-creds` before exposing
  this beyond `localhost`.
- **External access**: this recipe exposes the dashboard on
  `localhost:4564` only. For external access, put a TLS-terminating
  reverse proxy (Caddy, nginx, Traefik) in front of port 4564.
- **Persistence**: each service has its own named volumes for
  `/config` and `/vaults`. `just deploy compose-down` (and
  `docker compose down -v`) deletes them. Use
  `docker compose -p gastrolog-deploy -f compose.yml down` (without
  `-v`) from `deploy/` to stop without wiping.
- **Backups**: snapshot the named volumes (`docker run --rm -v
  gastrolog-0-config:/data alpine tar -cz -f - /data > backup.tgz`).
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
