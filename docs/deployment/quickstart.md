# Quickstart: single-node `docker run`

The fastest way to try GastroLog. One container, one port, runs on
anything that has Docker.

## Run it

```sh
docker run --rm -d --name gastrolog \
  -p 4564:4564 \
  -v gastrolog-config:/config \
  -v gastrolog-vaults:/vaults \
  -e GASTROLOG_INITIAL_ADMIN_USER=admin \
  -e GASTROLOG_INITIAL_ADMIN_PASSWORD=change-me-please \
  ghcr.io/kluzzebass/gastrolog:latest
```

Open http://localhost:4564 and log in as `admin` / `change-me-please`.

That's it. You have a working single-node GastroLog with a usable
web UI, the API on port 4564, and persistent storage via two named
volumes.

## What's happening

| Element | Why |
|---|---|
| `-p 4564:4564` | Exposes the HTTP / Connect-RPC server (web UI + API). |
| `-v gastrolog-config:/config` | Cluster TLS, Raft logs, FSM state. The image declares this as a `VOLUME`; without one mounted, container removal loses everything. |
| `-v gastrolog-vaults:/vaults` | Sealed chunk data. Same persistence rules. |
| `GASTROLOG_INITIAL_ADMIN_USER` / `GASTROLOG_INITIAL_ADMIN_PASSWORD` | Provisions the initial admin at startup so you don't see the first-access registration screen. **Change the password.** |

## Variations

### Even simpler (no admin provisioning, ephemeral)

```sh
docker run --rm -p 4564:4564 ghcr.io/kluzzebass/gastrolog:latest
```

Open the dashboard, register the first admin via the on-page
prompt, then start using it. Container removal wipes everything.

### Disable auth entirely (testing only)

```sh
docker run --rm -p 4564:4564 \
  -e GASTROLOG_NO_AUTH=true \
  ghcr.io/kluzzebass/gastrolog:latest
```

No login required; every request is treated as admin. Don't expose
this beyond `localhost`.

### Bind to a specific host port

```sh
docker run --rm -p 8080:4564 ... ghcr.io/kluzzebass/gastrolog:latest
```

Then visit http://localhost:8080.

## Health probe

The container's built-in `HEALTHCHECK` probes `/healthz` every 30
seconds. `docker ps` reflects the result:

```sh
docker ps --filter name=gastrolog --format 'table {{.Names}}\t{{.Status}}'
# NAMES        STATUS
# gastrolog    Up 2 minutes (healthy)
```

You can also probe manually:

```sh
curl -s http://localhost:4564/healthz   # 200 — process up
curl -s http://localhost:4564/readyz    # 200 — ready for traffic
```

## Next steps

- **Multi-node cluster on one host**: see [docker-compose.md](./docker-compose.md).
- **Production-ish on a Docker Swarm**: see [docker-swarm.md](./docker-swarm.md).
- **Kubernetes (OrbStack, kind, anywhere)**: see [kubernetes.md](./kubernetes.md).
- **Without containers** (release binary, systemd, etc.): see [uncontainerized.md](./uncontainerized.md).
- **All the env vars in one table**: see [container_environment.md](../container_environment.md).
