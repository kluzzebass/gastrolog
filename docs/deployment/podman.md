# Podman

[Podman](https://podman.io/) is a daemonless, rootless-friendly
alternative to Docker. The image and Compose recipe both work
under Podman with two small caveats.

## TL;DR

Use the same recipes as Docker:

```sh
# Single-node:
podman run --rm -d --name gastrolog \
  -p 4564:4564 \
  -v gastrolog-config:/config \
  -v gastrolog-vaults:/vaults \
  -e GASTROLOG_INITIAL_ADMIN_USER=admin \
  -e GASTROLOG_INITIAL_ADMIN_PASSWORD=change-me-please \
  ghcr.io/kluzzebass/gastrolog:latest

# Multi-node compose:
cd docker
podman compose up -d
```

See [quickstart.md](./quickstart.md) and
[docker-compose.md](./docker-compose.md) for the full recipes —
Podman uses the same compose file and the same env vars as Docker.

## Caveats

### Rootless networking

Podman defaults to rootless mode. In that mode, container DNS
between services on a Compose network works, but ports below 1024
require either:

- The setcap-granted CAP_NET_BIND_SERVICE on the Podman binary, or
- Running as root (`sudo podman`), or
- Using a port ≥ 1024 (which the GastroLog defaults already do —
  4564 and 4566 are above the threshold, so you're fine).

If you remap to a privileged port (e.g. `-p 80:4564`), expect
permission errors in rootless mode. The fix is `sudo podman` or
publishing on a high port and putting a privileged reverse proxy
in front.

### Volume permissions

In rootless mode, named volumes and bind mounts inside the
container are owned by the **rootless UID mapped into the
container's root** — typically `0` from the container's
perspective, mapped to your host UID. The image runs as `root`
inside (busybox), which is fine — no UID mismatches in the simple
case.

If you bind-mount a host directory (`-v ./mydata:/config`), make
sure the host directory's permissions allow your effective UID to
write to it, or you'll see EACCES on first write.

### `podman compose` vs `docker-compose`

Modern Podman (4.x+) ships a `podman compose` subcommand that's a
drop-in replacement for `docker compose` for the YAML versions
GastroLog uses (compose-spec-compliant). Older Podman versions
relied on a separate `podman-compose` binary which is feature-thin
in places. If `secrets:` doesn't work for you, upgrade Podman
or use `--env-file` with `GASTROLOG_INITIAL_ADMIN_USER` and
`GASTROLOG_INITIAL_ADMIN_PASSWORD` instead of the secret mount
(less secure — env shows up in `podman inspect`).

## Verifying it works

After `podman compose up -d`:

```sh
for p in 4564 4574 4584; do
  curl -s -o /dev/null -w "port $p ready=%{http_code}\n" http://localhost:$p/readyz
done
```

Expected: all three return 200, same as Docker. If a node returns
000 (no connection), check `podman logs gastrolog-<service>` for
networking or DNS issues — usually rootless port collisions or
the `podman compose` version not understanding the compose file.

## When to choose Podman over Docker

Podman is the right call when you want:
- **Daemonless operation** (no privileged background daemon).
- **Rootless containers** by default.
- **Drop-in for Docker** in CI environments that prefer not to
  ship the Docker daemon.

Otherwise, Docker is fine and slightly more battle-tested for
the GastroLog use case. Both work.
