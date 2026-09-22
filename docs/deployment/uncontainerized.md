# Running GastroLog without containers

You don't need Docker. The binary is a single static Go executable
that runs on Linux, macOS, and Windows with no system dependencies.

## Get the binary

### From source

```sh
git clone https://github.com/kluzzebass/gastrolog
cd gastrolog
just build           # produces build/gastrolog
```

Or directly with Go:

```sh
mkdir -p build
cd backend
go build -o ../build/gastrolog ./cmd/gastrolog
```

### From a release

When releases are published they'll appear at
https://github.com/kluzzebass/gastrolog/releases. Download the
binary for your OS/arch and `chmod +x gastrolog`.

### From Homebrew

```sh
brew install kluzzebass/tap/gastrolog
```

## Single-node quickstart

```sh
gastrolog server
```

That's it. The server picks a sensible default `--home` directory
(`~/.config/gastrolog` on Linux, `~/Library/Application Support/gastrolog`
on macOS, `%APPDATA%\gastrolog` on Windows), stores its data there,
and listens on `:4564`.

Open http://localhost:4564, complete the first-access admin
registration screen, and you're running.

To run unattended (no interactive admin prompt):

```sh
gastrolog server \
  --initial-admin-user admin \
  --initial-admin-password change-me
```

## Multi-node cluster (development)

For local development, the project ships a script that bootstraps
a multi-node cluster across a single machine using different
ports per node:

```sh
just backend cluster-init    # provision a fresh 4-node cluster
just backend cluster-run     # start it (interactive imux TUI)
```

The cluster runs node 1 at `localhost:4564`, node 2 at `localhost:4574`,
etc. Each node has its own data directory under `data/node{N}`.

See [`scripts/cluster.sh`](../../scripts/cluster.sh) for the
full bootstrap dance — admin registration, vault setup, route
configuration, and ingester wiring.

## Multi-node cluster (production)

Run the binary on each host. One host is the bootstrap node:

```sh
# host A — bootstrap
gastrolog server \
  --home /var/lib/gastrolog \
  --listen :4564 \
  --cluster-addr hostA:4566 \
  --initial-admin-file /etc/gastrolog/admin-creds
```

Mint a join token on any node already in the cluster, immediately
before starting the joiner. Tokens expire, so mint one when you are
about to use it rather than keeping one around:

```sh
gastrolog cluster join-token
```

Then on each joiner:

```sh
# host B — joiner
gastrolog server \
  --home /var/lib/gastrolog \
  --listen :4564 \
  --cluster-addr hostB:4566 \
  --join-addr hostA:4566 \
  --join-token <token>
```

`--join-addr` is any node already in the cluster, not a specific one:
the node reached serves the join wherever it sits in the configuration.

For unattended joins, have an existing node serve tokens with
`--bootstrap-token-serve-secret` and point joiners at it with
`--bootstrap-token-url` and `--bootstrap-token-secret`; it mints a fresh
token per request, so a joiner started much later still gets a live one.

## systemd unit example

```ini
# /etc/systemd/system/gastrolog.service
[Unit]
Description=GastroLog log aggregation service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=gastrolog
Group=gastrolog
ExecStart=/usr/local/bin/gastrolog server \
  --home /var/lib/gastrolog \
  --listen :4564 \
  --cluster-addr %H:4566 \
  --initial-admin-file /etc/gastrolog/admin-creds
Restart=on-failure
RestartSec=5s

# Hardening (optional but recommended)
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/gastrolog
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

`%H` is systemd's runtime hostname substitution — useful so the
same unit file can run on every host without needing per-host
edits.

## CLI flags reference

See `gastrolog server --help` for all flags. The most relevant for
non-container deployments:

| Flag | Purpose |
|---|---|
| `--home <path>` | Persistent state directory. Default: platform config dir. |
| `--vaults <path>` | Vault storage. Default: `<home>/vaults`. |
| `--listen <addr>` | HTTP / Connect-RPC listen address. Default: `:4564`. |
| `--cluster-addr <addr>` | Cluster gRPC listen address. Default: `:4566`. **Set to a routable address (`hostname:4566`) for multi-host clusters.** |
| `--name <name>` | Stable node name. Default: random petname. |
| `--join-addr <addr>` / `--join-token <token>` | Cluster enrollment for joiners. |
| `--bootstrap-token-serve-secret <secret>` | Serve minted join tokens at `GET /cluster/bootstrap-token`, gated on this secret. |
| `--bootstrap-token-url <url>` / `--bootstrap-token-secret <secret>` | Joiner only: fetch a minted token from that endpoint instead of passing `--join-token`. |
| `--initial-admin-file <path>` | Bootstrap node only: provision admin credentials from a file. |
| `--no-auth` | Disable authentication. Testing only. |

## Why bother containerizing?

Containers solve packaging and orchestration; they don't make a Go
binary "more correct." If you have a host with systemd, a static
binary plus a unit file is often the simpler, more debuggable
deployment. Use containers when you want orchestration features
(rolling updates, replicated services, cluster-wide secrets), not
because you think it's the only way to run modern software.

## Next steps

- **Containerized variants**: see the sibling docs for
  [docker-compose.md](./docker-compose.md),
  [docker-swarm.md](./docker-swarm.md),
  [kubernetes.md](./kubernetes.md).
- **All env vars** that the entrypoint understands: see
  [container_environment.md](../container_environment.md). They
  don't apply when running the binary directly — the entrypoint
  is what translates env vars to flags.
