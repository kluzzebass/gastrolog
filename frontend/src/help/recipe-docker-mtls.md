# Docker with mTLS

Ship container logs from a remote Docker daemon over TCP with mutual TLS authentication. This ensures both sides verify each other's identity — GastroLog trusts only the Docker daemon, and Docker trusts only GastroLog.

## Overview

```
Docker daemon (server)          GastroLog (client)
────────────────────           ─────────────────────
server.pem + server-key.pem    tls_cert → client cert
Verifies client cert via CA    tls_ca   → CA cert
Listens on :2376 (TLS)         Connects to host:2376
```

Both sides share the same CA. The CA signs the server certificate (Docker) and the client certificate (GastroLog).

## 1. Generate certificates

Use `openssl` to create a CA, server cert, and client cert. Replace `dockerhost.example.com` with the actual hostname or IP of the Docker machine.

```bash
# Create CA
openssl genrsa -out ca-key.pem 4096
openssl req -new -x509 -days 3650 -key ca-key.pem \
  -sha256 -out ca.pem -subj "/CN=docker-ca"

# Create server key and CSR
openssl genrsa -out server-key.pem 4096
openssl req -new -key server-key.pem \
  -subj "/CN=dockerhost.example.com" -out server.csr

# Sign server cert (include SANs for IP or hostname)
echo "subjectAltName=DNS:dockerhost.example.com,IP:192.168.1.10" > extfile.cnf
echo "extendedKeyUsage=serverAuth" >> extfile.cnf
openssl x509 -req -days 3650 -in server.csr \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-cert.pem -extfile extfile.cnf

# Create client key and CSR
openssl genrsa -out client-key.pem 4096
openssl req -new -key client-key.pem \
  -subj "/CN=gastrolog-client" -out client.csr

# Sign client cert
echo "extendedKeyUsage=clientAuth" > client-extfile.cnf
openssl x509 -req -days 3650 -in client.csr \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out client-cert.pem -extfile client-extfile.cnf

# Clean up CSRs
rm server.csr client.csr extfile.cnf client-extfile.cnf
```

You now have:

| File | Purpose | Goes to |
|------|---------|---------|
| `ca.pem` | CA certificate | Both sides |
| `server-cert.pem` | Server certificate | Docker daemon |
| `server-key.pem` | Server private key | Docker daemon |
| `client-cert.pem` | Client certificate | GastroLog |
| `client-key.pem` | Client private key | GastroLog |

## 2. Configure Docker daemon

Copy `ca.pem`, `server-cert.pem`, and `server-key.pem` to the Docker host (e.g., `/etc/docker/tls/`).

Edit `/etc/docker/daemon.json`:

```json
{
  "hosts": ["unix:///var/run/docker.sock", "tcp://0.0.0.0:2376"],
  "tls": true,
  "tlsverify": true,
  "tlscacert": "/etc/docker/tls/ca.pem",
  "tlscert": "/etc/docker/tls/server-cert.pem",
  "tlskey": "/etc/docker/tls/server-key.pem"
}
```

If Docker is managed by systemd and the unit file passes `-H fd://`, you need to override it — the `hosts` key in `daemon.json` conflicts with the `-H` flag:

```bash
sudo systemctl edit docker
```

Add:

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/dockerd
```

Then restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

Verify Docker is listening on TLS:

```bash
openssl s_client -connect dockerhost.example.com:2376 \
  -cert client-cert.pem -key client-key.pem -CAfile ca.pem
```

## 3. Upload certificates to GastroLog

In GastroLog, go to **Settings → Certificates** and add two certificates:

1. **CA certificate** — paste `ca.pem` content. Name it something like `docker-ca`.
2. **Client certificate** — paste both `client-cert.pem` and `client-key.pem`. Name it `docker-client`.

## 4. Configure the Docker ingester

In **Settings → Ingesters**, create a new Docker ingester with these parameters:

| Param | Value |
|-------|-------|
| `host` | `tcp://dockerhost.example.com:2376` |
| `tls` | `true` |
| `tls_ca` | `docker-ca` |
| `tls_cert` | `docker-client` |
| `tls_verify` | `true` |

Use the **Test Connection** button to verify connectivity before saving.

## Filtering containers

Use the `filter` param to limit which containers are logged. It accepts boolean expressions over container metadata (`name`, `image`, `label.<key>`):

- `filter` set to `label.logging=true` — only containers with the Docker label `logging=true`
- `filter` set to `name=myapp-*` — only containers whose name starts with `myapp-`
- `filter` set to `image=nginx*` — only containers running an nginx image
- `filter` set to `name=web* AND label.env=prod` — web containers in production
- `filter` set to `label.logging=*` — any container that has a `logging` label regardless of value

See [Docker ingester](help:ingester-docker) for the full parameter reference and expression syntax.
