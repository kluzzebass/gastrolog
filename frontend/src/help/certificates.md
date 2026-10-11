# Certificates

TLS certificates for HTTPS and secure ingester connections. Managed through [Settings → Certificates](settings:certificates) (admin only).

## Adding a Certificate

Each certificate has a **name** used for identification and SNI (Server Name Indication) matching. Two kinds of certificate, by where the key material lives, and the card's kind badge says which one it is:

- **Pasted certificate** (badge **pem**): Paste certificate and private key PEM data directly; it is stored in the cluster configuration. Supports drag-and-drop of `.pem`, `.crt`, `.key`, `.zip`, and `.tar.gz` files.
- **File-path certificate** (badge **files**): Point to certificate and key files on the node's disk. File paths take precedence when both are provided.

Every certificate picker in the system — the default certificate, an ingester's TLS certificate and client CA, the Docker ingester's CA and client certificate — lists what is stored here; when nothing is, the picker says so and links to this section.

## File-path certificates

A file-path certificate's files are watched: when a certificate file changes on disk, GastroLog reloads it without a restart. This supports zero-downtime certificate rotation (e.g., from Let's Encrypt renewals).

## SNI Support

When multiple certificates are configured, the server selects the correct one based on the hostname in the TLS handshake. Clients that don't send SNI receive the default certificate.

## Default Certificate

**Use as the default HTTPS certificate** marks the certificate the HTTPS listener serves. Enable TLS and configure the listener in [Cluster settings](settings:service) [![icon:help]()](help:service-settings).

## Security

Private keys are never returned by the API. When updating a certificate, sending an empty private key keeps the existing key.
