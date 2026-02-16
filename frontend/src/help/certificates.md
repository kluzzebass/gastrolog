# Certificates

TLS certificates for HTTPS and secure ingester connections. Managed through the Settings dialog (admin only).

## Adding a Certificate

Each certificate has a **name** used for identification and SNI (Server Name Indication) matching. Two ways to provide the key material:

- **PEM content**: Paste certificate and private key PEM data directly. Supports drag-and-drop of `.pem`, `.crt`, `.key`, `.zip`, and `.tar.gz` files.
- **File paths**: Point to certificate and key files on disk. File paths take precedence when both are provided.

## Hot Reload

Certificates loaded from file paths are watched for changes. When a certificate file is updated on disk, GastroLog reloads it automatically without requiring a restart. This supports zero-downtime certificate rotation (e.g., from Let's Encrypt renewals).

## SNI Support

When multiple certificates are configured, the server selects the correct one based on the hostname in the TLS handshake. Clients that don't send SNI receive the default certificate.

## Default Certificate

Mark a certificate as the default to use it for the HTTPS listener. Enable TLS and configure the listener in [Service settings](help:service-settings).

## Security

Private keys are never returned by the API. When updating a certificate, sending an empty private key keeps the existing key.
