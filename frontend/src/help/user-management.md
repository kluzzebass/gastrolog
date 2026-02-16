# Users & Security

GastroLog uses JWT-based authentication with role-based access control, and supports TLS with certificate management.

## First-Time Setup

On first launch, GastroLog has no users. The first user to register is automatically assigned the **admin** role. After the first user is created, registration is closed and new users must be created by an admin.

## Roles

| Role | Capabilities |
|------|-------------|
| **admin** | Full system access: query logs, manage configuration (stores, ingesters, filters, policies), manage users (create, delete, reset passwords, change roles), manage certificates |
| **user** | Query logs and change own password |

## Authentication

GastroLog uses **JWT (JSON Web Tokens)** signed with HMAC-SHA256.

- **Token lifetime**: Configurable in Service settings (default: 7 days). Uses Go duration syntax (e.g., `168h`, `720h`).
- **JWT secret**: The signing key for tokens. Changing it invalidates all existing sessions immediately.
- Tokens carry the username, role, and user ID as claims.

## User Operations

### As an Admin

- **Create user**: Set username, password, and role
- **List users**: See all accounts
- **Change role**: Promote a user to admin or demote to user
- **Reset password**: Set a new password for any user
- **Delete user**: Remove an account

### As a User

- **Change password**: Update your own password (requires current password)

## Password Policy

A configurable **minimum password length** is enforced for all password changes and new user creation. The default minimum is 8 characters. This is set in Service settings.

## TLS / HTTPS

GastroLog can serve traffic over HTTPS in addition to HTTP.

- **HTTP**: Always available on the main port (`:4564` by default)
- **HTTPS**: Available on a secondary port (`:4565`) when TLS is enabled and a default certificate is configured
- **HTTP-to-HTTPS redirect**: Optionally redirects HTTP requests to the HTTPS port

TLS is configured in Service settings with these options:

| Setting | Description |
|---------|-------------|
| **TLS Enabled** | Enable HTTPS listener |
| **Default Certificate** | Certificate used for non-SNI clients |
| **HTTP to HTTPS Redirect** | Redirect HTTP traffic to HTTPS |

## Certificate Management

Certificates are managed through the Settings dialog (admin only). Each certificate has a name used for identification and SNI (Server Name Indication) matching.

**Two ways to provide certificates:**

1. **PEM content**: Paste certificate and private key PEM data directly
2. **File paths**: Point to certificate and key files on disk. File paths take precedence when both are provided.

**Hot reload**: Certificates loaded from file paths are watched for changes. When a certificate file is updated on disk, GastroLog reloads it automatically without requiring a restart. This supports zero-downtime certificate rotation (e.g., from Let's Encrypt renewals).

**SNI support**: When multiple certificates are configured, the server selects the correct certificate based on the hostname in the TLS handshake. Clients that don't send SNI receive the default certificate.

**Security**: Private keys are never returned by the API. When updating a certificate, sending an empty private key keeps the existing key.

## Service Settings

Authentication and server-related settings:

| Setting | Description | Default |
|---------|-------------|---------|
| **Token Duration** | How long JWT tokens remain valid | `168h` (7 days) |
| **JWT Secret** | Signing key (never displayed; paste to replace) | |
| **Minimum Password Length** | Minimum characters for passwords | `8` |
| **Query Timeout** | Maximum query execution time (`0s` = disabled) | Disabled |
| **Max Concurrent Jobs** | Parallel background jobs (rotation, retention, indexing) | `4` |
