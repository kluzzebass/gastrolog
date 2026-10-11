# Users & Security

GastroLog uses JWT-based authentication with role-based access control.

## First-Time Setup

On first launch, GastroLog has no users. The first user to register is automatically assigned the **admin** role. After the first user is created, registration is closed and new users must be created by an admin.

## Roles

| Role | Capabilities |
|------|-------------|
| **admin** | Full system access: query logs, manage configuration (vaults, ingesters, filters, policies), manage users (create, delete, reset passwords, change roles), manage [certificates](help:certificates) |
| **user** | Query logs and change own password. Settings open read-only: every value is visible and copyable, nothing offers a save, and a note at the top of the dialog says so. |

## Authentication

GastroLog uses **JWT (JSON Web Tokens)** signed with HMAC-SHA256.

- **Token lifetime**: Configurable in [Cluster settings](settings:service) [![icon:help]()](help:service-settings) — access tokens default to `1h`, refresh tokens to `168h` (7 days). Uses Go duration syntax (e.g., `15m`, `1h`, `168h`).
- **JWT secret**: The signing key for tokens. Regenerating it invalidates all existing sessions immediately.
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

A configurable **minimum password length** is enforced for all password changes and new user creation. The default minimum is 8 characters. This is set in [Cluster settings](settings:service) [![icon:help]()](help:service-settings).

## No-Auth Mode

Starting the server with `--no-auth` disables all authentication. Every request is treated as an implicit admin user. The login page is skipped, and user management is hidden from settings. This is intended for trusted networks or local development where authentication is unnecessary overhead.
