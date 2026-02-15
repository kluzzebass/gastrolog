# User Management

GastroLog uses JWT-based authentication with role-based access control.

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
- **JWT secret**: The signing key for tokens. Changing it invalidates all existing sessions.
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

## Service Settings

Authentication-related settings in the Service configuration:

| Setting | Description |
|---------|-------------|
| **Token Duration** | How long JWT tokens remain valid |
| **JWT Secret** | Signing key (never displayed; paste to replace) |
| **Minimum Password Length** | Minimum characters for passwords |
