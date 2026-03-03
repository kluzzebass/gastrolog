# Security

GastroLog provides authentication, role-based access control, and TLS encryption.

| Topic | What it covers |
|-------|---------------|
| [**Users & Authentication**](help:user-management) | JWT-based auth, roles (admin/user), password policy, no-auth mode |
| [**Certificates**](help:certificates) | TLS certificates for HTTPS and secure ingester connections, SNI, hot reload |

Security is configured through [Settings → Cluster](settings:service). See [Cluster settings](help:service-settings) for token lifetime, JWT secret, and TLS listener options.
