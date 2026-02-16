# Settings

The Settings dialog is where you configure your GastroLog instance. Open it from the header bar (admin only for most tabs).

| Tab | What it configures |
|-----|-------------------|
| [**Service**](help:service-settings) | Token lifetime, JWT secret, password policy, query timeout, max jobs, TLS |
| [**Certificates**](help:certificates) | TLS certificates for HTTPS and secure connections |
| [**Users**](help:user-management) | User accounts, roles, and authentication |
| [**Ingesters**](help:ingesters) | Log sources — syslog, HTTP, RELP, tail, Docker, chatterbox |
| [**Filters**](help:routing) | Routing rules that control which records reach which stores |
| [**Rotation Policies**](help:policy-rotation) | When to seal the active chunk and start a new one |
| [**Retention Policies**](help:policy-retention) | When to delete old sealed chunks |
| [**Stores**](help:storage-engines) | Where logs are persisted — file or memory engines |

Changes take effect immediately. Use the [Inspector](help:inspector) to verify runtime state after making changes.
