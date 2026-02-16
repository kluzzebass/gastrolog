# Service Settings

Global server settings that affect authentication, query execution, and background processing.

| Setting | Description | Default |
|---------|-------------|---------|
| **Token Duration** | How long [JWT tokens](help:user-management) remain valid. Uses Go duration syntax (e.g., `168h`, `720h`) | `168h` (7 days) |
| **JWT Secret** | The signing key for authentication tokens. Never displayed; paste a new value to replace. Changing this invalidates all existing sessions immediately | |
| **Minimum Password Length** | Minimum characters required for [user](help:user-management) passwords | `8` |
| **Query Timeout** | Maximum [query](help:query-engine) execution time. Uses Go duration syntax (e.g., `30s`, `1m`). Set to empty or `0s` to disable | Disabled |
| **Max Concurrent Jobs** | How many [background jobs](help:inspector-jobs) ([rotation](help:policy-rotation), [retention](help:policy-retention), [indexing](help:indexers)) can run in parallel | `4` |

## TLS Configuration

When a [certificate](help:certificates) is configured as the default, additional TLS options appear:

| Setting | Description |
|---------|-------------|
| **TLS Enabled** | Enable the HTTPS listener on port `:4565` |
| **HTTP to HTTPS Redirect** | Redirect plain HTTP requests to the HTTPS port |
