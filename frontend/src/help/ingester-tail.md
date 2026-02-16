# Tail

Type: `tail`

Follows local log files, similar to `tail -f`. Tracks file offsets across restarts so no lines are missed or duplicated. Handles file rotation (detects inode changes) and truncation.

| Param | Description | Default |
|-------|-------------|---------|
| `paths` | JSON array of glob patterns (required) | |
| `poll_interval` | How often to check for new data | `30s` |

**Example paths**: `["/var/log/*.log", "/opt/app/logs/**/*.log"]`

## Attributes

| Attribute | Source |
|-----------|--------|
| `file` | Absolute path of the file being tailed |

Maximum line size is 1 MB. Uses filesystem notifications for efficient change detection, with polling as a fallback.
