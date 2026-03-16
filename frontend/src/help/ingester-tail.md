# Tail

Type: `tail`

Follows local log files, similar to `tail -f`. Tracks file offsets across restarts so no lines are missed or duplicated. Handles file rotation (detects inode changes) and truncation. Messages pass through [digestion](help:digesters) for level and timestamp extraction.

| Setting | Description | Default |
|---------|-------------|---------|
| File Patterns | Glob patterns, one per line (required) | |
| Poll Interval | How often to check for new data | `30s` |

**Example patterns** (one per line):
```
/var/log/*.log
/opt/app/logs/**/*.log
```

## Attributes

| Attribute | Source |
|-----------|--------|
| `file` | Absolute path of the file being tailed |

Maximum line size is 1 MB. Uses filesystem notifications for efficient change detection, with polling as a fallback.

## Timestamps

SourceTS is not set (log files don't carry per-line timestamps at the protocol level). IngestTS is set to the time the line was read. Use a [timestamp digester](help:digester-timestamp) to extract the original event time from the log line.
