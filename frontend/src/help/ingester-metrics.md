# Metrics

Type: `metrics`

A self-monitoring ingester that periodically emits process-level system metrics (CPU, memory, goroutines, ingest queue) as log records. This makes internal health data persistent, queryable, and visible in the timeline alongside application logs.

Each tick produces a single space-separated key=value record:

```
cpu_percent=1.2
heap_alloc_bytes=8388608
heap_inuse_bytes=12582912
heap_idle_bytes=4194304
heap_released_bytes=2097152
stack_inuse_bytes=524288
sys_bytes=20971520
rss_bytes=31457280
heap_objects=42156
num_gc=12
num_goroutine=15
ingest_queue_depth=3
ingest_queue_capacity=1000
```

## Settings

| Setting | Description | Default |
|---------|-------------|---------|
| Interval | How often to emit a metrics record | `30s` |

## Attributes

Every record is emitted with:

- `ingester_type=metrics`
- `level=info`

## Fields

| Key | Description |
|-----|-------------|
| `cpu_percent` | Process CPU usage since last sample (0–100+, can exceed 100% on multi-core) |
| `heap_alloc_bytes` | Bytes of live heap objects |
| `heap_inuse_bytes` | Bytes in in-use heap spans |
| `heap_idle_bytes` | Bytes in idle (unused) heap spans |
| `heap_released_bytes` | Heap bytes released back to the OS |
| `stack_inuse_bytes` | Bytes in stack spans |
| `sys_bytes` | Total virtual memory obtained from the OS |
| `rss_bytes` | Peak resident set size (from getrusage) |
| `heap_objects` | Number of live heap objects |
| `num_gc` | Completed GC cycles |
| `num_goroutine` | Current goroutine count |
| `ingest_queue_depth` | Messages currently in the ingest queue |
| `ingest_queue_capacity` | Total ingest queue capacity |

## Querying

Because the record uses key=value format, the KV index picks up all fields automatically. You can search with expressions like:

- `cpu_percent>50` — find high-CPU samples
- `heap_inuse_bytes>100000000` — find samples where heap exceeds ~100 MB
- `ingest_queue_depth>0` — find moments when the queue is non-empty
- `ingester_type=metrics` — show all metrics records
