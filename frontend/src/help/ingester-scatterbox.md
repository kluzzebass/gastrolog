# Scatterbox

Type: `scatterbox`

A deterministic test signal generator. Unlike [Chatterbox](help:ingester-chatterbox) (random data, random formats), Scatterbox produces predictable, traceable records with monotonic sequence numbers and precise generation timestamps. Every record can be verified end-to-end: gaps, reordering, duplicates, and latency are all detectable from the record body alone.

## Settings

| Setting | Description | Default |
|---------|-------------|---------|
| Interval | Emission interval (0 = one-shot mode, waits for trigger) | `1s` |
| Burst | Number of records emitted per interval or trigger | `1` |

## Modes

- **Continuous** (interval > 0): emits a burst of records every interval.
- **One-shot** (interval = 0): waits for external `Trigger()` calls, emits a burst each time. Useful for integration tests that need precise control over when records are produced.

## Record Format

Each record is a JSON object with embedded tracing fields:

```json
{"seq":42,"generated_at":"2026-03-16T12:00:00.000000000Z","ingester":"scatterbox-1"}
```

## Attributes

| Attribute | Source |
|-----------|--------|
| `seq` | Monotonic sequence number (never resets) |

## Timestamps

SourceTS is not set. IngestTS is set to GastroLog arrival time.
