# Indexers

When a chunk is sealed, GastroLog builds **inverted indexes** that map search terms to record positions. These indexes allow the query engine to skip records that can't match, dramatically reducing scan time on large chunks.

## Index Types

### Token Index

Maps individual **tokens** (words) extracted from record payloads to the positions where they appear.

- Used for bare-word searches like `error` or `timeout`
- Tokenization rules: ASCII alphanumeric plus underscore and hyphen, 2-16 characters, lowercased
- Pure numeric tokens and UUIDs are excluded to keep index size manageable
- The same tokenizer runs at both index time and query time, ensuring consistent matches

### Attribute Indexes

Three indexes built from record **attributes** (the `Attrs` key-value pairs stored alongside each record):

| Index | Maps | Used for |
|-------|------|----------|
| **AttrKey** | Key to positions | `key=*` (key exists) |
| **AttrValue** | Value to positions | `*=value` (value exists) |
| **AttrKV** | Key+Value to positions | `key=value` (exact match) |

Attribute indexes are authoritative — they reflect exactly what is stored in each record's attributes.

### KV Indexes

Three indexes built by **extracting key=value pairs from message text** (the raw payload):

| Index | Maps | Used for |
|-------|------|----------|
| **KVKey** | Key to positions | `key=*` on message text |
| **KVValue** | Value to positions | `*=value` on message text |
| **KV** | Key+Value to positions | `key=value` on message text |

KV indexes are **non-authoritative** — they are built by heuristic extraction and may be incomplete. A budget mechanism caps the number of distinct keys/values indexed per chunk. If the budget is exceeded, the index is marked as **capped** and the query engine falls back to runtime filtering for affected predicates.

Extraction format: `key=value` or `key="quoted value"` patterns in the log text. Key matching is case-insensitive.

## How Indexes Are Built

```mermaid
flowchart LR
    A[Active Chunk] -->|Seal| B[Sealed Chunk]
    B --> C[Index Build Job]
    C --> D[Token Index]
    C --> E[Attr Indexes]
    C --> F[KV Indexes]
```

1. A chunk is **sealed** (rotation policy triggers or manual seal)
2. The orchestrator schedules an asynchronous **index build job**
3. Each indexer reads the sealed chunk's records via a cursor and writes its index artifacts
4. Build jobs are deduplicated — concurrent requests to index the same chunk collapse into one operation
5. Indexers are **idempotent**: re-running a build overwrites any existing artifacts

Indexes are only built for sealed chunks. The active (unsealed) chunk is always scanned at runtime.

## Query Acceleration

When the query engine processes a sealed chunk:

```mermaid
flowchart TD
    A[Boolean Expression] --> B[DNF Compilation]
    B --> C[Branch 1: error AND level=error]
    B --> D[Branch 2: warn AND host=*]
    C --> E{Indexes\nAvailable?}
    D --> E
    E -->|Yes| F[Intersect Position Lists]
    E -->|No| G[Runtime Filter]
    F --> H[Union Results]
    G --> H
```

1. The boolean expression is converted to **Disjunctive Normal Form** (DNF) — a union of conjunctions
2. For each conjunction branch, the engine checks which predicates have index coverage
3. **Index-driven scan**: If all predicates in a branch are indexed, the engine intersects their position lists and reads only those records
4. **Runtime fallback**: If any predicate lacks index coverage (e.g., capped KV index, unsealed chunk), matching records are filtered at scan time
5. Multiple DNF branches are unioned together

The query plan (accessible via the Explain button) shows which indexes were used and which predicates fell back to runtime filtering.
