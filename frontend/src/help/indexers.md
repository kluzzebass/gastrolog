# Indexing

After a [chunk](help:general-concepts) is sealed, GastroLog builds **inverted indexes** on its contents in the background — data structures that map search terms to record positions. Instead of scanning every record, the engine looks up which positions match and reads only those. You don't configure or manage indexes — they're built automatically.

Index builds happen asynchronously after sealing, so write latency is unaffected.

## What Gets Indexed

Three things are extracted from each record and indexed:

**Words from the log text** — The raw message is split into tokens (words). When you search for `error` or `timeout`, the engine uses this index to find exactly which records contain those words. Tokens are 2–16 characters, case-insensitive, and exclude pure numbers and UUIDs.

**Record attributes** — The key-value pairs stored alongside each record (like `host=web-01` or `level=error`). These are indexed exactly as stored, so [`level=error` queries](help:query-language) are fast and precise.

**Key-value pairs from the log text** — The indexer also scans the raw message for `key=value` patterns (including logfmt, JSON fields, and access log fields). This lets you search for things like `status=500` even when the value only appears in the message body, not in the stored attributes. These indexes are best-effort — heuristic extraction may miss some values.

## What This Means for Your Searches

- **Bare words** like `error` use the token index — fast on sealed chunks
- **Key=value** like `level=error` checks both the attribute index and the text-extracted KV index
- **Numbers and UUIDs** can't be token-indexed, so they fall back to scanning (still works, just slower on large chunks)
- The **active chunk** (currently accepting writes) is always scanned — it hasn't been sealed yet so it has no indexes. This is fine because it's small.

If a KV index runs out of budget (too many distinct keys in one chunk), it's marked as **capped** and the engine falls back to scanning for those predicates. The [Explain](help:explain) view shows when this happens.
