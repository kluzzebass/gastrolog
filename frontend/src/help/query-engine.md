# Searching

Searching is how you retrieve records from GastroLog. When you run a search, the query engine figures out the fastest way to find matching records — narrowing down which chunks to look at, using indexes where available, and merging results from multiple stores into a single time-ordered stream.

For the query syntax, see [Query Language](help:query-language). To aggregate results into tables and charts, see [Pipeline Queries](help:pipeline).

## How a Search Works

1. Your query is parsed into a normalized form (OR of ANDs) so the engine can plan each branch independently
2. Time bounds (`start=` / `end=`) are used to skip chunks that fall outside the range
3. The remaining chunks are scanned — sealed chunks use their [indexes](help:indexers) to jump to matching records, while the active chunk is scanned directly
4. If you're searching across multiple stores, results are merged by timestamp

When no `store=` filter is specified, all stores are searched in parallel.

## Pagination

Results come back a page at a time. A **resume token** tracks where each store left off, so the next page picks up exactly where the previous one stopped. Tokens stay valid as long as the referenced chunks still exist — if a retention policy deletes a chunk between pages, the search restarts from the beginning.

## Follow (Live Tail)

Follow streams new records as they arrive, similar to `tail -f`. It only shows records that appear after you start following — existing data is skipped. When combined with a query filter, only matching new records are shown.

## Histogram

The timeline visualization uses the histogram to show record counts over time, bucketed into evenly-spaced intervals. Each bucket includes per-severity counts (error, warn, info, debug, trace).

Without a filter, this is very fast — it counts records from the index without reading any record data. With a filter, it runs a full search and buckets the results.

## Context View

Click a record to see what was happening around it. The context view shows a configurable number of records before and after the selected one (up to 50 each), pulled from all stores by timestamp.

## Explain

The [Explain](help:explain) view shows how the engine plans to process your query — which chunks it will scan, whether it can use indexes, and which predicates fall back to runtime filtering. Useful for understanding why a query is slow or for verifying that your indexes are being used.

## Timeout

An optional query timeout can be configured in [Service settings](help:service-settings). When set, queries that run too long are cancelled automatically. Uses Go duration syntax (e.g., `30s`, `1m`). Set to empty or `0s` to disable.
