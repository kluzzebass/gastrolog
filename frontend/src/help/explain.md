# Explain

The Explain view shows the query engine's execution plan — how it intends to process your query before running it. Open it with the **Show Plan** button next to the search bar.

## What It Shows

- **Direction**: Whether results are scanned forward (oldest-first) or reverse (newest-first)
- **Chunks evaluated**: How many chunks will be scanned out of the total available
- **Expression**: Your query in its parsed form
- **Cost summary**: Estimated rows and bytes to process

## Per-Chunk Breakdown

Expand individual chunks to see the pipeline steps the engine will use:

- Which index lookups it can perform (fast path)
- Which predicates fall back to runtime filtering (slow path — scanning each record)
- Whether a chunk is skipped entirely (outside the time range)

## When to Use It

- **Slow queries**: Check if the engine is scanning more chunks than expected, or if key predicates aren't using indexes
- **Index verification**: Confirm that your search terms are hitting the token or KV index rather than falling back to scanning
- **Understanding capped indexes**: When a KV index has too many distinct keys, it's marked as capped and some predicates fall back to scanning. Explain shows when this happens.
