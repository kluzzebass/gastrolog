# Retention Policies

A retention policy defines **when** sealed chunks fire a retention event. Multiple conditions can be combined — a chunk fires the event if **any** condition says so (union semantics).

## Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **TTL** | `maxAge` | Fire when chunks age past this duration | `720h` (30 days) |
| **Total size** | `maxBytes` | Keep total vault size under this limit, firing on oldest chunks first | `10GB` |
| **Chunk count** | `maxChunks` | Keep at most this many sealed chunks, firing on oldest excess | `100` |

## Size Budget

A retention policy can also carry a **Size Budget** — a per-node byte budget for the *whole local disk claim* of any vault this policy is attached to: sealed chunks, indexes, and pipeline segment backlog. This is a different mechanism from the conditions above: it does not itself fire a retention event or destroy anything.

At the budget, the cluster **refuses** new records for the vault (cap-and-refuse: everything already accepted is kept, the newest is nacked) until retention — the conditions above — drains the vault's disk claim back under the budget. This is the opposite durability trade from a size drain trigger (**Total size**, above), which keeps the newest by destroying the oldest (cap-and-drain). Set a size drain trigger *below* the Size Budget so retention drains ahead of the cap, with the budget as the hard backstop. A warning alarm raises at 90% of the budget.

A policy that sets **only** a Size Budget, with no drain condition, is legal and meaningful — the refuse bound applies even though the policy drains nothing.

**Min-wins across attached policies:** a vault can attach more than one retention policy. If more than one attached policy carries a Size Budget, the vault's effective budget is the **lowest** of them — the most restrictive budget wins.

**Default floor:** a vault with no retention rules, or whose attached policies carry no Size Budget at all, still gets a bound — the system default (`1GiB`) applies. An unbounded file vault is not representable.

## What Happens When a Retention Event Fires

A retention event always does the same thing:

1. The chunk's records are streamed through the routing engine with `source = retention-trigger(vault)`.
2. The original chunk is **destroyed** — indexes first, then the chunk data.

The routing engine's verdict drives placement: if any [retention-trigger route](help:routing) matches, the records are re-appended to that route's destination vaults (per-record filtering applies). If no route matches, the records are dropped — the chunk just goes away. Either way, the source chunk is gone.

## How Retention Runs

Retention policies are evaluated periodically by a [background scheduler](help:inspector-jobs). On each run:

1. The policy receives a snapshot of all sealed chunks in the vault
2. **TTL**: Flags any chunk whose **EndTS** (the WriteTS of its last record) is older than the configured duration
3. **Total size**: Walks chunks from newest to oldest, keeping those that fit within the byte budget. Everything beyond the budget is flagged.
4. **Chunk count**: Keeps the newest N chunks, flags the rest
5. The union of all flagged chunks fires retention events.

In a [cluster](help:clustering), retention runs independently on the [node](help:clustering-nodes) hosting each vault. A [vault](help:storage) with no retention policy keeps chunks indefinitely. See also [Rotation](help:policy-rotation) for when chunks are sealed.

## Re-routing Aged Chunks

To keep records (in another vault) instead of dropping them on retention:

1. Create a route with **Source = Retention trigger** in [Settings → Routes](settings:routes).
2. Configure its filter and destinations as needed — the destinations receive the routed records.
3. Routes with `Source = Retention trigger` are excluded from live ingestion automatically, so re-routed records can't loop back through the ingestion pipeline.

The vault's retention rule itself just specifies the trigger policy. The "what happens to the records" decision lives entirely on the routing table.

## Example

A retention policy with `maxAge: "720h"` and `maxBytes: "50GB"` will fire on chunks older than 30 days **and** also fire on the oldest chunks if total vault size exceeds 50 GB.

## Choosing a Strategy

Conditions use union semantics — a chunk fires if **any** condition matches. This means conditions work together to enforce the most restrictive limit.

**Common patterns:**

| Pattern | Configuration | Use case |
|---------|--------------|----------|
| **Fixed window** | `maxAge: 720h` (30 days) | Compliance or operational policy — data older than N days is gone |
| **Budget cap** | `maxBytes: 50GB` | Fixed disk allocation — oldest chunks are evicted when space runs low |
| **Rolling window** | `maxChunks: 100` | Keep a fixed number of chunks regardless of size or age |
| **Belt and suspenders** | `maxAge: 720h` + `maxBytes: 100GB` | TTL for predictable expiry, size cap as a safety net for bursts |

**Combining TTL with a size trigger:** Use TTL as the primary control and the size drain trigger as a guardrail. Under normal load, TTL governs what gets deleted. During traffic spikes, the size trigger prevents the vault from consuming all available disk before chunks age out.

**Layered storage via retention-trigger routes:** Instead of dropping old data, re-route it to a cloud-backed vault for long-term archival:

1. Create a [cloud-backed vault](help:storage-cloud) (e.g. S3) — this is your archive vault.
2. Create a route with `Source = Retention trigger`, a `*` filter, and the archive vault as destination.
3. On your upstream vault (e.g. a fast local vault), set a retention rule and choose `Send records to routing engine` as the disposition.

Records flow: upstream vault retention fires → routing engine matches the retention-trigger route → archive vault (cloud-backed). The upstream vault stays small and fast; the archive vault accumulates history in cheap cloud storage. Queries automatically search both.

**No retention:** Omitting a retention policy means chunks accumulate forever. This is fine for testing but will eventually fill the disk in production. Always configure retention for production vaults.
