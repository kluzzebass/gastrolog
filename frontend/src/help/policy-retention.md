# Retention Policies

A retention policy defines **when** sealed chunks fire a retention event. Multiple conditions can be combined — a chunk fires the event if **any** condition says so (union semantics).

## Conditions

| Condition | Config field | Description | Example |
|-----------|-------------|-------------|---------|
| **TTL** | `maxAge` | Fire when chunks age past this duration | `720h` (30 days) |
| **Max size** | `maxSize` | The vault's disk-claim bound — combined meaning, below | `10GB` |
| **Chunk count** | `maxChunks` | Keep at most this many sealed chunks, firing on oldest excess | `100` |

## Hard vs Soft Bounds

Every set condition (age, size, or chunk count) is a **soft bound** by default: it drains, but never refuses admission — only the node-level disk-protect floor backstops the vault. Toggling a policy's **Refuse** flag on makes its bounds **hard**: once a retention sweep fails to clear the violation (size is checked instantaneously instead), the cluster refuses new records for the vault until drain catches up.

## Max Size

**Max size** is the vault's disk-claim bound, and it can mean two things at once:

- **Drain:** oldest sealed chunks fire retention events once the **sealed-chunk store's** disk claim exceeds the bound. Scope: only what retention can act on — sealed chunks and their indexes. Unconditional — set `maxSize` always drains, regardless of **Refuse**.
- **Refuse:** while the vault's **whole local footprint** — the sealed-chunk store plus the pipeline segment backlog — is at or over the bound, the cluster **refuses** new records for the vault (everything already accepted is kept, the newest is nacked) — the backstop while drain catches up or is deferred. A warning alarm raises at 90% of the bound. Only when the policy's **Refuse** flag is on — off by default, so a plain `maxSize` only drains.

**Min-wins across attached policies:** a vault can attach more than one retention policy. Drain mins over **every** attached policy that carries a max size; refuse mins over only the ones with **Refuse** on — the tightest such policy's bound wins for refusal, even if a looser soft policy is what actually triggers the drain.

**Default floor:** a vault with no retention rules, or whose attached policies carry no max size at all, still gets a bound — the system default applies, refuse-only (a default never destroys data). An unbounded file vault is not representable.

## What Happens When a Retention Event Fires

What happens depends on the vault's [Retention Disposition](help:vaults-config):

- **Delete** (default) and **Route** both destroy the original chunk — indexes first, then the chunk data — and decide the records' fate via the routing engine: with `route`, the chunk's records are streamed through the routing engine with `source = retention-trigger(vault)`, and if any [retention-trigger route](help:routing) matches, they're re-appended to that route's destination vaults (per-record filtering applies); with `delete`, or with `route` and no matching route, the records are simply dropped.
- **Transfer** skips the routing engine entirely: the sealed chunk itself is re-homed unchanged to the vault's **Transfer Target**. No decode, no re-ingest, no per-record filtering — the chunk's records and identity are untouched, only its retention clock resets at the destination. The original chunk is only removed from the source once the target confirms it holds the chunk.

## How Retention Runs

Retention policies are evaluated periodically by a [background scheduler](help:inspector-jobs). On each run:

1. The policy receives a snapshot of all sealed chunks in the vault
2. **TTL**: Flags any chunk whose **EndTS** (the WriteTS of its last record) is older than the configured duration
3. **Max size**: Walks chunks from newest to oldest, keeping those that fit within the bound. Everything beyond the bound is flagged. (Admission refusal, the other half of max size, runs separately on the disk guard — not part of this sweep.)
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

A retention policy with `maxAge: "720h"` and `maxSize: "50GB"` will fire on chunks older than 30 days **and** also fire on the oldest chunks if the vault's disk claim exceeds 50 GB. With **Refuse** on, it also refuses new records while either bound stays violated (size instantly, age once a sweep fails to clear it); left off (the default), both bounds only drain.

## Choosing a Strategy

Conditions use union semantics — a chunk fires if **any** condition matches. This means conditions work together to enforce the most restrictive limit.

**Common patterns:**

| Pattern | Configuration | Use case |
|---------|--------------|----------|
| **Fixed window** | `maxAge: 720h` (30 days) | Compliance or operational policy — data older than N days is gone |
| **Bound cap** | `maxSize: 50GB` | Fixed disk allocation — oldest chunks drain when space runs low; add Refuse to also refuse admission if drain can't keep up |
| **Rolling window** | `maxChunks: 100` | Keep a fixed number of chunks regardless of size or age |
| **Belt and suspenders** | `maxAge: 720h` + `maxSize: 100GB` | TTL for predictable expiry, size bound as a safety net for bursts |

**Combining TTL with a size bound:** Use TTL as the primary control and max size as a guardrail. Under normal load, TTL governs what gets deleted. During traffic spikes, the size bound prevents the vault from consuming all available disk before chunks age out — draining what it can and refusing admission if it can't keep up.

**Layered storage via retention-trigger routes:** Instead of dropping old data, re-route it to a cloud-backed vault for long-term archival:

1. Create a [cloud-backed vault](help:storage-cloud) (e.g. S3) — this is your archive vault.
2. Create a route with `Source = Retention trigger`, a `*` filter, and the archive vault as destination.
3. On your upstream vault (e.g. a fast local vault), set a retention rule and choose `Send records to routing engine` as the disposition.

Records flow: upstream vault retention fires → routing engine matches the retention-trigger route → archive vault (cloud-backed). The upstream vault stays small and fast; the archive vault accumulates history in cheap cloud storage. Queries automatically search both.

**Layered storage via transfer (the common case):** When the archive target is a plain local file vault rather than a cloud-backed one, [Transfer disposition](help:vaults-config) is simpler than a retention-trigger route — no route to configure, no re-ingest through the routing engine. Set the upstream vault's retention rule, choose `Transfer records to another vault unchanged` as the disposition, and pick the archive vault as the **Transfer Target**. The sealed chunk moves to the target as-is; queries search both vaults exactly as with the routed pattern above. Both vaults must be plain (non-cloud) file vaults — for a cloud-backed archive, use the retention-trigger route pattern instead.

**No retention:** Omitting a retention policy means chunks accumulate forever. This is fine for testing but will eventually fill the disk in production. Always configure retention for production vaults.
