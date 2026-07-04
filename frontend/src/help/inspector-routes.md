# Routes

The Routes view shows live routing statistics aggregated across the whole
[cluster](help:clustering) — any node answers for all nodes.

## Totals

Counters since process start, summed across nodes:

- **Ingested** — records that entered the routing stage (after ingest and
  [digestion](help:digesters)).
- **Routed** — records that matched at least one [route](help:routing) and
  were delivered to a destination vault. A record fanned out to several
  vaults still counts once.
- **Dropped** — records that matched **no** route. These are silently
  discarded; a nonzero value usually means a producer is sending data no
  route claims.
- **Drop rate** — dropped as a percentage of ingested.

The three counters always satisfy `ingested = routed + dropped`.

## Throughput

Live rates computed from rolling windows on each node and summed across the
cluster:

- **Ingest rate** — records per second entering the routing stage.
- **Route rate** — records per second matching at least one route.

On a cluster where every record matches a route (zero drops), the two rates
are identical by construction — the gap between them is the *drop rate as it
happens*, which is the misconfiguration this pair is designed to surface.

Each rate shows three horizons:

- The **large number** is the instantaneous rate: the counter delta over the
  last stats tick (~5 seconds). It jumps with genuine burstiness — that is
  signal, not noise — and the **sparkline** beside it shows its recent
  per-tick history (computed server-side; it survives closing the panel).
- **1m / 5m / 15m** are exponentially weighted moving averages, the same
  technique as the Unix load average: one number per horizon, folding each
  tick in with exponential decay. These are the sustained-rate figures to
  use when comparing performance before and after a change.

Note that "ingest" here is measured at the router, not at the ingesters: when
the ingest queue backs up, this rate reflects what flows *through* routing.
The queue depth gauge in the [System view](help:inspector-system) covers the
front door.

## Per-vault and per-route delivery

Matched-record counters broken down by destination vault and by route, summed
across nodes. Per-vault *rates* (append throughput, durability, queue
pressure) live on each vault's card in the Vaults view and in the System
view's Throughput section.
