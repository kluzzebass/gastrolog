# Events

The Events view is the cluster's **event journal**: a record of things that
*happened*, as opposed to the alarm list, which shows conditions that
*currently need action*. Events require nothing of you — that's why this
page has no acknowledge or shelve controls and no severity coloring.

## What gets journaled

- **Alarm lifecycle transitions** — one entry per transition:
  `alarm-raised` (the condition annunciated), `alarm-cleared` (the
  condition resolved; the detail says whether the alarm released or was
  retained until acknowledged), `alarm-acked`, `alarm-shelved`,
  `alarm-unshelved` (operator actions, recorded with who), and
  `alarm-shelve-expired` (a shelve lapsed on its own). This is the audit
  trail the alarm list deliberately does not carry — history in the alarm
  list is what makes alarm lists unreadable.
- **Demoted diagnostics** — transition edges of conditions that are
  informative but need no operator action: `election-storm` and
  `raft-wal-latency` (engaged and back-to-normal edges) and
  `channel-pressure` (ingest pipeline pressure level changes).
- **`node-started`** — every node's journal begins with this entry at
  boot.

## Filtering

Filter by event type and by source component. Events from every cluster
node appear in one merged list, newest first, each attributed to the node
it happened on.

## Journals are per-node and in-memory

Each node keeps its own bounded journal (about 10,000 recent entries,
oldest dropped first) in memory. **It does not survive a node restart** —
it is a ring of recent occurrences, not durable history. Two consequences,
both made visible rather than implicit:

- After a restart, a node's history begins at its `node-started` entry.
  Anything before it is unknown, not absent. (Acknowledge/shelve *state*
  survives restart separately, via the alarm lifecycle journal on disk.)
- If a node cannot be reached when the list is collected, the page names
  it. Silence from an unreachable node is missing data, never quiet
  history.

## CLI

`gastrolog events` lists the same merged journal from any node, with
`--type`, `--source`, `--node`, `--since`, `--until` and `--limit`
filters. See the standing alarms with `gastrolog alerts`.
