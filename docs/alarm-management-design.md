# Alarm Management Design

Redesign of the alert system around EEMUA 191 / ISA-18.2 / IEC 62682
principles. This document is the decision record for the implementation;
the catalog table below is the source of truth for per-alarm behavior.

## The governing test

> The most important thing about any kind of alarm is to ask: **does the
> operator need to do something?** If no, then it's not an alarm, and it
> just goes in the log.

This test gates everything else. It is applied **before** rate limits,
priorities, or lifecycle mechanics, to every existing and future
`alerts.Set()` site. A condition with no operator action is an **event**
(journal) or a **metric** (health surface), never an alarm.

## Vocabulary

Three distinct concepts, currently conflated in one collector. These terms
go into `docs/ubiquitous_language.md` with the implementing change:

| Term | Meaning | Surface |
|------|---------|---------|
| **Alarm** | A condition requiring operator action, with documented cause and response. Has lifecycle state. | Alarm list (System Alerts panel, renamed) |
| **Event** | A record of something that happened; no action required. | Event journal (new) |
| **Metric** | A measured quantity trending over time. | Health/stats surfaces |

## Standards principles adopted

1. **Actionability** — every alarm requires a response (the governing test).
2. **Rate** — steady-state target ~1 alarm per operator per 10 minutes;
   flood recognition during upsets.
3. **Chattering suppression** — delay-on / delay-off timers and latching
   instead of raw Set/Clear flapping.
4. **Consequence-based priority** — severity derives from a documented
   consequence × urgency assessment, not ad-hoc choice at the call site.
5. **Standing-alarm management** — acknowledgment and shelving so the list
   reflects live, unhandled conditions.
6. **Lifecycle state model** — not a bare Set/Clear bit.
7. **Response guidance** — each alarm type carries documented cause +
   operator action, surfaced in the UI.
8. **Separation** — alarms vs events vs metrics (vocabulary above).
9. **Self-monitoring** — the alarm system measures its own rate and
   surfaces flood conditions.

## Alarm catalog

Priority derivation: **consequence** (what is at risk if unhandled) ×
**urgency** (how fast the risk compounds). `Critical` = data loss in
progress or imminent; `High` = durability/availability degraded, will
compound; `Low` = needs attention on a human timescale.

### Software faults are not process alarms

One row below is a **software fault**, not an alarm on this scale:
`orchestrator-lock-leak`. The distinction is the standard's own — EEMUA 191
and ISA-18.2 treat instrument and system faults as a class apart from
process alarms, because a broken instrument is not a process condition and
does not belong on a consequence × urgency scale. A software defect is the
same shape.

The test that separates them: **an alarm's response fixes the condition; a
software fault's response is to report it.** Restarting a wedged node
recovers service — it does not address why the lock leaked. Nothing an
operator can do at 3am fixes a lock-discipline bug.

Consequences for the model:

- **No rubric priority.** "The software is broken" is not consequence ×
  urgency. Rating it Critical would claim data is at risk (it is not —
  ingest is ack-after-fsync, so accepted records are already durable and
  in-flight ones were never acked); rating it High would file it beside
  routine degradation.
- **Never shelveable.** Shelving suppresses a condition for a while on the
  operator's judgement. A defect does not resolve itself and cannot be
  deferred — a shelved wedge is a lie. Phase 4 must let an alarm type refuse
  shelve; this is the case that requires it.
- **Latched** for the reason the code already gives: a leaked hold cannot be
  released by anything short of a restart, so the condition can never
  self-clear. Only an operator acknowledging it can.

`orchestrator-lock-leak` exists because of gastrolog-1ug3rq, a P0 where a
node zombified silently and no goroutine dump could name the leaker (the
holder had already returned). That root cause — a recursive `o.mu` read
acquisition in `schedulePipelineCloudUpload` — was found and fixed; the
tripwire stays armed for the defect class, not for that bug. It firing again
means a new one.

**Multi-setpoint conditions are separate alarms, not one alarm that changes
colour.** Where the code today raises one ID at Warning while approaching a
bound and Error at the bound, the catalog splits it in two. This is the
standard's own shape (the HI / HIHI pattern): two setpoints on one
measurement are two alarms, each with its own priority and its own response.
It is also what keeps `Priority` a static property of the type. See
[gastrolog-1cruar].

Every row below is grounded in a real `Set` call site as of the phase 1
demotions; the ID column is the **real** ID in code, not an abbreviation.

Verdicts under the governing test:

| ID (pattern) | Source | Condition | Verdict | Priority | Operator action (response text) |
|---|---|---|---|---|---|
| `segmentation-writer:<vault>` | segmentation | Durable segment commit failed; working segment abandoned for crash recovery — **accepted records at risk** | **Alarm** | Critical | Free disk space or replace the volume on the named node; ingest acks are failing until resolved |
| `chunking-unplannable-segment:<vault>` | chunking | Segment on-disk indexes unreadable; records stay unchunked and head copies cannot be purged | **Alarm** | Critical | Investigate segment file corruption on the named node. If the vault has a delete-disposition TTL these records are released **unchunked** at expiry — the loss is scheduled, not hypothetical |
| `wal-reserve:<wal>` | storage | Raft WAL space reserve lost | **Alarm** | Critical | Free disk space now — without the reserve, a full volume crashes consensus on this node |
| `orchestrator-lock-leak` | orchestrator | Orchestrator lock held or write-stuck past 1min | **Software fault** (latched, never shelveable) | — (see below) | **This should never fire.** If it does, it is a lock-discipline defect, not an operating condition: capture the acquisition stack from this node's log and file it. Restarting the node is a workaround to recover service, not the response |
| `chunking-build-blocked:<vault>` | chunking | Head-of-queue chunk blocked >2min on segments no local holder can supply, or a manifest referenced a released segment | **Alarm** | High | Restore a node holding the named segments, or accept the gap |
| `chunking-underreplicated:<vault>` | chunking | Segments below the replication minimum ≥2min; planning gated | **Alarm** | High | Check that all placement nodes are up and replication is progressing. If the origin node is permanently lost, the affected records exist only there |
| `chunking-glcb-corrupt:<vault>` | chunking | Sealed-chunk GLCB unreadable; quarantined with a `.corrupt` suffix | **Alarm** (DelayOn) | High | Heals on its own — rebuilt from source segments or re-pulled from a peer home. **Only actionable if it persists**, which is what the delay-on is for: then investigate disk health here and replica health on the vault's other homes |
| `chunk-unreadable:<chunk>` | retention | Chunk unreadable during retention; backoff retry scheduled | **Alarm** (DelayOn) | High | Retries automatically. Only actionable once retries stop resolving it: investigate disk health on this node |
| `chunk-suspect:<chunk>` | cloud-reconcile | Cloud-backed chunk 404s in the blob store; removed from the index after the grace period | **Alarm** | High | Check the blob store for the named chunk. After grace it leaves the index — restore it from a peer or accept the loss |
| `unknown-orphan:<vault>:<chunk>` | vault | Chunk on disk with records but not recognized by the FSM; preserved | **Alarm** | High | Decide restore vs delete for the named chunk. Do **not** delete manually without review — the cluster has no other copy |
| `cloud-store:<vault>` | cloud | Cloud store unreachable; uploads stopped | **Alarm** | High | Check cloud credentials/endpoint/network; sealed chunks accumulate locally until restored |
| `vault-leaderless:<vault>` | placement | Placements resolve to no leader ≥60s (beyond self-healing) | **Alarm** (DelayOn 60s) | High | Fix vault placements / node storage configs; retention, rotation, target refresh stopped |
| `vault-underreplicated:<vault>` | placement | Placed replicas below desired RF (insufficient eligible storages) | **Alarm** | High | Restore nodes or reduce RF; durability target unmet |
| `vault-storage-class-missing:<vault>` *(split from `vault-unplaced`)* | placement | Selected node has no storage of the vault's required class; leader placement refused | **Alarm** | High | Add storage of the required class on the named node, or change the vault's storage class |
| `vault-no-eligible-node:<vault>` *(split from `vault-unplaced`)* | placement | No currently-eligible node; existing placements retained | **Alarm** | High | Restore an eligible node, or relax the vault's storage requirements |
| `vault-soft-offline-leader:<vault>` | placement | Leader heartbeat lost while node still Live, or leader on an Unreachable/Maintenance node; rotation gated | **Alarm** | High | Investigate the named leader node |
| `node-unreachable:<node>` | node-lifecycle | Peer node Unreachable past grace | **Alarm** | High | Investigate or restart the node. Removal is operator-initiated, never automatic |
| `vault-init:<vault>` | orchestrator (factory, reconfig) | Vault instance failed to construct from config | **Alarm** | High | Fix the named config error; the vault is not serving |
| `pipeline-backlog-capped:<vault>` *(split)* | storage | Backlog **at** budget — new records for this vault are REFUSED | **Alarm** | High | Check chunking throughput, raise the budget, or reduce the ingest rate. This vault is refusing records now; others are unaffected |
| `vault-max-size-capped:<vault>` *(split)* | storage | Vault **at** its size budget — new records REFUSED | **Alarm** | High | Raise the budget or shorten retention. This vault is refusing records now; others are unaffected |
| `disk-space-exhausted:<vault>` *(split)* | storage | Vault volume out of space — admission for this vault SUSPENDED | **Alarm** | High | Free space, add capacity, raise the vault's threshold, or shorten its retention |
| `node-disk-space-exhausted` *(node, split)* | storage | Node volume out of space — ingest admission SUSPENDED on this node | **Alarm** | High | Free space, add capacity, or shorten retention. Retention and deletes keep running |
| `ingester-not-running` | ingestion | Ingesters that should run on this node are not running | **Alarm** | Low | Check the log for build/start errors; fix the ingester config or disable it |
| `pipeline-backlog-approaching:<vault>` *(split)* | storage | Backlog approaching budget; chunking not keeping pace | **Alarm** | Low | Check chunking throughput, raise the budget, or reduce the ingest rate — before records start being refused |
| `vault-max-size-approaching:<vault>` *(split)* | storage | Vault approaching its size budget | **Alarm** | Low | Raise the budget or shorten retention — before records start being refused |
| `disk-space-low:<vault>` *(split)* | storage | Vault volume below its free-space warn band | **Alarm** | Low | Free space, add capacity, raise the vault's threshold, or shorten its retention |
| `node-disk-space-low` *(node, split)* | storage | Node volume below its free-space warn band | **Alarm** | Low | Free space, add capacity, or shorten retention |
| `<kind>-rate:<vault>` (prod: `retention-rate:<vault>`) | ratealerter | Operator-configured rate threshold crossed | **Alarm** (operator-defined) | from the rule (lower threshold → Low, escalation threshold → High) | The operator defined the threshold; the response text comes from the rule. Not a catalog alarm — enters via `RaiseOperator`, beside the catalog. See [gastrolog-1cruar] |
| **`vault-home-cannot-store:<vault>`** | placement | A vault home node is disk-protected; collection and builds paused there | **NEEDS VERDICT** (interim: Alarm) | Low (interim) | Razor is unclear. When healthy replicas ≥ RF the text itself says replicas are "backfilled automatically" — handled, nothing waits on an operator. When healthy < RF, the action is "free space", which is already `disk-space-*`'s action on that node. What it uniquely adds is *which vault* is affected. Demote to a metric, or keep as the vault-scoped view of a node condition? Until the verdict lands, the phase 2 registry keeps it as a Low alarm (the code still raises it; a raised type must be cataloged) |
| *(retention unrouted destroy — row retired)* | retention | Chunk destroyed with zero records routed | **Not an alarm — prevented** | — | Resolved in [gastrolog-65riw5]: the condition no longer occurs, so there is nothing to alarm on. An unreadable cursor now flags the chunk for backoff retry and raises `chunk-unreadable:<chunk>` (which has its own row); a missing vault instance retains the chunk. No alarm was invented — the existing one covers it. **Partial** fan-out remains a deliberate tolerance and is reported with a dropped-record count, not an alarm; see the note below |
| chanwatch saturation | chanwatch | Internal channel saturated past watermark | **Event** (demoted ✓) | — | Landed: transition-edge logs. Journal surface lands with phase 5 |
| ingest-pressure | orchestrator | Ingest pipeline pressure elevated/critical; ingesters throttling | **Event** (demoted ✓) | — | Landed: `NodeStats.ingest_pressure_level`. If ingestion is throttled the matter is already handled — the throttle *is* the response, so nothing waits on an operator. Never logged: the self-ingester captures slog, so logging throttle transitions feeds the pressure |
| `self-ingester-drops` | ingester/self | Capture channel overflowing; diagnostic records discarded | **Event** (demoted ✓) | — | Landed: `NodeStats.self_ingester_drops_total`. Capacity tuning, not operator action. Never logged: a line about dropped logs feeds the channel dropping them |
| `raft-wal-latency` | statscollector | WAL append max over threshold | **Event** (demoted ✓) | — | Landed: transition-edge logs + stats (`RaftWalAppendMaxMs`) |
| scheduler-stall | schedwatch | Runtime stalled past leader lease | **Event** (demoted ✓) | — | Landed: log + counters |
| election-storm | statscollector | Elections/min over threshold | **Event** (demoted ✓) | — | Landed: transition-edge logs + stats |

Rows marked ✓ already landed; phase 1 is complete. Every surviving alarm
gets a catalog entry in code (see below) carrying its response text — the UI
shows it; no tracker IDs, no internal jargon.

One row is **not** settled and must not be implemented from this table as
written: `vault-home-cannot-store` needs a razor verdict.

**The best answer to "what priority should this alarm be?" is sometimes
"make the condition impossible."** The retention unrouted-destroy row was
Critical and unraised — an alarm the design wanted and the code never built.
Working out its response text (gastrolog-65riw5) showed the condition was a
bug, not a state: retention was destroying chunks with zero records routed
on an unreadable cursor. Fixing that removed the row instead of filling it
in. Reach for that before writing a catalog entry: a row that says "tell the
operator we lost their data" is a row that should have said "don't."

Partial fan-out — individual records failing to submit for non-terminal
reasons, dropped when the chunk is destroyed — is a **deliberate tolerance**
and stays one: one bad record must not strand a chunk forever. It is
reported with a count on a single line rather than a warn per record, and
whether a nonzero count deserves an alarm of its own is open.

Rows removed from this table as fiction — they described conditions no code
raises:

- *under-replicated (reconciler)* — no reconciler raises an under-replication
  alarm. The real ones are `vault-underreplicated:<vault>` (placement, RF) and
  `chunking-underreplicated:<vault>` (chunking, segments). The row conflated
  them with `unknown-orphan`, which is a different condition entirely and now
  has its own row.
- *ingester failure (`ingester:*`, source ingester/self)* — no `ingester:*`
  alarm exists. The real one is `ingester-not-running`, raised by the ingester
  reconciler in `internal/app` with source `ingestion`, and it is node-scoped
  rather than per-ingester.
- *archival sweep failures ("archive writes failing")* — the archival sweep
  raises `chunk-suspect:<chunk>`, which is a cloud-reconcile 404, not an
  archive write failure.

## Alarm-type catalog in code

A static registry, one entry per alarm type:

```go
type AlarmType struct {
    IDPrefix      string         // "vault-leaderless", "cloud-store", ... (full ID = IDPrefix[:instanceKey])
    Priority      alert.Priority // Critical | High | Low (replaces Severity at Set sites)
    Source        string
    Cause         string        // one-paragraph cause description
    Response      string        // what the operator should do
    DelayOn       time.Duration // suppression: condition must persist this long
    DelayOff      time.Duration // condition must stay clear this long before auto-clear
    Latching      bool          // stays active until acked even after condition clears
    SoftwareFault bool          // defect tripwire: outside the priority scale, never shelveable
}
```

*(Landed with phase 2 — `internal/alert/registry.go`. DelayOn/DelayOff/
Latching are declared but not yet enforced; suppression is phase 3.)*

`alerts.Set(id, severity, source, msg)` became
`alerts.Raise(typeID, instanceKey, detail)` — the collector looks up the
type and stamps priority/cause/response (suppression follows in phase 3);
`Clear(typeID, instanceKey)` is addressed the same way. Call sites
stopped choosing severities ad hoc; the three structurally identical sink
interfaces (orchestrator `AlertCollector`, segmentation and chunking
`AlertSink`) collapsed into the single `alert.Sink`. Operator-defined
alarms enter through `RaiseOperator(OperatorAlarm)`. A `Raise` for an
unregistered type is loud, never silent: it logs the defect and surfaces
a software-fault alarm carrying the raiser's detail. `alert.Info` never
existed — informational conditions are events by definition.

## Lifecycle state model

```
            condition true (after DelayOn)
  ┌────────────────────────────────────────────┐
  │                                            ▼
[inactive] ◄──(cleared + acked)──── [active-unacked] ──(operator ack)──► [active-acked]
  ▲                                            │                            │
  │                     condition false        │ (DelayOff, non-latching)   │ condition false
  └──── [cleared-unacked] ◄────────────────────┘◄───────────────────────────┘
                 (operator ack)
[shelved]: operator-initiated suppression with expiry, from any active state.
```

- **Ack** records operator awareness (who + when). Latching alarms
  (lock-leak) clear only via ack after the condition resolves.
- **Shelve** suppresses a specific alarm instance for a duration (with a
  mandatory expiry — no permanent shelves). Shelved alarms are visible in
  a collapsed section, never silently gone.
- Cleared-unacked keeps "it fired while you were away" visible without
  blocking the active list.

### Proto / API

`Alert` message gains: `state`, `priority`, `cause`, `response`,
`acked_by`, `acked_at`, `shelved_until`, `first_raised`, `occurrences`.
New RPCs: `AckAlarm(id)`, `ShelveAlarm(id, duration)`, `UnshelveAlarm(id)`.
Remove-and-renumber applies; no reserved fields.

### Cross-node semantics

Alarms are raised per-node and aggregated for the UI via the existing
PeerState broadcast. Ack/shelve are cluster-visible operations: the RPC
forwards to the owning node (any node can serve the request; the owning
node persists the ack in its collector). Cluster-wide conditions raised by
multiple nodes (vault-leaderless) deduplicate in the aggregation layer by
alarm ID; an ack applies to the deduplicated alarm and fans out to every
raiser. Ack state survives node restart via a small journal file under the
node home (not config; not Raft — an ack is operator telemetry, not
cluster state).

## Event journal

The collector's event-shaped traffic (demoted diagnostics, occurrence
records, state transitions) moves to a per-node ring-buffer journal
(bounded, e.g. 10k entries) with a `ListEvents` RPC and a UI page next to
the alarm list. Alarm lifecycle transitions (raised, acked, cleared,
shelved) are themselves journaled — the audit trail the alarm list itself
must not carry.

## Rate self-monitoring

The collector tracks alarms raised per rolling 10 minutes. Over threshold
(default 10/10min) it raises exactly one meta-alarm — `alarm-flood` —
which is itself an alarm by the razor: the operator's action is "the
alarm system is degraded; triage by priority, expect suppressed detail."
Flood mode collapses per-instance alarms of the same type into one row
with a count.

## UI

- Alarm list defaults to active + unacked, sorted priority then age.
- Each alarm row expands to cause + response text from the catalog.
- Ack / shelve controls per row; shelved and cleared-unacked sections
  collapsed below.
- Rate/flood indicator in the panel header.
- Event journal is a separate page: filterable, no controls, no color
  escalation — it is a record, not a call to action.
- CLI surface: `gastrolog alerts` (per-node attribution, `--node` filter,
  `-o json`) and a standing-alarm table in `cluster status` render the
  same per-node NodeStats aggregation the panel reads, over the local
  Unix socket with no auth — alarms stay readable from a bare shell when
  a suspended system writes no logs. The CLI's severity→display mapping
  is a single function (`alertSeverityStr`), so the phase-2 severity →
  priority change is a one-place edit there.

## Implementation phases

1. **Razor demotions** ✓ — scheduler-stall, election-storm, chanwatch
   saturation and raft-wal-latency demoted to transition-edge logs;
   ingest-pressure and self-ingester-drops demoted to NodeStats metrics.
   Note the two demotion targets are not interchangeable: anything the
   self-ingester would capture must become a **metric**, never a log, or
   reporting the pressure adds to it.
2. **Catalog + priorities** — the AlarmType registry; every Set site
   migrates to Raise; severity → priority; response text written per type
   (operator-reviewed).
3. **Suppression** — DelayOn/DelayOff/Latching in the collector
   (vault-leaderless already hand-rolls delay-on; migrate it onto the
   primitive).
4. **Lifecycle + proto + RPCs** — state model, ack/shelve, cross-node ack
   fan-out, ack persistence.
5. **Event journal** — ring buffer, RPC, UI page; move demoted
   diagnostics and lifecycle transitions onto it.
6. **Self-monitoring** — rate gauge, flood meta-alarm, flood collapse.
7. **Vocabulary** — ubiquitous_language.md entries land with phase 2.

Each phase is independently shippable; phases 2–4 touch every alert call
site and the proto and should land on one stack.
