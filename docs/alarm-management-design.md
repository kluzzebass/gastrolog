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

Verdicts under the governing test:

| ID (pattern) | Source | Condition | Verdict | Priority | Operator action (response text) |
|---|---|---|---|---|---|
| `segmentation:<vault>` | segmentation | Durable segment writes failing (disk full, I/O error) — **accepted records at risk** | **Alarm** | Critical | Free disk space / replace volume on the named node; ingest acks are failing until resolved |
| `cloud-store:<vault>` | cloud | Cloud store unreachable; uploads stopped | **Alarm** | High | Check cloud credentials/endpoint/network; sealed chunks accumulate locally until restored |
| `vault-leaderless:<vault>` | placement | Placements resolve to no leader ≥60s (beyond self-healing) | **Alarm** | High | Fix vault placements / node storage configs; retention, rotation, target refresh stopped |
| lock-leak | orchestrator | Orchestrator lock held past deadline — wedge in progress | **Alarm** (latched) | Critical | Capture the logged stack, restart the wedged node; deliberately sticky until acked |
| under-replicated / unknown-orphan (`vault:*`) | reconciler | Sealed chunk below required replicas / orphan with no FSM entry | **Alarm** | High | Check placement nodes; decide restore vs accept loss for the named chunk |
| blocked-build | chunking | GLCB build blocked on segments no holder can supply | **Alarm** | High | Restore a node holding the named segments, or accept the gap |
| RF-unmeetable (placement keys) | placement | Live placement members below configured RF | **Alarm** | High | Restore nodes or reduce RF; durability target unmet |
| node soft-offline / unreachable | placement, node-lifecycle | Peer node unreachable past grace | **Alarm** | High | Investigate/restart the node; removal is operator-initiated, never automatic |
| vault-init failure | orchestrator (factory, reconfig) | Vault instance failed to construct from config | **Alarm** | High | Fix the named config error; vault is not serving |
| ingester failure (`ingester:*`) | ingester/self | Configured ingester cannot start/run | **Alarm** | Low | Fix ingester config or disable it |
| rate-alert (`rate:*`) | ratealerter | Operator-configured rate threshold crossed | **Alarm** (operator-defined) | Low | Operator defined the threshold; response text comes from the rule |
| archival sweep failures | archival | Archive writes failing | **Alarm** | High | Check archive target storage |
| retention route-fan-out terminal failure | retention | Chunk destroyed without routing (pipeline down at expiry) | **Alarm** | Critical | Records ejected unrouted — investigate pipeline availability; potential data loss at retention boundary |
| chanwatch saturation | chanwatch | Internal channel saturated past watermark | **Event** (demoted ✓) | — | Landed: transition-edge logs. Journal surface lands with phase 5 |
| ingest-pressure | orchestrator | Ingest pipeline pressure elevated/critical; ingesters throttling | **Event** (demoted ✓) | — | Landed: `NodeStats.ingest_pressure_level`. If ingestion is throttled the matter is already handled — the throttle *is* the response, so nothing waits on an operator. Never logged: the self-ingester captures slog, so logging throttle transitions feeds the pressure |
| `self-ingester-drops` | ingester/self | Capture channel overflowing; diagnostic records discarded | **Event** (demoted ✓) | — | Landed: `NodeStats.self_ingester_drops_total`. Capacity tuning, not operator action. Never logged: a line about dropped logs feeds the channel dropping them |
| `raft-wal-latency` | statscollector | WAL append max over threshold | **Event** (demoted ✓) | — | Landed: transition-edge logs + stats (`RaftWalAppendMaxMs`) |
| scheduler-stall | schedwatch | Runtime stalled past leader lease | **Event** (demoted ✓) | — | Landed: log + counters |
| election-storm | statscollector | Elections/min over threshold | **Event** (demoted ✓) | — | Landed: transition-edge logs + stats |

Rows marked ✓ already landed; phase 1 is complete. Every surviving alarm
gets a catalog entry in code (see below) carrying its response text — the UI
shows it; no tracker IDs, no internal jargon.

## Alarm-type catalog in code

A static registry, one entry per alarm type:

```go
type AlarmType struct {
    IDPrefix    string        // "vault-leaderless:", "cloud-store:", ...
    Priority    alert.Priority // Critical | High | Low (replaces Severity at Set sites)
    Source      string
    Cause       string        // one-paragraph cause description
    Response    string        // what the operator should do
    DelayOn     time.Duration // suppression: condition must persist this long
    DelayOff    time.Duration // condition must stay clear this long before auto-clear
    Latching    bool          // stays active until acked even after condition clears
}
```

`alerts.Set(id, severity, source, msg)` becomes
`alerts.Raise(typeID, instanceKey, detail)` — the collector looks up the
type, applies suppression, and stamps priority/cause/response. Call sites
stop choosing severities ad hoc. `alert.Info` disappears — informational
conditions are events by definition.

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
