# Fan-out V2 Architecture Overview

Status: draft, untracked design note.

This document is the single architectural overview for fan-out v2.

## Design intent

V2 keeps the leaderless traffic shape and removes hot-path synchronization on chunk identity.

The write path is:

- route fan-out (source record -> destination vault set),
- per-destination sequence assignment,
- per-destination replica fan-out with `W-of-N` durability,
- asynchronous fence/materialize/reconcile for sealed output.

## V1 vs V2 At A Glance

| Topic | V1 (rewound) | V2 (current direction) |
|---|---|---|
| Hot-path chunk identity | Coordinated on write path | Removed from write path; handled asynchronously at seal/materialize time |
| Canonical inclusion boundary | Ambiguous under churn | Deterministic sequence-range fences (`prev < seq <= curr`) |
| Ordering model | Emergent from local timing/state | Vault-wide acceptance sequence + high watermark |
| Fan-out semantics | Mechanically present but overloaded with synchronous invariants | Route fan-out and replica fan-out explicitly separated |
| Reconcile role | Correctness + ambiguity cleanup | Coverage healing against explicit assigned sequence/fence expectations |
| Decision ownership | Mixed/implicit in places | Explicit authority map (router, destination pipeline, vault-ctl leader, materializer, reconciler) |
| Implementation posture | Implementation-first, discover issues late | Design-first with gate + contract + lifecycle docs |

## Two fan-outs (different layers)

- **Route fan-out**: router sends one source record to multiple destination vaults.
- **Replica fan-out**: destination write pipeline replicates one destination-vault write to that vault's replicas.

These are separate operations and must not be conflated.

## Placement-leader field migration scope

`VaultPlacement.Leader` is currently wired through multiple subsystems (replication,
retention, forwarder target derivation, and related tests). Under V2's leaderless
write model, that bit cannot remain the write-path authority as-is.

V2 implementation scope must include one explicit migration decision:

- remove the write-path meaning of `VaultPlacement.Leader`, or
- redefine it to a non-write-path role with clear semantics.

This is a migration-scope item, not a feasibility blocker, but it must be named before issue decomposition.

## Core pipeline

For each source record:

1. Router computes destination vault set.
2. For each destination vault:
   - assign destination-vault `seq` (from leased range),
   - dispatch replica fan-out to destination replica set,
   - resolve write success/failure from `W-of-N`.
3. Accepted writes advance destination vault high watermark (`H`).
4. Fence coordinator cuts fence boundaries (`F_n`) by policy.
5. Materializer converts fenced spool ranges into local sealed chunks.
6. Reconcile converges sealed record sets by `EventID`.

Spool implementation note:

- spool segments are identified by `first_seq`,
- write durability visibility follows index-last commit semantics (same crash model as active chunks),
- restart recovery drops unindexed tail bytes.

## Locked sequencing rule

V2 uses one explicit timing rule:

- destination-vault `seq` is assigned in destination write pipeline before replica fan-out,
- `H` advances only after `W-of-N` durable success for that `(EventID, seq)` write.

Replicas and materializer treat `seq` as part of the write payload, not a local post-write annotation.

## Decision Authority Matrix

| Decision | Decisionmaker | Execution location | Decision time | Durable source of truth |
|---|---|---|---|---|
| Route destination vault set | Router on the node processing the source record | Routing evaluation pipeline on processing node | During ingest or retention-route evaluation | Route config + per-write routing context |
| Destination-vault sequence range allocation | Vault-ctl leader for that destination vault | Vault-ctl allocator endpoint | On lease request / lease renewal | Vault-ctl Raft log (allocator state + range reservations) |
| Destination-vault per-record `seq` assignment | Destination write pipeline (using leased range) | Router-side destination write stage | After destination selection, before replica fan-out | Destination append metadata (`EventID`, destination vault, `seq`) |
| Destination-vault replica fan-out target snapshot (`N`) | Destination write pipeline | Replica dispatch coordinator | At write dispatch | In-flight write context + responder append metadata |
| Destination-vault write success/failure (`W-of-N`) | Destination write pipeline coordinator | Replica fan-out result aggregator | After responses or timeout | Ack outcome + durability/error telemetry |
| Fence cut boundary (`F_n`) | Vault-ctl leader (authoritative fence coordinator) | Fence coordinator path on vault-ctl leader | On policy trigger evaluation (count/time/age) | Vault-ctl Raft log + persisted fence record |
| Fence trigger evidence (`FenceHint`) | Data-bearing replica nodes | Replica hint emitter path | When local policy evidence crosses trigger threshold | Ephemeral hint channel only (not authoritative) |
| Materialization progression (`M_r`) | Local materializer on each replica | Local spool-to-chunk materializer | During batch materialization | Local durable checkpoint/watermark state |
| Reconcile completion (`ConvergeSealed`) | Reconcile coordinator + participating replicas | Reconcile worker + local apply paths | After hole classification + fill completion | Convergence marker / reconcile completion metadata |
| Retention-route destination sequencing | Router-side destination write pipeline for destination vault | Retention route -> destination write path | When routing retained records into destination vault(s) | Destination append metadata (new destination `seq`) |

## Why this fixes v1 failure mode

V1 failed primarily on ambiguous chunk membership under churn.

V2 makes membership explicit:

- deterministic inclusion rule: `prev_fence < seq <= curr_fence`,
- chunk naming moved off hot path,
- reconcile heals replica coverage gaps against known sequence/fence boundaries.

## What remains asynchronous by design

- per-replica arrival timing,
- per-replica spool/materialization progress,
- reconcile completion timing.

These are expected and modeled; they are not correctness failures by default.

## Read behavior before convergence (policy scope)

Before `ConvergeSealed`, the system policy must specify:

- coverage set: which replicas/spool ranges are queried,
- visibility contract: whether missing non-converged records are acceptable,
- merge behavior: how spool and chunk sources combine in one result stream,
- labeling: how counts/results are marked as provisional vs authoritative.

## Source docs

- [feasibility-gate.md](feasibility-gate.md)
- [high-watermark-contract.md](high-watermark-contract.md)
- [spool-state-machine.md](spool-state-machine.md)
