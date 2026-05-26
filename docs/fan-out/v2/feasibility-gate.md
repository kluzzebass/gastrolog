# Fan-Out V2 Feasibility Gate

Status: draft, untracked design note.

This is the gate document for deciding whether v2 is ready to split into implementation issues.

For architecture and mechanics:

- [write-path-lock.md](write-path-lock.md) — **locked write path**
- [architecture-overview.md](architecture-overview.md)
- [high-watermark-contract.md](high-watermark-contract.md)
- [spool-state-machine.md](spool-state-machine.md)

## Feasibility Verdict

Feasible if v2 keeps asynchronous data movement and uses explicit contracts for:

- `W-of-N` write durability,
- deterministic sequence/fence inclusion,
- seal-time reconcile before full sealed semantics.

Not feasible if v2 reintroduces synchronous global invariants on the hot write path.

## Primary Root Cause Addressed

V1 failed primarily on ambiguous chunk inclusion under churn.

V2 resolves this with:

- vault-wide sequence assignment,
- high-watermark-driven fence cuts,
- deterministic inclusion rule `prev_fence < seq <= curr_fence`.

## Required Contracts Checklist

All must be explicit in design text:

- Write durability: per destination vault `W-of-N`, snapshot-at-dispatch.
- Identity: `EventID` is canonical identity; dedup at materialize, search, and other choke points — **not** ingest idempotency.
- Ordering: `VaultSeq` is vault-scoped accept label, not `EventID.IngestSeq`.
- Seq timing: assign `VaultSeq` on ingesting router from local swath before replica fan-out.
- Spool accept: slot write by `VaultSeq` within sequence windows; OOO arrival required.
- Convergence: sealed semantics requiring reconcile completion marker.
- Read semantics: explicit behavior before convergence.
- Measurement semantics: authoritative vs provisional values separated.
- Decision authority: one owner + one durable source of truth per decision.

## Decision Authority Sanity Check

V2 must keep this split:

- route fan-out decisions in routing layer,
- sequence swath authority at vault-ctl leader (multi-holder grants),
- per-record `VaultSeq` assignment on ingesting router (local swath consume),
- fence cut authority at vault-ctl leader (with replica hints),
- materialization local per replica,
- converge-sealed marker from reconcile completion.

If any decision is "owned by everyone", the design is not ready.

## Remaining Non-Foundational Risks

These are not blockers if explicitly handled:

- allocator lease failover semantics,
- assigned-missing vs unassigned-gap hole classification,
- pre-convergence read behavior,
- materialization lag/backpressure policy,
- retention-route destination re-sequencing.

Clarification:

- These are guardrails after the foundational model is set; they are not evidence that V2 is infeasible.

## No-Go Conditions

Reject v2 issue decomposition if any of the following are true:

- chunk naming returns to hot path,
- sequence/fence contract is ambiguous,
- hole classification is absent,
- read semantics before convergence are unspecified,
- durability/count telemetry can overstate provisional state.

## Minimal Validation Gate

Required before merge-to-main planning:

- single-node baseline correctness,
- **4+ node write-path gate** (write-path-lock.md) with asymmetric ingesters,
- 4+ node tests with churn and burst load,
- node loss/return with catch-up verification,
- repeated fence/materialize/reconcile cycles,
- proof that sealed record sets converge by `EventID`,
- proof that provisional metrics are not presented as authoritative.

Spool-specific additions:

- crash/restart coverage at append/fence/materialize/watermark boundaries,
- lag-and-catch-up behavior under sustained ingest,
- materialization replay idempotency,
- active-read behavior over chunk + spool visibility windows.
