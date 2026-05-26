# Fan-out V2 design set

This directory contains the active fan-out v2 design documents.

## Reading order

1. [architecture-overview.md](architecture-overview.md)
2. [feasibility-gate.md](feasibility-gate.md)
3. [high-watermark-contract.md](high-watermark-contract.md)
4. [spool-state-machine.md](spool-state-machine.md)
5. [implementation-plan.md](implementation-plan.md)

## Purpose of each doc

- `architecture-overview.md`: single architectural narrative and decision-authority map.
- `feasibility-gate.md`: go/no-go criteria, risk checklist, and validation expectations.
- `high-watermark-contract.md`: `seq`/`H`/`F_n` semantics, allocator contract, hole classification.
- `spool-state-machine.md`: spool lifecycle, transitions, crash/recovery progression.
- `implementation-plan.md`: practical phased migration plan including backend, CLI/UI, and k8s validation ladder.
