# Obsoleted documents

Nothing in this directory describes the current system. These are superseded
designs and point-in-time artifacts: audits, reviews and implementation plans.
They may contradict the code and the operator's decisions. Read them for
background only, never as a reference for how GastroLog works or should work.
Current documentation lives in the rest of [`docs/`](../).

| Path | What it is | Current reference |
|------|------------|-------------------|
| [`fan-out-v1/`](fan-out-v1/) | The first fan-out data-plane design, and its record pull | [V3 pipeline](../fan-out/v3/pipeline.md) |
| [`fan-out-v2/`](fan-out-v2/) | The second fan-out design, abandoned before it shipped | [V3 pipeline](../fan-out/v3/pipeline.md) |
| [`fan-out-v3-exploration/`](fan-out-v3-exploration/) | V3 design notes and the chunking design exploration (approaches considered, open questions since settled) | [V3 pipeline](../fan-out/v3/pipeline.md) |
| [`audits-2p313/`](audits-2p313/) | A codebase audit: findings, an invariant ledger and an issue map | The current design docs; its invariants are not normative |
| [`defaults-audit.md`](defaults-audit.md) | An audit of product defaults | [Product defaults policy](../product-defaults-policy-design.md) |
| [`disk-authority-audit.md`](disk-authority-audit.md) | An audit of disk versus FSM authority over chunks | [Vault control plane](../vault-control-plane-architecture.md) (chunk tombstones, unknown orphans) |
| [`security-audit-2026-08/`](security-audit-2026-08/) | A security review and its remediation map | — |
| [`superpowers-plans/`](superpowers-plans/) | Implementation plans for finished work | [Retention disk protection](../retention-disk-protect-design.md), [Raft WAL scavenging](../raftwal-scavenging-design.md) |
| [`vision-issues.md`](vision-issues.md) | The vision broken into planned epics and issues | [Vision](../vision.md); work is tracked in the issue tracker |
| [`mockups/`](mockups/) | The seal-pips design mockup | The `inspector-vaults` help topic |
| [`chunk_append_pipeline.md`](chunk_append_pipeline.md) | The chunk manager's two-phase append pipeline | [V3 pipeline](../fan-out/v3/pipeline.md) (Segmentation) |
| [`chunk_redesign.md`](chunk_redesign.md) | A chunk redesign draft | [V3 pipeline](../fan-out/v3/pipeline.md), [file formats](../file_formats.md) |
| [`vault_redesign.md`](vault_redesign.md) | The vault redesign decisions | [Vocabulary](../ubiquitous_language.md) (Storage) |
| [`distributed_leadership_patterns.md`](distributed_leadership_patterns.md) | Research on leadership patterns in production systems | [Vault control plane](../vault-control-plane-architecture.md) |
