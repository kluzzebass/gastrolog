# Phase 9 — Shared libraries audit

**Packages:** `glid/`, `home/`, `convert/`, `btree/`, `callgroup/`, `chanwatch/`, `safeutf8/`, `units/`, `notify/`  
**Epic:** gastrolog-2p313

## Clean

`btree/`, `callgroup/`, `chanwatch/`, `safeutf8/`. `convert/` is the canonical proto boundary (good).

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-shared-001 | 8 | P2 | `glid/glid.go` `Parse("")` | Empty → `Nil` without error | `ParseOptional` or error | N | — |
| audit-shared-002 | 8 | P2 | `home/home.go` comment | `node_id` file advisory vs Raft StableStore | Document `app.resolveNodeID` | N | — |
| audit-shared-003 | 7,8 | P2 | `convert/record.go` + query call sites | `Raw` bytes; attrs must use `safeutf8` at each site | All string fields through `convert` | Partial | — |
| audit-shared-004 | 5 | P2 | `units/bytes.go` `FormatBytesCompact` | No fractional KB/MB | `FormatBytesDisplay` at UI | N | — |
| audit-shared-005 | 8 | P2 | `notify/signal.go` vs `bus.go` | Two broadcast primitives | Lint: Signal=wake, Bus=payload | N | — |
