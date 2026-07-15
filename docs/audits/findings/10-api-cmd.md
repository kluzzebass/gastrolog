# Phase 10 — API contract + cmd audit

**Packages:** `api/proto/`, `api/gen/`, `cmd/gastrolog/`, `cmd/walinspect/`, `cmd/multirun/`, `cmd/compress-assets/`  
**Epic:** gastrolog-2p313

## Clean

`api/gen/` (generated; proto findings only), `cmd/multirun/`, `cmd/compress-assets/`, most `gastrolog` CLI inspect/query/vault/cluster paths (RPC-only).

## audit-api-NNN

| ID | Crit | Sev | Location | Violation | dcat |
|----|------|-----|----------|-----------|------|
| audit-api-001 | 8 | P2 | `system.proto` `system_raft_index` | Ubiquitous language: cluster-ctl Raft, not "system Raft" | TBD |
| audit-api-002 | 8 | P2 | `vaultctlfsm.proto` `open_chunk` | Canonical: active chunk manifest | TBD |
| audit-api-003 | 8 | P2 | `query.proto` `cloud_count` comments | Canonical: cloud-backed | TBD |
| audit-api-004 | 8 | P2 | `system.proto` `filter_set_active` | Filter → route table | TBD |
| audit-api-005 | 7,8 | P2 | `vault.proto` `VaultInfo.filter` | Dead field never set | TBD |
| audit-api-006 | 4,8 | P1 | `RetentionRule` comment vs `retention_disposition` | Comment says always engine; default is delete | TBD |
| audit-api-007 | 8 | P2 | `replica_count` / placements | "replica" ambiguous vs vault replication | TBD |
| audit-api-008 | 8 | P2 | `GetConfig` comment vs `GetSystem` RPC | Name drift | TBD |
| audit-api-009 | 8 | P2 | `GetSystemResponse` flattening | Operator config + runtime merged on wire | TBD |

## audit-cmd-NNN

| ID | Crit | Sev | Location | Violation | dcat |
|----|------|-----|----------|-----------|------|
| audit-cmd-001 | 3 | P2 | `cli/cli.go` default `--addr` | TCP default bypasses unix-socket-first | TBD |
| audit-cmd-002 | 4 | P1 | `cli/node.go` `add-storage` | `glid.FromBytes(nsc.NodeId)` vs `string(nsc.NodeId)` in list-storage | TBD |
| audit-cmd-003 | 3,4 | P2 | `cmd/walinspect` | Direct WAL read — local disk truth, not quorum | — (dev tool) |
| audit-cmd-004 | 8 | P2 | `cli/resolve.go` comment | Says GetConfig; uses GetSystem | TBD |
| audit-cmd-005 | 8 | P2 | `export.go` / `import.go` | Bundles runtime with operator config | TBD |
| audit-cmd-006 | 3 | P2 | `cluster.go` join | Operator must know leader address | — |
| audit-cmd-007 | 8 | P2 | `--config-type memory` | Bypasses cluster-ctl persistence | — |

## Priority dcat children

1. **audit-cmd-002** — add-storage NodeId bug (P1)
2. **audit-api-006** — retention comment vs disposition (P1)
