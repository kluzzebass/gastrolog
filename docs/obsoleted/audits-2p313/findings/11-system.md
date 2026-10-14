# Phase 11 — system/config audit

**Packages:** `system/` excluding `raftfsm`, `raftstore`, `command` Raft paths  
**Epic:** gastrolog-2p313

## Clean

`policies/`, `cloud_validation/`, `duration/`, `size/`, `proxy/` helpers. `bootstrap.go` JWT-at-rest tradeoff documented.

## Findings

| ID | Crit | Sev | Location | Violation | Correct pattern | Workaround? | dcat |
|----|------|-----|----------|-----------|-----------------|-------------|------|
| audit-system-001 | 4 | P1 | `storage.go` `StorageIDForNode` L278–281 | Class mismatch → silent `FileStorages[0]` fallback | Fail loud; match `eligibleStorages` strictness | N | TBD |
| audit-system-002 | 4,8 | P1 | `StorageIDForNode` + `app/placement.go` | Leader placement uses fallback; followers strict | Single strict selector | N | TBD |
| audit-system-003 | 8 | P2 | `memory/store.go` placements | Dual source: map + `VaultConfig.Placements`; delete doesn't purge map | Authoritative `Runtime.VaultPlacements` | Y | TBD |
| audit-system-004 | 8 | P2 | `config.go` Config vs ServerSettings vs Runtime | Duplicated server fields; wizard flag in Runtime | One path per concern | Y | TBD |
| audit-system-005 | 4,7 | P2 | `store.go` interface contract | No semantic validation in Store | Validate at server boundary | Y | TBD |
| audit-system-006 | 7 | P2 | `NodeStorageConfig.NodeID` string | vs `glid.GLID` elsewhere | Typed alias + consistent proto encoding | Y | TBD |
| audit-system-007 | 8 | P2 | `memory/store.go` comment | Says ConfigStore; interface is Store | Fix comment | N | — |
| audit-system-008 | 3 | P2 | `proxy.go` `SetJoining` | All ops fail during join | Documented `ErrJoining` | N | — |
