# Fan-Out V2 Anchor Model (Locked)

Status: locked in Phase 0.5 (gastrolog-1gqh7). Full rollout in Phase 6 (gastrolog-1n79l).

## Decision

V2 decouples write acceptance from chunk materialization. **GetContext and other anchor-driven reads must not assume a materialized `ChunkID` exists at acceptance time.**

Two anchor forms are canonical:

| Lifecycle | Anchor key | Proto surface | Ordering axis for context window |
|---|---|---|---|
| Materialized (V1 vaults; V2 after materialize) | `(vault_id, chunk_id, pos)` | `RecordRef` with `chunk_id` + `pos` | `IngestTS` / write timestamp (unchanged) |
| Pre-materialized (V2 spool, not yet in sealed chunk) | `(vault_id, vault_seq)` | `RecordRef` with `vault_seq`, empty `chunk_id` | same timestamp window; identity match uses `EventID` |

**`EventID` remains dedup/identity.** **`vault_seq` is the destination-vault ordering axis** (assigned before replica fan-out). Context dedup during mixed-vault search compares `EventID` + `vault_id`, not chunk position alone.

## Compatibility rules

1. **V1 vaults (`writeModel` empty or `v1`):** anchors MUST use materialized `(vault_id, chunk_id, pos)`. `vault_seq` MUST be zero.
2. **V2 materialized records:** MAY use either form when both are known; prefer materialized chunk ref in UI/API responses.
3. **V2 pre-materialized records:** anchors MUST use `(vault_id, vault_seq)` until materialization assigns `chunk_id` + `pos`.
4. **Mutual exclusion:** a single anchor MUST NOT set both a non-empty `chunk_id` and a non-zero `vault_seq`.
5. **Phase 0.5–5:** spool anchor resolution is wired through `query.SpoolAnchorReader`; until Phase 6 rollout, callers without a reader get a clear error — not a silent chunk lookup.

## Migration surfaces (declared Phase 0.5)

- `backend/api/proto/gastrolog/v1/query.proto` — `RecordRef.vault_seq`
- `backend/api/proto/gastrolog/v1/cluster.proto` — `ForwardGetContextRequest.vault_seq`
- `backend/internal/query/anchor.go` — validation + resolution contract
- `backend/internal/query/context.go` — `GetContext` mixed-lifecycle path
- `backend/internal/server/query_context.go` — RPC validation + forward shape

Phase 6 completes proto/backend/UI rollout; Phase 0.5 only locks the contract and proves mixed-state behavior in tests.

## Out of scope here

- Spool segment storage implementation (Phase 3)
- Assigning `vault_seq` on ingest (Phase 2)
- Removing chunk-ref assumptions from search pagination tokens (Phase 6)
