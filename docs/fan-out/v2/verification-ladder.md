# V2 Verification Ladder

Status: active runbook for Phase 10 (`gastrolog-22d15`).

Execute stages in order. Each stage must pass before advancing. Evidence = command, exit code, and a one-line outcome note.

## Stage 0 — P0 write-path gate (prerequisite)

**Goal:** 4+ node asymmetric ingest with spool parity and no chunk-append landing.

```bash
cd backend
go test ./internal/orchestrator/ -run 'TestWritePathGate' -count=1 -timeout 120s
```

**Expected:** pass; covers asymmetric ingesters, burst skew, multi-vault independent `vault_seq`, cross-node route guardrail.

## Stage 1 — Unit + multinode invariants (`gastrolog-1w60p`)

**Goal:** sequencing, fence/materialize, reconcile, heal, churn, and restart contracts.

```bash
cd backend
go test ./internal/orchestrator/ -run 'TestWritePathGate|TestSequenced|TestBurnedTail' -count=1 -timeout 600s
```

**Covers:**

| Test | Axis |
|---|---|
| `TestWritePathGateFourNodeAsymmetricIngest` | P0 gate, RF≥3, two routers |
| `TestWritePathGateBurstAsymmetricIngesters` | slow + burst rate skew |
| `TestWritePathGateMultiVaultIndependentVaultSeq` | independent seq per vault |
| `TestSequencedCrossNodeRouteGuardrailNoChunkAppend` | no `SetRecordAppender→Append` |
| `TestSequencedMaterializeReconcileHealthyIngest` | fence + `C_r` parity |
| `TestSequencedSpoolSlotHealRecovery` | assigned-missing heal |
| `TestSequencedRestartPreservesReplicaWatermarks` | durable `M_r`/`C_r` |
| `TestSequencedLeaderFailoverDuringMaterialize` | vault-ctl leader transfer |
| `TestSequencedRepeatedFenceMaterializeReconcileCycles` | multi-cycle fence/reconcile |
| `TestSequencedPausedPeerIngestCompletes` | churn: paused peer |
| `TestSequencedSlowPeerBurstIngestAbsorbs` | churn: slow peer + burst |
| `TestSequencedFollowerWipeReconcileHealCatchup` | churn: wipe + reconcile heal |
| `TestBurnedTailMaterializeAfterAsymmetricIngest` | burned-tail materialize |

**Reliability matrix (chunk-append era + shared harness):**

```bash
cd backend
just test-reliability
```

The `test-reliability` recipe includes the V2 sequenced suite above plus existing orchestrator/vaultraft matrices.

**Flake check (optional, pre-cutover):**

```bash
cd backend
go test ./internal/orchestrator/ -run 'TestWritePathGate|TestSequenced|TestBurnedTail' -count=3 -timeout 1800s
```

## Stage 2 — Live k8s + CLI (`gastrolog-1cqdh`)

Primary operator validation path. The Docker Compose Playwright E2E stack is stale and not gating for P10; use k8s plus the `gastrolog` CLI instead.

**Prerequisites:** OrbStack/kind/minikube cluster, `gastrolog:test` image built from the epic branch.

```bash
cd deploy
just kubernetes-redeploy    # roll pods to current gastrolog:test
# UI: http://localhost:30564  (admin / change-me from deploy/k8s.yml)
```

**CLI setup** (from repo root, against NodePort):

```bash
just backend build
export GASTROLOG_ADDR=http://localhost:30564
./build/gastrolog login    # interactive; or use token from login output
```

**Core commands for V2 parity:**

```bash
./gastrolog config vault create --name seq-test --write-model sequenced --replication-factor 3 ...
./gastrolog config vault get <vault-id>
./gastrolog inspect vault <vault-id>          # includes sequenced_diagnostics when write_model=sequenced
./gastrolog inspect cluster sequenced           # per-node replica watermarks
```

**Required scenarios** (capture commands + output in evidence):

1. **Burst ingest** — point kafka-producer or an ingester at a sequenced vault; confirm spool spread via `inspect vault` / `inspect cluster sequenced`.
2. **Node restart/eviction** — `kubectl -n gastrolog delete pod gastrolog-2` during traffic; confirm heal/convergence after pod returns.
3. **Leader failover during active leases** — transfer vault-ctl leadership during ingest; confirm assign continues and watermarks converge.
4. **Retention-route sequencing** — retention destination vault receives sequenced writes with correct `vault_seq`.
5. **CLI/UI parity** — write model in config matches inspector panels and backend diagnostics RPC.

Scale to 4+ nodes when testing multi-node edge cases:

```bash
cd deploy && just kubernetes-scale 4
```

**Evidence template:**

```text
Date:
Cluster: (node count, image tag, kubectl context)
Scenario:
Commands:
Outcome: (pass/fail + observed H/S_r/F_n/M_r/C_r or error)
Recovery steps (if failure):
```

## Stage 3 — E2E (optional, deferred)

The `test/e2e` Playwright stack needs broader refresh (auth flows, compose wiring). Not required for P10 sign-off while k8s+CLI evidence is captured. Re-enable when Playwright/docker are brought back in sync and auth setup is repaired.

```bash
just e2e    # after fixing remaining auth/setup failures
```

## Sign-off

Phase 10 closes when Stages 0–2 pass with captured evidence. Phase 11 cutover (`gastrolog-390uk`) follows parent approval. Phase 12 router delivery queue (`gastrolog-2qrec`) follows P11 — not gating this ladder.
