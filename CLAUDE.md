# CLAUDE.md

The role of this file is to describe common mistakes and confusion points that agents might encounter as they work in this project. If you ever encounter something in the project that surprises you, please alert the developer working with you and indicate that this is the case in the CLAUDE.md file to help prevent future agents from having the same issue.

# DO
# NOT
# FUCKING
# TOUCH
# THE
# CLUSTER!

**Unless the user explicitly tells you to in that message — not a plan todo, not “validation,” not “finish the task.”**

Never run `cluster-kill`, `cluster-run`, `pkill` on gastrolog servers, start/stop/restart nodes, or bounce the live cluster to “validate” or “recover.” Use in-process `go test` only. The user’s cluster (e.g. `/Volumes/Storage/Gastrolog`, `data/node*`) is theirs; killing it wastes soak state and trust. See [`MEMORY.md`](./MEMORY.md).

## Vocabulary

Read [`docs/ubiquitous_language.md`](./docs/ubiquitous_language.md) before writing prose (commit messages, issue titles, comments, code identifiers) that names domain concepts. It defines 8 bounded contexts and ~75 canonical terms, plus a consistency-rules table naming the synonyms to phase out (`primary` → leader, `cloud chunk` → cloud-backed chunk, etc.). Use the canonical terms. Extend the doc in the same PR when introducing a new concept.

## Agent workflow (read first)

These are the highest-impact process rules; follow them before writing code or touching git in ways that affect the whole repo.

### Scope: touch only what belongs to the task

Agents lose this in fresh context; **keep it in this file** so it survives compaction.

- **Do not modify, stage, or “clean up” files that are not part of the change you were asked to make.** No drive-by edits, no opportunistic refactors, no fixing unrelated linter noise in the same commit unless the user asked for that scope.
- **Do not run git (or other commands) that rewrite the working tree outside the task’s paths** — for example `git restore` / `git checkout` on unrelated files to “fix” staging or status. If something looks wrong outside scope, **tell the user** and wait; do not repair it yourself.
- Applies to **docs, config, and generated output** as much as code: unrelated moves, renames, and restores are especially harmful because they look intentional in history.
- **Exception: pre-existing issues that block your progress.** If a hook, lint rule, or compile error on unrelated code in the same file stops you from landing your change, fix it — do not punt. Mention the unrelated fix in the commit body so it is not hidden. The rule against drive-by edits is about avoiding noise in history, not about leaving yourself stuck.

### Building the backend binary

**Never** run ad-hoc `go build -o …` to random paths (`backend/`, repo root, `/tmp`, etc.). Local builds go to **`build/gastrolog`** at the repo root only:

- **`just build`** or **`just backend build`** — canonical
- **`go run ./cmd/gastrolog …`** from `backend/` — when you do not need a binary on disk (cluster dev, CLI against a socket)

To remove artifacts: **`just clean`**. Do not commit binaries; `build/` is gitignored.

### Issue tracking (dcat)

This project uses **dcat** for issue tracking. Run `dcat prime --opinionated` for instructions, then `dcat list --agent-only` for the issue list. Work on bugs first, high priority first.

**Issue context is live, not memorized.** Before diagnosing, planning, or citing “what’s open” / “what epic owns this” / “what’s done,” run `dcat list --agent-only` and `dcat show` on the issues you are actually working. Do **not** reason from closed, tombstoned, or superseded tracker entries, old audit deliverables, or `docs/**/obsoleted/` design docs — they describe history, not current scope. The active epic, stack branch, and spec path live on the **open** issue you are on; if unsure, ask the user or read the issue text. **Do not embed specific issue IDs in CLAUDE.md or other long-lived agent docs** — they go stale and mislead the next session.

**ALWAYS** run `dcat update --status in_progress $issueId` when starting work.

When picking up a child issue, consider whether it can truly be started before the parent is done. If the child genuinely needs the parent first, add a dependency with `dcat dep <child_id> add --depends-on <parent_id>`.

It is okay to work on multiple issues at the same time — just mark all as in_progress, and ask the user which to prioritize if there is a conflict.

If the user brings up a new bug, feature or anything else that warrants code changes, first ask if we should create an issue before starting.

When creating a **question** issue, always draft the title and description first and confirm with the user before running `dcat create`.

### Issue status and branches

`open` → `in_progress` → `in_review` → `closed`

**Always create new branches before picking up issues.** Branch names **must** include the issue ID.

**Stacked branches are allowed** when a follow-up issue naturally builds on an in-review branch (e.g. you discover a related bug while validating the parent fix and don't want to wait for merge). Branch the child off the parent's HEAD, keep each branch single-issue, and either (a) merge the stack as one when both close together, or (b) merge the parent into the stack branch first and rebase the child onto that stack tip. What is NOT allowed is **lumping** — multiple issues' commits intermixed on a single branch with no clean revertable history. Stacking ≠ lumping: each branch still owns exactly one issue's work.

### Closing issues

**NEVER** close issues without explicit user approval:

1. Set status to `in_review`
2. Ask the user to test
3. Ask if we can close it
4. Only run `dcat close` after user confirms
5. **Upon closing:** commit (including tracker), **merge to the issue’s stack branch**, and **push that branch** — in that order after `dcat close`. Do not merge to the default branch or push the merge **before** the issue is closed.

Do not open PRs for routine issue closes on a stack branch — merge the feature branch into its **stack branch** directly. PRs are **only** for landing work on **`main`**.

### Main branch protection

**`main` is the stable line.** Feature and stack work does **not** merge to `main` on issue close. **`main` updates only via PR** on GitHub (review + merge there).

| Branch kind | Role | Examples |
|-------------|------|----------|
| **`main`** | Released/stable baseline. Direct push blocked. | — |
| **Stack / integration branch** | Epic or program integration tip; **merge target on issue close** for work in that stack. | `pipeline-v3`, `feat/gastrolog-4ecqt-fan-out-v2` |
| **Feature branch** | Single-issue work (`<type>/gastrolog-<id>-…`). Branch off the stack branch (or its parent feature branch when stacked). | `feat/gastrolog-5u73c-chunking-materialize-at-seal` |

Before starting work, confirm which **stack branch** the issue belongs to (epic description, issue text, or ask the user). **Never assume `main`.**

**Updating `main`:** open a PR **`<stack-branch>` → `main`** (or the relevant branch → `main`), get review approval, merge on GitHub. No direct push — GitHub requires a PR with ≥1 approval; **`enforce_admins`** applies.

**Agents MUST NOT:**

- `git push origin main`
- `git merge` into `main` locally and push
- `gh pr merge` without explicit user instruction to complete a PR **they** opened for `main`
- Use `main` as the default merge target on `dcat close`

**On issue close:** `git checkout <stack-branch> && git merge <feature-branch> && git push origin <stack-branch>`.

**Enforcement:** GitHub branch protection on `main` — direct push blocked; PR with ≥1 approval required; `enforce_admins` on. Cursor hook blocks `gh pr merge` (merge in the GitHub UI).

## Cluster-First: Every Feature Must Work on Every Node

GastroLog is a fully distributed system. No node has cluster-wide authority — vault leaders and Raft leaders are elected per vault/group, not assigned to a single privileged node. Any node can serve any request. Every feature, handler, and piece of state must work correctly regardless of which node the user is connected to. If a correct implementation requires the user to be connected to a specific node, it is wrong.

When implementing anything new, ask: **"Does this work if the user is on a different node than the data?"** If the answer is no, redesign before proceeding.

### Local cluster nodes: Unix sockets and repo-local data

**Agents: DO NOT run `cluster-kill`, `cluster-run`, or restart nodes unless the user explicitly asked. See top of this file.**

When talking to **local** GastroLog processes (dev cluster, `just cluster-run`, etc.), **prefer the Unix socket**, not HTTP + JWT on `--listen`.

- The **`gastrolog` CLI** uses the socket automatically when **`--addr` is not set** and there is no token: it dials **`gastrolog.sock`** under the node’s **`--home`** directory (`tryUnixSocket` / `home.SocketPath()`). Passing **`--addr http://localhost:4564`** disables that path and forces TCP, which then requires **`--token`** / **`GASTROLOG_TOKEN`** — avoid that for local nodes unless you mean to test HTTP explicitly.
- **Agents:** do not conclude “inspect needs a JWT” from a failed **`--addr http://…`** attempt on a local dev node — that failure is usually self-inflicted. Use **`gastrolog --home <node-home> inspect …`** with **`--addr` omitted** (or **`--addr unix://…/gastrolog.sock`**). If there is no socket yet, the cluster is not running under that home — say so instead of blaming auth.
- To target a specific local node: **`gastrolog --home <path-to-node-home> …`** (still omit `--addr` unless you need TCP), or **`--addr unix://<absolute-path-to>/gastrolog.sock`**.
- **Cluster dev layout:** keep node state **inside the repo** under **`data/node{N}`** (or another directory already **gitignored**, e.g. root `.gitignore` includes **`data`**). That way agents and scripts have a **stable path** to sockets and stores; relying only on **`/tmp/gastrolog`** is brittle in sandboxes and fresh environments. Align **`GLOG_DATA_DIR`** / **`scripts/cluster.sh --data-dir`** with that layout when you bootstrap the cluster (`backend/justfile` **`cluster-kill`** assumes homes like **`data/node*`**).
- **Local admin login (HTTP/JWT):** the root and `backend/` **justfiles** use **`set dotenv-load`**, so a repo-root **`.env`** is picked up automatically. Cluster bootstrap (`scripts/cluster.sh`, `just cluster-*`) uses **`GLOG_ADMIN_USER`** and **`GLOG_ADMIN_PASS`** from the environment when set; otherwise it falls back to **`admin` / `admin123`**. If **`curl Login`** or the UI says invalid credentials, **do not assume** script defaults or **e2e** passwords — check **`.env`** (gitignored) for the real pair, or use **`"$GLOG_ADMIN_USER"`** / **`"$GLOG_ADMIN_PASS"`** in shell after sourcing. Never paste secret values into chat or commits.

### Cursor embedded browser tab

Cursor’s **embedded browser tab** (in the editor, not a separate Chrome/Safari window) can **degrade over a session**: RPCs appear to **hang forever** (no response), as if the backend or proxy died, when the server is actually fine. **Closing the tab and opening a new embedded browser tab** often restores normal behavior immediately.

If that pattern fits ( **`curl`** and a real browser still work), mention it to the user before chasing cluster or Vite theories. This is still a **secondary** hypothesis—most hangs are app, proxy, or auth bugs—so do not treat “close and reopen the embedded browser tab” as the default fix for every networking symptom.

When **`curl`**ing the local Vite dev server, use the hostname (e.g. **`http://localhost:3001`**), not **`http://127.0.0.1:3001`**, so the target matches Vite’s default **`server.host`** (`localhost`, which on many setups binds only the IPv6 loopback `::1`).

## Shared WAL (`raftwal`): prioritize correctness

The shared write-ahead log (**`backend/internal/raftwal`**) backs Raft log and stable state for multiple groups on each node. Bugs there are cluster-wide (wrong consensus, panics, divergent replay), not local UI glitches.

When choosing what to do next or how deep to test: **favor WAL-related work** (correctness, compaction, replay, `LogStore`/`StableStore` semantics, multi-group isolation, failure injection) over unrelated refactors until the user shifts priority. Prefer issues that touch this layer (e.g. **gastrolog-4b1p7** harness and any redesign children that land on `raftwal` or Raft persistence).

## React compiler is enabled — never use React.memo, useMemo, or useCallback.

## Always use Bun — `bun test`, `bunx tsc`, `bun install`. Never npm/npx.

## Test Coverage — MANDATORY

Every feature must have tests across ALL of these dimensions:
- **Single-node**: basic correctness
- **Multi-node**: cluster behavior with 4+ nodes, file-backed vaults, real transferrers
- **Happy path**: feature works as designed
- **Unhappy path**: failures, errors, races, partial operations, recovery, disk full, corrupt data
- **Edge cases**: boundary conditions, concurrent access, restart survival, empty inputs

Single-node happy-path tests are NOT sufficient. A feature is not done until all dimensions are covered. This applies to every new feature, every bug fix, every refactor that changes behavior.

**Two test tiers, not one.** `just test` (backend: `go test -short ./...`) is the fast developer loop — multi-second convergence/stress/large-I/O tests are skipped via `testing.Short()`. `just backend test-full` (`go test ./...`, no `-short`) is the full acceptance gate and remains mandatory once before declaring work done or handing off — it is not optional just because the fast loop was green. New slow tests (multi-node raft convergence, WAL segment stress, restart-survival cycles) get a `testing.Short()` skip with a one-line reason, never deletion or weakened coverage.

## Renaming: Always Rename Through the Entire Stack

When renaming a concept, type, field, or variable, rename it consistently across the entire stack: proto definitions, generated code (re-run `buf generate`), Go backend types, frontend TypeScript types, UI labels, and tests. Never leave a partial rename.

## Proto changes require regenerating both sides

```bash
just gen                    # both sides at once
just backend gen            # Go only
just frontend gen           # TypeScript only
```

## Proto cleanup: remove and renumber, never `reserved`

When removing a proto field, message, oneof case, or enum value, **delete it entirely and renumber the remaining tags**. Do NOT leave `reserved 5;` declarations or `reserved "old_field_name";` annotations behind.

**Why:** GastroLog has zero production deployments. There is no compatibility window with old wire formats. `reserved` exists to prevent tag collisions across deployed versions of a schema; with a single coordinated change against `main`, that collision risk doesn't exist. Reserved declarations accumulate as visual debt that never pays off.

**This is the opposite of the protobuf community's general advice.** Standard advice ("always reserve removed tags") assumes long-lived deployed schemas. GastroLog's atomic-refactor model invalidates that assumption. Apply project-specific rule, not the generic one.

Precedent: gastrolog-4k5mg removed all `reserved` declarations from prior cleanup passes and renumbered the remaining tags. That is the standard pattern; follow it.

## Data Integrity: Facts Before Speculation

Never present derived or approximate data as if it were authoritative. If it comes from the system, show it. If it's reconstructed client-side via heuristics, either don't show it or label it as derived. When in doubt, leave it out.

## Design Context

### Users
Mixed audience: SREs investigating incidents under pressure, developers debugging during development or post-deploy, and ops/platform teams managing log pipelines and retention. The UI must serve all three — fast search for the urgent, clear configuration for the methodical, and readable log output for everyone.

### Design Principles
1. **Instrument, not dashboard.** Each view has a focused purpose. Avoid cramming metrics into every surface. Show what's needed for the task at hand.
2. **Quiet until needed.** Default state is calm. Color, motion, and emphasis appear only when they carry meaning (errors, matches, state changes). Avoid visual noise.
3. **Typography is the interface.** The three-level text hierarchy (bright → normal → muted) does most of the visual heavy lifting. Lean on type weight, size, and opacity before reaching for borders or backgrounds.
4. **Respect the palette.** Every color in the system has a semantic role. Never use raw hex values — always reference design tokens. New UI elements inherit the active palette automatically.
5. **Crafted details.** Custom scrollbars, resize handles, focus rings, grain texture — these small touches compound into the feeling of quality. Don't skip them for expediency.
